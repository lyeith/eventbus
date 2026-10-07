package eventsource

import (
	"context"
	"errors"
	"io"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/lyeith/eventbus/internal/devcapture"
)

// DevDeliveryCaptureConfig is an optional, private development evidence sink.
// LogWriter is borrowed test output and cannot be configured by native requests.
// The service owns a LogPath file and closes it after all mapping workers join.
type DevDeliveryCaptureConfig struct {
	LogPath   string
	LogWriter io.Writer
}

type InvocationState string

const (
	InvocationSucceeded  InvocationState = "succeeded"
	InvocationFailed     InvocationState = "failed"
	InvocationTimedOut   InvocationState = "timed_out"
	InvocationCanceled   InvocationState = "canceled"
	InvocationNotStarted InvocationState = "not_started"
)

type InvocationMetadata struct {
	RequestID   string
	FunctionARN string
}

// InvocationOutcome contains supervisory facts, never handler-controlled output.
// OwnershipErr is private uncertainty about cleanup, distinct from handler failure.
type InvocationOutcome struct {
	Metadata     InvocationMetadata
	State        InvocationState
	OwnershipErr error
}

// ObservedFunctionInvoker invokes the same native runner as FunctionInvoker. Its
// synchronous callback receives the actual identity after lifetime admission and
// before child launch; callback failure prevents launch. Return joins invocation,
// callback and cleanup, retaining identity even on timeout or cancellation.
type ObservedFunctionInvoker interface {
	InvokeObservedTarget(context.Context, string, []byte, func(InvocationMetadata) error) (InvocationOutcome, error)
}

type ReceiptOutcome string

const (
	ReceiptMappingSettled   ReceiptOutcome = "mapping_settled"
	ReceiptNativeSettled    ReceiptOutcome = "native_settled"
	ReceiptUnacknowledged   ReceiptOutcome = "unacknowledged"
	ReceiptStaleOrExpired   ReceiptOutcome = "stale_or_expired"
	ReceiptUnknown          ReceiptOutcome = "unknown"
	ReceiptQueueUnavailable ReceiptOutcome = "queue_unavailable"
)

// ReceiptEvidenceQueue projects canonical queue-owner classification. ACK is an
// atomic mutation/classification of the original receipt; inspection never mutates
// visibility, retries or settlement. Both calls join local receipt work before
// returning. Inspection accepts an independent completion context after execution
// joins, so mapping cancellation cannot erase already completed native settlement.
type ReceiptEvidenceQueue interface {
	AcknowledgeReceipt(context.Context, string) (ReceiptOutcome, error)
	InspectReceipt(context.Context, string) (ReceiptOutcome, error)
}

const DeliverySchemaVersion = "eventbus.sqs.delivery.v1"

// DeliveryMessage exposes identity/count and canonical disposition only. Receipt
// handles, message bodies, attributes and arbitrary errors never enter evidence.
type DeliveryMessage struct {
	MessageID            string         `json:"message_id"`
	ReceiveCount         int            `json:"receive_count"`
	Settlement           ReceiptOutcome `json:"settlement,omitempty"`
	AcknowledgeAttempted bool           `json:"acknowledge_attempted,omitempty"`
	EvidenceError        bool           `json:"evidence_error,omitempty"`
}

// DeliveryRecord correlates a private delivery attempt with its actual Lambda
// identity. An admitted record is not completion. A terminal is written after
// invocation/cleanup and receipt processing return; uncertainty never succeeds.
// delivery_id also identifies rejected attempts with no native RequestID.
type DeliveryRecord struct {
	SchemaVersion      string            `json:"schema_version"`
	DeliveryID         string            `json:"delivery_id"`
	MappingUUID        string            `json:"mapping_uuid"`
	EventSourceARN     string            `json:"event_source_arn"`
	FunctionARN        string            `json:"function_arn"`
	InvokedFunctionARN string            `json:"invoked_function_arn,omitempty"`
	RequestID          string            `json:"request_id,omitempty"`
	State              string            `json:"state"`
	InvocationState    InvocationState   `json:"invocation_state,omitempty"`
	Joined             bool              `json:"joined"`
	Time               time.Time         `json:"time"`
	Messages           []DeliveryMessage `json:"messages"`
}

func openDeliveryCapture(config *DevDeliveryCaptureConfig) (*devcapture.Sink, string, error) {
	if config == nil {
		return nil, "", nil
	}
	if config.LogWriter != nil {
		if config.LogPath != "" {
			return nil, "", invalid("Delivery capture must select one output")
		}
		return devcapture.NewWriter(config.LogWriter, "SQS delivery"), "", nil
	}
	capture, err := devcapture.OpenPrivate(config.LogPath, "SQS delivery")
	return capture, config.LogPath, err
}

// EvidenceErr checks availability and retained capture/ownership uncertainty
// without closing anything. Healthy service-owned closure is not uncertainty.
func (s *Service) EvidenceErr() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deliveryCapture == nil || s.deliveryClosed {
		return s.deliveryEvidenceErr
	}
	return errors.Join(s.deliveryEvidenceErr, s.deliveryCapture.Err())
}

func (s *Service) DevEvidence() error { return s.EvidenceErr() }
func (s *Service) DeliveryLogPath() string {
	if s == nil {
		return ""
	}
	return s.deliveryLogPath
}

func (s *Service) retainDeliveryError(err error) error {
	if err != nil {
		s.mu.Lock()
		// One terminal failure is enough to make all future attestations unsafe.
		// Repeated retries must not retain an unbounded error history.
		if s.deliveryEvidenceErr == nil {
			s.deliveryEvidenceErr = err
		}
		s.mu.Unlock()
	}
	return err
}

func (s *Service) appendDelivery(record DeliveryRecord) error {
	record.Time = s.dev.Clock().UTC()
	return s.retainDeliveryError(s.deliveryCapture.Append(record))
}

func knownInvocation(state InvocationState) bool {
	switch state {
	case InvocationSucceeded, InvocationFailed, InvocationTimedOut, InvocationCanceled, InvocationNotStarted:
		return true
	}
	return false
}

func knownReceipt(state ReceiptOutcome) bool {
	switch state {
	case ReceiptMappingSettled, ReceiptNativeSettled, ReceiptUnacknowledged, ReceiptStaleOrExpired, ReceiptUnknown, ReceiptQueueUnavailable:
		return true
	}
	return false
}

func settledReceipt(state ReceiptOutcome) bool {
	return state == ReceiptMappingSettled || state == ReceiptNativeSettled
}

// Pure inspection has its own bounded lifetime after execution joins. It cannot
// mutate a canceled delivery or revive a later lease. Native ACK always uses ctx.
const receiptInspectionTimeout = 2 * time.Second

func (s *Service) deliverObserved(ctx context.Context, item *entry, records []Record, payload []byte) (keepPolling bool, evidenceErr error) {
	evidenceErr = s.EvidenceErr() // A peer may have failed while this receive was in flight.
	record := DeliveryRecord{SchemaVersion: DeliverySchemaVersion, DeliveryID: uuid.NewString(), MappingUUID: item.mapping.UUID, EventSourceARN: item.mapping.EventSourceARN, FunctionARN: item.mapping.FunctionARN, Messages: make([]DeliveryMessage, len(records))}
	for index, message := range records {
		count, err := strconv.Atoi(message.Attributes["ApproximateReceiveCount"])
		if message.MessageID == "" || err != nil || count < 1 {
			evidenceErr = errors.Join(evidenceErr, s.retainDeliveryError(errors.New("SQS delivery source identity/count is unavailable")))
		}
		record.Messages[index] = DeliveryMessage{MessageID: message.MessageID, ReceiveCount: count}
	}
	var admission InvocationMetadata
	admitted := false
	outcome := InvocationOutcome{State: InvocationNotStarted}
	var invokeErr error
	if evidenceErr == nil {
		outcome, invokeErr = s.observedFunctions.InvokeObservedTarget(ctx, item.mapping.FunctionARN, payload, func(metadata InvocationMetadata) error {
			if admitted || metadata.RequestID == "" || metadata.FunctionARN == "" {
				return s.retainDeliveryError(errors.New("SQS delivery invocation admission is invalid"))
			}
			admission, admitted = metadata, true
			record.RequestID, record.InvokedFunctionARN = metadata.RequestID, metadata.FunctionARN
			record.State = "admitted"
			return s.appendDelivery(record)
		})
	}
	observationValid := knownInvocation(outcome.State) && (!admitted || outcome.Metadata == admission) && (admitted || outcome.State == InvocationNotStarted)
	if !observationValid {
		evidenceErr = errors.Join(evidenceErr, s.retainDeliveryError(errors.New("SQS delivery invocation observation is inconsistent")))
		if !knownInvocation(outcome.State) {
			outcome.State = InvocationNotStarted
		}
	}
	evidenceErr = errors.Join(evidenceErr, s.retainDeliveryError(outcome.OwnershipErr))
	// Capture failure is sticky, including an admission failure returned by the
	// actual runner. Never replace the actual ID with a generated native ID.
	evidenceErr = errors.Join(evidenceErr, s.EvidenceErr())
	record.InvocationState = outcome.State
	record.Joined = admitted && observationValid && outcome.OwnershipErr == nil
	acknowledge := record.Joined && outcome.State == InvocationSucceeded && invokeErr == nil && ctx.Err() == nil
	allSettled, receiptErr := s.finishReceiptEvidence(ctx, item, records, record.Messages, acknowledge)
	evidenceErr = errors.Join(evidenceErr, receiptErr)
	record.State = string(outcome.State)
	switch {
	case evidenceErr != nil:
		record.State = "uncertain"
	case ctx.Err() != nil:
		record.State = "canceled"
	case invokeErr != nil && outcome.State == InvocationSucceeded:
		record.State = "failed"
	case outcome.State == InvocationSucceeded && !allSettled:
		record.State = "ack_failed"
	}
	evidenceErr = errors.Join(evidenceErr, s.appendDelivery(record))
	if evidenceErr != nil {
		s.disableSource(item, "Delivery evidence unavailable")
		return false, evidenceErr // Invocation and all receipt work already joined.
	}
	if ctx.Err() != nil {
		return false, evidenceErr
	}
	if invokeErr != nil || outcome.State != InvocationSucceeded {
		s.setResult(item, "Function invocation failed")
	} else if allSettled {
		s.setResult(item, "OK")
	} else {
		s.setResult(item, "Source acknowledgment failed")
	}
	return true, evidenceErr
}

// ACK retains the mapping lifetime. Only after admitted mutations return does a
// separate bounded inspection phase collect failed/canceled receipt facts.
func (s *Service) finishReceiptEvidence(ctx context.Context, item *entry, records []Record, messages []DeliveryMessage, acknowledge bool) (allSettled bool, evidenceErr error) {
	inspect := make([]bool, len(records))
	receiptErrors := make([]error, len(records))
	allSettled = acknowledge
	for index, message := range records {
		if acknowledge && ctx.Err() == nil {
			messages[index].AcknowledgeAttempted = true
			messages[index].Settlement, receiptErrors[index] = item.receiptEvidence.AcknowledgeReceipt(ctx, message.ReceiptHandle)
			if receiptErrors[index] != nil {
				allSettled = false
				inspect[index] = true
			}
		} else {
			inspect[index] = true
		}
	}
	inspectCtx, cancelInspection := context.WithTimeout(context.Background(), receiptInspectionTimeout)
	defer cancelInspection()
	for index, message := range records {
		if inspect[index] {
			messages[index].Settlement, receiptErrors[index] = item.receiptEvidence.InspectReceipt(inspectCtx, message.ReceiptHandle)
		}
		result, err := messages[index].Settlement, receiptErrors[index]
		if !knownReceipt(result) {
			result = ReceiptUnknown
			messages[index].Settlement = result
			err = errors.Join(err, errors.New("SQS delivery receipt observation is invalid"))
		}
		if err != nil && result != ReceiptQueueUnavailable {
			messages[index].EvidenceError = true
			evidenceErr = errors.Join(evidenceErr, s.retainDeliveryError(err))
		}
		if err != nil || !settledReceipt(result) {
			allSettled = false
		}
	}
	return allSettled, evidenceErr
}
