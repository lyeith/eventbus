package messaging

import (
	"errors"
	"io"
	"time"

	"github.com/lyeith/eventbus/internal/devcapture"
)

// SNSCaptureRecord is the agent-readable local intent for a send or sandbox OTP.
// Binary attribute values use JSON's base64 encoding. Local admission and
// destination execution are separate evidence; no external delivery occurs.
type SNSCaptureRecord struct {
	SchemaVersion          string                      `json:"schema_version"`
	CapturedAt             time.Time                   `json:"captured_at"`
	Operation              string                      `json:"operation"`
	RequestID              string                      `json:"request_id,omitempty"`
	MessageID              string                      `json:"message_id,omitempty"`
	TargetARN              string                      `json:"target_arn,omitempty"`
	PhoneNumber            string                      `json:"phone_number,omitempty"`
	Subject                string                      `json:"subject,omitempty"`
	Message                string                      `json:"message,omitempty"`
	MessageStructure       string                      `json:"message_structure,omitempty"`
	MessageAttributes      map[string]MessageAttribute `json:"message_attributes,omitempty"`
	MessageGroupID         string                      `json:"message_group_id,omitempty"`
	MessageDeduplicationID string                      `json:"message_deduplication_id,omitempty"`
	SequenceNumber         string                      `json:"sequence_number,omitempty"`
	Deliveries             []SNSCaptureDelivery        `json:"deliveries,omitempty"`
	Details                map[string]any              `json:"details,omitempty"`
}

type SNSCaptureDelivery struct {
	Protocol            string `json:"protocol"`
	Endpoint            string `json:"endpoint"`
	Status              string `json:"status"`
	MessageID           string `json:"message_id,omitempty"`
	Error               string `json:"error,omitempty"`
	SubscriptionARN     string `json:"subscription_arn,omitempty"`
	InvocationRequestID string `json:"invocation_request_id,omitempty"`
}

// SNSCapture owns the SNS record schema and delegates durable append/close to
// the shared harness sink. SNS acceptance and fanout ordering stay with SNS.
type SNSCapture struct {
	sink *devcapture.Sink
}

func OpenSNSCapture(path string) (*SNSCapture, error) {
	sink, err := devcapture.Open(path, "SNS")
	if err != nil {
		return nil, err
	}
	return &SNSCapture{sink: sink}, nil
}

// NewSNSCapture borrows writer for embedded hosts; Close leaves writer open.
func NewSNSCapture(writer io.Writer) *SNSCapture {
	return &SNSCapture{sink: devcapture.NewWriter(writer, "SNS")}
}

func (capture *SNSCapture) Append(record SNSCaptureRecord) error {
	if capture == nil {
		return errors.New("SNS capture is not configured")
	}
	if capture.sink == nil {
		return errors.New("SNS capture writer is not configured")
	}
	record.SchemaVersion = "eventbus.sns.capture.v1"
	if record.CapturedAt.IsZero() {
		record.CapturedAt = time.Now().UTC()
	}
	return capture.sink.Append(record)
}

// Err exposes retained evidence failures without closing capture, so a
// retained-owner barrier can refuse unsafe cleanup while its sinks stay live.
func (capture *SNSCapture) Err() error {
	if capture == nil {
		return errors.New("SNS capture is not configured")
	}
	if capture.sink == nil {
		return errors.New("SNS capture writer is not configured")
	}
	return capture.sink.Err()
}

func (capture *SNSCapture) Close() error {
	if capture == nil {
		return nil
	}
	return capture.sink.Close()
}

func (b *Broker) SetSNSCapture(capture *SNSCapture) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.capture = capture
}

func (b *Broker) CaptureSNS(record SNSCaptureRecord) error {
	b.mu.RLock()
	capture := b.capture
	b.mu.RUnlock()
	// Embedded broker hosts may opt out; the EventBus CLI always configures it.
	if capture == nil {
		return nil
	}
	return capture.Append(record)
}
