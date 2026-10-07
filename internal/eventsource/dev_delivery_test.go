package eventsource

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type observedInvoker struct {
	fakeInvoker
	observe func(context.Context, func(InvocationMetadata) error) (InvocationOutcome, error)
}

func (f observedInvoker) InvokeObservedTarget(ctx context.Context, _ string, _ []byte, admit func(InvocationMetadata) error) (InvocationOutcome, error) {
	return f.observe(ctx, admit)
}

type evidenceQueue struct {
	*fakeQueue
	ack     func(context.Context, string) (ReceiptOutcome, error)
	inspect func(context.Context, string) (ReceiptOutcome, error)
}

func (q evidenceQueue) AcknowledgeReceipt(ctx context.Context, receipt string) (ReceiptOutcome, error) {
	return q.ack(ctx, receipt)
}
func (q evidenceQueue) InspectReceipt(ctx context.Context, receipt string) (ReceiptOutcome, error) {
	return q.inspect(ctx, receipt)
}

func newDeliveryService(t *testing.T, queue Queue, invoker observedInvoker, config *DevDeliveryCaptureConfig) *Service {
	t.Helper()
	s, err := New(Options{Region: "us-east-1", AccountID: "000000000000", Dev: DevOptions{DeliveryCapture: config}}, fakeSource{queue: queue}, invoker)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

func deliveryRows(t *testing.T, data []byte) []DeliveryRecord {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(data))
	var rows []DeliveryRecord
	for {
		var row DeliveryRecord
		err := decoder.Decode(&row)
		if errors.Is(err, io.EOF) {
			return rows
		}
		require.NoError(t, err)
		rows = append(rows, row)
	}
}

func deliverySignal(t *testing.T, signal <-chan struct{}, why string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal(why)
	}
}

func successfulObserved(metadata InvocationMetadata) observedInvoker {
	return observedInvoker{observe: func(_ context.Context, admit func(InvocationMetadata) error) (InvocationOutcome, error) {
		if err := admit(metadata); err != nil {
			return InvocationOutcome{Metadata: metadata, State: InvocationNotStarted}, err
		}
		return InvocationOutcome{Metadata: metadata, State: InvocationSucceeded}, nil
	}}
}

func TestDeliveryEvidenceUsesActualIdentityAndWaitsForExecutionAndAllReceipts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delivery.jsonl")
	metadata := InvocationMetadata{RequestID: "actual-native-id", FunctionARN: targetARN}
	batch := leasedBatch("owned", 5, 4)
	for index := range batch {
		batch[index].Body = "BODY_PASSWORD_SECRET"
		batch[index].ReceiptHandle = "RAW_RECEIPT_SECRET-" + batch[index].ReceiptHandle
	}
	started, invokeCleanup, ackStarted, ackCleanup := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var invokeOnce, ackOnce sync.Once
	releaseInvoke := func() { invokeOnce.Do(func() { close(invokeCleanup) }) }
	releaseAck := func() { ackOnce.Do(func() { close(ackCleanup) }) }
	invoker := observedInvoker{observe: func(ctx context.Context, admit func(InvocationMetadata) error) (InvocationOutcome, error) {
		if err := admit(metadata); err != nil {
			return InvocationOutcome{Metadata: metadata, State: InvocationNotStarted}, err
		}
		close(started)
		select {
		case <-invokeCleanup:
			return InvocationOutcome{Metadata: metadata, State: InvocationSucceeded}, nil
		case <-ctx.Done():
			return InvocationOutcome{Metadata: metadata, State: InvocationCanceled}, ctx.Err()
		}
	}}
	q := evidenceQueue{fakeQueue: newFakeQueue(), ack: func(ctx context.Context, receipt string) (ReceiptOutcome, error) {
		if receipt == batch[0].ReceiptHandle {
			close(ackStarted)
			select {
			case <-ackCleanup:
			case <-ctx.Done():
				return ReceiptUnknown, ctx.Err()
			}
		}
		if receipt == batch[0].ReceiptHandle || receipt == batch[1].ReceiptHandle {
			return ReceiptNativeSettled, nil // Queue-port projection, not a receipt classifier.
		}
		return ReceiptMappingSettled, nil
	}, inspect: func(context.Context, string) (ReceiptOutcome, error) { return ReceiptUnacknowledged, nil }}
	q.batches <- batch
	s := newDeliveryService(t, q, invoker, &DevDeliveryCaptureConfig{LogPath: path})
	t.Cleanup(func() { releaseInvoke(); releaseAck() })
	mapping, err := s.Create(t.Context(), batchInput(5, 0))
	require.NoError(t, err)
	deliverySignal(t, started, "actual observed invocation did not admit")
	admissionData, err := os.ReadFile(path)
	require.NoError(t, err)
	rows := deliveryRows(t, admissionData)
	require.Len(t, rows, 1, "started execution cannot emit a terminal")
	require.Equal(t, "admitted", rows[0].State)
	require.Equal(t, metadata.RequestID, rows[0].RequestID)
	require.Equal(t, metadata.FunctionARN, rows[0].InvokedFunctionARN)
	require.False(t, rows[0].Joined)
	releaseInvoke()
	deliverySignal(t, ackStarted, "joined execution did not enter receipt processing")
	whileAck, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Len(t, deliveryRows(t, whileAck), 1, "actual receipt processing must join before terminal capture")
	releaseAck()
	require.Eventually(t, func() bool { got, _ := s.Get(mapping.UUID); return got.LastProcessingResult == "OK" }, time.Second, time.Millisecond)
	require.NoError(t, s.Close(t.Context()))
	require.NoError(t, s.EvidenceErr())
	captured, err := os.ReadFile(path)
	require.NoError(t, err)
	rows = deliveryRows(t, captured)
	require.Len(t, rows, 2)
	admission, terminal := rows[0], rows[1]
	require.Equal(t, DeliverySchemaVersion, terminal.SchemaVersion)
	require.Equal(t, admission.DeliveryID, terminal.DeliveryID)
	require.Equal(t, mapping.UUID, terminal.MappingUUID)
	require.Equal(t, sourceARN, terminal.EventSourceARN)
	require.Equal(t, targetARN, terminal.FunctionARN)
	require.Equal(t, metadata.RequestID, terminal.RequestID)
	require.Equal(t, "succeeded", terminal.State)
	require.Equal(t, InvocationSucceeded, terminal.InvocationState)
	require.True(t, terminal.Joined)
	for index, message := range terminal.Messages {
		require.Equal(t, batch[index].MessageID, message.MessageID)
		require.Equal(t, 4, message.ReceiveCount)
		require.True(t, message.AcknowledgeAttempted)
		want := ReceiptMappingSettled
		if index < 2 {
			want = ReceiptNativeSettled
		}
		require.Equal(t, want, message.Settlement)
	}
	require.NotContains(t, string(captured), "BODY_PASSWORD_SECRET")
	require.NotContains(t, string(captured), "RAW_RECEIPT_SECRET")
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0600), info.Mode().Perm())
}

func TestFailedDeliveryInspectsCanonicalReceiptOutcomesWithoutMutation(t *testing.T) {
	for _, state := range []InvocationState{InvocationFailed, InvocationTimedOut} {
		t.Run(string(state), func(t *testing.T) {
			var capture bytes.Buffer
			metadata := InvocationMetadata{RequestID: "actual-failure-id", FunctionARN: targetARN}
			states := []ReceiptOutcome{ReceiptNativeSettled, ReceiptUnacknowledged, ReceiptStaleOrExpired, ReceiptUnknown, ReceiptQueueUnavailable}
			batch := leasedBatch("failure", len(states), 2)
			var inspected atomic.Int32
			q := evidenceQueue{fakeQueue: newFakeQueue(), ack: func(context.Context, string) (ReceiptOutcome, error) {
				t.Error("failed invocation attempted a mutating ACK")
				return ReceiptUnknown, errors.New("unexpected ACK")
			}, inspect: func(ctx context.Context, receipt string) (ReceiptOutcome, error) {
				require.NoError(t, ctx.Err())
				_, bounded := ctx.Deadline()
				require.True(t, bounded)
				index := int(inspected.Add(1)) - 1
				require.Equal(t, batch[index].ReceiptHandle, receipt)
				if states[index] == ReceiptQueueUnavailable {
					return states[index], errors.New("bound queue unavailable")
				}
				return states[index], nil
			}}
			invoker := observedInvoker{observe: func(_ context.Context, admit func(InvocationMetadata) error) (InvocationOutcome, error) {
				if err := admit(metadata); err != nil {
					return InvocationOutcome{Metadata: metadata, State: InvocationNotStarted}, err
				}
				return InvocationOutcome{Metadata: metadata, State: state}, errors.New("HANDLER_ERROR_SECRET")
			}}
			q.batches <- batch
			s := newDeliveryService(t, q, invoker, &DevDeliveryCaptureConfig{LogWriter: &capture})
			mapping, err := s.Create(t.Context(), batchInput(5, 0))
			require.NoError(t, err)
			require.Eventually(t, func() bool {
				got, _ := s.Get(mapping.UUID)
				return got.LastProcessingResult == "Function invocation failed"
			}, time.Second, time.Millisecond)
			require.NoError(t, s.Close(t.Context()))
			require.NoError(t, s.EvidenceErr(), "known queue removal is an ordinary outcome, not capture uncertainty")
			rows := deliveryRows(t, capture.Bytes())
			require.Len(t, rows, 2)
			require.Equal(t, string(state), rows[1].State)
			require.True(t, rows[1].Joined)
			for index, message := range rows[1].Messages {
				require.Equal(t, states[index], message.Settlement)
				require.False(t, message.AcknowledgeAttempted)
			}
			require.NotContains(t, capture.String(), "HANDLER_ERROR_SECRET")
		})
	}
}

func TestCanceledDeliveryJoinsFreshReceiptInspectionBeforeClose(t *testing.T) {
	var capture bytes.Buffer
	metadata := InvocationMetadata{RequestID: "actual-canceled-id", FunctionARN: targetARN}
	started, canceled, invokeCleanup, inspected, receiptCleanup := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var invokeOnce, receiptOnce sync.Once
	releaseInvoke := func() { invokeOnce.Do(func() { close(invokeCleanup) }) }
	releaseReceipt := func() { receiptOnce.Do(func() { close(receiptCleanup) }) }
	invoker := observedInvoker{observe: func(ctx context.Context, admit func(InvocationMetadata) error) (InvocationOutcome, error) {
		if err := admit(metadata); err != nil {
			return InvocationOutcome{Metadata: metadata, State: InvocationNotStarted}, err
		}
		close(started)
		<-ctx.Done()
		close(canceled)
		<-invokeCleanup
		return InvocationOutcome{Metadata: metadata, State: InvocationCanceled}, ctx.Err()
	}}
	q := evidenceQueue{fakeQueue: newFakeQueue(), ack: func(context.Context, string) (ReceiptOutcome, error) {
		t.Error("canceled invocation mutated receipt ownership")
		return ReceiptUnknown, errors.New("unexpected ACK")
	}, inspect: func(ctx context.Context, _ string) (ReceiptOutcome, error) {
		require.NoError(t, ctx.Err(), "inspection cannot reuse canceled execution context")
		_, bounded := ctx.Deadline()
		require.True(t, bounded)
		close(inspected)
		select {
		case <-receiptCleanup:
			return ReceiptNativeSettled, nil
		case <-ctx.Done():
			return ReceiptUnknown, ctx.Err()
		}
	}}
	q.batches <- leasedBatch("canceled", 1, 1)
	s := newDeliveryService(t, q, invoker, &DevDeliveryCaptureConfig{LogWriter: &capture})
	t.Cleanup(func() { releaseInvoke(); releaseReceipt() })
	_, err := s.Create(t.Context(), batchInput(1, 0))
	require.NoError(t, err)
	deliverySignal(t, started, "invocation did not start")
	closed := make(chan error, 1)
	go func() { closed <- s.Close(context.Background()) }()
	deliverySignal(t, canceled, "Close did not cancel invocation")
	require.Len(t, deliveryRows(t, capture.Bytes()), 1)
	releaseInvoke()
	deliverySignal(t, inspected, "joined invocation did not inspect original receipt")
	select {
	case err := <-closed:
		t.Fatalf("Close returned before receipt inspection joined: %v", err)
	default:
	}
	require.Len(t, deliveryRows(t, capture.Bytes()), 1)
	releaseReceipt()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Close did not join the finished evidence lifetime")
	}
	rows := deliveryRows(t, capture.Bytes())
	require.Len(t, rows, 2)
	require.Equal(t, "canceled", rows[1].State)
	require.Equal(t, metadata.RequestID, rows[1].RequestID)
	require.True(t, rows[1].Joined)
	require.Equal(t, ReceiptNativeSettled, rows[1].Messages[0].Settlement)
	require.False(t, rows[1].Messages[0].AcknowledgeAttempted)
}

type failingDeliveryWriter struct {
	bytes.Buffer
	failAt int32
	writes atomic.Int32
	err    error
}

func (w *failingDeliveryWriter) Write(data []byte) (int, error) {
	if w.writes.Add(1) >= w.failAt {
		return 0, w.err
	}
	return w.Buffer.Write(data)
}

type retainedDeliveryQueue struct {
	*retainedMappingQueue
	ack     func(context.Context, string) (ReceiptOutcome, error)
	inspect func(context.Context, string) (ReceiptOutcome, error)
}

func (q retainedDeliveryQueue) AcknowledgeReceipt(ctx context.Context, receipt string) (ReceiptOutcome, error) {
	return q.ack(ctx, receipt)
}
func (q retainedDeliveryQueue) InspectReceipt(ctx context.Context, receipt string) (ReceiptOutcome, error) {
	return q.inspect(ctx, receipt)
}

type deliveryCompletionGate struct {
	*mappingDevGate
	finished chan error
}

func (g deliveryCompletionGate) BeginSource(kind, id string) (func(error), <-chan struct{}, error) {
	complete, changed, err := g.mappingDevGate.BeginSource(kind, id)
	if err != nil {
		return nil, changed, err
	}
	return func(err error) { complete(err); g.finished <- err }, changed, nil
}

func TestDeliveryCaptureFailureStopsFreshLeasesAfterReceiptJoin(t *testing.T) {
	captureFailure := errors.New("capture unavailable")
	writer := &failingDeliveryWriter{failAt: 1, err: captureFailure}
	metadata := InvocationMetadata{RequestID: "refused-actual-id", FunctionARN: targetARN}
	var launched atomic.Int32
	invoker := observedInvoker{observe: func(_ context.Context, admit func(InvocationMetadata) error) (InvocationOutcome, error) {
		if err := admit(metadata); err != nil {
			return InvocationOutcome{Metadata: metadata, State: InvocationNotStarted}, err
		}
		launched.Add(1)
		return InvocationOutcome{Metadata: metadata, State: InvocationSucceeded}, nil
	}}
	inspecting, inspectionCleanup := make(chan struct{}), make(chan struct{})
	var cleanupOnce sync.Once
	releaseInspection := func() { cleanupOnce.Do(func() { close(inspectionCleanup) }) }
	q := retainedDeliveryQueue{retainedMappingQueue: newRetainedMappingQueue(), ack: func(context.Context, string) (ReceiptOutcome, error) {
		t.Error("refused child attempted a mutating ACK")
		return ReceiptUnknown, errors.New("unexpected ACK")
	}, inspect: func(ctx context.Context, _ string) (ReceiptOutcome, error) {
		close(inspecting)
		select {
		case <-inspectionCleanup:
			return ReceiptUnacknowledged, nil
		case <-ctx.Done():
			return ReceiptUnknown, ctx.Err()
		}
	}}
	q.batches <- leasedBatch("refused", 1, 1)
	q.batches <- leasedBatch("fresh-backlog", 1, 1)
	gate := deliveryCompletionGate{mappingDevGate: newMappingDevGate(true), finished: make(chan error, 2)}
	s, err := New(Options{Region: "us-east-1", AccountID: "000000000000", Dev: DevOptions{Source: gate, Activity: gate, DeliveryCapture: &DevDeliveryCaptureConfig{LogWriter: writer}}}, fakeSource{queue: q}, invoker)
	require.NoError(t, err)
	t.Cleanup(func() { releaseInspection(); _ = s.Close(context.Background()) })
	mapping, err := s.Create(t.Context(), batchInput(1, 0))
	require.NoError(t, err)
	deliverySignal(t, inspecting, "refused delivery did not inspect original receipt")
	active, _, _ := gate.counts()
	require.Equal(t, 1, active, "source custody must span actual receipt work")
	select {
	case <-gate.finished:
		t.Fatal("source completion escaped before receipt work joined")
	default:
	}
	require.Zero(t, launched.Load(), "admission capture failure must prevent child launch")
	releaseInspection()
	select {
	case err := <-gate.finished:
		require.ErrorIs(t, err, captureFailure, "uncertainty must reach the retained owner")
	case <-time.After(time.Second):
		t.Fatal("source evidence lifetime did not complete")
	}
	require.Eventually(t, func() bool { got, _ := s.Get(mapping.UUID); return got.State == "Disabled" }, time.Second, time.Millisecond)
	require.ErrorIs(t, s.Close(t.Context()), captureFailure)
	require.ErrorIs(t, s.DevEvidence(), captureFailure)
	got, err := s.Get(mapping.UUID)
	require.NoError(t, err)
	require.Equal(t, "Delivery evidence unavailable", got.LastProcessingResult)
	require.Len(t, q.batches, 1, "fresh backlog cannot be leased repeatedly against a failed capture")
	require.Len(t, q.entered, 1)
	require.Equal(t, int32(1), writer.writes.Load(), "failed sink never attempts additional writes")
	registered, unregistered := q.registrations()
	require.Equal(t, 1, registered)
	require.Equal(t, 1, unregistered)
}

func TestDeliveryFailureRetentionIsBounded(t *testing.T) {
	captureFailure := errors.New("persistent capture failure")
	writer := &failingDeliveryWriter{failAt: 1, err: captureFailure}
	q := evidenceQueue{fakeQueue: newFakeQueue()}
	s := newDeliveryService(t, q, successfulObserved(InvocationMetadata{}), &DevDeliveryCaptureConfig{LogWriter: writer})
	first := s.appendDelivery(DeliveryRecord{SchemaVersion: DeliverySchemaVersion})
	require.ErrorIs(t, first, captureFailure)
	for range 1000 {
		require.Same(t, first, s.appendDelivery(DeliveryRecord{SchemaVersion: DeliverySchemaVersion}))
		s.retainDeliveryError(errors.New("later unrelated uncertainty"))
	}
	s.mu.Lock()
	retained := s.deliveryEvidenceErr
	s.mu.Unlock()
	require.Same(t, first, retained, "retain one terminal failure rather than a per-attempt error history")
	require.Equal(t, int32(1), writer.writes.Load())
	require.ErrorIs(t, s.Close(t.Context()), captureFailure)
}

func TestTerminalCaptureAndOwnershipFailuresCannotAttestSuccess(t *testing.T) {
	for _, privateOwnership := range []bool{false, true} {
		name := "terminal_capture"
		if privateOwnership {
			name = "private_ownership"
		}
		t.Run(name, func(t *testing.T) {
			failure := errors.New("PRIVATE_FAILURE_SECRET")
			writer := &failingDeliveryWriter{failAt: 2, err: failure}
			if privateOwnership {
				writer.failAt = 100
			}
			metadata := InvocationMetadata{RequestID: "joined-actual-id", FunctionARN: targetARN}
			invoker := successfulObserved(metadata)
			if privateOwnership {
				invoker.observe = func(_ context.Context, admit func(InvocationMetadata) error) (InvocationOutcome, error) {
					if err := admit(metadata); err != nil {
						return InvocationOutcome{Metadata: metadata, State: InvocationNotStarted}, err
					}
					return InvocationOutcome{Metadata: metadata, State: InvocationSucceeded, OwnershipErr: failure}, nil
				}
			}
			var acked atomic.Int32
			q := evidenceQueue{fakeQueue: newFakeQueue(), ack: func(context.Context, string) (ReceiptOutcome, error) {
				acked.Add(1)
				return ReceiptMappingSettled, nil
			}, inspect: func(context.Context, string) (ReceiptOutcome, error) { return ReceiptUnacknowledged, nil }}
			q.batches <- leasedBatch("uncertain", 1, 1)
			q.batches <- leasedBatch("untouched", 1, 1)
			s := newDeliveryService(t, q, invoker, &DevDeliveryCaptureConfig{LogWriter: writer})
			mapping, err := s.Create(t.Context(), batchInput(1, 0))
			require.NoError(t, err)
			require.Eventually(t, func() bool { got, _ := s.Get(mapping.UUID); return got.State == "Disabled" }, time.Second, time.Millisecond)
			require.ErrorIs(t, s.Close(t.Context()), failure)
			require.ErrorIs(t, s.EvidenceErr(), failure)
			require.Len(t, q.batches, 1)
			rows := deliveryRows(t, writer.Bytes())
			require.Equal(t, "admitted", rows[0].State)
			if privateOwnership {
				require.Zero(t, acked.Load(), "private cleanup uncertainty cannot authorize ACK")
				require.Len(t, rows, 2)
				require.Equal(t, "uncertain", rows[1].State)
				require.False(t, rows[1].Joined)
				require.Equal(t, InvocationSucceeded, rows[1].InvocationState, "runner success is distinct from safe completion")
			} else {
				require.Equal(t, int32(1), acked.Load(), "terminal capture runs only after joined receipt processing")
				require.Len(t, rows, 1, "failed terminal write cannot be presented as successful completion")
			}
			require.NotContains(t, writer.String(), "PRIVATE_FAILURE_SECRET")
		})
	}
}

func TestDeliveryEvidenceRequiresTypedPortsBeforeWorkersStart(t *testing.T) {
	options := Options{Region: "us-east-1", AccountID: "000000000000", Dev: DevOptions{DeliveryCapture: &DevDeliveryCaptureConfig{LogPath: filepath.Join(t.TempDir(), "unused.jsonl")}}}
	_, err := New(options, fakeSource{}, fakeInvoker{})
	assertCode(t, err, "InvalidParameterValueException", 400)
	_, err = os.Stat(options.Dev.DeliveryCapture.LogPath)
	require.ErrorIs(t, err, os.ErrNotExist, "unavailable observation must be rejected before capture is opened")
	var capture bytes.Buffer
	q := newFakeQueue()
	s := newDeliveryService(t, q, successfulObserved(InvocationMetadata{}), &DevDeliveryCaptureConfig{LogWriter: &capture})
	_, err = s.Create(t.Context(), batchInput(1, 0))
	assertCode(t, err, "ServiceException", 503)
	require.Empty(t, q.entered)
	input := batchInput(1, 0)
	disabled := false
	input.Enabled = &disabled
	mapping, err := s.Create(t.Context(), input)
	require.NoError(t, err)
	require.Equal(t, "Disabled", mapping.State)
	require.Empty(t, capture.Bytes())
	require.NoError(t, s.Close(t.Context()))
}
