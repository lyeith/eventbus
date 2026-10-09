package eventsource

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const sourceARN = "arn:aws:sqs:us-east-1:000000000000:owned.fifo"
const targetARN = "arn:aws:lambda:us-east-1:000000000000:function:worker:live"

type fakeSource struct {
	queue Queue
	err   error
}

func (s fakeSource) ResolveQueue(ctx context.Context, arn string) (Queue, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.queue, s.err
}

type fakeQueue struct {
	info       QueueInfo
	messages   chan *Record
	batches    chan []Record
	receiveErr chan error
	deleted    chan string
	entered    chan struct{}
}

func newFakeQueue() *fakeQueue {
	return &fakeQueue{info: QueueInfo{ARN: sourceARN, VisibilityTimeout: time.Second}, messages: make(chan *Record, 10), batches: make(chan []Record, 8), receiveErr: make(chan error, 1), deleted: make(chan string, 10), entered: make(chan struct{}, 10)}
}
func (q *fakeQueue) Info() QueueInfo { return q.info }
func (q *fakeQueue) Receive(ctx context.Context, max int) (Batch, error) {
	select {
	case q.entered <- struct{}{}:
	default:
	}
	select {
	case record := <-q.messages:
		if record == nil {
			return Batch{}, nil
		}
		records := []Record{*record}
		for len(records) < max {
			select {
			case record := <-q.messages:
				records = append(records, *record)
			default:
				return fixtureBatch(records)
			}
		}
		return fixtureBatch(records)
	case records := <-q.batches:
		return fixtureBatch(records)
	case err := <-q.receiveErr:
		return Batch{}, err
	case <-ctx.Done():
		return Batch{}, ctx.Err()
	}
}
func fixtureBatch(records []Record) (Batch, error) {
	payload, err := json.Marshal(SQSEvent{Records: records})
	return Batch{Records: records, Payload: string(payload)}, err
}
func (q *fakeQueue) Delete(ctx context.Context, receipt string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	q.deleted <- receipt
	return true, nil
}

type fakeInvoker struct {
	timeout  time.Duration
	validate error
	invoke   func(context.Context, string, []byte) error
}

func (f fakeInvoker) ValidateTarget(ctx context.Context, arn string) (time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if f.timeout == 0 {
		f.timeout = time.Second
	}
	return f.timeout, f.validate
}
func (f fakeInvoker) InvokeTarget(ctx context.Context, arn string, payload []byte) error {
	if f.invoke != nil {
		return f.invoke(ctx, arn, payload)
	}
	return nil
}
func createInput() CreateInput {
	one := 1
	return CreateInput{EventSourceARN: sourceARN, FunctionName: "worker:live", BatchSize: &one}
}
func newTestService(t *testing.T, q Queue, invoker fakeInvoker, dev ...DevOptions) *Service {
	t.Helper()
	options := Options{Region: "us-east-1", AccountID: "000000000000"}
	if len(dev) > 0 {
		options.Dev = dev[0]
	}
	s, err := New(options, fakeSource{queue: q}, invoker)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t, s.Close(ctx))
	})
	return s
}
func assertCode(t *testing.T, err error, code string, status int) {
	t.Helper()
	var failure *APIError
	require.ErrorAs(t, err, &failure)
	assert.Equal(t, code, failure.Code)
	assert.Equal(t, status, failure.Status)
}

func TestCreateGetDisabledDeleteAndResourceIdentities(t *testing.T) {
	q := newFakeQueue()
	fixed := time.Unix(12345678, 500000000)
	s := newTestService(t, q, fakeInvoker{}, DevOptions{Clock: func() time.Time { return fixed }, MaxMappings: 1})
	input := createInput()
	disabled := false
	input.Enabled = &disabled
	input.FunctionName = "000000000000:function:worker:live"
	mapping, err := s.Create(t.Context(), input)
	require.NoError(t, err)
	assert.Equal(t, "Disabled", mapping.State)
	assert.Equal(t, targetARN, mapping.FunctionARN)
	assert.Equal(t, sourceARN, mapping.EventSourceARN)
	assert.Equal(t, "arn:aws:lambda:us-east-1:000000000000:event-source-mapping:"+mapping.UUID, mapping.EventSourceMappingARN)
	assert.Equal(t, float64(12345678.5), mapping.LastModified)
	assert.Equal(t, 1, mapping.BatchSize)
	assert.Empty(t, q.entered)
	assert.Empty(t, mapping.FunctionResponseTypes)
	got, err := s.Get(mapping.UUID)
	require.NoError(t, err)
	assert.Equal(t, mapping, got)
	_, err = s.Create(t.Context(), input)
	assertCode(t, err, "ResourceConflictException", 409)
	input.FunctionName = "another"
	_, err = s.Create(t.Context(), input)
	assertCode(t, err, "TooManyRequestsException", 429)
	deleted, err := s.Delete(t.Context(), mapping.UUID)
	require.NoError(t, err)
	assert.Equal(t, "Deleting", deleted.State)
	_, err = s.Get(mapping.UUID)
	assertCode(t, err, "ResourceNotFoundException", 404)
	_, err = s.Delete(t.Context(), mapping.UUID)
	assertCode(t, err, "ResourceNotFoundException", 404)
	_, err = s.Create(t.Context(), input)
	require.NoError(t, err, "deleted mappings release local capacity")
}

func TestCreateRejectsUnsupportedOrNonlocalSelection(t *testing.T) {
	q := newFakeQueue()
	s := newTestService(t, q, fakeInvoker{})
	for _, tc := range []struct {
		name   string
		change func(*CreateInput)
	}{
		{"batch eleven", func(i *CreateInput) { n := 11; i.BatchSize = &n }},
		{"negative batch", func(i *CreateInput) { n := -1; i.BatchSize = &n }},
		{"concurrency one", func(i *CreateInput) { n := 1; i.ScalingConfig = &ScalingConfig{MaximumConcurrency: &n} }},
		{"concurrency too high", func(i *CreateInput) { n := 1001; i.ScalingConfig = &ScalingConfig{MaximumConcurrency: &n} }},
		{"concurrency zero", func(i *CreateInput) { n := 0; i.ScalingConfig = &ScalingConfig{MaximumConcurrency: &n} }},
		{"batch zero", func(i *CreateInput) { n := 0; i.BatchSize = &n }},
		{"window", func(i *CreateInput) { n := 1; i.MaximumBatchingWindowInSeconds = &n }},
		{"partial", func(i *CreateInput) { i.FunctionResponseTypes = []string{"ReportBatchItemFailures"} }},
		{"source region", func(i *CreateInput) { i.EventSourceARN = "arn:aws:sqs:eu-west-1:000000000000:owned" }},
		{"source account", func(i *CreateInput) { i.EventSourceARN = "arn:aws:sqs:us-east-1:123456789012:owned" }},
		{"source service", func(i *CreateInput) { i.EventSourceARN = "arn:aws:sns:us-east-1:000000000000:owned" }},
		{"source name", func(i *CreateInput) { i.EventSourceARN = "arn:aws:sqs:us-east-1:000000000000:owned/slash" }},
		{"target region", func(i *CreateInput) { i.FunctionName = "arn:aws:lambda:eu-west-1:000000000000:function:worker" }},
		{"target account", func(i *CreateInput) { i.FunctionName = "arn:aws:lambda:us-east-1:123456789012:function:worker" }},
		{"partial account", func(i *CreateInput) { i.FunctionName = "123456789012:function:worker" }},
		{"target bad", func(i *CreateInput) { i.FunctionName = "worker:alias:extra" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := createInput()
			tc.change(&input)
			_, err := s.Create(t.Context(), input)
			assertCode(t, err, "InvalidParameterValueException", 400)
		})
	}
	assert.Empty(t, q.entered)
	_, err := s.Get("not-a-uuid")
	assertCode(t, err, "InvalidParameterValueException", 400)
	_, err = s.Get(uuid.NewString())
	assertCode(t, err, "ResourceNotFoundException", 404)
	_, err = s.Delete(t.Context(), "invalid")
	assertCode(t, err, "InvalidParameterValueException", 400)
}

func TestCreateChecksOwnedQueueRegisteredTargetAndVisibility(t *testing.T) {
	for _, tc := range []struct {
		name string
		q    Queue
		f    fakeInvoker
		code string
	}{
		{"absent source", nil, fakeInvoker{}, "ResourceNotFoundException"},
		{"wrong source identity", &fakeQueue{info: QueueInfo{ARN: "other"}}, fakeInvoker{}, "ResourceNotFoundException"},
		{"absent target", newFakeQueue(), fakeInvoker{validate: errors.New("absent alias")}, "ResourceNotFoundException"},
		{"visibility shorter than function", newFakeQueue(), fakeInvoker{timeout: 2 * time.Second}, "InvalidParameterValueException"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestService(t, tc.q, tc.f)
			_, err := s.Create(t.Context(), createInput())
			status := 400
			if tc.code == "ResourceNotFoundException" {
				status = 404
			}
			assertCode(t, err, tc.code, status)
		})
	}
}

func TestAcknowledgesOnlyAfterCompletionAndPassesNativeRecord(t *testing.T) {
	q := newFakeQueue()
	started := make(chan []byte, 1)
	release := make(chan struct{})
	f := fakeInvoker{invoke: func(ctx context.Context, arn string, payload []byte) error {
		assert.Equal(t, targetARN, arn)
		started <- payload
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	s := newTestService(t, q, f)
	_, err := s.Create(t.Context(), createInput())
	require.NoError(t, err)
	value := "value"
	record := &Record{MessageID: "message", ReceiptHandle: "current-receipt", Body: "exact body", Attributes: map[string]string{"ApproximateReceiveCount": "3", "MessageGroupId": "group", "SequenceNumber": "123"}, MessageAttributes: map[string]MessageAttribute{"text": {DataType: "String.custom", StringValue: &value, StringListValues: []string{}, BinaryListValues: [][]byte{}}, "binary": {DataType: "Binary", BinaryValue: []byte{0, 255}, StringListValues: []string{}, BinaryListValues: [][]byte{}}}, MD5OfBody: "digest", EventSource: "aws:sqs", EventSourceARN: sourceARN, AWSRegion: "us-east-1"}
	q.messages <- record
	var payload []byte
	select {
	case payload = <-started:
	case <-time.After(time.Second):
		t.Fatal("handler not invoked")
	}
	assert.Empty(t, q.deleted, "admission/execution start is not acknowledgment")
	var event SQSEvent
	require.NoError(t, json.Unmarshal(payload, &event))
	require.Len(t, event.Records, 1)
	assert.Equal(t, *record, event.Records[0])
	assert.Contains(t, string(payload), `"binaryValue":"AP8="`)
	close(release)
	select {
	case receipt := <-q.deleted:
		assert.Equal(t, "current-receipt", receipt)
	case <-time.After(time.Second):
		t.Fatal("completed handler not acknowledged")
	}
}

func TestFailedExecutionKeepsReceiptAndRetryAcknowledgesCurrentDelivery(t *testing.T) {
	q := newFakeQueue()
	attempts := make(chan string, 2)
	f := fakeInvoker{invoke: func(ctx context.Context, arn string, payload []byte) error {
		var event SQSEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			return err
		}
		receipt := event.Records[0].ReceiptHandle
		attempts <- receipt
		if receipt == "failed-receipt" {
			return errors.New("handler failed or timed out")
		}
		return nil
	}}
	s := newTestService(t, q, f)
	mapping, err := s.Create(t.Context(), createInput())
	require.NoError(t, err)
	q.messages <- &Record{MessageID: "same-message", ReceiptHandle: "failed-receipt"}
	assert.Equal(t, "failed-receipt", <-attempts)
	require.Eventually(t, func() bool {
		got, _ := s.Get(mapping.UUID)
		return got.LastProcessingResult == "Function invocation failed"
	}, time.Second, time.Millisecond)
	assert.Empty(t, q.deleted)
	// Only the queue owner produces the retried lease; mappings do not reset
	// visibility, manufacture a receipt or implement their own redrive policy.
	q.messages <- &Record{MessageID: "same-message", ReceiptHandle: "retry-receipt"}
	assert.Equal(t, "retry-receipt", <-attempts)
	select {
	case receipt := <-q.deleted:
		assert.Equal(t, "retry-receipt", receipt)
	case <-time.After(time.Second):
		t.Fatal("retry not acknowledged")
	}
}

func TestDeleteDeadlineCancelsAndJoinsPendingInvocationWithoutAcknowledgment(t *testing.T) {
	q := newFakeQueue()
	started, canceled, cleanup, joined := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	f := fakeInvoker{invoke: func(ctx context.Context, arn string, payload []byte) error {
		close(started)
		<-ctx.Done()
		close(canceled)
		<-cleanup
		close(joined)
		return ctx.Err()
	}}
	s := newTestService(t, q, f)
	t.Cleanup(func() { releaseOnce.Do(func() { close(cleanup) }) })
	mapping, err := s.Create(t.Context(), createInput())
	require.NoError(t, err)
	q.messages <- &Record{ReceiptHandle: "unsettled"}
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := s.Delete(ctx, mapping.UUID); result <- err }()
	<-canceled
	<-ctx.Done()
	assert.Empty(t, q.deleted)
	select {
	case <-result:
		t.Fatal("Delete returned before owned invocation cleanup")
	default:
	}
	got, err := s.Get(mapping.UUID)
	require.NoError(t, err)
	assert.Equal(t, "Deleting", got.State)
	releaseOnce.Do(func() { close(cleanup) })
	require.ErrorIs(t, <-result, context.DeadlineExceeded)
	select {
	case <-joined:
	default:
		t.Fatal("invocation cleanup was not joined")
	}
	_, err = s.Get(mapping.UUID)
	assertCode(t, err, "ResourceNotFoundException", 404)
	assert.Empty(t, q.deleted)
}

func TestCloseDeadlineStillJoinsAndRetainsFailure(t *testing.T) {
	q := newFakeQueue()
	started, canceled, cleanup := make(chan struct{}), make(chan struct{}), make(chan struct{})
	f := fakeInvoker{invoke: func(ctx context.Context, arn string, payload []byte) error {
		close(started)
		<-ctx.Done()
		close(canceled)
		<-cleanup
		return ctx.Err()
	}}
	s, err := New(Options{Region: "us-east-1", AccountID: "000000000000"}, fakeSource{queue: q}, f)
	require.NoError(t, err)
	_, err = s.Create(t.Context(), createInput())
	require.NoError(t, err)
	q.messages <- &Record{ReceiptHandle: "unsettled"}
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- s.Close(ctx) }()
	<-canceled
	<-ctx.Done()
	select {
	case <-result:
		t.Fatal("Close returned before canceled handler cleanup joined")
	default:
	}
	close(cleanup)
	require.ErrorIs(t, <-result, context.DeadlineExceeded)
	require.ErrorIs(t, s.Close(context.Background()), context.DeadlineExceeded)
	assert.Empty(t, q.deleted)
	_, err = s.Create(t.Context(), createInput())
	assertCode(t, err, "ServiceException", 503)
}

func TestUnavailableBoundSourceStopsPollerAndNeverConsumesReplacement(t *testing.T) {
	q := newFakeQueue()
	invoked := make(chan struct{}, 1)
	f := fakeInvoker{invoke: func(context.Context, string, []byte) error { invoked <- struct{}{}; return nil }}
	s := newTestService(t, q, f)
	mapping, err := s.Create(t.Context(), createInput())
	require.NoError(t, err)
	<-q.entered
	q.receiveErr <- errors.New("bound source deleted")
	require.Eventually(t, func() bool {
		got, _ := s.Get(mapping.UUID)
		return got.State == "Disabled" && got.StateTransitionReason == "LAMBDA_INITIATED" && got.LastProcessingResult == "Source receive failed"
	}, time.Second, time.Millisecond)
	s.mu.Lock()
	done := s.entries[mapping.UUID].done
	s.mu.Unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("unavailable source did not stop its bound poller")
	}
	q.messages <- &Record{ReceiptHandle: "replacement-source"}
	assert.Empty(t, q.entered, "mapping must not poll a deleted/replaced source")
	assert.Empty(t, invoked)
	assert.Empty(t, q.deleted)
}

func TestConcurrentClosePublishesFirstOwnerResultAfterCleanup(t *testing.T) {
	for _, deadlineFirst := range []bool{true, false} {
		name := "healthy owner"
		if deadlineFirst {
			name = "expired owner"
		}
		t.Run(name, func(t *testing.T) {
			q := newFakeQueue()
			started, canceled, cleanup := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var release sync.Once
			f := fakeInvoker{invoke: func(ctx context.Context, arn string, payload []byte) error {
				close(started)
				<-ctx.Done()
				close(canceled)
				<-cleanup
				return ctx.Err()
			}}
			s, err := New(Options{Region: "us-east-1", AccountID: "000000000000"}, fakeSource{queue: q}, f)
			require.NoError(t, err)
			t.Cleanup(func() {
				release.Do(func() { close(cleanup) })
				_ = s.Close(context.Background())
			})
			_, err = s.Create(t.Context(), createInput())
			require.NoError(t, err)
			q.messages <- &Record{ReceiptHandle: "pending-concurrent-close"}
			<-started
			expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			defer cancel()
			first, later := context.Background(), expired
			if deadlineFirst {
				first, later = expired, context.Background()
			}
			const peers = 32
			results := make(chan error, peers+1)
			go func() { results <- s.Close(first) }()
			// Execution cancellation proves the first call established shutdown
			// ownership before the other callers race to observe completion.
			<-canceled
			entered := make(chan struct{}, peers)
			for range peers {
				go func() {
					entered <- struct{}{}
					results <- s.Close(later)
				}()
			}
			for range peers {
				<-entered
			}
			select {
			case <-results:
				t.Fatal("Close published before owned execution cleanup joined")
			default:
			}
			release.Do(func() { close(cleanup) })
			for range peers + 1 {
				select {
				case err := <-results:
					if deadlineFirst {
						require.ErrorIs(t, err, context.DeadlineExceeded)
					} else {
						require.NoError(t, err, "a later canceled caller cannot replace the governing healthy owner")
					}
				case <-time.After(time.Second):
					t.Fatal("concurrent Close did not receive the published terminal result")
				}
			}
			if deadlineFirst {
				require.ErrorIs(t, s.Close(context.Background()), context.DeadlineExceeded)
			} else {
				require.NoError(t, s.Close(expired), "later calls retain the already-published result")
			}
			assert.Empty(t, q.deleted, "canceled execution must remain unacknowledged")
		})
	}
}
