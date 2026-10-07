package eventsource

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func batchInput(size, concurrency int) CreateInput {
	input := createInput()
	input.BatchSize = &size
	if concurrency != 0 {
		input.ScalingConfig = &ScalingConfig{MaximumConcurrency: &concurrency}
	}
	return input
}

func leasedBatch(prefix string, size, attempt int) []Record {
	records := make([]Record, size)
	for i := range records {
		records[i] = Record{
			MessageID: fmt.Sprintf("%s-%d", prefix, i), ReceiptHandle: fmt.Sprintf("%s-%d-receipt-%d", prefix, i, attempt),
			Body: fmt.Sprintf("body-%s-%d", prefix, i), EventSource: "aws:sqs", EventSourceARN: sourceARN, AWSRegion: "us-east-1",
			Attributes: map[string]string{"ApproximateReceiveCount": fmt.Sprint(attempt)}, MessageAttributes: map[string]MessageAttribute{},
		}
	}
	return records
}

func nextBatch(t *testing.T, batches <-chan []Record) []Record {
	t.Helper()
	select {
	case records := <-batches:
		return records
	case <-time.After(time.Second):
		t.Fatal("configured batch did not reach the invocation port")
		return nil
	}
}

func TestBatchConfigurationNativeDefaultsAndSnapshotOwnership(t *testing.T) {
	q := newFakeQueue()
	s := newTestService(t, q, fakeInvoker{})
	input := batchInput(5, 2)
	disabled := false
	input.Enabled = &disabled
	created, err := s.Create(t.Context(), input)
	require.NoError(t, err)
	assert.Equal(t, 5, created.BatchSize)
	require.NotNil(t, created.ScalingConfig)
	assert.Equal(t, 2, *created.ScalingConfig.MaximumConcurrency)
	// Input, Create and Get snapshots must not mutate the native selection or
	// the worker limit used by the owner after admission.
	*input.ScalingConfig.MaximumConcurrency = 1000
	*created.ScalingConfig.MaximumConcurrency = 3
	got, err := s.Get(created.UUID)
	require.NoError(t, err)
	assert.Equal(t, 2, *got.ScalingConfig.MaximumConcurrency)
	*got.ScalingConfig.MaximumConcurrency = 4
	got, err = s.Get(created.UUID)
	require.NoError(t, err)
	assert.Equal(t, 2, *got.ScalingConfig.MaximumConcurrency)
	_, err = s.Delete(t.Context(), created.UUID)
	require.NoError(t, err)
	input.BatchSize = nil
	input.ScalingConfig = &ScalingConfig{}
	defaults, err := s.Create(t.Context(), input)
	require.NoError(t, err)
	assert.Equal(t, 10, defaults.BatchSize)
	assert.Nil(t, defaults.ScalingConfig, "empty native scaling selects no ceiling")
}

func TestBatchWholeCompletionAndMaximumConcurrencyBound(t *testing.T) {
	q := newFakeQueue()
	q.deleted = make(chan string, 20)
	for _, name := range []string{"first", "second", "third"} {
		q.batches <- leasedBatch(name, 5, 1)
	}
	started := make(chan []Record, 3)
	permit := make(chan struct{}, 3)
	var active, peak atomic.Int32
	f := fakeInvoker{invoke: func(ctx context.Context, arn string, payload []byte) error {
		var event SQSEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			return err
		}
		current := active.Add(1)
		defer active.Add(-1)
		for prior := peak.Load(); current > prior && !peak.CompareAndSwap(prior, current); prior = peak.Load() {
		}
		started <- event.Records
		select {
		case <-permit:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	s := newTestService(t, q, f)
	_, err := s.Create(t.Context(), batchInput(5, 2))
	require.NoError(t, err)
	first, second := nextBatch(t, started), nextBatch(t, started)
	require.Len(t, first, 5)
	require.Len(t, second, 5)
	assert.Equal(t, int32(2), active.Load(), "two complete batches must actually overlap")
	assert.Empty(t, q.deleted, "starting every handler is not whole-batch completion")
	require.Never(t, func() bool { return len(started) != 0 }, 20*time.Millisecond, time.Millisecond, "third invocation must wait for an available worker")
	permit <- struct{}{}
	third := nextBatch(t, started)
	require.Len(t, third, 5)
	assert.Equal(t, int32(2), active.Load())
	permit <- struct{}{}
	permit <- struct{}{}
	receipts := make(map[string]bool)
	for range 15 {
		select {
		case receipt := <-q.deleted:
			require.False(t, receipts[receipt], "each successful batch receipt is acknowledged once")
			receipts[receipt] = true
		case <-time.After(time.Second):
			t.Fatal("whole-batch completion did not acknowledge every receipt")
		}
	}
	for _, records := range [][]Record{first, second, third} {
		for _, record := range records {
			assert.True(t, receipts[record.ReceiptHandle])
		}
	}
	assert.Equal(t, int32(2), peak.Load(), "configured ceiling is execution behavior")
}

func TestBatchLocalWorkerCapAndUnconfiguredSerialPolicy(t *testing.T) {
	for _, tc := range []struct {
		name         string
		ceiling, cap int
	}{
		{"native ceiling reduced by explicit local cap", 2, 1},
		{"unset native ceiling preserves local serial policy", 0, 32},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := newFakeQueue()
			q.batches <- leasedBatch("first", 5, 1)
			q.batches <- leasedBatch("second", 5, 1)
			started := make(chan []Record, 2)
			permit := make(chan struct{}, 2)
			f := fakeInvoker{invoke: func(ctx context.Context, arn string, payload []byte) error {
				var event SQSEvent
				if err := json.Unmarshal(payload, &event); err != nil {
					return err
				}
				started <- event.Records
				select {
				case <-permit:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}}
			s := newTestService(t, q, f, DevOptions{MaxWorkersPerMapping: tc.cap})
			created, err := s.Create(t.Context(), batchInput(5, tc.ceiling))
			require.NoError(t, err)
			nextBatch(t, started)
			require.Never(t, func() bool { return len(started) != 0 }, 20*time.Millisecond, time.Millisecond, "serial owner must not invoke the waiting batch")
			assert.Empty(t, q.deleted)
			if tc.ceiling != 0 {
				assert.Equal(t, tc.ceiling, *created.ScalingConfig.MaximumConcurrency, "dev cap must preserve the selected native ceiling")
			}
			permit <- struct{}{}
			nextBatch(t, started)
			assert.Len(t, q.deleted, 5, "next serial invocation starts after whole-batch settlement")
			permit <- struct{}{}
		})
	}
}

func TestFailedBatchRetainsEveryReceiptAndRetriesOwnedCurrentBatch(t *testing.T) {
	q := newFakeQueue()
	q.batches <- leasedBatch("retry", 5, 1)
	attempted := make(chan []Record, 2)
	f := fakeInvoker{invoke: func(ctx context.Context, arn string, payload []byte) error {
		var event SQSEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			return err
		}
		attempted <- event.Records
		if event.Records[0].Attributes["ApproximateReceiveCount"] == "1" {
			return errors.New("whole handler failed or timed out")
		}
		return nil
	}}
	s := newTestService(t, q, f)
	created, err := s.Create(t.Context(), batchInput(5, 0))
	require.NoError(t, err)
	failed := nextBatch(t, attempted)
	require.Len(t, failed, 5)
	require.Eventually(t, func() bool {
		got, _ := s.Get(created.UUID)
		return got.LastProcessingResult == "Function invocation failed"
	}, time.Second, time.Millisecond)
	assert.Empty(t, q.deleted)
	// Visibility/retry belongs to Queue: provide its second delivery snapshots,
	// preserving message identities and replacing every current receipt.
	q.batches <- leasedBatch("retry", 5, 2)
	current := nextBatch(t, attempted)
	require.Len(t, current, 5)
	for i, record := range current {
		assert.Equal(t, failed[i].MessageID, record.MessageID)
		assert.NotEqual(t, failed[i].ReceiptHandle, record.ReceiptHandle)
		select {
		case receipt := <-q.deleted:
			assert.Equal(t, record.ReceiptHandle, receipt)
		case <-time.After(time.Second):
			t.Fatal("successful retried whole batch was not acknowledged")
		}
	}
}

func TestDeleteAndCloseJoinEveryConcurrentBatch(t *testing.T) {
	for _, shutdown := range []string{"delete", "close"} {
		t.Run(shutdown, func(t *testing.T) {
			q := newFakeQueue()
			q.batches <- leasedBatch("first", 5, 1)
			q.batches <- leasedBatch("second", 5, 1)
			started, canceled, joined := make(chan string, 2), make(chan string, 2), make(chan string, 2)
			cleanup := map[string]chan struct{}{"first-0": make(chan struct{}), "second-0": make(chan struct{})}
			var releases [2]sync.Once
			f := fakeInvoker{invoke: func(ctx context.Context, arn string, payload []byte) error {
				var event SQSEvent
				if err := json.Unmarshal(payload, &event); err != nil {
					return err
				}
				id := event.Records[0].MessageID
				started <- id
				<-ctx.Done()
				canceled <- id
				<-cleanup[id]
				joined <- id
				return ctx.Err()
			}}
			s := newTestService(t, q, f)
			t.Cleanup(func() {
				releases[0].Do(func() { close(cleanup["first-0"]) })
				releases[1].Do(func() { close(cleanup["second-0"]) })
			})
			created, err := s.Create(t.Context(), batchInput(5, 2))
			require.NoError(t, err)
			<-started
			<-started
			result := make(chan error, 1)
			go func() {
				if shutdown == "delete" {
					_, err := s.Delete(context.Background(), created.UUID)
					result <- err
				} else {
					result <- s.Close(context.Background())
				}
			}()
			<-canceled
			<-canceled
			releases[0].Do(func() { close(cleanup["first-0"]) })
			assert.Equal(t, "first-0", <-joined)
			select {
			case <-result:
				t.Fatal("mapping completed shutdown before every worker joined")
			default:
			}
			assert.Empty(t, q.deleted, "canceled batches retain every receipt")
			releases[1].Do(func() { close(cleanup["second-0"]) })
			require.NoError(t, <-result)
			assert.Equal(t, "second-0", <-joined)
			assert.Empty(t, q.deleted)
		})
	}
}

func TestUnavailableSourceCancelsAndJoinsPeerInvocations(t *testing.T) {
	q := newFakeQueue()
	q.batches <- leasedBatch("active", 5, 1)
	started, canceled, cleanup := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var release sync.Once
	f := fakeInvoker{invoke: func(ctx context.Context, arn string, payload []byte) error {
		close(started)
		<-ctx.Done()
		close(canceled)
		<-cleanup
		return errors.New("canceled peer execution")
	}}
	s := newTestService(t, q, f)
	t.Cleanup(func() { release.Do(func() { close(cleanup) }) })
	created, err := s.Create(t.Context(), batchInput(5, 2))
	require.NoError(t, err)
	<-started
	q.receiveErr <- errors.New("source no longer exists")
	<-canceled
	got, err := s.Get(created.UUID)
	require.NoError(t, err)
	assert.Equal(t, "Disabled", got.State)
	assert.Equal(t, "LAMBDA_INITIATED", got.StateTransitionReason)
	assert.Equal(t, "Source receive failed", got.LastProcessingResult)
	result := make(chan error, 1)
	go func() { _, err := s.Delete(context.Background(), created.UUID); result <- err }()
	select {
	case <-result:
		t.Fatal("unavailable source detached a peer invocation before cleanup")
	default:
	}
	release.Do(func() { close(cleanup) })
	require.NoError(t, <-result)
	assert.Empty(t, q.deleted)
}

func TestInvalidAdapterBatchIsNotInvokedOrAcknowledged(t *testing.T) {
	for _, tc := range []struct {
		name    string
		records []Record
	}{
		{"too many leased records", leasedBatch("invalid", 6, 1)},
		{"payload exceeds synchronous limit", []Record{{Body: strings.Repeat("x", MaxBatchPayloadBytes)}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := newFakeQueue()
			q.batches <- tc.records
			invoked := make(chan struct{}, 1)
			s := newTestService(t, q, fakeInvoker{invoke: func(context.Context, string, []byte) error { invoked <- struct{}{}; return nil }})
			created, err := s.Create(t.Context(), batchInput(5, 2))
			require.NoError(t, err)
			require.Eventually(t, func() bool {
				got, _ := s.Get(created.UUID)
				return got.State == "Disabled"
			}, time.Second, time.Millisecond)
			assert.Empty(t, invoked)
			assert.Empty(t, q.deleted)
		})
	}
}
