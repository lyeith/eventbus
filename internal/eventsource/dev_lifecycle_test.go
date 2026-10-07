package eventsource

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type mappingDevGate struct {
	mu                               sync.Mutex
	open                             bool
	fence, wake                      chan struct{}
	active, sourceStarts, activities int
}

func newMappingDevGate(open bool) *mappingDevGate {
	gate := &mappingDevGate{open: open, fence: make(chan struct{}), wake: make(chan struct{})}
	if !open {
		close(gate.fence)
	}
	return gate
}
func (gate *mappingDevGate) notifyLocked() { close(gate.wake); gate.wake = make(chan struct{}) }
func (gate *mappingDevGate) leaseLocked() func(error) {
	gate.active++
	return func(error) {
		gate.mu.Lock()
		defer gate.mu.Unlock()
		gate.active--
		gate.notifyLocked()
	}
}
func (gate *mappingDevGate) BeginSource(string, string) (func(error), <-chan struct{}, error) {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if !gate.open {
		return nil, gate.wake, errors.New("source fenced")
	}
	gate.sourceStarts++
	return gate.leaseLocked(), gate.fence, nil
}
func (gate *mappingDevGate) BeginActivity(string, string) (func(error), error) {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	gate.activities++
	return gate.leaseLocked(), nil
}
func (gate *mappingDevGate) stop() {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if gate.open {
		gate.open = false
		close(gate.fence)
		gate.notifyLocked()
	}
}
func (gate *mappingDevGate) resume() {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	gate.open = true
	gate.fence = make(chan struct{})
	gate.notifyLocked()
}
func (gate *mappingDevGate) counts() (active, sources, activities int) {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	return gate.active, gate.sourceStarts, gate.activities
}

// This queue fixture models only the consumer port: owned records are supplied
// explicitly by tests. Native visibility, redrive and pre-lease selection are
// covered by messaging's queue integration tests, not reimplemented here.
type retainedMappingQueue struct {
	*fakeQueue
	mu                                sync.Mutex
	pending, registered, unregistered int
	changed                           chan struct{}
	owned                             chan []Record
	ownedEntered                      chan struct{}
	onPending                         func()
}

func newRetainedMappingQueue() *retainedMappingQueue {
	return &retainedMappingQueue{fakeQueue: newFakeQueue(), changed: make(chan struct{}), owned: make(chan []Record, 4), ownedEntered: make(chan struct{}, 8)}
}
func (q *retainedMappingQueue) notifyLocked() { close(q.changed); q.changed = make(chan struct{}) }
func (q *retainedMappingQueue) RegisterRetained() (func(), error) {
	q.mu.Lock()
	q.registered++
	q.mu.Unlock()
	return func() {
		q.mu.Lock()
		defer q.mu.Unlock()
		q.unregistered++
		q.pending = 0
		q.notifyLocked()
	}, nil
}
func (q *retainedMappingQueue) PendingRetained() (bool, <-chan struct{}, error) {
	q.mu.Lock()
	pending, changed, hook := q.pending > 0, q.changed, q.onPending
	q.onPending = nil
	q.mu.Unlock()
	if hook != nil {
		hook()
	}
	return pending, changed, nil
}
func (q *retainedMappingQueue) Receive(ctx context.Context, max int) ([]Record, error) {
	records, err := q.fakeQueue.Receive(ctx, max)
	q.mu.Lock()
	q.pending += len(records)
	q.notifyLocked()
	q.mu.Unlock()
	return records, err
}
func (q *retainedMappingQueue) ReceiveRetained(ctx context.Context, max int) ([]Record, error) {
	select {
	case q.ownedEntered <- struct{}{}:
	default:
	}
	for {
		pending, changed, err := q.PendingRetained()
		if err != nil || !pending {
			return nil, err
		}
		select {
		case records := <-q.owned:
			return records, nil
		case <-changed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}
func (q *retainedMappingQueue) Delete(ctx context.Context, receipt string) (bool, error) {
	deleted, err := q.fakeQueue.Delete(ctx, receipt)
	if deleted {
		q.mu.Lock()
		q.pending--
		q.notifyLocked()
		q.mu.Unlock()
	}
	return deleted, err
}
func (q *retainedMappingQueue) registrations() (registered, unregistered int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.registered, q.unregistered
}
func (q *retainedMappingQueue) enqueueOwned(records []Record) {
	q.mu.Lock()
	q.pending += len(records)
	q.owned <- records
	q.notifyLocked()
	q.mu.Unlock()
}
func devMappingService(t *testing.T, q Queue, gate *mappingDevGate, functions fakeInvoker) *Service {
	return newTestService(t, q, functions, DevOptions{Source: gate, Activity: gate, EmptyPollDelay: time.Millisecond})
}

func TestRetainedMappingFenceKeepsAcceptedBatchesAndResumesSameIdentity(t *testing.T) {
	q := newRetainedMappingQueue()
	q.deleted = make(chan string, 20)
	q.batches <- leasedBatch("first", 5, 1)
	q.batches <- leasedBatch("second", 5, 1)
	gate := newMappingDevGate(true)
	started, permit := make(chan []Record, 3), make(chan struct{}, 3)
	functions := fakeInvoker{invoke: func(ctx context.Context, _ string, payload []byte) error {
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
	s := devMappingService(t, q, gate, functions)
	mapping, err := s.Create(t.Context(), batchInput(5, 2))
	require.NoError(t, err)
	require.Len(t, nextBatch(t, started), 5)
	require.Len(t, nextBatch(t, started), 5)
	gate.stop()
	active, _, _ := gate.counts()
	require.Equal(t, 2, active, "both accepted attempts remain owned after source fencing")
	require.Empty(t, q.deleted, "source fencing is not handler completion")
	permit <- struct{}{}
	require.Eventually(t, func() bool { return len(q.deleted) == 5 }, time.Second, time.Millisecond)
	require.Never(t, func() bool { return len(q.deleted) > 5 }, 10*time.Millisecond, time.Millisecond)
	permit <- struct{}{}
	require.Eventually(t, func() bool {
		active, _, _ := gate.counts()
		return active == 0 && len(q.deleted) == 10
	}, time.Second, time.Millisecond, "every successful batch settles before its attempt release")
	q.batches <- leasedBatch("next-epoch", 5, 1)
	q.mu.Lock()
	q.notifyLocked()
	q.mu.Unlock()
	require.Never(t, func() bool { return len(started) != 0 }, 20*time.Millisecond, time.Millisecond, "fresh backlog is not an accepted continuation")
	got, err := s.Get(mapping.UUID)
	require.NoError(t, err)
	require.Equal(t, mapping.UUID, got.UUID)
	require.Equal(t, "Enabled", got.State)
	require.Equal(t, mapping.LastModified, got.LastModified, "developer pause must not rewrite native state")
	gate.resume()
	require.Equal(t, "next-epoch-0", nextBatch(t, started)[0].MessageID)
	permit <- struct{}{}
	require.Eventually(t, func() bool { return len(q.deleted) == 15 }, time.Second, time.Millisecond)
	gate.stop()
	_, err = s.Delete(t.Context(), mapping.UUID)
	require.NoError(t, err)
	registered, unregistered := q.registrations()
	require.Equal(t, 1, registered)
	require.Equal(t, 1, unregistered)
}

func TestRetainedMappingContinuesOwnedRetryAcrossVisibilityGap(t *testing.T) {
	q := newRetainedMappingQueue()
	q.batches <- leasedBatch("retry", 5, 1)
	gate := newMappingDevGate(true)
	attempts, firstRelease := make(chan []Record, 2), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(firstRelease) }) }
	functions := fakeInvoker{invoke: func(ctx context.Context, _ string, payload []byte) error {
		var event SQSEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			return err
		}
		attempts <- event.Records
		if event.Records[0].Attributes["ApproximateReceiveCount"] == "1" {
			select {
			case <-firstRelease:
				return errors.New("ordinary whole-batch failure")
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}}
	s := devMappingService(t, q, gate, functions)
	t.Cleanup(release)
	_, err := s.Create(t.Context(), batchInput(5, 0))
	require.NoError(t, err)
	failed := nextBatch(t, attempts)
	gate.stop()
	release()
	select {
	case <-q.ownedEntered:
	case <-time.After(time.Second):
		t.Fatal("fenced mapping did not enter its queue-owned retry wait")
	}
	active, sources, activities := gate.counts()
	require.Equal(t, 1, active, "restricted receive is acquired before the retry wait")
	require.Equal(t, 1, sources, "fenced retry is not a fresh source admission")
	require.Equal(t, 1, activities)
	require.Empty(t, q.deleted)
	// The queue owner supplies a later delivery with fresh current receipts.
	q.owned <- leasedBatch("retry", 5, 2)
	current := nextBatch(t, attempts)
	for index, record := range current {
		require.Equal(t, failed[index].MessageID, record.MessageID)
		require.NotEqual(t, failed[index].ReceiptHandle, record.ReceiptHandle)
	}
	require.Eventually(t, func() bool {
		active, _, _ := gate.counts()
		return active == 0 && len(q.deleted) == 5
	}, time.Second, time.Millisecond)
}

func TestRetainedMappingIdleFenceAndParkedDeleteClose(t *testing.T) {
	for _, closeService := range []bool{false, true} {
		t.Run(map[bool]string{false: "Delete", true: "Close"}[closeService], func(t *testing.T) {
			q := newRetainedMappingQueue()
			gate := newMappingDevGate(true)
			s := devMappingService(t, q, gate, fakeInvoker{})
			mapping, err := s.Create(t.Context(), createInput())
			require.NoError(t, err)
			select {
			case <-q.entered:
			case <-time.After(time.Second):
				t.Fatal("mapping did not enter its initial empty long poll")
			}
			gate.stop()
			require.Eventually(t, func() bool { active, _, _ := gate.counts(); return active == 0 }, time.Second, time.Millisecond)
			if closeService {
				err = s.Close(t.Context())
			} else {
				_, err = s.Delete(t.Context(), mapping.UUID)
			}
			require.NoError(t, err, "parked worker must wake on its lifetime cancellation")
			registered, unregistered := q.registrations()
			require.Equal(t, 1, registered)
			require.Equal(t, 1, unregistered)
		})
	}
}

func TestRetainedRegistrationWaitsForEveryActualWorkerCleanup(t *testing.T) {
	q := newRetainedMappingQueue()
	q.batches <- leasedBatch("first", 5, 1)
	q.batches <- leasedBatch("second", 5, 1)
	gate := newMappingDevGate(true)
	started, canceled, cleanup := make(chan struct{}, 2), make(chan struct{}, 2), make(chan struct{}, 2)
	functions := fakeInvoker{invoke: func(ctx context.Context, _ string, _ []byte) error {
		started <- struct{}{}
		<-ctx.Done()
		canceled <- struct{}{}
		<-cleanup
		return ctx.Err()
	}}
	s := devMappingService(t, q, gate, functions)
	t.Cleanup(func() { cleanup <- struct{}{}; cleanup <- struct{}{} })
	mapping, err := s.Create(t.Context(), batchInput(5, 2))
	require.NoError(t, err)
	<-started
	<-started
	deleted := make(chan error, 1)
	go func() { _, err := s.Delete(context.Background(), mapping.UUID); deleted <- err }()
	<-canceled
	<-canceled
	cleanup <- struct{}{}
	require.Eventually(t, func() bool { active, _, _ := gate.counts(); return active == 1 }, time.Second, time.Millisecond)
	_, unregistered := q.registrations()
	require.Zero(t, unregistered, "registration still owns the second cleanup")
	select {
	case err := <-deleted:
		t.Fatalf("Delete returned before every worker joined: %v", err)
	default:
	}
	cleanup <- struct{}{}
	require.NoError(t, <-deleted)
	_, unregistered = q.registrations()
	require.Equal(t, 1, unregistered)
	require.Empty(t, q.deleted, "canceled invocations never acknowledge records")
}

func TestRetainedQueueChangeBeforeParkCannotLoseCleanupContinuation(t *testing.T) {
	q := newRetainedMappingQueue()
	gate := newMappingDevGate(false)
	q.onPending = func() { q.enqueueOwned(leasedBatch("cleanup", 1, 1)) }
	invoked := make(chan []Record, 1)
	functions := fakeInvoker{invoke: func(_ context.Context, _ string, payload []byte) error {
		var event SQSEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			return err
		}
		invoked <- event.Records
		return nil
	}}
	s := devMappingService(t, q, gate, functions)
	_, err := s.Create(t.Context(), createInput())
	require.NoError(t, err)
	require.Equal(t, "cleanup-0", nextBatch(t, invoked)[0].MessageID)
	require.Eventually(t, func() bool { active, _, _ := gate.counts(); return active == 0 }, time.Second, time.Millisecond)
	_, sources, activities := gate.counts()
	require.Zero(t, sources)
	require.Equal(t, 1, activities, "accepted cleanup work uses descendant activity, not root admission")
}

func TestRetainedDisabledMappingSkipsCustodyAndMissingPortFailsClosed(t *testing.T) {
	gate := newMappingDevGate(false)
	q := newRetainedMappingQueue()
	q.batches <- leasedBatch("disabled-backlog", 1, 1)
	s := devMappingService(t, q, gate, fakeInvoker{})
	input := createInput()
	disabled := false
	input.Enabled = &disabled
	mapping, err := s.Create(t.Context(), input)
	require.NoError(t, err)
	require.Equal(t, "Disabled", mapping.State)
	registered, unregistered := q.registrations()
	require.Zero(t, registered)
	require.Zero(t, unregistered)
	require.Empty(t, q.entered)
	missing := devMappingService(t, newFakeQueue(), gate, fakeInvoker{})
	_, err = missing.Create(t.Context(), createInput())
	assertCode(t, err, "ServiceException", 503)
}
