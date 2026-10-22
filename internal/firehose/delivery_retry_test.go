package firehose

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type retryClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *retryClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *retryClock) advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	c.mu.Unlock()
}

type retryLogs struct {
	mu     sync.Mutex
	events []deliveryLogEvent
}

func (l *retryLogs) add(event deliveryLogEvent) {
	l.mu.Lock()
	l.events = append(l.events, event)
	l.mu.Unlock()
}
func (l *retryLogs) snapshot() []deliveryLogEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]deliveryLogEvent(nil), l.events...)
}
func retryManager(t *testing.T, handler http.Handler) (*FirehoseManager, *retryClock, *retryLogs) {
	t.Helper()
	sink := httptest.NewServer(handler)
	t.Cleanup(sink.Close)
	clock := &retryClock{now: time.Now()}
	logs := &retryLogs{}
	manager := NewFirehoseManager("us-east-1", "000000000000", sink.URL, "test", "test")
	manager.now = clock.Now
	manager.logDelivery = logs.add
	t.Cleanup(func() { _ = manager.Shutdown() })
	return manager, clock, logs
}
func dormantRetryStream(t *testing.T, manager *FirehoseManager, name string) *DeliveryStream {
	t.Helper()
	stream, err := manager.CreateStream(name, "bucket", "destination/", "", 100, 3600)
	require.NoError(t, err)
	stream.cancel()
	awaitFirehoseSignal(t, stream.done)
	return stream
}
func forceRetryBuffer(ctx context.Context, manager *FirehoseManager, stream *DeliveryStream) error {
	return manager.flush(ctx, stream, flushOptions{forceBuffer: true})
}
func onlyRetryObject(t *testing.T, stream *DeliveryStream) (time.Time, int) {
	t.Helper()
	stream.mu.Lock()
	defer stream.mu.Unlock()
	require.Len(t, stream.pending, 1)
	return stream.pending[0].retryAt, stream.pending[0].retryFailures
}

func TestFirehoseDeliveryRetryBackoffCoalescesFailuresAndRecovers(t *testing.T) {
	for _, initialStatus := range []int{http.StatusForbidden, http.StatusServiceUnavailable} {
		t.Run(strconv.Itoa(initialStatus), func(t *testing.T) {
			type request struct {
				key  string
				data []byte
			}
			var status atomic.Int32
			status.Store(int32(initialStatus))
			var mu sync.Mutex
			var original []request
			committed := map[string]int{}
			manager, clock, logs := retryManager(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					w.WriteHeader(500)
					return
				}
				mu.Lock()
				if string(body) == "original" {
					original = append(original, request{r.URL.Path, body})
				}
				selected := http.StatusOK
				if string(body) == "original" {
					selected = int(status.Load())
				}
				if selected == http.StatusOK {
					committed[string(body)]++
				}
				mu.Unlock()
				w.WriteHeader(selected)
			}))
			t.Cleanup(func() { status.Store(http.StatusOK) })
			stream := dormantRetryStream(t, manager, "persistent")
			_, err := manager.PutRecord(stream, []byte("original"))
			require.NoError(t, err)
			require.Error(t, forceRetryBuffer(t.Context(), manager, stream))
			initialEvents := logs.snapshot()
			require.Len(t, initialEvents, 1)
			require.Equal(t, "first_failure", initialEvents[0].reason)
			retryAt, failures := onlyRetryObject(t, stream)
			require.Equal(t, 1, failures)
			require.Equal(t, time.Second, retryAt.Sub(clock.Now()))
			// Wakeups and new buffered admissions cannot bypass a prepared object's
			// native retry deadline or manufacture a recovery diagnostic.
			for range 200 {
				require.NoError(t, manager.flushReady(t.Context(), stream, false))
			}
			require.Len(t, logs.snapshot(), 1)
			clock.advance(time.Second)
			require.Error(t, manager.flushReady(t.Context(), stream, false))
			require.Len(t, logs.snapshot(), 1, "same failure is suppressed")
			clock.advance(2 * time.Second)
			changed := http.StatusForbidden
			if initialStatus == http.StatusForbidden {
				changed = http.StatusServiceUnavailable
			}
			status.Store(int32(changed))
			require.Error(t, manager.flushReady(t.Context(), stream, false))
			events := logs.snapshot()
			require.Len(t, events, 2)
			require.Equal(t, "error_changed", events[1].reason)
			_, err = manager.PutRecord(stream, []byte("new-record"))
			require.NoError(t, err)
			require.NoError(t, forceRetryBuffer(t.Context(), manager, stream), "healthy new object is independent of old cooldown")
			other := dormantRetryStream(t, manager, "peer")
			_, err = manager.PutRecord(other, []byte("peer-record"))
			require.NoError(t, err)
			require.NoError(t, forceRetryBuffer(t.Context(), manager, other))
			require.Len(t, logs.snapshot(), 2, "record-count changes and healthy peers do not change failure identity or report recovery")
			for failed := 4; failed <= 10; failed++ {
				retryAt, _ = onlyRetryObject(t, stream)
				clock.advance(retryAt.Sub(clock.Now()))
				require.Error(t, manager.flushReady(t.Context(), stream, false))
				next, count := onlyRetryObject(t, stream)
				require.Equal(t, failed, count)
				require.Equal(t, deliveryRetryDelay(failed), next.Sub(clock.Now()))
				before := len(logs.snapshot())
				for range 20 {
					require.NoError(t, manager.flushReady(t.Context(), stream, false))
				}
				require.Len(t, logs.snapshot(), before)
			}
			events = logs.snapshot()
			for index, event := range events {
				if index > 1 {
					require.Equal(t, "retry_summary", event.reason)
					require.GreaterOrEqual(t, event.retryAfter, time.Second)
				}
				require.NotEqual(t, "recovered", event.reason)
			}
			retryAt, _ = onlyRetryObject(t, stream)
			clock.advance(retryAt.Sub(clock.Now()))
			status.Store(http.StatusOK)
			require.NoError(t, manager.flushReady(t.Context(), stream, false))
			snapshot, _ := manager.Snapshot(stream.Name)
			require.Zero(t, snapshot.BufferedRecords)
			require.Zero(t, snapshot.PendingObjects)
			require.Empty(t, snapshot.LastDeliveryError)
			events = logs.snapshot()
			require.Equal(t, "recovered", events[len(events)-1].reason)
			count := len(events)
			require.NoError(t, manager.flushReady(t.Context(), stream, false))
			require.Len(t, logs.snapshot(), count, "recovery is emitted only once")
			mu.Lock()
			defer mu.Unlock()
			require.Len(t, original, 11, "ten bounded failures and one successful retry")
			for _, attempt := range original {
				require.Equal(t, original[0].key, attempt.key)
				require.Equal(t, original[0].data, attempt.data)
			}
			require.Equal(t, map[string]int{"original": 1, "new-record": 1, "peer-record": 1}, committed)
		})
	}
}
func TestFirehoseDeliveryRetryCapAndDuplicateLogInterval(t *testing.T) {
	for failures, want := range []time.Duration{0, time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 32 * time.Second, time.Minute, time.Minute} {
		require.Equal(t, want, deliveryRetryDelay(failures))
	}
	require.Equal(t, time.Minute, deliveryRetryDelay(1000000))
	var calls atomic.Int32
	manager, clock, logs := retryManager(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		calls.Add(1)
		w.WriteHeader(http.StatusForbidden)
	}))
	stream := dormantRetryStream(t, manager, "rate")
	_, err := manager.PutRecord(stream, []byte("retained"))
	require.NoError(t, err)
	require.Error(t, forceRetryBuffer(t.Context(), manager, stream))
	// Explicit manual/final attempts may bypass cooldown, but repeated failures
	// still cannot flood the stream's diagnostic sink.
	for range 20 {
		require.Error(t, manager.flushBuffer(t.Context(), stream))
	}
	require.Equal(t, int32(21), calls.Load())
	require.Len(t, logs.snapshot(), 1)
	clock.advance(time.Minute - time.Nanosecond)
	require.Error(t, manager.flushBuffer(t.Context(), stream))
	require.Len(t, logs.snapshot(), 1)
	clock.advance(time.Nanosecond)
	require.Error(t, manager.flushBuffer(t.Context(), stream))
	events := logs.snapshot()
	require.Len(t, events, 2)
	require.Equal(t, "retry_summary", events[1].reason)
	require.Equal(t, 22, events[1].attemptsSinceLog)
}
func TestFirehoseDynamicRetryDeadlinePreemptsObjectCooldown(t *testing.T) {
	type request struct {
		key  string
		data []byte
	}
	var available atomic.Bool
	var mu sync.Mutex
	var attempts []request
	manager, clock, _ := retryManager(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		attempts = append(attempts, request{r.URL.Path, body})
		mu.Unlock()
		if !available.Load() {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(200)
	}))
	t.Cleanup(func() { available.Store(true) })
	require.NoError(t, manager.SetMetadataExtractor(NewGoJQMetadataExtractor()))
	config := partitionConfig("dynamic-deadline")
	config.DynamicPartitioningConfiguration.RetryOptions = &RetryOptions{DurationInSeconds: 2}
	stream, err := manager.CreateConfiguredStream(t.Context(), config)
	require.NoError(t, err)
	stream.cancel()
	awaitFirehoseSignal(t, stream.done)
	record := []byte(`{"customer_id":"north","id":1}`)
	_, err = manager.PutRecord(stream, record)
	require.NoError(t, err)
	require.Error(t, forceRetryBuffer(t.Context(), manager, stream))
	clock.advance(time.Second)
	require.Error(t, manager.flushReady(t.Context(), stream, false))
	at, _ := onlyRetryObject(t, stream)
	require.Equal(t, 2*time.Second, at.Sub(clock.Now()))
	clock.advance(time.Second)
	require.Error(t, manager.flushReady(t.Context(), stream, false), "native error-prefix deadline takes precedence over remaining cooldown")
	at, count := onlyRetryObject(t, stream)
	require.Equal(t, 1, count)
	require.Equal(t, time.Second, at.Sub(clock.Now()))
	clock.advance(time.Second)
	require.Error(t, manager.flushReady(t.Context(), stream, false))
	clock.advance(2 * time.Second)
	available.Store(true)
	require.NoError(t, manager.flushReady(t.Context(), stream, false))
	snapshot, _ := manager.Snapshot(stream.Name)
	require.Zero(t, snapshot.BufferedRecords)
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, attempts, 5)
	require.Equal(t, attempts[0].key, attempts[1].key)
	require.Equal(t, attempts[0].data, attempts[1].data)
	require.Contains(t, attempts[2].key, "/errors/dynamic-partitioning-failed/")
	for _, attempt := range attempts[3:] {
		require.Equal(t, attempts[2].key, attempt.key)
		require.Equal(t, attempts[2].data, attempt.data)
	}
	var diagnostic map[string]any
	require.NoError(t, json.Unmarshal(attempts[2].data, &diagnostic))
	require.Equal(t, base64.StdEncoding.EncodeToString(record), diagnostic["rawData"])
	require.Equal(t, float64(2), diagnostic["attemptsMade"])
}
func TestFirehoseFinalFlushBypassesCooldownAndReportsRetainedFailure(t *testing.T) {
	for _, operation := range []string{"delete", "shutdown"} {
		for _, recover := range []bool{false, true} {
			t.Run(operation+"/"+strconv.FormatBool(recover), func(t *testing.T) {
				var available atomic.Bool
				var calls atomic.Int32
				manager, _, _ := retryManager(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = io.Copy(io.Discard, r.Body)
					calls.Add(1)
					if !available.Load() {
						w.WriteHeader(403)
						return
					}
					w.WriteHeader(200)
				}))
				t.Cleanup(func() { available.Store(true) })
				stream := dormantRetryStream(t, manager, "final")
				_, err := manager.PutRecord(stream, []byte("retained"))
				require.NoError(t, err)
				require.Error(t, forceRetryBuffer(t.Context(), manager, stream))
				available.Store(recover)
				if operation == "delete" {
					err = manager.DeleteStream(t.Context(), stream.Name)
				} else {
					err = manager.ShutdownContext(t.Context())
				}
				require.Equal(t, int32(2), calls.Load(), "final operation must try once instead of treating cooldown as success")
				if recover {
					require.NoError(t, err)
				} else {
					require.ErrorContains(t, err, "status 403")
					snapshot, _ := manager.Snapshot(stream.Name)
					require.Equal(t, 1, snapshot.BufferedRecords)
					require.Equal(t, 1, snapshot.PendingObjects)
				}
			})
		}
	}
}
func TestFirehoseRetainedForceDrainHonorsPreparedObjectCooldown(t *testing.T) {
	var available atomic.Bool
	var calls atomic.Int32
	manager, clock, logs := retryManager(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		calls.Add(1)
		if !available.Load() {
			w.WriteHeader(403)
			return
		}
		w.WriteHeader(200)
	}))
	t.Cleanup(func() { available.Store(true) })
	activity := &firehoseTestActivity{}
	require.NoError(t, manager.SetDevActivity(activity))
	stream := dormantRetryStream(t, manager, "retained-backoff")
	_, err := manager.PutRecord(stream, []byte("retained"))
	require.NoError(t, err)
	require.NoError(t, manager.DevBeginDrain())
	require.Eventually(t, func() bool { return len(logs.snapshot()) == 1 }, time.Second, time.Millisecond)
	// Gate the existing force loop at its flush slot to prove a real resumable
	// forced-buffer pass occurs during cooldown, rather than only testing helpers.
	stream.flushSlot <- struct{}{}
	manager.devWake()
	require.Eventually(t, func() bool { manager.dev.mu.Lock(); defer manager.dev.mu.Unlock(); return manager.dev.attempts > 0 }, time.Second, time.Millisecond)
	<-stream.flushSlot
	require.Eventually(t, func() bool { manager.dev.mu.Lock(); defer manager.dev.mu.Unlock(); return manager.dev.attempts == 0 }, time.Second, time.Millisecond)
	require.Equal(t, int32(1), calls.Load())
	require.Len(t, logs.snapshot(), 1)
	retained, _, dirty := activity.counts()
	require.Equal(t, 1, retained)
	require.Zero(t, dirty)
	clock.advance(time.Second)
	available.Store(true)
	manager.devWake()
	awaitFirehoseSettled(t, manager, activity)
	require.Equal(t, int32(2), calls.Load())
	events := logs.snapshot()
	require.Equal(t, "recovered", events[len(events)-1].reason)
	require.NoError(t, manager.DevResume())
}
