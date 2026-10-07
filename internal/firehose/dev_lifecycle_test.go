package firehose

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/devquiescence"
	"github.com/stretchr/testify/require"
)

type firehoseTestActivity struct {
	mu                        sync.Mutex
	count, completions, dirty int
	refuseAfter               int
}

func (activity *firehoseTestActivity) BeginActivity(_, _ string) (func(error), error) {
	activity.mu.Lock()
	defer activity.mu.Unlock()
	if activity.refuseAfter > 0 && activity.count >= activity.refuseAfter {
		return nil, errors.New("test activity admission refused")
	}
	activity.count++
	return func(err error) {
		activity.mu.Lock()
		defer activity.mu.Unlock()
		activity.count--
		activity.completions++
		if err != nil {
			activity.dirty++
		}
	}, nil
}

func (activity *firehoseTestActivity) counts() (int, int, int) {
	activity.mu.Lock()
	defer activity.mu.Unlock()
	return activity.count, activity.completions, activity.dirty
}

type firehoseTestTransport func(*http.Request) (*http.Response, error)

func (transport firehoseTestTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

func firehoseTestResponse(body io.ReadCloser) *http.Response {
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: body}
}

func awaitFirehoseSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatal("Firehose owner did not reach the expected lifetime boundary")
	}
}

func awaitFirehoseSettled(t *testing.T, manager *FirehoseManager, activity *firehoseTestActivity) {
	t.Helper()
	require.Eventually(t, func() bool {
		count, _, _ := activity.counts()
		return count == 0 && manager.DevEvidence() == nil
	}, 3*time.Second, time.Millisecond)
}

func newDevFirehose(t *testing.T, transport http.RoundTripper) (*FirehoseManager, *DeliveryStream, *firehoseTestActivity) {
	t.Helper()
	manager := NewFirehoseManager("us-east-1", "000000000000", "http://127.0.0.1:1", "test", "test")
	if transport != nil {
		manager.httpClient.Transport = transport
	}
	activity := &firehoseTestActivity{}
	require.NoError(t, manager.SetDevActivity(activity))
	stream, err := manager.CreateStream("retained", "bucket", "", "", 100, 3600)
	require.NoError(t, err)
	return manager, stream, activity
}

func TestDevDrainForcesBelowThresholdAndCleanupPutsThenResumesSameStream(t *testing.T) {
	var mu sync.Mutex
	var objects [][]byte
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		mu.Lock()
		objects = append(objects, body)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(sink.Close)
	manager := NewFirehoseManager("us-east-1", "000000000000", sink.URL, "test", "test")
	activity := &firehoseTestActivity{}
	require.NoError(t, manager.SetDevActivity(activity))
	stream, err := manager.CreateStream("retained", "bucket", "", "", 100, 3600)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Shutdown()) })
	require.Error(t, manager.SetDevActivity(activity), "observation is construction-time configuration")

	_, err = manager.PutRecordBatch(stream, [][]byte{[]byte("first"), []byte("second")})
	require.NoError(t, err)
	count, _, _ := activity.counts()
	require.Equal(t, 2, count)
	require.NoError(t, manager.DevEvidence(), "pending accepted work is counted, not dirty evidence")
	require.NoError(t, manager.DevBeginDrain())
	awaitFirehoseSettled(t, manager, activity)
	mu.Lock()
	delivered := append([][]byte(nil), objects...)
	mu.Unlock()
	require.Equal(t, [][]byte{[]byte("firstsecond")}, delivered)

	// A held-cleanup callback may put after the first drain has become settled.
	// Force mode remains enabled; it needs no second begin-drain call.
	_, err = manager.PutRecord(stream, []byte("cleanup"))
	require.NoError(t, err)
	awaitFirehoseSettled(t, manager, activity)
	require.NoError(t, manager.DevResume())
	require.Same(t, stream, manager.GetStream(stream.Name))
	_, err = manager.PutRecord(stream, []byte("resumed"))
	require.NoError(t, err)
	require.Never(t, func() bool {
		snapshot, _ := manager.Snapshot(stream.Name)
		return snapshot.BufferedRecords != 1
	}, 100*time.Millisecond, time.Millisecond)
	mu.Lock()
	delivered = append([][]byte(nil), objects...)
	mu.Unlock()
	require.Equal(t, [][]byte{[]byte("firstsecond"), []byte("cleanup")}, delivered)
	count, completions, dirty := activity.counts()
	require.Equal(t, 1, count)
	require.Equal(t, 3, completions)
	require.Zero(t, dirty)
}

func TestDevResumeDoesNotLetQueuedForceFlushNewNormalRecords(t *testing.T) {
	manager, stream, activity := newDevFirehose(t, firehoseTestTransport(func(*http.Request) (*http.Response, error) {
		return firehoseTestResponse(io.NopCloser(bytes.NewReader(nil))), nil
	}))
	t.Cleanup(func() { require.NoError(t, manager.Shutdown()) })
	stream.cancel()
	<-stream.done
	require.NoError(t, manager.DevBeginDrain())
	stream.flushSlot <- struct{}{}
	finished := make(chan error, 1)
	go func() { finished <- manager.flushReady(manager.dev.forceContext, stream, true) }()
	require.Eventually(t, func() bool {
		manager.dev.mu.Lock()
		defer manager.dev.mu.Unlock()
		return manager.dev.attempts != 0
	}, time.Second, time.Millisecond)
	require.NoError(t, manager.DevResume())
	_, err := manager.PutRecord(stream, []byte("new normal record"))
	require.NoError(t, err)
	<-stream.flushSlot
	select {
	case err := <-finished:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("queued force flush did not join after resume")
	}
	snapshot, _ := manager.Snapshot(stream.Name)
	require.Equal(t, 1, snapshot.BufferedRecords)
	count, _, _ := activity.counts()
	require.Equal(t, 1, count)
}

func TestDevDrainRetainsLeaseAcrossLostResponseAndStableRetry(t *testing.T) {
	var mu sync.Mutex
	var keys []string
	var bodies [][]byte
	first := make(chan struct{})
	second := make(chan struct{})
	release := make(chan struct{})
	var attempts atomic.Int32
	manager, stream, activity := newDevFirehose(t, firehoseTestTransport(func(request *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
		mu.Lock()
		keys = append(keys, request.URL.Path)
		bodies = append(bodies, body)
		mu.Unlock()
		if attempts.Add(1) == 1 {
			close(first)
			return nil, io.ErrUnexpectedEOF // S3 may have accepted the first object.
		}
		select {
		case <-second:
		default:
			close(second)
		}
		select {
		case <-release:
			return firehoseTestResponse(io.NopCloser(bytes.NewReader(nil))), nil
		case <-request.Context().Done():
			return nil, request.Context().Err()
		}
	}))
	t.Cleanup(func() { close(release); require.NoError(t, manager.Shutdown()) })
	_, err := manager.PutRecord(stream, []byte("original accepted record"))
	require.NoError(t, err)
	require.NoError(t, manager.DevBeginDrain())
	awaitFirehoseSignal(t, first)
	require.Eventually(t, func() bool {
		snapshot, _ := manager.Snapshot(stream.Name)
		return snapshot.PendingObjects == 1 && snapshot.LastDeliveryError != ""
	}, time.Second, time.Millisecond)
	count, completions, dirty := activity.counts()
	require.Equal(t, 1, count)
	require.Zero(t, completions)
	require.Zero(t, dirty, "ordinary destination errors are retryable ownership")
	manager.dev.mu.Lock()
	evidence := manager.dev.evidence
	manager.dev.mu.Unlock()
	require.Nil(t, evidence)
	awaitFirehoseSignal(t, second)
	mu.Lock()
	retryKeys := append([]string(nil), keys...)
	retryBodies := append([][]byte(nil), bodies...)
	mu.Unlock()
	require.GreaterOrEqual(t, len(retryKeys), 2)
	require.Equal(t, retryKeys[0], retryKeys[1])
	require.Equal(t, retryBodies[0], retryBodies[1])
	count, _, _ = activity.counts()
	require.Equal(t, 1, count, "retry gap must not release accepted work")
	release <- struct{}{}
	awaitFirehoseSettled(t, manager, activity)
}

type firehoseBlockingClose struct {
	entered, release chan struct{}
}

func (body *firehoseBlockingClose) Read([]byte) (int, error) { return 0, io.EOF }
func (body *firehoseBlockingClose) Close() error {
	close(body.entered)
	<-body.release
	return nil
}

func TestDevDrainTimeoutKeepsLeaseThroughResponseBodyCleanup(t *testing.T) {
	body := &firehoseBlockingClose{entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce, cleanupOnce sync.Once
	releaseBody := func() { releaseOnce.Do(func() { close(body.release) }) }
	cleanupEntered := make(chan struct{})
	cleanupGate := make(chan struct{})
	releaseCleanup := func() { cleanupOnce.Do(func() { close(cleanupGate) }) }
	t.Cleanup(func() { releaseBody(); releaseCleanup() })
	var requestContext context.Context
	var attempts atomic.Int32
	manager := NewFirehoseManager("us-east-1", "000000000000", "http://127.0.0.1:1", "test", "test")
	manager.httpClient.Transport = firehoseTestTransport(func(request *http.Request) (*http.Response, error) {
		switch attempts.Add(1) {
		case 1:
			requestContext = request.Context()
			return firehoseTestResponse(body), nil
		case 2:
			close(cleanupEntered)
			select {
			case <-cleanupGate:
			case <-request.Context().Done():
				return nil, request.Context().Err()
			}
		}
		return firehoseTestResponse(io.NopCloser(bytes.NewReader(nil))), nil
	})
	coordinator := devquiescence.NewWithOptions(devquiescence.Options{
		Checks:     []func() error{manager.DevEvidence},
		DrainHooks: []devquiescence.DrainHook{{Start: manager.DevBeginDrain, Resume: manager.DevResume}},
	})
	require.NoError(t, manager.SetDevActivity(coordinator))
	stream, err := manager.CreateStream("body-cleanup", "bucket", "", "", 100, 3600)
	require.NoError(t, err)
	t.Cleanup(func() { releaseBody(); releaseCleanup(); require.NoError(t, manager.Shutdown()) })
	_, err = manager.PutRecord(stream, []byte("accepted"))
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	snapshot, err := coordinator.Quiesce(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	awaitFirehoseSignal(t, body.entered)
	require.False(t, snapshot.FixtureSafe)
	require.Equal(t, 1, snapshot.WorkCount)
	require.NoError(t, requestContext.Err(), "drain wait cancellation must not cancel accepted delivery")
	require.NoError(t, manager.DevEvidence(), "pending accepted work is counted, not dirty evidence")
	snapshotStream, _ := manager.Snapshot(stream.Name)
	require.Equal(t, 1, snapshotStream.BufferedRecords, "2xx is not joined until body close returns")
	releaseBody()
	joinedContext, joinedCancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer joinedCancel()
	snapshot, err = coordinator.Quiesce(joinedContext)
	require.NoError(t, err)
	require.True(t, snapshot.FixtureSafe)
	_, err = manager.PutRecord(stream, []byte("undeclared held root"))
	require.Error(t, err)
	cleanupHandler := coordinator.Wrap(devquiescence.Callback,
		http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			t.Error("held cleanup was routed as ordinary work")
			writer.WriteHeader(http.StatusInternalServerError)
		}),
		http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			require.Equal(t, devquiescence.CleanupOnly, devquiescence.RequestMode(request))
			complete, err := coordinator.BeginCleanup(snapshot.Generation, "cleanup", "fixture")
			require.NoError(t, err)
			defer complete(nil)
			_, err = manager.PutRecord(stream, []byte("declared cleanup continuation"))
			require.NoError(t, err)
			writer.WriteHeader(http.StatusNoContent)
		}))
	response := httptest.NewRecorder()
	cleanupHandler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/declared-cleanup", nil))
	require.Equal(t, http.StatusNoContent, response.Code)
	awaitFirehoseSignal(t, cleanupEntered)
	pendingCleanup := coordinator.Snapshot()
	require.Equal(t, 1, pendingCleanup.WorkCount, "the returned cleanup still owns its pending Firehose descendant")
	require.Zero(t, pendingCleanup.CleanupEnvelopes)
	require.Empty(t, pendingCleanup.EvidenceFailure, "healthy pending delivery must not poison cleanup evidence")
	require.False(t, pendingCleanup.FixtureSafe)
	require.NoError(t, manager.DevEvidence())
	require.Error(t, manager.DevResume(), "resume still requires settled accepted records")
	releaseCleanup()
	cleanupSnapshot, err := coordinator.Quiesce(joinedContext)
	require.NoError(t, err)
	require.True(t, cleanupSnapshot.FixtureSafe, "late cleanup puts must settle under the existing force mode")
	require.Equal(t, snapshot.Generation, cleanupSnapshot.Generation)
	_, err = coordinator.Resume(cleanupSnapshot.Generation)
	require.NoError(t, err)
	require.Same(t, stream, manager.GetStream(stream.Name))
	_, err = manager.PutRecord(stream, []byte("resumed native buffer"))
	require.NoError(t, err)
	require.Never(t, func() bool {
		snapshot, _ := manager.Snapshot(stream.Name)
		return snapshot.BufferedRecords != 1
	}, 50*time.Millisecond, time.Millisecond)
}

func TestDevDrainRecoversFailedDeleteWithJoinedNativeWorker(t *testing.T) {
	var available atomic.Bool
	var mu sync.Mutex
	var paths []string
	manager, stream, activity := newDevFirehose(t, firehoseTestTransport(func(request *http.Request) (*http.Response, error) {
		mu.Lock()
		paths = append(paths, request.URL.Path)
		mu.Unlock()
		if !available.Load() {
			return &http.Response{StatusCode: http.StatusServiceUnavailable, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(nil))}, nil
		}
		return firehoseTestResponse(io.NopCloser(bytes.NewReader(nil))), nil
	}))
	t.Cleanup(func() { available.Store(true); require.NoError(t, manager.Shutdown()) })
	_, err := manager.PutRecord(stream, []byte("retained through failed delete"))
	require.NoError(t, err)
	require.Error(t, manager.DeleteStream(t.Context(), stream.Name))
	awaitFirehoseSignal(t, stream.done)
	stream.mu.Lock()
	key, data := stream.pending[0].key, bytes.Clone(stream.pending[0].data)
	stream.mu.Unlock()
	available.Store(true)
	require.NoError(t, manager.DevBeginDrain())
	awaitFirehoseSettled(t, manager, activity)
	require.Same(t, stream, manager.GetStream(stream.Name))
	snapshot, _ := manager.Snapshot(stream.Name)
	require.Equal(t, "DELETING", snapshot.Status, "drain must preserve resource state")
	stream.mu.Lock()
	pending := len(stream.pending)
	stream.mu.Unlock()
	require.Zero(t, pending)
	require.NotEmpty(t, key)
	mu.Lock()
	deliveredPaths := append([]string(nil), paths...)
	mu.Unlock()
	require.Equal(t, []string{"/bucket/" + key, "/bucket/" + key}, deliveredPaths)
	require.Equal(t, []byte("retained through failed delete"), data)
	require.NoError(t, manager.DeleteStream(t.Context(), stream.Name))
	require.Nil(t, manager.GetStream(stream.Name))
}

func TestDevActivityRefusalPrecedesOrderedAcceptanceMutation(t *testing.T) {
	manager, stream, activity := newDevFirehose(t, firehoseTestTransport(func(*http.Request) (*http.Response, error) {
		return firehoseTestResponse(io.NopCloser(bytes.NewReader(nil))), nil
	}))
	t.Cleanup(func() { require.NoError(t, manager.Shutdown()) })
	activity.refuseAfter = 1
	results, err := manager.AcceptRecordBatch(t.Context(), stream, [][]byte{[]byte("accepted"), []byte("refused")})
	require.NoError(t, err)
	require.NotEmpty(t, results[0].RecordID)
	require.Empty(t, results[0].ErrorCode)
	require.Empty(t, results[1].RecordID)
	require.Equal(t, "ServiceUnavailableException", results[1].ErrorCode)
	snapshot, _ := manager.Snapshot(stream.Name)
	require.Equal(t, 1, snapshot.BufferedRecords)
	stream.mu.Lock()
	source := bytes.Clone(stream.buffer[0].source)
	stream.mu.Unlock()
	require.Equal(t, []byte("accepted"), source)
	count, _, _ := activity.counts()
	require.Equal(t, 1, count)
}

func TestDevAbortJoinWaitsForCanceledHTTPAttemptThenMarksRetainedRecordsDirty(t *testing.T) {
	for _, shutdownFirst := range []bool{false, true} {
		name := "retained-abort"
		if shutdownFirst {
			name = "failed-shutdown-then-abort"
		}
		t.Run(name, func(t *testing.T) {
			entered := make(chan struct{})
			canceled := make(chan struct{})
			cleanup := make(chan struct{})
			var enteredOnce, cleanupOnce sync.Once
			releaseCleanup := func() { cleanupOnce.Do(func() { close(cleanup) }) }
			manager, stream, activity := newDevFirehose(t, firehoseTestTransport(func(request *http.Request) (*http.Response, error) {
				enteredOnce.Do(func() { close(entered) })
				<-request.Context().Done()
				close(canceled)
				<-cleanup
				return nil, request.Context().Err()
			}))
			t.Cleanup(func() {
				releaseCleanup()
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_ = manager.DevAbortJoin(ctx)
			})
			_, err := manager.PutRecord(stream, []byte("original retained record"))
			require.NoError(t, err)
			require.NoError(t, manager.DevBeginDrain())
			awaitFirehoseSignal(t, entered)
			if shutdownFirst {
				ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
				require.ErrorIs(t, manager.ShutdownContext(ctx), context.DeadlineExceeded)
				cancel()
				count, _, _ := activity.counts()
				require.Equal(t, 1, count, "ShutdownContext timeout must not substitute for join")
				select {
				case <-manager.dev.forceDone:
					t.Fatal("blocked delivery cleanup was reported joined")
				default:
				}
			}
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
			defer cancel()
			require.ErrorIs(t, manager.DevAbortJoin(ctx), context.DeadlineExceeded)
			awaitFirehoseSignal(t, canceled)
			count, completions, dirty := activity.counts()
			require.Equal(t, 1, count, "canceling HTTP is not its joined completion")
			require.Zero(t, completions)
			require.Zero(t, dirty)
			require.Error(t, manager.DevEvidence())
			snapshot, _ := manager.Snapshot(stream.Name)
			require.Equal(t, 1, snapshot.BufferedRecords)
			releaseCleanup()
			joinedContext, joinedCancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer joinedCancel()
			err = manager.DevAbortJoin(joinedContext)
			require.ErrorContains(t, err, "1 retained records")
			awaitFirehoseSignal(t, stream.done)
			awaitFirehoseSignal(t, manager.dev.forceDone)
			count, completions, dirty = activity.counts()
			require.Zero(t, count)
			require.Equal(t, 1, completions)
			require.Equal(t, 1, dirty)
			stream.mu.Lock()
			pending := len(stream.pending)
			source := bytes.Clone(stream.pending[0].records[0].source)
			deliveryError := stream.lastDeliveryError
			stream.mu.Unlock()
			require.Equal(t, 1, pending)
			require.Equal(t, []byte("original retained record"), source)
			require.NotEmpty(t, deliveryError)
			require.Error(t, manager.DevResume())
			require.ErrorContains(t, manager.DevEvidence(), "1 retained records")
			_, err = manager.PutRecord(stream, []byte("refused after abort"))
			require.Error(t, err)
			require.EqualError(t, manager.DevAbortJoin(t.Context()), manager.DevEvidence().Error())
		})
	}
}

func TestDevShutdownJoinsForceOwnerAndSettlesFinalBufferedLease(t *testing.T) {
	manager, stream, activity := newDevFirehose(t, firehoseTestTransport(func(*http.Request) (*http.Response, error) {
		return firehoseTestResponse(io.NopCloser(bytes.NewReader(nil))), nil
	}))
	_, err := manager.PutRecord(stream, []byte("final buffered record"))
	require.NoError(t, err)
	require.NoError(t, manager.Shutdown())
	awaitFirehoseSignal(t, manager.dev.forceDone)
	awaitFirehoseSignal(t, stream.done)
	count, completions, dirty := activity.counts()
	require.Zero(t, count)
	require.Equal(t, 1, completions)
	require.Zero(t, dirty)
	require.NoError(t, manager.DevEvidence())
	require.NoError(t, manager.Shutdown())
}

func TestDevActivityOwnsDefaultTransportBeforeAnyStream(t *testing.T) {
	manager := NewFirehoseManager("us-east-1", "000000000000", "http://127.0.0.1:1", "test", "test")
	require.Nil(t, manager.httpClient.Transport, "normal mode preserves its existing client configuration")
	require.NoError(t, manager.SetDevActivity(&firehoseTestActivity{}))
	require.IsType(t, &http.Transport{}, manager.httpClient.Transport)
	require.NotSame(t, http.DefaultTransport, manager.httpClient.Transport)
	require.NoError(t, manager.Shutdown())
	awaitFirehoseSignal(t, manager.dev.forceDone)
}

func TestDevDrainKeepsOriginalLeaseThroughDynamicErrorObjectReplacement(t *testing.T) {
	var available atomic.Bool
	manager := NewFirehoseManager("us-east-1", "000000000000", "http://127.0.0.1:1", "test", "test")
	manager.httpClient.Transport = firehoseTestTransport(func(*http.Request) (*http.Response, error) {
		if !available.Load() {
			return &http.Response{StatusCode: http.StatusServiceUnavailable, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(nil))}, nil
		}
		return firehoseTestResponse(io.NopCloser(bytes.NewReader(nil))), nil
	})
	activity := &firehoseTestActivity{}
	require.NoError(t, manager.SetDevActivity(activity))
	require.NoError(t, manager.SetMetadataExtractor(NewGoJQMetadataExtractor()))
	config := partitionConfig("dynamic-retained")
	config.DynamicPartitioningConfiguration.RetryOptions = &RetryOptions{DurationInSeconds: 0}
	stream, err := manager.CreateConfiguredStream(t.Context(), config)
	require.NoError(t, err)
	t.Cleanup(func() { available.Store(true); require.NoError(t, manager.Shutdown()) })
	original := []byte(`{"customer_id":"north","id":1}`)
	_, err = manager.PutRecord(stream, original)
	require.NoError(t, err)
	require.NoError(t, manager.DevBeginDrain())
	require.Eventually(t, func() bool {
		stream.mu.Lock()
		defer stream.mu.Unlock()
		return len(stream.pending) == 1 && stream.pending[0].records[0].errorType == "dynamic-partitioning-failed"
	}, 3*time.Second, time.Millisecond)
	stream.mu.Lock()
	retained := stream.pending[0]
	source := bytes.Clone(retained.records[0].source)
	errorData := bytes.Clone(retained.data)
	lease := retained.records[0].lease
	stream.mu.Unlock()
	require.Equal(t, original, source)
	require.Contains(t, string(errorData), "DynamicPartitioning.DeliveryFailed")
	require.NotNil(t, lease)
	count, completions, dirty := activity.counts()
	require.Equal(t, 1, count)
	require.Zero(t, completions)
	require.Zero(t, dirty)
	available.Store(true)
	manager.devWake()
	awaitFirehoseSettled(t, manager, activity)
	count, completions, dirty = activity.counts()
	require.Zero(t, count)
	require.Equal(t, 1, completions)
	require.Zero(t, dirty)
}
