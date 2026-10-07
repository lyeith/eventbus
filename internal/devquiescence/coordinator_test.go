package devquiescence

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func awaitState(t *testing.T, c *Coordinator, state State) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	for {
		c.mu.Lock()
		current, wake := c.state, c.wake
		c.mu.Unlock()
		if current == state {
			return
		}
		select {
		case <-wake:
		case <-ctx.Done():
			t.Fatalf("owner did not reach %s", state)
		}
	}
}

func TestPreParseEnvelopeSurvivesCallerCancellationAndJoinsTaskRetryLifetime(t *testing.T) {
	c := New()
	entered, release := make(chan struct{}), make(chan struct{})
	var taskDone func(error)
	handler := c.Wrap(Source, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		_, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		var admissionErr error
		taskDone, admissionErr = c.BeginActivity("lambda.async", "invocation-id")
		require.NoError(t, admissionErr)
	}), nil)
	ctx, cancel := context.WithCancel(t.Context())
	envelopeDone := make(chan struct{})
	go func() {
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", strings.NewReader("publication")).WithContext(ctx))
		close(envelopeDone)
	}()
	<-entered
	cancel()
	joined := make(chan error, 1)
	go func() { _, err := c.Quiesce(t.Context()); joined <- err }()
	awaitState(t, c, Draining)
	require.Equal(t, 1, c.Snapshot().WorkCount)
	close(release)
	<-envelopeDone
	require.Equal(t, 1, c.Snapshot().WorkCount, "async queued/running/retry intervals share a task lease")
	select {
	case err := <-joined:
		t.Fatalf("joined before async completion: %v", err)
	default:
	}
	callback, err := c.BeginActivity("lambda.sync", "nested-id")
	require.NoError(t, err)
	taskDone(nil)
	require.Equal(t, 1, c.Snapshot().WorkCount, "independent nested callback outlives outer task")
	callback(nil)
	require.NoError(t, <-joined)
	require.True(t, c.Snapshot().FixtureSafe)
	_, err = c.Resume(c.Snapshot().Generation)
	require.NoError(t, err)
	second, err := c.BeginActivity("lambda.async", "second-suite")
	require.NoError(t, err)
	second(nil)
}

func TestTimeoutRetainsFenceAndRetryDoesNotCancelBusinessWork(t *testing.T) {
	c := New()
	done, err := c.BeginActivity("lambda.async", "retry-wait")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	snapshot, err := c.Quiesce(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, snapshot.FixtureSafe)
	require.Equal(t, Draining, snapshot.State)
	require.Equal(t, 1, snapshot.WorkCount)
	_, err = c.Resume(snapshot.Generation)
	require.ErrorIs(t, err, ErrNotSafe)
	source := c.Wrap(Source, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("fenced source ran") }), nil)
	recorder := httptest.NewRecorder()
	source.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/", nil))
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	done(nil)
	snapshot, err = c.Quiesce(t.Context())
	require.NoError(t, err)
	require.True(t, snapshot.FixtureSafe)
	require.Empty(t, snapshot.LastTimeout)
}

func TestCleanupOnlyEnvelopeBlocksResumeAndKeepsImmutableMode(t *testing.T) {
	c := New()
	held, err := c.Quiesce(t.Context())
	require.NoError(t, err)
	entered, release := make(chan struct{}), make(chan struct{})
	var retainedRequest *http.Request
	wrapper := c.Wrap(Callback, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("cleanup envelope became work") }), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		retainedRequest = r
		close(entered)
		<-release
		require.Equal(t, CleanupOnly, RequestMode(r))
	}))
	done := make(chan struct{})
	go func() {
		wrapper.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", nil))
		close(done)
	}()
	<-entered
	require.False(t, c.Snapshot().FixtureSafe)
	require.Equal(t, 1, c.Snapshot().CleanupEnvelopes)
	_, err = c.Resume(held.Generation)
	require.ErrorIs(t, err, ErrNotSafe)
	close(release)
	<-done
	resumed, err := c.Resume(held.Generation)
	require.NoError(t, err)
	require.Equal(t, held.Generation+1, resumed.Generation)
	require.Equal(t, CleanupOnly, RequestMode(retainedRequest), "an old envelope retains its admission mode after resume")
}

func TestShutdownAllowsAcceptedCallbacksAndNeverResumes(t *testing.T) {
	c := New()
	done, err := c.BeginActivity("lambda.async", "outer")
	require.NoError(t, err)
	closing := c.Shutdown()
	require.Equal(t, Shutdown, closing.State)
	callback, err := c.BeginActivity("lambda.sync", "inner")
	require.NoError(t, err)
	var called bool
	c.Wrap(Callback, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true; require.Equal(t, Work, RequestMode(r)) }), nil).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", nil))
	require.True(t, called)
	done(nil)
	callback(nil)
	joined, err := c.Quiesce(t.Context())
	require.NoError(t, err)
	require.False(t, joined.FixtureSafe)
	require.Equal(t, Shutdown, joined.State)
	_, err = c.BeginActivity("lambda.sync", "late")
	require.ErrorIs(t, err, ErrShutdown)
	_, err = c.Resume(joined.Generation)
	require.ErrorIs(t, err, ErrShutdown)
}

func TestEvidenceFailureIsStickyRedactedAndChecksRunOutsideLock(t *testing.T) {
	var c *Coordinator
	c = New(func() error { require.Equal(t, 0, c.Snapshot().WorkCount); return errors.New("private capture error") })
	snapshot, err := c.Quiesce(t.Context())
	require.ErrorIs(t, err, ErrEvidence)
	require.False(t, snapshot.FixtureSafe)
	encoded, err := json.Marshal(snapshot)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "private capture error")
	_, err = c.Resume(snapshot.Generation)
	require.ErrorIs(t, err, ErrEvidence)
	_, err = c.Quiesce(t.Context())
	require.ErrorIs(t, err, ErrEvidence)
	other := New()
	complete, err := other.BeginActivity("bad kind with secrets", "not/an/id")
	require.NoError(t, err)
	require.Equal(t, "activity", other.Snapshot().Activities[0].Kind)
	require.Empty(t, other.Snapshot().Activities[0].RequestID)
	complete(errors.New("private child cleanup error"))
	complete(nil)
	_, err = other.Quiesce(t.Context())
	require.ErrorIs(t, err, ErrEvidence)
	require.Zero(t, other.Snapshot().WorkCount)
}

func TestEvidenceCheckRechecksConcurrentProvisionalEnvelopeBeforeHolding(t *testing.T) {
	checked, releaseCheck := make(chan struct{}), make(chan struct{})
	var once sync.Once
	c := New(func() error { once.Do(func() { close(checked); <-releaseCheck }); return nil })
	joined := make(chan error, 1)
	go func() { _, err := c.Quiesce(t.Context()); joined <- err }()
	<-checked
	mode, release, err := c.beginEnvelope(Callback)
	require.ErrorIs(t, err, ErrNotSafe)
	require.Equal(t, CleanupOnly, mode)
	close(releaseCheck)
	select {
	case err := <-joined:
		t.Fatalf("held with active provisional envelope: %v", err)
	default:
	}
	release(nil)
	require.NoError(t, <-joined)
	require.True(t, c.Snapshot().FixtureSafe)
}

func TestConcurrentResumeGenerationAndShutdownRemainExclusive(t *testing.T) {
	c := New()
	held, err := c.Quiesce(t.Context())
	require.NoError(t, err)
	start := make(chan struct{})
	results := make(chan error, 2)
	for range 2 {
		go func() { <-start; _, err := c.Resume(held.Generation); results <- err }()
	}
	close(start)
	first, second := <-results, <-results
	require.True(t, first == nil && errors.Is(second, ErrGeneration) || second == nil && errors.Is(first, ErrGeneration))
	c.Shutdown()
	_, err = c.Resume(c.Snapshot().Generation)
	require.ErrorIs(t, err, ErrShutdown)
}

func TestSnapshotActivityMetadataIsBounded(t *testing.T) {
	c := New()
	var completions []func(error)
	for range 100 {
		complete, err := c.BeginActivity("lambda.async", "id")
		require.NoError(t, err)
		completions = append(completions, complete)
	}
	snapshot := c.Snapshot()
	require.Len(t, snapshot.Activities, maxVisibleActivities)
	require.Equal(t, 100, snapshot.WorkCount)
	require.Equal(t, 36, snapshot.UnlistedActivities)
	for _, complete := range completions {
		complete(nil)
	}
	require.Zero(t, c.Snapshot().WorkCount)
}

func TestEvidenceFailureFencesSourcesButJoinsExistingCallbackChain(t *testing.T) {
	c := New()
	failed, err := c.BeginActivity("lambda.async", "failed-evidence")
	require.NoError(t, err)
	outer, err := c.BeginActivity("lambda.async", "accepted-chain")
	require.NoError(t, err)
	failed(errors.New("private writer error"))
	require.Equal(t, Draining, c.Snapshot().State)
	callback, err := c.BeginActivity("lambda.sync", "nested-after-evidence-failure")
	require.NoError(t, err, "accepted business chains must still join")
	called := false
	c.Wrap(Callback, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }), nil).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", nil))
	require.True(t, called)
	joined := make(chan error, 1)
	go func() { _, err := c.Quiesce(t.Context()); joined <- err }()
	outer(nil)
	select {
	case err := <-joined:
		t.Fatalf("failed barrier skipped owned callback: %v", err)
	default:
	}
	callback(nil)
	require.ErrorIs(t, <-joined, ErrEvidence)
	require.Zero(t, c.Snapshot().WorkCount)
	require.False(t, c.Snapshot().FixtureSafe)
}

func TestOldQuiescerCannotRefenceResumedGeneration(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var checkMu sync.Mutex
	checks := 0
	c := New(func() error {
		checkMu.Lock()
		checks++
		first := checks == 1
		checkMu.Unlock()
		if first {
			close(entered)
			<-release
		}
		return nil
	})
	oldResult := make(chan error, 1)
	go func() { _, err := c.Quiesce(t.Context()); oldResult <- err }()
	<-entered
	held, err := c.Quiesce(t.Context())
	require.NoError(t, err)
	resumed, err := c.Resume(held.Generation)
	require.NoError(t, err)
	close(release)
	require.ErrorIs(t, <-oldResult, ErrGeneration)
	snapshot := c.Snapshot()
	require.Equal(t, Open, snapshot.State)
	require.Equal(t, resumed.Generation, snapshot.Generation)
	complete, err := c.BeginActivity("lambda.async", "resumed-suite")
	require.NoError(t, err)
	complete(nil)
}

func TestConcurrentResumeAndShutdownNeverReopensClosingOwner(t *testing.T) {
	c := New()
	held, err := c.Quiesce(t.Context())
	require.NoError(t, err)
	start := make(chan struct{})
	resumed := make(chan error, 1)
	closed := make(chan struct{})
	go func() { <-start; _, err := c.Resume(held.Generation); resumed <- err }()
	go func() { <-start; c.Shutdown(); close(closed) }()
	close(start)
	resumeErr := <-resumed
	<-closed
	require.True(t, resumeErr == nil || errors.Is(resumeErr, ErrShutdown))
	snapshot := c.Snapshot()
	require.Equal(t, Shutdown, snapshot.State)
	require.False(t, snapshot.FixtureSafe)
	_, err = c.Resume(snapshot.Generation)
	require.ErrorIs(t, err, ErrShutdown)
	_, err = c.BeginActivity("lambda.async", "late")
	require.ErrorIs(t, err, ErrShutdown)
}

func TestExpiredDeadlineRetainsSourceFenceAndReportsRedactedTimeout(t *testing.T) {
	c := New()
	complete, err := c.BeginActivity("lambda.async", "accepted")
	require.NoError(t, err)
	ctx, cancel := context.WithDeadline(t.Context(), time.Unix(1, 0))
	defer cancel()
	snapshot, err := c.Quiesce(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, "deadline_exceeded", snapshot.LastTimeout)
	require.Equal(t, Draining, snapshot.State)
	require.False(t, snapshot.FixtureSafe)
	require.Equal(t, 1, snapshot.WorkCount)
	complete(nil)
	snapshot, err = c.Quiesce(t.Context())
	require.NoError(t, err)
	require.True(t, snapshot.FixtureSafe)
	require.Empty(t, snapshot.LastTimeout)
}
