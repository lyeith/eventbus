package devquiescence

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSourceFenceSignalAndAtomicRefusalWake(t *testing.T) {
	c := New()
	root, fence, err := c.BeginSource("mapping.batch", "batch")
	require.NoError(t, err)
	child, err := c.BeginActivity("lambda.sync", "child")
	require.NoError(t, err)
	child(nil)
	select {
	case <-fence:
		t.Fatal("ordinary activity changes fenced intake")
	default:
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = c.Quiesce(ctx)
	require.ErrorIs(t, err, context.Canceled)
	select {
	case <-fence:
	default:
		t.Fatal("quiescence did not fence the admitted intake")
	}
	_, changed, err := c.BeginSource("scheduler.dispatch", "later")
	require.ErrorIs(t, err, ErrFenced)
	child, err = c.BeginActivity("lambda.sync", "accepted-descendant")
	require.NoError(t, err)
	select {
	case <-changed:
	default:
		t.Fatal("refused source lost an activity change wake")
	}
	root(nil)
	child(nil)
	held, err := c.Quiesce(t.Context())
	require.NoError(t, err)
	_, changed, err = c.BeginSource("mapping.batch", "held")
	require.ErrorIs(t, err, ErrFenced)
	_, err = c.Resume(held.Generation)
	require.NoError(t, err)
	select {
	case <-changed:
	default:
		t.Fatal("refused source lost Resume wake")
	}
	next, nextFence, err := c.BeginSource("mapping.batch", "next")
	require.NoError(t, err)
	require.NotEqual(t, fence, nextFence)
	select {
	case <-nextFence:
		t.Fatal("new generation inherited a closed intake fence")
	default:
	}
	next(nil)
}

func TestRegisteredCleanupJoinsDescendantsWithoutReopeningSource(t *testing.T) {
	c := New()
	held, err := c.Quiesce(t.Context())
	require.NoError(t, err)
	var descendant func(error)
	wrapper := c.Wrap(Callback, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("held root bypassed cleanup admission") }), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, CleanupOnly, RequestMode(r))
		root, err := c.BeginCleanup(held.Generation, "cleanup.lambda", "exact-cleanup")
		require.NoError(t, err, "the cleanup's received envelope may already be counted")
		_, _, err = c.BeginSource("scheduler.dispatch", "unrelated-root")
		require.ErrorIs(t, err, ErrFenced)
		descendant, err = c.BeginActivity("lambda.async", "cleanup-descendant")
		require.NoError(t, err)
		root(nil)
	}))
	wrapper.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/registered-cleanup", nil))
	require.Equal(t, 1, c.Snapshot().WorkCount)
	_, err = c.Resume(held.Generation)
	require.ErrorIs(t, err, ErrNotSafe)
	descendant(nil)
	require.False(t, c.Snapshot().FixtureSafe, "cleanup completion still requires an explicit evidence barrier")
	_, err = c.Resume(held.Generation)
	require.ErrorIs(t, err, ErrNotSafe)
	held, err = c.Quiesce(t.Context())
	require.NoError(t, err)
	require.True(t, held.FixtureSafe)
	_, err = c.Resume(held.Generation)
	require.NoError(t, err)
	_, err = c.BeginCleanup(held.Generation, "cleanup.lambda", "stale")
	require.ErrorIs(t, err, ErrGeneration)
}

func TestDrainHookTransitionPreventsConcurrentEarlyBarrier(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	var c *Coordinator
	hooks := []DrainHook{{Start: func() error {
		require.Equal(t, 1, c.Snapshot().WorkCount, "the hook is owned and runs outside the coordinator lock")
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return nil
	}}}
	c = NewWithOptions(Options{DrainHooks: hooks})
	hooks[0].Start = func() error { t.Error("construction-time hooks were mutated"); return nil }
	joined := make(chan error, 1)
	go func() { _, err := c.Quiesce(t.Context()); joined <- err }()
	<-entered
	_, err := c.BeginActivity("lambda.sync", "unrelated")
	require.ErrorIs(t, err, ErrFenced, "a transition alone is not an accepted business ancestor")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	snapshot, err := c.Quiesce(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, snapshot.FixtureSafe)
	require.Equal(t, 1, snapshot.WorkCount)
	close(release)
	require.NoError(t, <-joined)
	require.True(t, c.Snapshot().FixtureSafe)
	require.Equal(t, int32(1), calls.Load(), "retrying a fence does not restart drain hooks")
}

func TestResumeHookKeepsSourceFencedAndExcludesControlsUntilCommitted(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var c *Coordinator
	c = NewWithOptions(Options{DrainHooks: []DrainHook{{Resume: func() error {
		require.Equal(t, 1, c.Snapshot().WorkCount)
		close(entered)
		<-release
		return nil
	}}}})
	held, err := c.Quiesce(t.Context())
	require.NoError(t, err)
	resumed := make(chan error, 1)
	go func() { _, err := c.Resume(held.Generation); resumed <- err }()
	<-entered
	_, _, err = c.BeginSource("mapping.batch", "during-resume")
	require.ErrorIs(t, err, ErrFenced)
	_, err = c.BeginActivity("lambda.sync", "during-resume")
	require.ErrorIs(t, err, ErrFenced)
	_, err = c.BeginCleanup(held.Generation, "cleanup.lambda", "racing-cleanup")
	require.ErrorIs(t, err, ErrNotSafe)
	_, err = c.Quiesce(t.Context())
	require.ErrorIs(t, err, ErrNotSafe)
	require.False(t, c.Snapshot().FixtureSafe)
	close(release)
	require.NoError(t, <-resumed)
	require.Equal(t, held.Generation+1, c.Snapshot().Generation)
	root, _, err := c.BeginSource("mapping.batch", "after-resume")
	require.NoError(t, err)
	root(nil)
}

func TestShutdownWinsBlockedResumeHookWithoutOpeningIntake(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	c := NewWithOptions(Options{DrainHooks: []DrainHook{{Resume: func() error { close(entered); <-release; return nil }}}})
	held, err := c.Quiesce(t.Context())
	require.NoError(t, err)
	resumed := make(chan error, 1)
	go func() { _, err := c.Resume(held.Generation); resumed <- err }()
	<-entered
	c.Shutdown()
	close(release)
	require.ErrorIs(t, <-resumed, ErrShutdown)
	require.Equal(t, held.Generation, c.Snapshot().Generation)
	_, _, err = c.BeginSource("mapping.batch", "late")
	require.ErrorIs(t, err, ErrShutdown)
	joined, err := c.Quiesce(t.Context())
	require.NoError(t, err)
	require.Equal(t, Shutdown, joined.State)
	require.False(t, joined.FixtureSafe)
}

func TestHookErrorsAndPanicsRemainStickyAndRedacted(t *testing.T) {
	for _, callback := range []func() error{
		func() error { return errors.New("private transition failure") },
		func() error { panic("private transition panic") },
	} {
		c := NewWithOptions(Options{DrainHooks: []DrainHook{{Start: callback}}})
		snapshot, err := c.Quiesce(t.Context())
		require.ErrorIs(t, err, ErrEvidence)
		require.Zero(t, snapshot.WorkCount)
		require.False(t, snapshot.FixtureSafe)
		require.NotContains(t, snapshot.EvidenceFailure, "private")
		_, err = c.Resume(snapshot.Generation)
		require.ErrorIs(t, err, ErrEvidence)
		other := NewWithOptions(Options{DrainHooks: []DrainHook{{Resume: callback}}})
		held, err := other.Quiesce(t.Context())
		require.NoError(t, err)
		_, err = other.Resume(held.Generation)
		require.ErrorIs(t, err, ErrEvidence)
		_, _, err = other.BeginSource("mapping.batch", "late")
		require.ErrorIs(t, err, ErrEvidence)
	}
}
