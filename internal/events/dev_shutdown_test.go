package events

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/devquiescence"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDevDiscardPendingAdmissionsCancelsOwnedRuleRetry(t *testing.T) {
	owner := devquiescence.New()
	source := newRealSource(owner)
	clock := newClock()
	rejected := make(chan struct{}, 1)
	observed := make(chan Outcome, 1)
	releaseObservation := make(chan struct{})
	var calls atomic.Int32
	s := serviceFor(t, clock, invoker{admit: func(context.Context, string, []byte) error {
		calls.Add(1)
		rejected <- struct{}{}
		return admissionError(true)
	}}, DevOptions{Source: source, Observe: func(outcome Outcome) {
		observed <- outcome
		<-releaseObservation
	}})
	t.Cleanup(s.DevDiscardPendingAdmissions)
	release := cleanupRelease(t, releaseObservation)
	putRule(t, s, "discard-retry")
	putTarget(t, s, "discard-retry", Target{})
	await(t, clock.registered)
	clock.advance(time.Minute)
	require.NoError(t, await(t, source.attempts).err)
	await(t, rejected)
	// The fake clock never advances through this retry. Only the explicit
	// final-shutdown policy may wake the accepted admission owner.
	for duration := await(t, clock.registered); duration >= time.Minute; duration = await(t, clock.registered) {
	}
	assert.Equal(t, 1, owner.Snapshot().WorkCount)
	owner.Shutdown()
	s.DevDiscardPendingAdmissions()
	s.DevDiscardPendingAdmissions()
	outcome := await(t, observed)
	assert.Equal(t, "canceled", outcome.Status)
	assert.Equal(t, "TargetAdmissionCanceled", outcome.Code)
	assert.Equal(t, 1, outcome.Attempts)
	assert.Equal(t, int32(1), calls.Load())
	assert.Equal(t, 1, owner.Snapshot().WorkCount, "observation retains the actual source lease")

	closed := make(chan error, 1)
	go func() { closed <- s.Close(context.Background()) }()
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.closing
	}, time.Second, time.Millisecond)
	select {
	case err := <-closed:
		t.Fatalf("Close crossed the held outcome callback: %v", err)
	default:
	}
	release()
	require.NoError(t, await(t, source.completed))
	require.NoError(t, await(t, closed))
	snapshot, err := owner.Quiesce(t.Context())
	require.NoError(t, err)
	assert.Equal(t, devquiescence.Shutdown, snapshot.State)
	assert.Zero(t, snapshot.WorkCount)
	assert.False(t, snapshot.FixtureSafe, "disposable join never grants fixture-cleanup proof")
	require.NoError(t, s.DevEvidence())
	rule, err := s.DescribeRule(RuleInput{Name: "discard-retry"})
	require.NoError(t, err)
	assert.Equal(t, "ENABLED", rule.State, "the dev policy must not rewrite native rule state")
	clock.advance(time.Hour)
	select {
	case <-rejected:
		t.Fatal("discarded recurring intake admitted another target")
	case attempt := <-source.attempts:
		t.Fatalf("discarded recurring intake acquired another source: %+v", attempt)
	default:
	}
}

func TestDevDiscardPendingAdmissionsPreservesAcceptedRuleTransferAndJoinsCallback(t *testing.T) {
	owner := devquiescence.New()
	source := newRealSource(owner)
	clock := newClock()
	entered := make(chan context.Context, 1)
	child := make(chan func(error), 1)
	releaseCallback := make(chan struct{})
	observed := make(chan Outcome, 1)
	s := serviceFor(t, clock, invoker{admit: func(ctx context.Context, _ string, _ []byte) error {
		complete, err := owner.BeginActivity("lambda_async", "accepted-target")
		if err != nil {
			return err
		}
		child <- complete
		entered <- ctx
		<-releaseCallback
		// The native owner already accepted the event. A late context
		// cancellation must not relabel that successful ownership transfer.
		return nil
	}}, DevOptions{Source: source, Observe: func(outcome Outcome) { observed <- outcome }})
	t.Cleanup(s.DevDiscardPendingAdmissions)
	release := cleanupRelease(t, releaseCallback)
	putRule(t, s, "accepted-discard")
	putTarget(t, s, "accepted-discard", Target{})
	await(t, clock.registered)
	clock.advance(time.Minute)
	require.NoError(t, await(t, source.attempts).err)
	invocationCtx := await(t, entered)
	completeChild := await(t, child)
	t.Cleanup(func() { completeChild(nil) })
	assert.Equal(t, 2, owner.Snapshot().WorkCount)
	owner.Shutdown()
	s.DevDiscardPendingAdmissions()
	await(t, invocationCtx.Done())
	closed := make(chan error, 1)
	go func() { closed <- s.Close(context.Background()) }()
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.closing
	}, time.Second, time.Millisecond)
	select {
	case err := <-closed:
		t.Fatalf("Close crossed a real accepted admission callback: %v", err)
	default:
	}
	release()
	outcome := await(t, observed)
	assert.Equal(t, "accepted", outcome.Status)
	assert.Empty(t, outcome.Code)
	assert.Equal(t, 1, outcome.Attempts)
	require.NoError(t, await(t, source.completed))
	require.NoError(t, await(t, closed))
	assert.Equal(t, 1, owner.Snapshot().WorkCount, "Lambda retains accepted event ownership")
	completeChild(nil)
	snapshot, err := owner.Quiesce(t.Context())
	require.NoError(t, err)
	assert.Zero(t, snapshot.WorkCount)
	require.NoError(t, s.DevEvidence())
}
