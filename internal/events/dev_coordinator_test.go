package events

import (
	"context"
	"encoding/json"
	"github.com/lyeith/eventbus/internal/devactivity"
	"github.com/lyeith/eventbus/internal/devquiescence"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type sourceAttempt struct {
	kind, id string
	err      error
}
type realSourceProbe struct {
	source    devactivity.Source
	attempts  chan sourceAttempt
	completed chan error
	refused   func()
	release   func()
}

func newRealSource(source devactivity.Source) *realSourceProbe {
	return &realSourceProbe{source: source, attempts: make(chan sourceAttempt, 30), completed: make(chan error, 30)}
}
func (p *realSourceProbe) BeginSource(kind, id string) (func(error), <-chan struct{}, error) {
	complete, changed, err := p.source.BeginSource(kind, id)
	p.attempts <- sourceAttempt{kind, id, err}
	if err != nil && p.refused != nil {
		p.refused()
	}
	if complete == nil {
		return nil, changed, err
	}
	return func(failure error) {
		complete(failure)
		p.completed <- failure
		if p.release != nil {
			p.release()
		}
	}, changed, err
}
func hold(t *testing.T, owner *devquiescence.Coordinator) devquiescence.Snapshot {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	snapshot, err := owner.Quiesce(ctx)
	require.NoError(t, err)
	require.True(t, snapshot.FixtureSafe)
	return snapshot
}
func TestRealSourceDormantHeldDueResumeAndJoinedObservation(t *testing.T) {
	owner := devquiescence.New()
	source := newRealSource(owner)
	clock := newClock()
	observed := make(chan Outcome, 1)
	release := make(chan struct{})
	var once sync.Once
	s := serviceFor(t, clock, invoker{}, DevOptions{Source: source, Observe: func(out Outcome) { observed <- out; <-release }})
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	putRule(t, s, "held")
	putTarget(t, s, "held", Target{})
	await(t, clock.registered)
	assert.Zero(t, owner.Snapshot().WorkCount, "dormant timer owns no source work")
	held := hold(t, owner)
	clock.advance(time.Minute)
	attempt := await(t, source.attempts)
	assert.Equal(t, "events.dispatch", attempt.kind)
	assert.Equal(t, "held", attempt.id)
	require.ErrorIs(t, attempt.err, devquiescence.ErrFenced)
	rule, err := s.DescribeRule(RuleInput{Name: "held"})
	require.NoError(t, err)
	assert.Equal(t, "ENABLED", rule.State)
	assert.Zero(t, owner.Snapshot().WorkCount)
	clock.advance(5 * time.Minute)
	_, err = owner.Resume(held.Generation)
	require.NoError(t, err)
	require.NoError(t, await(t, source.attempts).err)
	outcome := await(t, observed)
	assert.Equal(t, "accepted", outcome.Status)
	assert.Equal(t, 1, owner.Snapshot().WorkCount, "actual source lease includes observation")
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	snapshot, err := owner.Quiesce(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, 1, snapshot.WorkCount)
	assert.False(t, snapshot.FixtureSafe)
	once.Do(func() { close(release) })
	require.NoError(t, await(t, source.completed))
	s.mu.Lock()
	assert.Zero(t, s.pending, "pending publication must precede source completion")
	s.mu.Unlock()
	hold(t, owner)
	require.NoError(t, s.Close(t.Context()))
}
func TestRealSourceResumeBetweenRefusalAndWaitCannotLoseWake(t *testing.T) {
	owner := devquiescence.New()
	held := hold(t, owner)
	source := newRealSource(owner)
	resumed := make(chan error, 1)
	source.refused = func() { _, err := owner.Resume(held.Generation); resumed <- err }
	clock := newClock()
	outcomes := make(chan Outcome, 1)
	s := serviceFor(t, clock, invoker{}, DevOptions{Source: source, Observe: func(out Outcome) { outcomes <- out }})
	putRule(t, s, "race")
	putTarget(t, s, "race", Target{})
	await(t, clock.registered)
	clock.advance(time.Minute)
	require.NoError(t, await(t, resumed))
	require.ErrorIs(t, await(t, source.attempts).err, devquiescence.ErrFenced)
	require.NoError(t, await(t, source.attempts).err)
	assert.Equal(t, "accepted", await(t, outcomes).Status)
	require.NoError(t, await(t, source.completed))
}
func TestNativeDisableDeleteWakeRealFencedDueWorkers(t *testing.T) {
	for _, mutation := range []string{"disable", "delete"} {
		t.Run(mutation, func(t *testing.T) {
			owner := devquiescence.New()
			held := hold(t, owner)
			source := newRealSource(owner)
			clock := newClock()
			var calls atomic.Int32
			s := serviceFor(t, clock, invoker{admit: func(context.Context, string, []byte) error { calls.Add(1); return nil }}, DevOptions{Source: source})
			putRule(t, s, "native")
			if mutation == "disable" {
				putTarget(t, s, "native", Target{})
			}
			await(t, clock.registered)
			clock.advance(time.Minute)
			require.ErrorIs(t, await(t, source.attempts).err, devquiescence.ErrFenced)
			if mutation == "disable" {
				require.NoError(t, s.DisableRule(t.Context(), RuleInput{Name: "native"}))
				rule, err := s.DescribeRule(RuleInput{Name: "native"})
				require.NoError(t, err)
				assert.Equal(t, "DISABLED", rule.State)
				_, err = owner.Resume(held.Generation)
				require.NoError(t, err)
			} else {
				require.NoError(t, s.DeleteRule(t.Context(), RuleInput{Name: "native"}))
				_, err := s.DescribeRule(RuleInput{Name: "native"})
				nativeError(t, err, "ResourceNotFoundException")
				// This join occurs before Close can cancel the timer context:
				// deleting a parked due rule must itself wake its native owner.
				joined := make(chan struct{})
				go func() { s.wg.Wait(); close(joined) }()
				await(t, joined)
			}
			require.NoError(t, s.Close(t.Context()))
			assert.Zero(t, calls.Load())
			assert.Zero(t, owner.Snapshot().WorkCount)
		})
	}
}
func TestAcceptedRetryKeepsRealSourceLeaseWhileIntakeIsFenced(t *testing.T) {
	owner := devquiescence.New()
	source := newRealSource(owner)
	clock := newClock()
	rejected := make(chan struct{}, 1)
	observed := make(chan Outcome, 1)
	release := make(chan struct{})
	var once sync.Once
	var calls atomic.Int32
	s := serviceFor(t, clock, invoker{admit: func(context.Context, string, []byte) error {
		if calls.Add(1) == 1 {
			rejected <- struct{}{}
			return admissionError(true)
		}
		return nil
	}}, DevOptions{Source: source, Observe: func(out Outcome) { observed <- out; <-release }})
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	putRule(t, s, "retry")
	putTarget(t, s, "retry", Target{RetryPolicy: &RetryPolicy{MaximumRetryAttempts: pointer(1)}})
	await(t, clock.registered)
	clock.advance(time.Minute)
	require.NoError(t, await(t, source.attempts).err)
	await(t, rejected)
	for duration := await(t, clock.registered); duration >= time.Minute; duration = await(t, clock.registered) {
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	snapshot, err := owner.Quiesce(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, 1, snapshot.WorkCount)
	assert.Equal(t, devquiescence.Draining, snapshot.State)
	require.NoError(t, s.DisableRule(t.Context(), RuleInput{Name: "retry"}))
	_, err = s.RemoveTargets(t.Context(), RemoveTargetsInput{Rule: "retry", Ids: []string{"target"}})
	require.NoError(t, err)
	clock.advance(time.Second)
	outcome := await(t, observed)
	assert.Equal(t, "accepted", outcome.Status)
	assert.Equal(t, 2, outcome.Attempts)
	assert.Equal(t, 1, owner.Snapshot().WorkCount)
	once.Do(func() { close(release) })
	require.NoError(t, await(t, source.completed))
	hold(t, owner)
	assert.Equal(t, int32(2), calls.Load())
}
func TestCapacityParksDueOccurrenceAndWakesWithoutDiscarding(t *testing.T) {
	owner := devquiescence.New()
	source := newRealSource(owner)
	clock := newClock()
	payloads := make(chan []byte, 3)
	observed := make(chan Outcome, 3)
	release := make(chan struct{})
	var once sync.Once
	var observations atomic.Int32
	s := serviceFor(t, clock, invoker{admit: func(_ context.Context, _ string, payload []byte) error {
		payloads <- append([]byte(nil), payload...)
		return nil
	}}, DevOptions{Source: source, MaxPending: 1, Observe: func(out Outcome) {
		observed <- out
		if observations.Add(1) == 1 {
			<-release
		}
	}})
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	putRule(t, s, "capacity")
	putTarget(t, s, "capacity", Target{})
	await(t, clock.registered)
	clock.advance(time.Minute)
	require.NoError(t, await(t, source.attempts).err)
	await(t, payloads)
	await(t, observed)
	clock.advance(2 * time.Minute)
	require.NoError(t, await(t, source.attempts).err)
	require.NoError(t, await(t, source.completed), "capacity refusal immediately releases its tentative source lease")
	s.mu.Lock()
	assert.Equal(t, 1, s.pending)
	assert.Equal(t, initialTime.Add(2*time.Minute), s.entries["capacity"].next)
	s.mu.Unlock()
	assert.Equal(t, 1, owner.Snapshot().WorkCount, "capacity parking retains no additional source work")
	once.Do(func() { close(release) })
	require.NoError(t, await(t, source.completed))
	require.NoError(t, await(t, source.attempts).err)
	later := await(t, payloads)
	await(t, observed)
	require.NoError(t, await(t, source.completed))
	var event scheduledEvent
	require.NoError(t, json.Unmarshal(later, &event))
	assert.Equal(t, initialTime.Add(3*time.Minute).Format(time.RFC3339), event.Time)
	require.NoError(t, s.Close(t.Context()))
	assert.Zero(t, owner.Snapshot().WorkCount)
}

type brokenSource struct {
	admit func() (func(error), <-chan struct{}, error)
}

func (s brokenSource) BeginSource(string, string) (func(error), <-chan struct{}, error) {
	return s.admit()
}
func TestBrokenSourceContractsRetainStickyEvidenceAndJoin(t *testing.T) {
	for _, test := range []struct {
		name   string
		source brokenSource
	}{
		{"nil-completion", brokenSource{admit: func() (func(error), <-chan struct{}, error) { return nil, make(chan struct{}), nil }}},
		{"panic-admission", brokenSource{admit: func() (func(error), <-chan struct{}, error) { panic("private") }}},
		{"panic-release", brokenSource{admit: func() (func(error), <-chan struct{}, error) { return func(error) { panic("private") }, nil, nil }}},
	} {
		t.Run(test.name, func(t *testing.T) {
			clock := newClock()
			s := serviceFor(t, clock, invoker{}, DevOptions{Source: test.source})
			putRule(t, s, "owned")
			putTarget(t, s, "owned", Target{})
			await(t, clock.registered)
			clock.advance(time.Minute)
			require.Eventually(t, func() bool { return s.DevEvidence() != nil }, time.Second, time.Millisecond)
			err := s.Close(context.Background())
			require.Error(t, err)
			assert.NotContains(t, err.Error(), "private")
		})
	}

}
func TestRealCoordinatorRejectsIncompleteDeliveryEvidenceAfterJoinedWork(t *testing.T) {
	for _, interruption := range []string{"invoke-panic", "observe-panic", "observe-goexit"} {
		t.Run(interruption, func(t *testing.T) {
			owner := devquiescence.New()
			source := newRealSource(owner)
			clock := newClock()
			inv := invoker{}
			dev := DevOptions{Source: source}
			switch interruption {
			case "invoke-panic":
				inv.admit = func(context.Context, string, []byte) error { panic("private input") }
			case "observe-panic":
				dev.Observe = func(Outcome) { panic("private output") }
			case "observe-goexit":
				dev.Observe = func(Outcome) { runtime.Goexit() }
			}
			s := serviceFor(t, clock, inv, dev)
			putRule(t, s, "uncertain")
			putTarget(t, s, "uncertain", Target{})
			await(t, clock.registered)
			clock.advance(time.Minute)
			require.NoError(t, await(t, source.attempts).err)
			require.Error(t, await(t, source.completed))
			snapshot, err := owner.Quiesce(t.Context())
			require.ErrorIs(t, err, devquiescence.ErrEvidence)
			assert.False(t, snapshot.FixtureSafe)
			assert.NotEmpty(t, snapshot.EvidenceFailure)
			assert.Zero(t, snapshot.WorkCount)
			require.Error(t, s.Close(context.Background()))
			assert.NotContains(t, s.DevEvidence().Error(), "private")
		})
	}
}
func TestCloseJoinsSourceCompletionAfterNativePublication(t *testing.T) {
	owner := devquiescence.New()
	source := newRealSource(owner)
	clock := newClock()
	entered, release := make(chan struct{}), make(chan struct{})
	source.release = func() { close(entered); <-release }
	s := serviceFor(t, clock, invoker{}, DevOptions{Source: source})
	releaseCallback := cleanupRelease(t, release)
	putRule(t, s, "finalizer")
	putTarget(t, s, "finalizer", Target{})
	await(t, clock.registered)
	clock.advance(time.Minute)
	require.NoError(t, await(t, source.attempts).err)
	require.NoError(t, await(t, source.completed))
	await(t, entered)
	s.mu.Lock()
	assert.Zero(t, s.pending)
	s.mu.Unlock()
	assert.Zero(t, owner.Snapshot().WorkCount)
	hold(t, owner)
	closed := make(chan error, 1)
	go func() { closed <- s.Close(context.Background()) }()
	select {
	case err := <-closed:
		t.Fatalf("Close crossed source release callback: %v", err)
	default:
	}
	releaseCallback()
	require.NoError(t, await(t, closed))
}
