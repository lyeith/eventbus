package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/devquiescence"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDevDiscardPendingAdmissionsCancelsOwnedScheduleRetry(t *testing.T) {
	owner := devquiescence.New()
	source := newSchedulerSourceProbe(owner)
	rejected := make(chan struct{}, 1)
	observed := make(chan Outcome, 1)
	releaseObservation := make(chan struct{})
	var releaseOnce sync.Once
	var calls atomic.Int32
	service := testService(t, testInvoker{admit: func(context.Context, string, []byte) error {
		calls.Add(1)
		rejected <- struct{}{}
		return testAdmissionError{retry: true}
	}}, DevOptions{Source: source, ExactSeconds: true, RetryDelay: time.Hour, Observe: func(outcome Outcome) {
		observed <- outcome
		<-releaseObservation
	}})
	t.Cleanup(service.DevDiscardPendingAdmissions)
	release := func() { releaseOnce.Do(func() { close(releaseObservation) }) }
	t.Cleanup(release)
	input := dueSchedulerInput("discard-retry")
	_, err := service.Create(t.Context(), input)
	require.NoError(t, err)
	require.NoError(t, awaitSchedulerValue(t, source.attempts).err)
	awaitSchedulerValue(t, rejected)
	assert.Equal(t, 1, owner.Snapshot().WorkCount)
	owner.Shutdown()
	service.DevDiscardPendingAdmissions()
	service.DevDiscardPendingAdmissions()
	outcome := awaitOutcome(t, observed)
	assert.Equal(t, "canceled", outcome.Status)
	assert.Empty(t, outcome.Code)
	assert.Equal(t, 1, outcome.Attempts)
	assert.Equal(t, int32(1), calls.Load())
	assert.Equal(t, 1, owner.Snapshot().WorkCount, "observation retains the actual source lease")

	closed := make(chan error, 1)
	go func() { closed <- service.Close(context.Background()) }()
	require.Eventually(t, func() bool {
		service.mu.Lock()
		defer service.mu.Unlock()
		return service.closing
	}, time.Second, time.Millisecond)
	select {
	case err := <-closed:
		t.Fatalf("Close crossed the held outcome callback: %v", err)
	default:
	}
	release()
	require.NoError(t, awaitSchedulerValue(t, source.completed))
	require.NoError(t, awaitSchedulerValue(t, closed))
	snapshot, err := owner.Quiesce(t.Context())
	require.NoError(t, err)
	assert.Equal(t, devquiescence.Shutdown, snapshot.State)
	assert.Zero(t, snapshot.WorkCount)
	assert.False(t, snapshot.FixtureSafe)
	readback, err := service.Get("", input.Name)
	require.NoError(t, err)
	assert.Equal(t, "ENABLED", readback.State, "the dev policy must not rewrite native schedule state")
}

func TestDevDiscardPendingAdmissionsPreservesAcceptedScheduleTransferAndJoinsCallback(t *testing.T) {
	owner := devquiescence.New()
	source := newSchedulerSourceProbe(owner)
	entered := make(chan context.Context, 1)
	child := make(chan func(error), 1)
	releaseCallback := make(chan struct{})
	observed := make(chan Outcome, 1)
	var releaseOnce sync.Once
	service := testService(t, testInvoker{admit: func(ctx context.Context, _ string, _ []byte) error {
		complete, err := owner.BeginActivity("lambda_async", "accepted-schedule")
		if err != nil {
			return err
		}
		child <- complete
		entered <- ctx
		<-releaseCallback
		return nil
	}}, DevOptions{Source: source, ExactSeconds: true, Observe: func(outcome Outcome) { observed <- outcome }})
	t.Cleanup(service.DevDiscardPendingAdmissions)
	release := func() { releaseOnce.Do(func() { close(releaseCallback) }) }
	t.Cleanup(release)
	_, err := service.Create(t.Context(), dueSchedulerInput("accepted-discard"))
	require.NoError(t, err)
	require.NoError(t, awaitSchedulerValue(t, source.attempts).err)
	invocationCtx := awaitSchedulerValue(t, entered)
	completeChild := awaitSchedulerValue(t, child)
	t.Cleanup(func() { completeChild(nil) })
	assert.Equal(t, 2, owner.Snapshot().WorkCount)
	owner.Shutdown()
	service.DevDiscardPendingAdmissions()
	awaitSchedulerValue(t, invocationCtx.Done())
	closed := make(chan error, 1)
	go func() { closed <- service.Close(context.Background()) }()
	require.Eventually(t, func() bool {
		service.mu.Lock()
		defer service.mu.Unlock()
		return service.closing
	}, time.Second, time.Millisecond)
	select {
	case err := <-closed:
		t.Fatalf("Close crossed a real accepted admission callback: %v", err)
	default:
	}
	release()
	outcome := awaitOutcome(t, observed)
	assert.Equal(t, "accepted", outcome.Status)
	assert.Empty(t, outcome.Code)
	assert.Equal(t, 1, outcome.Attempts)
	require.NoError(t, awaitSchedulerValue(t, source.completed))
	require.NoError(t, awaitSchedulerValue(t, closed))
	assert.Equal(t, 1, owner.Snapshot().WorkCount, "Lambda retains accepted event ownership")
	completeChild(nil)
	snapshot, err := owner.Quiesce(t.Context())
	require.NoError(t, err)
	assert.Zero(t, snapshot.WorkCount)
}

func TestDevDiscardPendingAdmissionsStopsUnclaimedSchedules(t *testing.T) {
	for _, due := range []bool{false, true} {
		name := "future"
		if due {
			name = "fenced-due"
		}
		t.Run(name, func(t *testing.T) {
			owner := devquiescence.New()
			holdSchedulerSources(t, owner)
			source := newSchedulerSourceProbe(owner)
			observed := make(chan Outcome, 1)
			var calls atomic.Int32
			service := testService(t, testInvoker{admit: func(context.Context, string, []byte) error {
				calls.Add(1)
				return nil
			}}, DevOptions{Source: source, ExactSeconds: true, Observe: func(outcome Outcome) { observed <- outcome }})
			t.Cleanup(service.DevDiscardPendingAdmissions)
			input := testCreate("discard-" + name)
			input.State = "ENABLED"
			if due {
				input.ScheduleExpression = dueSchedulerInput(input.Name).ScheduleExpression
			}
			_, err := service.Create(t.Context(), input)
			require.NoError(t, err)
			if due {
				require.ErrorIs(t, awaitSchedulerValue(t, source.attempts).err, devquiescence.ErrFenced)
			}
			owner.Shutdown()
			service.DevDiscardPendingAdmissions()
			outcome := awaitOutcome(t, observed)
			assert.Equal(t, "canceled", outcome.Status)
			assert.Zero(t, outcome.Attempts)
			assert.Zero(t, calls.Load())
			require.NoError(t, service.Close(t.Context()))
			assert.Zero(t, owner.Snapshot().WorkCount)
			select {
			case <-source.completed:
				t.Fatal("discard fabricated an unclaimed source lifetime")
			default:
			}
		})
	}
}
