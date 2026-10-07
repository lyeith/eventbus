package scheduler

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/devactivity"
	"github.com/lyeith/eventbus/internal/devquiescence"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type schedulerSourceAttempt struct {
	id  string
	err error
}

// The probe preserves the real coordinator's atomic refusal/wake contract and
// supplies deterministic receipts for source admission and final completion.
type schedulerSourceProbe struct {
	source    devactivity.Source
	attempts  chan schedulerSourceAttempt
	completed chan error
	refused   func()
}

func newSchedulerSourceProbe(source devactivity.Source) *schedulerSourceProbe {
	return &schedulerSourceProbe{source: source, attempts: make(chan schedulerSourceAttempt, 32), completed: make(chan error, 8)}
}

func (p *schedulerSourceProbe) BeginSource(kind, id string) (func(error), <-chan struct{}, error) {
	complete, changed, err := p.source.BeginSource(kind, id)
	p.attempts <- schedulerSourceAttempt{id: id, err: err}
	if err != nil && p.refused != nil {
		p.refused()
	}
	if complete == nil {
		return nil, changed, err
	}
	return func(evidenceErr error) {
		complete(evidenceErr)
		p.completed <- evidenceErr
	}, changed, err
}

func awaitSchedulerValue[T any](t *testing.T, values <-chan T) T {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("Scheduler source ownership did not reach the expected step")
		var zero T
		return zero
	}
}

func dueSchedulerInput(name string) CreateInput {
	input := testCreate(name)
	input.State = "ENABLED"
	input.ScheduleExpression = "at(" + testNow.Format("2006-01-02T15:04:05") + ")"
	return input
}

func holdSchedulerSources(t *testing.T, owner *devquiescence.Coordinator) devquiescence.Snapshot {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	snapshot, err := owner.Quiesce(ctx)
	require.NoError(t, err)
	require.True(t, snapshot.FixtureSafe)
	return snapshot
}

func TestSchedulerSourceFencePreservesDueScheduleAndJoinsObservation(t *testing.T) {
	owner := devquiescence.New()
	held := holdSchedulerSources(t, owner)
	source := newSchedulerSourceProbe(owner)
	payloads := make(chan []byte, 1)
	observed := make(chan Outcome, 1)
	releaseObservation := make(chan struct{})
	service := testService(t, testInvoker{admit: func(ctx context.Context, arn string, payload []byte) error {
		payloads <- append([]byte(nil), payload...)
		return nil
	}}, DevOptions{Source: source, ExactSeconds: true, Observe: func(outcome Outcome) {
		observed <- outcome
		<-releaseObservation
	}})
	t.Cleanup(func() {
		select {
		case <-releaseObservation:
		default:
			close(releaseObservation)
		}
	})
	input := dueSchedulerInput("held-due")
	_, err := service.Create(t.Context(), input)
	require.NoError(t, err)
	attempt := awaitSchedulerValue(t, source.attempts)
	require.Equal(t, input.Name, attempt.id)
	require.ErrorIs(t, attempt.err, devquiescence.ErrFenced)
	before, err := service.Get("", input.Name)
	require.NoError(t, err)
	service.mu.Lock()
	item := service.entries["default/"+input.Name]
	due, scheduledAt := item.due, item.scheduledAt
	service.mu.Unlock()

	assert.Equal(t, 0, owner.Snapshot().WorkCount)
	select {
	case <-item.done:
		t.Fatal("fencing completed an unclaimed schedule")
	case <-observed:
		t.Fatal("fencing produced a terminal outcome")
	case <-payloads:
		t.Fatal("fencing dispatched an unclaimed schedule")
	default:
	}
	after, err := service.Get("", input.Name)
	require.NoError(t, err)
	assert.Equal(t, before, after)
	service.mu.Lock()
	assert.Same(t, item, service.entries["default/"+input.Name])
	assert.Equal(t, due, item.due)
	assert.Equal(t, scheduledAt, item.scheduledAt)
	service.mu.Unlock()

	_, err = owner.Resume(held.Generation)
	require.NoError(t, err)
	require.NoError(t, awaitSchedulerValue(t, source.attempts).err)
	assert.Equal(t, []byte(*input.Target.Input), awaitSchedulerValue(t, payloads))
	outcome := awaitSchedulerValue(t, observed)
	assert.Equal(t, "accepted", outcome.Status)
	assert.Equal(t, 1, outcome.Attempts)
	assert.Equal(t, 1, owner.Snapshot().WorkCount, "observation remains part of the accepted source lifetime")
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	snapshot, err := owner.Quiesce(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.False(t, snapshot.FixtureSafe)
	assert.Equal(t, 1, snapshot.WorkCount)

	close(releaseObservation)
	require.NoError(t, awaitSchedulerValue(t, source.completed))
	select {
	case <-item.done:
	default:
		t.Fatal("source ownership released before schedule completion was published")
	}
	holdSchedulerSources(t, owner)
	after, err = service.Get("", input.Name)
	require.NoError(t, err)
	assert.Equal(t, before, after, "resume preserves native configuration and timestamps")
}

func TestSchedulerSourceResumeBetweenRefusalAndWaitCannotLoseWake(t *testing.T) {
	owner := devquiescence.New()
	held := holdSchedulerSources(t, owner)
	source := newSchedulerSourceProbe(owner)
	resumed := make(chan error, 1)
	source.refused = func() {
		_, err := owner.Resume(held.Generation)
		resumed <- err
	}
	outcomes := make(chan Outcome, 1)
	service := testService(t, testInvoker{}, DevOptions{Source: source, ExactSeconds: true, Observe: func(outcome Outcome) { outcomes <- outcome }})
	input := dueSchedulerInput("resume-race")
	_, err := service.Create(t.Context(), input)
	require.NoError(t, err)
	require.NoError(t, awaitSchedulerValue(t, resumed))
	require.ErrorIs(t, awaitSchedulerValue(t, source.attempts).err, devquiescence.ErrFenced)
	require.NoError(t, awaitSchedulerValue(t, source.attempts).err)
	outcome := awaitOutcome(t, outcomes)
	assert.Equal(t, "accepted", outcome.Status)
	assert.Equal(t, 1, outcome.Attempts)
	require.NoError(t, awaitSchedulerValue(t, source.completed))
}

func TestSchedulerAcceptedRetryKeepsLeaseAndTransfersTargetOwnershipWhileFenced(t *testing.T) {
	owner := devquiescence.New()
	source := newSchedulerSourceProbe(owner)
	var calls atomic.Int32
	firstRejected := make(chan struct{})
	descendant := make(chan func(error), 1)
	outcomes := make(chan Outcome, 1)
	service := testService(t, testInvoker{admit: func(ctx context.Context, arn string, payload []byte) error {
		if calls.Add(1) == 1 {
			close(firstRejected)
			return testAdmissionError{retry: true}
		}
		complete, err := owner.BeginActivity("lambda.accepted", "retry-target")
		if err != nil {
			return err
		}
		descendant <- complete
		return nil
	}}, DevOptions{Source: source, ExactSeconds: true, RetryDelay: 200 * time.Millisecond, Observe: func(outcome Outcome) { outcomes <- outcome }})
	input := dueSchedulerInput("retained-retry")
	_, err := service.Create(t.Context(), input)
	require.NoError(t, err)
	require.NoError(t, awaitSchedulerValue(t, source.attempts).err)
	awaitSchedulerValue(t, firstRejected)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	snapshot, err := owner.Quiesce(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, devquiescence.Draining, snapshot.State)
	assert.Equal(t, 1, snapshot.WorkCount)
	assert.Equal(t, int32(1), calls.Load(), "the barrier timed out in the accepted retry interval")

	childComplete := awaitSchedulerValue(t, descendant)
	outcome := awaitOutcome(t, outcomes)
	assert.Equal(t, "accepted", outcome.Status)
	assert.Equal(t, 2, outcome.Attempts, "fencing does not cancel or re-admit accepted retries")
	require.NoError(t, awaitSchedulerValue(t, source.completed))
	snapshot = owner.Snapshot()
	assert.Equal(t, devquiescence.Draining, snapshot.State)
	assert.Equal(t, 1, snapshot.WorkCount, "target ownership transfers before the Scheduler lease completes")
	select {
	case attempt := <-source.attempts:
		t.Fatalf("an accepted retry acquired another source lease: %+v", attempt)
	default:
	}

	childComplete(nil)
	held := holdSchedulerSources(t, owner)
	assert.Equal(t, 0, held.WorkCount)
	readback, err := service.Get("", input.Name)
	require.NoError(t, err)
	assert.Equal(t, "ENABLED", readback.State)
}

func TestSchedulerUnclaimedAgeAndDeletionWaitForResume(t *testing.T) {
	owner := devquiescence.New()
	held := holdSchedulerSources(t, owner)
	source := newSchedulerSourceProbe(owner)
	var now atomic.Int64
	now.Store(testNow.UnixNano())
	var calls atomic.Int32
	outcomes := make(chan Outcome, 1)
	service := testService(t, testInvoker{admit: func(context.Context, string, []byte) error {
		calls.Add(1)
		return nil
	}}, DevOptions{Source: source, ExactSeconds: true, Clock: func() time.Time { return time.Unix(0, now.Load()) }, Observe: func(outcome Outcome) { outcomes <- outcome }})
	input := dueSchedulerInput("held-expiry")
	age := 60
	input.Target.RetryPolicy = &RetryPolicy{MaximumEventAgeInSeconds: &age}
	input.ActionAfterCompletion = "DELETE"
	arn, err := service.Create(t.Context(), input)
	require.NoError(t, err)
	require.ErrorIs(t, awaitSchedulerValue(t, source.attempts).err, devquiescence.ErrFenced)
	before, err := service.Get("", input.Name)
	require.NoError(t, err)
	now.Store(testNow.Add(61 * time.Second).UnixNano())
	retriedARN, err := service.Create(t.Context(), input)
	require.NoError(t, err)
	assert.Equal(t, arn, retriedARN, "the accepted idempotency token survives held time")
	after, err := service.Get("", input.Name)
	require.NoError(t, err)
	assert.Equal(t, before, after, "held time does not consume native schedule state")
	assert.Equal(t, int32(0), calls.Load())
	select {
	case <-outcomes:
		t.Fatal("an unclaimed schedule processed expiry under a held fence")
	default:
	}

	_, err = owner.Resume(held.Generation)
	require.NoError(t, err)
	require.NoError(t, awaitSchedulerValue(t, source.attempts).err)
	outcome := awaitOutcome(t, outcomes)
	assert.Equal(t, "failed", outcome.Status)
	assert.Equal(t, "EventAgeExceeded", outcome.Code)
	assert.Equal(t, 0, outcome.Attempts)
	require.NoError(t, awaitSchedulerValue(t, source.completed))
	assert.Equal(t, int32(0), calls.Load())
	_, err = service.Get("", input.Name)
	requireAPIError(t, err, "ResourceNotFoundException", 404)
	holdSchedulerSources(t, owner)
}

func TestSchedulerFutureDoesNotHoldSourceAndNativeCloseCancelsHeldWaiters(t *testing.T) {
	for _, due := range []bool{false, true} {
		name := "future"
		if due {
			name = "due"
		}
		t.Run(name, func(t *testing.T) {
			owner := devquiescence.New()
			holdSchedulerSources(t, owner)
			source := newSchedulerSourceProbe(owner)
			outcomes := make(chan Outcome, 1)
			var calls atomic.Int32
			service := testService(t, testInvoker{admit: func(context.Context, string, []byte) error {
				calls.Add(1)
				return nil
			}}, DevOptions{Source: source, ExactSeconds: true, Observe: func(outcome Outcome) { outcomes <- outcome }})
			input := testCreate("native-close-" + name)
			input.State = "ENABLED"
			if due {
				input.ScheduleExpression = dueSchedulerInput(input.Name).ScheduleExpression
			}
			_, err := service.Create(t.Context(), input)
			require.NoError(t, err)
			if due {
				require.ErrorIs(t, awaitSchedulerValue(t, source.attempts).err, devquiescence.ErrFenced)
			}
			assert.Equal(t, 0, owner.Snapshot().WorkCount)
			require.NoError(t, service.Close(t.Context()))
			assert.Equal(t, "canceled", awaitOutcome(t, outcomes).Status)
			assert.Equal(t, int32(0), calls.Load())
			select {
			case <-source.completed:
				t.Fatal("native close fabricated an accepted source lifetime")
			default:
			}
			if !due {
				select {
				case <-source.attempts:
					t.Fatal("a future timer claimed source ownership before becoming due")
				default:
				}
			}
			assert.True(t, owner.Snapshot().FixtureSafe)
		})
	}
}

type permanentlyRefusedSchedulerSource struct{ attempted chan struct{} }

func (source permanentlyRefusedSchedulerSource) BeginSource(string, string) (func(error), <-chan struct{}, error) {
	close(source.attempted)
	return nil, nil, errors.New("fixture permanent refusal")
}

func TestSchedulerNativeDeleteJoinsPermanentSourceRefusal(t *testing.T) {
	source := permanentlyRefusedSchedulerSource{attempted: make(chan struct{})}
	outcomes := make(chan Outcome, 1)
	service := testService(t, testInvoker{}, DevOptions{Source: source, ExactSeconds: true, Observe: func(outcome Outcome) { outcomes <- outcome }})
	input := dueSchedulerInput("permanent-refusal")
	_, err := service.Create(t.Context(), input)
	require.NoError(t, err)
	awaitSchedulerValue(t, source.attempted)
	require.NoError(t, service.Delete(t.Context(), "", input.Name, "delete-permanent"))
	assert.Equal(t, "canceled", awaitOutcome(t, outcomes).Status)
	_, err = service.Get("", input.Name)
	requireAPIError(t, err, "ResourceNotFoundException", 404)
}

func TestSchedulerCloseTimeoutRetainsAcceptedTargetUntilJoined(t *testing.T) {
	owner := devquiescence.New()
	source := newSchedulerSourceProbe(owner)
	targetStarted, targetCanceled, targetJoined := make(chan struct{}), make(chan struct{}), make(chan struct{})
	outcomes := make(chan Outcome, 1)
	service := testServiceWithCloseError(t, testInvoker{admit: func(ctx context.Context, arn string, payload []byte) error {
		close(targetStarted)
		<-ctx.Done()
		close(targetCanceled)
		<-targetJoined
		return ctx.Err()
	}}, DevOptions{Source: source, ExactSeconds: true, Observe: func(outcome Outcome) { outcomes <- outcome }}, context.DeadlineExceeded)
	t.Cleanup(func() {
		select {
		case <-targetJoined:
		default:
			close(targetJoined)
		}
	})
	_, err := service.Create(t.Context(), dueSchedulerInput("close-accepted"))
	require.NoError(t, err)
	awaitSchedulerValue(t, targetStarted)
	closed := make(chan error, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	go func() { closed <- service.Close(ctx) }()
	awaitSchedulerValue(t, targetCanceled)
	barrierCtx, barrierCancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer barrierCancel()
	snapshot, err := owner.Quiesce(barrierCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, 1, snapshot.WorkCount)
	select {
	case <-closed:
		t.Fatal("native close returned before the admitted target callback joined")
	case <-source.completed:
		t.Fatal("source ownership released before the admitted target callback joined")
	default:
	}
	close(targetJoined)
	require.ErrorIs(t, awaitSchedulerValue(t, closed), context.DeadlineExceeded)
	assert.Equal(t, "canceled", awaitOutcome(t, outcomes).Status)
	require.NoError(t, awaitSchedulerValue(t, source.completed))
	assert.True(t, holdSchedulerSources(t, owner).FixtureSafe)
}
