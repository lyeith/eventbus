package lambda

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRuntimePhaseOrdinaryReadyReplacesInitDeadline(t *testing.T) {
	phase := newRuntimePhase(context.Background(), 500*time.Millisecond, 150*time.Millisecond, "initial", nil)
	defer phase.stop()
	deadline, err := phase.beginInvoke()
	if err != nil || time.Until(deadline) < 450*time.Millisecond {
		t.Fatalf("ready deadline: %v %v", deadline, err)
	}
	// The original Init timer must not cancel the newly admitted Invoke.
	select {
	case <-phase.ctx.Done():
		t.Fatalf("retired Init timer stopped Invoke: %v", context.Cause(phase.ctx))
	case <-time.After(200 * time.Millisecond):
	}
}

func TestRuntimePhaseFallbackDoesNotRenewConfiguredBudget(t *testing.T) {
	phase := newRuntimePhase(context.Background(), 400*time.Millisecond, time.Second, "fallback", nil)
	defer phase.stop()
	time.Sleep(150 * time.Millisecond)
	deadline, err := phase.beginInvoke()
	if err != nil || time.Until(deadline) > 275*time.Millisecond || time.Until(deadline) < 100*time.Millisecond {
		t.Fatalf("fallback renewed: %v %v", time.Until(deadline), err)
	}
	<-phase.ctx.Done()
	completion := snapshotDiagnosticCompletion(phase.ctx, true)
	if completion.cause != "function_timeout" || completion.contextError != "deadline_exceeded" {
		t.Fatalf("fallback cause: %#v", completion)
	}
}

func TestRuntimePhaseInitialTimeoutNeverAdmitsHandler(t *testing.T) {
	phase := newRuntimePhase(context.Background(), time.Second, 30*time.Millisecond, "initial", nil)
	defer phase.stop()
	<-phase.ctx.Done()
	if _, err := phase.beginInvoke(); err == nil || phase.invoked() {
		t.Fatal("expired Init admitted handler")
	}
	if !errors.Is(context.Cause(phase.ctx), errInitializationBudget) {
		t.Fatalf("Init cause: %v", context.Cause(phase.ctx))
	}
}

func TestRuntimePhaseCallerDeadlineIsNotRenewedAtReady(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	phase := newRuntimePhase(parent, time.Second, time.Second, "initial", nil)
	defer phase.stop()
	deadline, err := phase.beginInvoke()
	callerDeadline, _ := parent.Deadline()
	if err != nil || !deadline.Equal(callerDeadline) {
		t.Fatalf("caller deadline changed: %v %v", deadline, err)
	}
	<-phase.ctx.Done()
	completion := snapshotDiagnosticCompletion(phase.ctx, true)
	if completion.cause != "caller_deadline" {
		t.Fatalf("caller became own timeout: %#v", completion)
	}
}

func TestRuntimePhaseNativeCompletionStopsBudgetBeforeEvidenceJoin(t *testing.T) {
	phase := newRuntimePhase(context.Background(), 30*time.Millisecond, time.Second, "command", nil)
	defer phase.stop()
	phase.complete(1, invocationResult{})
	select {
	case <-phase.ctx.Done():
		t.Fatalf("completed native budget still armed: %v", context.Cause(phase.ctx))
	case <-time.After(60 * time.Millisecond):
	}
}

// A delayed parent timer must not let the later owned budget change the cause.
type delayedDeadlineContext struct {
	context.Context
	deadline time.Time
}

func (ctx delayedDeadlineContext) Deadline() (time.Time, bool) { return ctx.deadline, true }

func TestRuntimePhaseOwnTimerPreservesEarlierCallerDeadlineCause(t *testing.T) {
	parent := delayedDeadlineContext{Context: context.Background(), deadline: time.Now().Add(30 * time.Millisecond)}
	phase := newRuntimePhase(parent, time.Hour, time.Hour, "initial", nil)
	defer phase.stop()
	select {
	case <-phase.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("parent deadline cap not enforced")
	}
	completion := snapshotDiagnosticCompletion(phase.ctx, true)
	if completion.cause != "caller_deadline" || completion.contextError != "deadline_exceeded" {
		t.Fatalf("parent cause changed: %#v", completion)
	}
}

func TestRuntimePhaseCompletionEnforcesDeadlineBeforeTimerDelivery(t *testing.T) {
	for _, mode := range []string{"initial", "fallback", "command"} {
		t.Run(mode, func(t *testing.T) {
			phase := newRuntimePhase(context.Background(), time.Hour, time.Hour, mode, nil)
			defer phase.stop()
			phase.mu.Lock()
			phase.timer.Stop()
			phase.deadline = time.Now().Add(-time.Millisecond)
			phase.mu.Unlock()
			record, completion := phase.complete(1, invocationResult{})
			cause := "function_timeout"
			if mode == "initial" {
				cause = "initialization_timeout"
			}
			if completion.cause != cause || completion.contextError != "deadline_exceeded" || (record.InitState != "timed_out" && record.InvokeState != "timed_out") {
				t.Fatalf("late timer bypassed budget: %#v %#v", record, completion)
			}
		})
	}
}
