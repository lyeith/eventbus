package lambda

import (
	"context"
	"errors"
	"sync"
	"time"
)

// On-demand initial Init has its own budget. A timed-out Init may be retried
// once with the configured timeout shared by initialization and invocation.
const initialInitTimeout = 10 * time.Second

var errInitializationBudget = errors.New("Lambda initial initialization budget expired")
var errPhaseProtocol = errors.New("Lambda managed readiness protocol failed")
var errFunctionBudget = errors.New("Lambda configured function budget expired")
var errServiceCancellation = errors.New("Lambda service stopped execution")

// runtimePhase owns deadlines for one actual process launch. Adapters report
// readiness; they never renew budgets or classify cancellation themselves.
// Its context stays stable while the initial Init timer becomes an Invoke timer.
type runtimePhase struct {
	mu             sync.Mutex
	ctx            context.Context
	cancel         context.CancelCauseFunc
	timer          *time.Timer
	generation     uint64
	deadline       time.Time
	deadlineCause  error
	started, ready time.Time
	timeout        time.Duration
	mode           string
	finished       bool
	onReady        func(time.Time)
}

// Immutable execution facts. Private JSON field names and capture projection
// belong to the diagnostics adapter, never to runtime deadline policy.
type runtimePhaseRecord struct {
	InitAttempt int
	Mode        string
	InitMS      float64
	InvokeMS    float64
	InitState   string
	InvokeState string
}

func newRuntimePhase(parent context.Context, timeout, initTimeout time.Duration, mode string, onReady func(time.Time)) *runtimePhase {
	ctx, cancel := context.WithCancelCause(parent)
	phase := &runtimePhase{ctx: ctx, cancel: cancel, started: time.Now(), timeout: timeout, mode: mode, onReady: onReady}
	budget, cause := timeout, errFunctionBudget
	if mode == "initial" {
		budget, cause = initTimeout, errInitializationBudget
	}
	if mode == "command" {
		phase.ready = phase.started
	}
	phase.armLocked(budget, cause)
	return phase
}

func (phase *runtimePhase) armLocked(budget time.Duration, cause error) {
	if phase.timer != nil {
		phase.timer.Stop()
	}
	phase.generation++
	generation := phase.generation
	phase.deadline = time.Now().Add(budget)
	phase.deadlineCause = cause
	if deadline, ok := phase.ctx.Deadline(); ok && deadline.Before(phase.deadline) {
		phase.deadline = deadline
		phase.deadlineCause = context.DeadlineExceeded
	}
	deadlineCause := phase.deadlineCause
	phase.timer = time.AfterFunc(max(0, time.Until(phase.deadline)), func() {
		phase.mu.Lock()
		defer phase.mu.Unlock()
		if !phase.finished && phase.generation == generation {
			phase.cancel(deadlineCause)
		}
	})
}

func (phase *runtimePhase) beginInvoke() (time.Time, error) {
	phase.mu.Lock()
	defer phase.mu.Unlock()
	if phase.finished {
		return time.Time{}, errClosed
	}
	if err := phase.ctx.Err(); err != nil {
		return time.Time{}, err
	}
	// Timer delivery can lag its expiration. Never admit a handler after Init's
	// hard limit, nor renew a fallback which has already exhausted its budget.
	if !time.Now().Before(phase.deadline) {
		phase.cancel(phase.deadlineCause)
		return time.Time{}, phase.ctx.Err()
	}
	if !phase.ready.IsZero() {
		return time.Time{}, errPhaseProtocol
	}
	phase.ready = time.Now()
	if phase.mode == "initial" {
		phase.armLocked(phase.timeout, errFunctionBudget)
	}
	if phase.onReady != nil {
		phase.onReady(phase.deadline)
	}
	return phase.deadline, nil
}

func (phase *runtimePhase) invoked() bool {
	phase.mu.Lock()
	defer phase.mu.Unlock()
	return !phase.ready.IsZero()
}

// complete freezes phase durations after actual process/listener/result joins,
// before optional diagnostics joining. stop also handles interrupted runners.
func (phase *runtimePhase) complete(attempt int, result invocationResult) (runtimePhaseRecord, diagnosticCompletion) {
	phase.mu.Lock()
	defer phase.mu.Unlock()
	at := time.Now()
	// Timer delivery may be delayed by scheduling. Enforce the actual deadline
	// here, preserving any already-won caller/service cancellation cause.
	if !at.Before(phase.deadline) && phase.ctx.Err() == nil {
		phase.cancel(phase.deadlineCause)
	}
	completion := snapshotDiagnosticCompletion(phase.ctx, true)
	phase.finished = true
	phase.timer.Stop()
	record := runtimePhaseRecord{InitAttempt: attempt, Mode: phase.mode}
	state := "succeeded"
	if result.functionError {
		state = "failed"
	}
	if result.state == InvocationNotStarted {
		state = "not_started"
	}
	if completion.contextError == "deadline_exceeded" {
		state = "timed_out"
	} else if completion.contextError != "" {
		state = "canceled"
	}
	if completion.cause == "runtime_protocol_error" {
		state = "failed"
	}
	if phase.mode == "command" {
		record.InvokeMS, record.InvokeState = float64(at.Sub(phase.started))/float64(time.Millisecond), state
	} else if phase.ready.IsZero() {
		record.InitMS, record.InitState = float64(at.Sub(phase.started))/float64(time.Millisecond), state
	} else {
		record.InitMS, record.InitState = float64(phase.ready.Sub(phase.started))/float64(time.Millisecond), "succeeded"
		record.InvokeMS, record.InvokeState = float64(at.Sub(phase.ready))/float64(time.Millisecond), state
	}
	return record, completion
}

func (phase *runtimePhase) stop() {
	if phase == nil {
		return
	}
	phase.mu.Lock()
	phase.finished = true
	phase.timer.Stop()
	phase.cancel(nil)
	phase.mu.Unlock()
}
