// Opt-in faults select only an existing native invocation attempt. The ordinary
// runtime owner launches, terminates and joins the process; this adapter owns
// selection and its one-shot decision, never a second queue or process registry.
package lambda

import (
	"context"
	"errors"
	"net/http"
	"os/exec"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

type DevWorkerCrashConfig struct {
	Enabled bool `yaml:"enabled"`
}

var errDevWorkerCrash = errors.New("development worker crash selected")

type DevWorkerCrash struct {
	RequestID    string `json:"request_id"`
	FunctionName string `json:"function_name"`
	Attempt      int    `json:"attempt"`
	Armed        bool   `json:"armed"`
	Fired        bool   `json:"fired"`
}

type DevWorkerCrashError struct {
	Status int
	Code   string
}

func (err *DevWorkerCrashError) Error() string       { return err.Code }
func workerCrashError(status int, code string) error { return &DevWorkerCrashError{status, code} }

// This mutex joins selection to the actual launch and native result boundary.
// No code takes Service.mu while holding it. A closed launch can never cancel
// an idle or reassigned warm worker, even while diagnostics still own activity.
type devWorkerCrashTarget struct {
	mu                                        sync.Mutex
	armed, fired, closed, launched, completed bool
	cancel                                    func() bool
	ctx                                       context.Context
}

func (target *devWorkerCrashTarget) start(start func() error, phase *runtimePhase) error {
	if target == nil {
		return start()
	}
	target.mu.Lock()
	defer target.mu.Unlock()
	if target.completed {
		return errPhaseProtocol
	}
	err := start()
	if err != nil {
		target.closed = true
		return err
	}
	target.attachLocked(phase)
	return nil
}
func (target *devWorkerCrashTarget) attach(phase *runtimePhase) {
	if target == nil {
		return
	}
	target.mu.Lock()
	defer target.mu.Unlock()
	if target.closed || target.completed {
		return
	}
	target.attachLocked(phase)
}
func (target *devWorkerCrashTarget) attachLocked(phase *runtimePhase) {
	if target.completed {
		return
	}
	target.closed = false
	target.launched = true
	target.cancel = phase.devCrashIfLive
	target.ctx = phase.ctx
	if target.armed && !target.fired {
		_ = target.fireLocked()
	}
}
func (target *devWorkerCrashTarget) fireLocked() error {
	won := target.cancel()
	target.armed = false
	if !won || !errors.Is(context.Cause(target.ctx), errDevWorkerCrash) {
		target.closed = true
		target.cancel = nil
		return workerCrashError(409, "attempt_not_selectable")
	}
	target.fired = true
	return nil
}
func (target *devWorkerCrashTarget) change(arm bool) (armed, fired bool, err error) {
	target.mu.Lock()
	defer target.mu.Unlock()
	if target.closed || target.completed || target.fired {
		return false, target.fired, workerCrashError(http.StatusConflict, "attempt_not_selectable")
	}
	if !arm {
		target.armed = false
		return false, false, nil
	}
	if target.armed {
		return true, false, workerCrashError(http.StatusConflict, "attempt_already_armed")
	}
	target.armed = true
	if target.cancel != nil {
		err = target.fireLocked()
	}
	return target.armed, target.fired, err
}
func (target *devWorkerCrashTarget) acceptResult() bool {
	if target == nil {
		return true
	}
	target.mu.Lock()
	defer target.mu.Unlock()
	target.closed = true
	target.completed = true
	target.cancel = nil
	return !target.fired
}
func (target *devWorkerCrashTarget) finish() (fired, launched bool) {
	if target == nil {
		return false, false
	}
	target.mu.Lock()
	defer target.mu.Unlock()
	target.closed = true
	target.cancel = nil
	return target.fired, target.launched
}
func (input invocation) startRuntime(command *exec.Cmd) error {
	if input.workerCrash == nil {
		return command.Start()
	}
	return input.workerCrash.start(command.Start, input.phase)
}
func (service *Service) newWorkerCrashLocked(input invocation) *devWorkerCrashTarget {
	if !service.workerCrashEnabled {
		return nil
	}
	target := &devWorkerCrashTarget{}
	if task := service.asyncTasks[input.requestID]; task != nil {
		target.armed = task.workerCrashAttempt == input.attempt
	}
	return target
}
func (service *Service) finishWorkerCrash(input invocation, err error) {
	if input.workerCrash == nil {
		return
	}
	var rejection *InvokeError
	throttled := errors.As(err, &rejection) && rejection.Status == http.StatusTooManyRequests
	// Publish final runtime selection closure and the async task decision in
	// one transaction. Only an unlaunched native throttle leaves that exact
	// existing upcoming task selectable through the retained diagnostics gap.
	service.mu.Lock()
	defer service.mu.Unlock()
	fired, launched := input.workerCrash.finish()
	input.workerCrash.mu.Lock()
	input.workerCrash.completed = true
	input.workerCrash.mu.Unlock()
	if task := service.asyncTasks[input.requestID]; task != nil && !throttled {
		task.workerCrashDecidedAttempt = input.attempt
		if fired || launched || task.workerCrashAttempt == input.attempt {
			task.workerCrashAttempt = 0
		}
	}
}

func (service *Service) DevArmWorkerCrash(ctx context.Context, function, requestID string, attempt int) (DevWorkerCrash, error) {
	return service.changeWorkerCrash(ctx, function, requestID, attempt, true)
}
func (service *Service) DevClearWorkerCrash(ctx context.Context, function, requestID string, attempt int) (DevWorkerCrash, error) {
	return service.changeWorkerCrash(ctx, function, requestID, attempt, false)
}
func (service *Service) changeWorkerCrash(ctx context.Context, function, requestID string, attempt int, arm bool) (DevWorkerCrash, error) {
	if service == nil || !service.workerCrashEnabled {
		return DevWorkerCrash{}, workerCrashError(404, "worker_crash_disabled")
	}
	if err := ctx.Err(); err != nil {
		return DevWorkerCrash{}, err
	}
	if function == "" || attempt < 1 || attempt > 3 {
		return DevWorkerCrash{}, workerCrashError(400, "invalid_worker_crash_target")
	}
	if _, err := uuid.Parse(requestID); err != nil {
		return DevWorkerCrash{}, workerCrashError(400, "invalid_native_request_id")
	}
	_, name, err := service.resolveTarget(function, "")
	if err != nil {
		return DevWorkerCrash{}, workerCrashError(404, "function_not_found")
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return DevWorkerCrash{}, err
	}
	if service.closed || service.aborted {
		return DevWorkerCrash{}, workerCrashError(503, "worker_crash_unavailable")
	}
	result := DevWorkerCrash{RequestID: requestID, FunctionName: name, Attempt: attempt}
	for _, owner := range service.active {
		if owner.metadata.RequestID != requestID {
			continue
		}
		if owner.metadata.FunctionName != name {
			return result, workerCrashError(404, "native_invocation_not_found")
		}
		if owner.metadata.Attempt != attempt || owner.workerCrash == nil {
			return result, workerCrashError(409, "attempt_not_selectable")
		}
		owner.workerCrash.mu.Lock()
		deferred := owner.workerCrash.completed && !owner.workerCrash.launched
		owner.workerCrash.mu.Unlock()
		if task := service.asyncTasks[requestID]; deferred && task != nil && task.workerCrashDecidedAttempt < attempt {
			break // The native unlaunched throttle still owns its upcoming task.
		}
		result.Armed, result.Fired, err = owner.workerCrash.change(arm)
		if err == nil {
			if task := service.asyncTasks[requestID]; task != nil {
				if result.Armed {
					task.workerCrashAttempt = attempt
				} else {
					task.workerCrashAttempt = 0
				}
			}
		}
		return result, err
	}
	task := service.asyncTasks[requestID]
	if task == nil || task.record.FunctionName != name {
		return result, workerCrashError(404, "native_invocation_not_found")
	}
	expected := task.record.Attempts + 1
	if task.record.State == "running" {
		expected = task.record.Attempts
	}
	if attempt != expected || task.workerCrashDecidedAttempt >= attempt {
		return result, workerCrashError(409, "attempt_not_selectable")
	}
	if !arm {
		if task.workerCrashAttempt != 0 && task.workerCrashAttempt != attempt {
			return result, workerCrashError(409, "different_attempt_armed")
		}
		task.workerCrashAttempt = 0
		return result, nil
	}
	if task.workerCrashAttempt != 0 {
		return result, workerCrashError(409, "attempt_already_armed")
	}
	task.workerCrashAttempt = attempt
	result.Armed = true
	return result, nil
}

type DevWorkerInvocation struct {
	RequestID    string `json:"request_id"`
	FunctionName string `json:"function_name"`
	FunctionARN  string `json:"function_arn"`
	Attempt      int    `json:"attempt"`
	State        string `json:"state"`
}
type DevWorkerCrashSnapshot struct {
	Invocations []DevWorkerInvocation `json:"invocations"`
	Events      []AsyncRecord         `json:"events"`
}

func (service *Service) DevWorkerInvocations() DevWorkerCrashSnapshot {
	result := DevWorkerCrashSnapshot{Invocations: []DevWorkerInvocation{}, Events: []AsyncRecord{}}
	if service == nil || !service.workerCrashEnabled {
		return result
	}
	service.mu.Lock()
	for _, owner := range service.active {
		target := owner.workerCrash
		if target == nil {
			continue
		}
		target.mu.Lock()
		if !target.closed {
			state := "pending"
			if target.launched {
				state = "running"
			}
			if target.fired {
				state = "crashing"
			}
			result.Invocations = append(result.Invocations, DevWorkerInvocation{owner.metadata.RequestID, owner.metadata.FunctionName, owner.metadata.FunctionARN, owner.metadata.Attempt, state})
		}
		target.mu.Unlock()
	}
	service.mu.Unlock()
	sort.Slice(result.Invocations, func(i, j int) bool { return result.Invocations[i].RequestID < result.Invocations[j].RequestID })
	result.Events = service.AsyncSnapshot()
	return result
}

// devCrashIfLive delegates deadline arbitration to the real phase owner. A
// delayed timer cannot let a fault replace an already exhausted native budget.
func (phase *runtimePhase) devCrashIfLive() bool {
	phase.mu.Lock()
	defer phase.mu.Unlock()
	if phase.finished {
		return false
	}
	if !time.Now().Before(phase.deadline) && phase.ctx.Err() == nil {
		phase.cancel(phase.deadlineCause)
	}
	if phase.ctx.Err() == nil {
		phase.cancel(errDevWorkerCrash)
	}
	return errors.Is(context.Cause(phase.ctx), errDevWorkerCrash)
}
