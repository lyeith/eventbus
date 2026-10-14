package lambda

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func workerCrashTestOwner(state string, attempts int) (*Service, *asyncTask) {
	name, id := "processor:live", uuid.NewString()
	task := &asyncTask{record: AsyncRecord{RequestID: id, FunctionName: name, State: state, Attempts: attempts}}
	service := &Service{workerCrashEnabled: true, functions: map[string]executableFunction{name: {name: name}, "other": {name: "other"}}, asyncTasks: map[string]*asyncTask{id: task}, active: map[uint64]invocationOwner{}}
	return service, task
}
func requireWorkerCrashError(t *testing.T, err error, status int) {
	t.Helper()
	var control *DevWorkerCrashError
	if !errors.As(err, &control) || control.Status != status {
		t.Fatalf("expected control%d, got%v", status, err)
	}
}

func TestWorkerCrashQueuedSelectionAndTaskOwnerGaps(t *testing.T) {
	service, task := workerCrashTestOwner("queued", 0)
	id, name := task.record.RequestID, task.record.FunctionName
	selected, err := service.DevArmWorkerCrash(context.Background(), name, id, 1)
	if err != nil || !selected.Armed || selected.Fired {
		t.Fatalf("queued selection:%+v %v", selected, err)
	}
	_, err = service.DevArmWorkerCrash(context.Background(), name, id, 1)
	requireWorkerCrashError(t, err, 409)
	_, err = service.DevArmWorkerCrash(context.Background(), "other", id, 1)
	requireWorkerCrashError(t, err, 404)
	_, err = service.DevArmWorkerCrash(context.Background(), name, id, 2)
	requireWorkerCrashError(t, err, 409)
	// Native queue publication precedes active runtime registration.
	task.record.State, task.record.Attempts = "running", 1
	input := invocation{requestID: id, name: name, attempt: 1}
	target := service.newWorkerCrashLocked(input)
	input.workerCrash = target
	service.active[1] = invocationOwner{metadata: InvocationMetadata{RequestID: id, FunctionName: name, Attempt: 1}, workerCrash: target}
	if !target.armed {
		t.Fatal("existing queued arm lost during owner creation")
	}
	_, err = service.DevClearWorkerCrash(context.Background(), name, id, 1)
	if err != nil || target.armed || task.workerCrashAttempt != 0 {
		t.Fatalf("clear did not join task/owner:%v", err)
	}
	service.finishWorkerCrash(input, invocationError(429, "TooManyRequestsException", "native saturation"))
	delete(service.active, 1)
	task.record.State, task.record.Attempts = "retrying", 0
	target = service.newWorkerCrashLocked(input)
	if target.armed {
		t.Fatal("cleared arm resurrected after nonlaunch throttle")
	}
	// A new prelaunch active arm survives that same native throttle boundary.
	service.active[2] = invocationOwner{metadata: InvocationMetadata{RequestID: id, FunctionName: name, Attempt: 1}, workerCrash: target}
	input.workerCrash = target
	_, err = service.DevArmWorkerCrash(context.Background(), name, id, 1)
	if err != nil || task.workerCrashAttempt != 1 {
		t.Fatalf("active prelaunch arm not mirrored:%v", err)
	}
	service.finishWorkerCrash(input, invocationError(429, "TooManyRequestsException", "native saturation"))
	// Optional diagnostics still own the old nonlaunch activity. Clear and
	// rearm apply to its exact upcoming native task, not the closed runtime.
	_, err = service.DevClearWorkerCrash(context.Background(), name, id, 1)
	if err != nil || task.workerCrashAttempt != 0 {
		t.Fatalf("held throttle diagnostics refused pending clear:%v", err)
	}
	_, err = service.DevArmWorkerCrash(context.Background(), name, id, 1)
	if err != nil || task.workerCrashAttempt != 1 {
		t.Fatalf("held throttle diagnostics lost upcoming arm:%v", err)
	}
	delete(service.active, 2)
	target = service.newWorkerCrashLocked(input)
	if !target.armed {
		t.Fatal("arm lost after nonlaunch throttle")
	}
	input.workerCrash = target
	// Runtime completion wins before diagnostics and native async settlement.
	target.acceptResult()
	service.finishWorkerCrash(input, nil)
	_, err = service.DevArmWorkerCrash(context.Background(), name, id, 1)
	requireWorkerCrashError(t, err, 409)
	delete(service.asyncTasks, id)
	_, err = service.DevArmWorkerCrash(context.Background(), name, id, 1)
	requireWorkerCrashError(t, err, 404)
}

func TestWorkerCrashStartClearAndConcurrentOneShot(t *testing.T) {
	target := &devWorkerCrashTarget{}
	phase := newRuntimePhase(context.Background(), time.Second, time.Second, "command", nil)
	defer phase.stop()
	if armed, _, err := target.change(true); err != nil || !armed {
		t.Fatal(err)
	}
	if _, _, err := target.change(false); err != nil {
		t.Fatal(err)
	}
	started := false
	if err := target.start(func() error { started = true; return nil }, phase); err != nil {
		t.Fatal(err)
	}
	if !started || phase.ctx.Err() != nil {
		t.Fatal("clear before launch left ongoing fault")
	}
	var wg sync.WaitGroup
	results := make(chan error, 16)
	for index := 0; index < cap(results); index++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _, err := target.change(true); results <- err }()
	}
	wg.Wait()
	success := 0
	for index := 0; index < cap(results); index++ {
		err := <-results
		if err == nil {
			success++
		} else {
			requireWorkerCrashError(t, err, 409)
		}
	}
	if success != 1 || !errors.Is(context.Cause(phase.ctx), errDevWorkerCrash) {
		t.Fatalf("one-shot winners:%d cause:%v", success, context.Cause(phase.ctx))
	}
	if target.acceptResult() {
		t.Fatal("runtime accepted success after fault won")
	}
	_, _, err := target.change(false)
	requireWorkerCrashError(t, err, 409)
	completedPeer := newRuntimePhase(context.Background(), time.Second, time.Second, "warm", nil)
	defer completedPeer.stop()
	target.attach(completedPeer)
	if !target.closed || !target.completed {
		t.Fatal("completed target reopened by warm attachment")
	}
}

func TestWorkerCrashHardDeadlineAndFirstCauseWin(t *testing.T) {
	for _, cause := range []error{errFunctionBudget, errInitializationBudget, errServiceCancellation, context.Canceled} {
		phase := newRuntimePhase(context.Background(), time.Second, time.Second, "command", nil)
		target := &devWorkerCrashTarget{}
		target.attach(phase)
		if cause == errFunctionBudget || cause == errInitializationBudget {
			phase.mu.Lock()
			phase.timer.Stop()
			phase.deadline = time.Now().Add(-time.Second)
			phase.deadlineCause = cause
			phase.mu.Unlock()
		} else {
			phase.cancel(cause)
		}
		_, fired, err := target.change(true)
		requireWorkerCrashError(t, err, 409)
		if fired || !errors.Is(context.Cause(phase.ctx), cause) {
			t.Fatalf("fault replaced native cause:%v", context.Cause(phase.ctx))
		}
		phase.stop()
	}
	// A real internal Init fallback can attach a new launch after the old
	// launch closes, while a successfully accepted native result cannot.
	target := &devWorkerCrashTarget{}
	first := newRuntimePhase(context.Background(), time.Second, time.Second, "initial", nil)
	target.attach(first)
	target.finish()
	_, _, err := target.change(true)
	requireWorkerCrashError(t, err, 409)
	fallback := newRuntimePhase(context.Background(), time.Second, time.Second, "fallback", nil)
	if err := target.start(func() error { return nil }, fallback); err != nil {
		t.Fatal(err)
	}
	if _, fired, err := target.change(true); err != nil || !fired {
		t.Fatalf("live fallback:%v", err)
	}
	first.stop()
	fallback.stop()
}

func TestWorkerCrashRuntimeResponseClaimClosesSelection(t *testing.T) {
	for _, faultFirst := range []bool{false, true} {
		phase := newRuntimePhase(context.Background(), time.Second, time.Second, "command", nil)
		target := &devWorkerCrashTarget{}
		target.attach(phase)
		id := uuid.NewString()
		runtime := &runtimeInvocation{ctx: phase.ctx, input: invocation{requestID: id, workerCrash: target}, delivered: true, result: make(chan invocationResult, 1)}
		if faultFirst {
			if _, fired, err := target.change(true); err != nil || !fired {
				t.Fatal(err)
			}
		}
		response := httptest.NewRecorder()
		runtime.ServeHTTP(response, httptest.NewRequest(http.MethodPost, runtimePrefix+"invocation/"+id+"/response", strings.NewReader("{}")))
		if faultFirst {
			if response.Code != 409 {
				t.Fatalf("accepted response after injected death:%d", response.Code)
			}
		} else {
			if response.Code != 202 {
				t.Fatalf("native result not acknowledged:%d", response.Code)
			}
			_, _, err := target.change(true)
			requireWorkerCrashError(t, err, 409)
			if phase.ctx.Err() != nil {
				t.Fatal("stale active record canceled completed worker")
			}
		}
		phase.stop()
	}
}
