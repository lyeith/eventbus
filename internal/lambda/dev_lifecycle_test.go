//go:build linux || darwin

package lambda

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/localexec"
)

type activityLease struct {
	kind, id string
	err      error
}
type activityRecorder struct {
	mu        sync.Mutex
	active    map[string]string
	begun     []activityLease
	completed []activityLease
	reject    bool
	onRelease func(error)
}

func (observer *activityRecorder) BeginActivity(kind, id string) (func(error), error) {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if observer.reject {
		return nil, errors.New("fixture fence")
	}
	if observer.active == nil {
		observer.active = make(map[string]string)
	}
	observer.active[id] = kind
	observer.begun = append(observer.begun, activityLease{kind: kind, id: id})
	return func(err error) {
		if observer.onRelease != nil {
			observer.onRelease(err)
		}
		observer.mu.Lock()
		defer observer.mu.Unlock()
		delete(observer.active, id)
		observer.completed = append(observer.completed, activityLease{kind: kind, id: id, err: err})
	}, nil
}
func (observer *activityRecorder) snapshot() (active int, begun, completed []activityLease) {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	return len(observer.active), append([]activityLease(nil), observer.begun...), append([]activityLease(nil), observer.completed...)
}

func newActivityService(t *testing.T, functions map[string]Function, directory string, observer DevActivity, writer io.Writer, retryDelays []time.Duration) *Service {
	t.Helper()
	if writer == nil {
		writer = io.Discard
	}
	service, err := NewService(&Config{Functions: functions, DevActivity: observer, DevAsync: &DevAsyncConfig{Workers: 1, LogWriter: writer, RetryDelays: retryDelays}}, directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = service.Close(ctx)
	})
	return service
}

func assertActivityFinished(t *testing.T, observer *activityRecorder, kind string, wantErr error) {
	t.Helper()
	active, begun, completed := observer.snapshot()
	if active != 0 || len(begun) != 1 || len(completed) != 1 || completed[0].id != begun[0].id || completed[0].kind != kind || completed[0].id == "" {
		t.Fatalf("activity lifetime: active=%d begun=%#v completed=%#v", active, begun, completed)
	}
	if wantErr == nil && completed[0].err != nil || wantErr != nil && !errors.Is(completed[0].err, wantErr) {
		t.Fatalf("release result: %v; want %v", completed[0].err, wantErr)
	}
}

type gatedActivityCapture struct {
	observer *activityRecorder
	entered  chan int
	gate     chan struct{}
	mu       sync.Mutex
	records  []AsyncRecord
}

func (capture *gatedActivityCapture) Write(data []byte) (int, error) {
	var record AsyncRecord
	if err := json.Unmarshal(bytes.TrimSpace(data), &record); err != nil {
		return 0, err
	}
	if record.State == "queued" {
		active, _, _ := capture.observer.snapshot()
		capture.entered <- active
		<-capture.gate
	}
	capture.mu.Lock()
	capture.records = append(capture.records, record)
	capture.mu.Unlock()
	return len(data), nil
}
func (capture *gatedActivityCapture) lastState() string {
	capture.mu.Lock()
	defer capture.mu.Unlock()
	if len(capture.records) == 0 {
		return ""
	}
	return capture.records[len(capture.records)-1].State
}

func TestDevActivityAdmissionPrecedesCaptureAndTerminalRelease(t *testing.T) {
	directory := t.TempDir()
	output := filepath.Join(directory, "events")
	observer := &activityRecorder{}
	capture := &gatedActivityCapture{observer: observer, entered: make(chan int, 1), gate: make(chan struct{})}
	var gateOnce sync.Once
	openGate := func() { gateOnce.Do(func() { close(capture.gate) }) }
	releasedState := make(chan string, 1)
	observer.onRelease = func(error) { releasedState <- capture.lastState() }
	service := newActivityService(t, map[string]Function{"event": asyncCommand(t, output, "", "", "success")}, directory, observer, capture, []time.Duration{0, 0})
	t.Cleanup(openGate)
	admitted := make(chan error, 1)
	go func() {
		_, err := service.Admit(context.Background(), InvokeInput{FunctionName: "event", Payload: []byte(`{"id":"accepted"}`)})
		admitted <- err
	}()
	select {
	case active := <-capture.entered:
		if active != 1 {
			t.Fatalf("admission evidence started without a retained lease: %d", active)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("admission never reached evidence writer")
	}
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		t.Fatalf("handler launched before capture: %v", err)
	}
	select {
	case err := <-admitted:
		t.Fatalf("admission returned before capture completed: %v", err)
	default:
	}
	openGate()
	if err := <-admitted; err != nil {
		t.Fatal(err)
	}
	select {
	case state := <-releasedState:
		if state != "succeeded" {
			t.Fatalf("lease released before terminal evidence: %q", state)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("terminal task did not release activity")
	}
	// The callback has returned once the service lock is available again.
	service.AsyncSnapshot()
	assertActivityFinished(t, observer, "lambda_async", nil)
}

func TestDevActivityRetryWaitRetainsTaskWithoutAnActiveProcess(t *testing.T) {
	directory := t.TempDir()
	observer := &activityRecorder{}
	service := newActivityService(t, map[string]Function{"event": asyncCommand(t, filepath.Join(directory, "events"), "", "", "retry")}, directory, observer, nil, []time.Duration{time.Hour, time.Hour})
	admission, err := service.Admit(context.Background(), InvokeInput{FunctionName: "event", Payload: []byte(`{"id":"retry"}`)})
	if err != nil {
		t.Fatal(err)
	}
	waitAsyncState(t, service, admission.RequestID, "retrying")
	service.mu.Lock()
	activeFunctions := len(service.active)
	service.mu.Unlock()
	active, begun, completed := observer.snapshot()
	if activeFunctions != 0 || active != 1 || len(begun) != 1 || len(completed) != 0 {
		t.Fatalf("retry interval appeared quiescent: processes=%d activities=%d begun=%#v completed=%#v", activeFunctions, active, begun, completed)
	}
	service.mu.Lock()
	service.asyncTasks[admission.RequestID].readyAt = time.Now().Add(-time.Second)
	service.wakeAsyncLocked()
	service.mu.Unlock()
	waitAsyncState(t, service, admission.RequestID, "succeeded")
	assertActivityFinished(t, observer, "lambda_async", nil)
}

type processCleanupPoint struct {
	pid     int
	runtime string
}

func TestDevActivitySynchronousInvokeJoinsProcessAndRuntimeBeforeRelease(t *testing.T) {
	for _, runtime := range []string{"command", "provided"} {
		for _, boundary := range []string{"http", "execute"} {
			t.Run(runtime+"/"+boundary, func(t *testing.T) {
				directory := t.TempDir()
				observer := &activityRecorder{}
				function := providedFunction(t, "echo")
				if runtime == "command" {
					function.Runtime = "command"
					function.Environment["EVENTBUS_LAMBDA_TEST_MODE"] = "command"
				}
				service := newActivityService(t, map[string]Function{"echo": function}, directory, observer, nil, nil)
				cleanupEntered := make(chan processCleanupPoint, 1)
				gate := make(chan struct{})
				var once sync.Once
				openGate := func() { once.Do(func() { close(gate) }) }
				t.Cleanup(openGate)
				var point processCleanupPoint
				service.processCleanup = func(command *exec.Cmd) error {
					err := localexec.Cleanup(command)
					point.pid = command.Process.Pid
					for _, value := range command.Env {
						if name, address, found := strings.Cut(value, "="); found && name == "AWS_LAMBDA_RUNTIME_API" {
							point.runtime = address
						}
					}
					cleanupEntered <- point
					<-gate
					return err
				}
				released := make(chan error, 1)
				observer.onRelease = func(error) {
					if processAlive(point.pid) {
						released <- fmt.Errorf("process %d was still running at release", point.pid)
						return
					}
					if point.runtime != "" {
						connection, err := net.DialTimeout("tcp", point.runtime, time.Second)
						if err == nil {
							_ = connection.Close()
							released <- errors.New("Runtime API listener was still open at release")
							return
						}
					}
					released <- nil
				}
				invoked := make(chan error, 1)
				go func() {
					if boundary == "execute" {
						output, err := service.Execute(context.Background(), InvokeInput{FunctionName: "echo", Payload: []byte(`{"id":"sync"}`)})
						if err == nil && (output.FunctionError || !json.Valid(output.Payload)) {
							err = fmt.Errorf("invalid output: %#v", output)
						}
						invoked <- err
					} else {
						response := requestInvoke(service, "echo", `{"id":"sync"}`, nil)
						if response.Code != 200 || response.Header().Get("X-Amz-Function-Error") != "" || !json.Valid(response.Body.Bytes()) {
							invoked <- fmt.Errorf("response: %d %s", response.Code, response.Body.String())
							return
						}
						invoked <- nil
					}
				}()
				select {
				case <-cleanupEntered:
				case <-time.After(10 * time.Second):
					t.Fatal("invocation never reached cleanup")
				}
				active, begun, completed := observer.snapshot()
				if active != 1 || len(begun) != 1 || len(completed) != 0 {
					t.Fatalf("cleanup was not retained: active=%d begun=%#v completed=%#v", active, begun, completed)
				}
				select {
				case err := <-invoked:
					t.Fatalf("invocation returned before cleanup joined: %v", err)
				default:
				}
				openGate()
				if err := <-invoked; err != nil {
					t.Fatal(err)
				}
				if err := <-released; err != nil {
					t.Fatal(err)
				}
				assertActivityFinished(t, observer, "lambda_invoke", nil)
			})
		}
	}
}

type failingActivityCapture struct{ err error }

func (capture failingActivityCapture) Write([]byte) (int, error) { return 0, capture.err }

func TestDevActivityCaptureFailureReleasesDirtyWithoutPublishingTask(t *testing.T) {
	directory := t.TempDir()
	observer := &activityRecorder{}
	evidenceErr := errors.New("fixture admission evidence failure")
	output := filepath.Join(directory, "events")
	service := newActivityService(t, map[string]Function{"event": asyncCommand(t, output, "", "", "success")}, directory, observer, failingActivityCapture{err: evidenceErr}, nil)
	_, err := service.Admit(context.Background(), InvokeInput{FunctionName: "event", Payload: []byte(`{}`)})
	var rejected *InvokeError
	if !errors.As(err, &rejected) || rejected.Status != 500 {
		t.Fatalf("capture failure admission: %v", err)
	}
	assertActivityFinished(t, observer, "lambda_async", evidenceErr)
	if records := service.AsyncSnapshot(); len(records) != 0 {
		t.Fatalf("failed capture published a detached task: %#v", records)
	}
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		t.Fatalf("handler ran despite admission capture failure: %v", err)
	}
	_, err = service.Admit(context.Background(), InvokeInput{FunctionName: "event", Payload: []byte(`{}`)})
	if !errors.As(err, &rejected) || rejected.Status != 500 {
		t.Fatalf("evidence failure was not sticky: %v", err)
	}
	assertActivityFinished(t, observer, "lambda_async", evidenceErr)
}

func TestDevActivityAcceptedEvidenceFailureMakesTerminalReleaseDirty(t *testing.T) {
	directory := t.TempDir()
	observer := &activityRecorder{}
	service := newActivityService(t, map[string]Function{"event": asyncCommand(t, filepath.Join(directory, "events"), "", "", "success")}, directory, observer, &failAsyncEvidenceWriter{}, []time.Duration{0, 0})
	admission, err := service.Admit(context.Background(), InvokeInput{FunctionName: "event", Payload: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	waitAsyncState(t, service, admission.RequestID, "succeeded")
	active, begun, completed := observer.snapshot()
	if active != 0 || len(begun) != 1 || len(completed) != 1 || completed[0].err == nil {
		t.Fatalf("lost accepted-task evidence failure: active=%d begun=%#v completed=%#v", active, begun, completed)
	}
}

func TestDevActivityFenceRefusesNativeBoundariesWithoutDetachedWork(t *testing.T) {
	directory := t.TempDir()
	observer := &activityRecorder{reject: true}
	var capture bytes.Buffer
	output := filepath.Join(directory, "events")
	service := newActivityService(t, map[string]Function{"event": asyncCommand(t, output, "", "", "success")}, directory, observer, &capture, nil)
	for _, invocationType := range []string{"RequestResponse", "Event"} {
		response := requestInvoke(service, "event", `{}`, map[string]string{"X-Amz-Invocation-Type": invocationType})
		if response.Code != http.StatusServiceUnavailable || response.Header().Get("X-Amzn-ErrorType") != "ServiceException" || response.Header().Get("X-Amz-Function-Error") != "" {
			t.Fatalf("fenced %s: %d %s %#v", invocationType, response.Code, response.Body.String(), response.Header())
		}
	}
	_, err := service.Execute(context.Background(), InvokeInput{FunctionName: "event", Payload: []byte(`{}`)})
	var rejected *InvokeError
	if !errors.As(err, &rejected) || rejected.Status != 503 || rejected.Code != "ServiceException" {
		t.Fatalf("fenced Execute: %v", err)
	}
	if active, begun, completed := observer.snapshot(); active != 0 || len(begun) != 0 || len(completed) != 0 {
		t.Fatalf("fenced admission retained work: active=%d begun=%#v completed=%#v", active, begun, completed)
	}
	if capture.Len() != 0 || len(service.AsyncSnapshot()) != 0 {
		t.Fatal("fenced admission wrote evidence or published an Event")
	}
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		t.Fatalf("fenced admission launched function: %v", err)
	}
}

func TestDevActivityCopiedAtStartupAndAbsentPreservesNativeErrors(t *testing.T) {
	observer := &activityRecorder{}
	config := &Config{Functions: map[string]Function{"error": providedFunction(t, "error")}, DevActivity: observer, DevAsync: &DevAsyncConfig{LogWriter: io.Discard}}
	service, err := NewService(config, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close(context.Background()) })
	config.DevActivity = &activityRecorder{reject: true}
	response := requestInvoke(service, "error", `{}`, nil)
	if response.Code != 200 || response.Header().Get("X-Amz-Function-Error") != "Unhandled" || !strings.Contains(response.Body.String(), "Unauthorized") {
		t.Fatalf("ordinary native handler failure: %d %s", response.Code, response.Body.String())
	}
	assertActivityFinished(t, observer, "lambda_invoke", nil)
	unobserved := newActivityService(t, map[string]Function{"error": providedFunction(t, "error")}, t.TempDir(), nil, nil, nil)
	without := requestInvoke(unobserved, "error", `{}`, nil)
	if without.Code != response.Code || without.Body.String() != response.Body.String() || without.Header().Get("X-Amz-Function-Error") != "Unhandled" {
		t.Fatalf("observer changed native contract: with=%d %s without=%d %s", response.Code, response.Body.String(), without.Code, without.Body.String())
	}
}

func TestDevActivityPrivateCleanupUncertaintySurvivesAsyncRetrySuccess(t *testing.T) {
	directory := t.TempDir()
	observer := &activityRecorder{}
	service := newActivityService(t, map[string]Function{"event": asyncCommand(t, filepath.Join(directory, "events"), "", "", "success")}, directory, observer, nil, []time.Duration{0, 0})
	ownershipErr := errors.New("fixture process cleanup uncertainty")
	var attempts int
	var pids []int
	service.processCleanup = func(command *exec.Cmd) error {
		err := localexec.Cleanup(command)
		attempts++
		pids = append(pids, command.Process.Pid)
		if attempts == 1 {
			return errors.Join(err, ownershipErr)
		}
		return err
	}
	admission, err := service.Admit(context.Background(), InvokeInput{FunctionName: "event", Payload: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	record := waitAsyncState(t, service, admission.RequestID, "succeeded")
	if record.Attempts != 2 {
		t.Fatalf("native retry/result contract changed: %#v", record)
	}
	assertActivityFinished(t, observer, "lambda_async", ownershipErr)
	// Private joined uncertainty does not become native async evidence failure:
	// another Event is still accepted and follows ordinary retry policy.
	next, err := service.Admit(context.Background(), InvokeInput{FunctionName: "event", Payload: []byte(`{"id":"after-uncertainty"}`)})
	if err != nil {
		t.Fatalf("private ownership uncertainty changed native admission: %v", err)
	}
	if record := waitAsyncState(t, service, next.RequestID, "succeeded"); record.Attempts != 1 {
		t.Fatalf("private ownership uncertainty changed later native retries: %#v", record)
	}
	if err := service.DrainAsync(context.Background()); !errors.Is(err, ownershipErr) {
		t.Fatalf("successful retry erased async join uncertainty: %v", err)
	}
	if err := service.DrainAsync(context.Background()); !errors.Is(err, ownershipErr) {
		t.Fatalf("repeated drain erased async join uncertainty: %v", err)
	}
	if err := service.Close(context.Background()); !errors.Is(err, ownershipErr) {
		t.Fatalf("strict owner close hid joined uncertainty: %v", err)
	}
	for _, pid := range pids {
		if processAlive(pid) {
			t.Fatalf("fault injection left child %d running", pid)
		}
	}
}

func TestDevActivityTimeoutDistinguishesPrivateCleanupUncertainty(t *testing.T) {
	for _, runtime := range []string{"command", "provided"} {
		for _, uncertain := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/uncertain=%t", runtime, uncertain), func(t *testing.T) {
				function := providedFunction(t, "wait")
				function.Runtime = runtime
				if runtime == "provided" {
					function.Environment["EVENTBUS_LAMBDA_TEST_MODE"] = "post-next-wait"
				}
				function.Timeout = 40 * time.Millisecond
				observer := &activityRecorder{}
				service := newActivityService(t, map[string]Function{"wait": function}, t.TempDir(), observer, nil, nil)
				var ownershipErr error
				if uncertain {
					ownershipErr = errors.New("fixture timed-out cleanup uncertainty")
				}
				var pid int
				service.processCleanup = func(command *exec.Cmd) error {
					pid = command.Process.Pid
					return errors.Join(localexec.Cleanup(command), ownershipErr)
				}
				output, err := service.Execute(context.Background(), InvokeInput{FunctionName: "wait", Payload: []byte(`{}`)})
				if err != nil || !output.FunctionError || !strings.Contains(string(output.Payload), "Sandbox.Timedout") || strings.Contains(string(output.Payload), "fixture timed-out cleanup uncertainty") {
					t.Fatalf("private uncertainty changed native timeout: %#v %v", output, err)
				}
				assertActivityFinished(t, observer, "lambda_invoke", ownershipErr)
				if err := service.DrainAsync(context.Background()); err != nil {
					t.Fatalf("synchronous uncertainty poisoned independent async drain: %v", err)
				}
				closeErr := service.Close(context.Background())
				if uncertain && !errors.Is(closeErr, ownershipErr) || !uncertain && closeErr != nil {
					t.Fatalf("strict close ownership result: %v; want %v", closeErr, ownershipErr)
				}
				if processAlive(pid) {
					t.Fatalf("fault injection left child %d running", pid)
				}
			})
		}
	}
}

func TestDevActivityHandlerControlledInternalErrorDoesNotMakeOwnerDirty(t *testing.T) {
	// Native provided runtimes control their errorType field. A handler cannot
	// claim process cleanup uncertainty by returning an internal-looking error.
	directory := t.TempDir()
	observer := &activityRecorder{}
	function := providedFunction(t, "internal-error")
	service := newActivityService(t, map[string]Function{"event": function}, directory, observer, nil, []time.Duration{0, 0})
	admission, err := service.Admit(context.Background(), InvokeInput{FunctionName: "event", Payload: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	record := waitAsyncState(t, service, admission.RequestID, "failed")
	if record.Attempts != 3 || record.ErrorType != "Runtime.InternalError" {
		t.Fatalf("native handler error/retry projection: %#v", record)
	}
	assertActivityFinished(t, observer, "lambda_async", nil)
}
