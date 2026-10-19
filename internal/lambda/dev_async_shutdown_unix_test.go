//go:build linux || darwin

package lambda

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/localexec"
)

func shutdownNativeService(t *testing.T) (*Service, string, *activityRecorder) {
	t.Helper()
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("Node unavailable")
	}
	directory := t.TempDir()
	source := `import fs from 'node:fs';
export async function handler(event) {
 const path=process.env.DIRECTORY+'/'+event.label;
 fs.writeFileSync(path+'-started',String(process.pid));
 fs.appendFileSync(process.env.DIRECTORY+'/calls',event.label+'\n');
 while(event.gate&&!fs.existsSync(path+'-release')) await new Promise(r=>setTimeout(r,2));
 if(event.fail) throw new Error('real native failure');
 fs.writeFileSync(path+'-done','complete');
 return {ok:true};
}`
	if err := os.WriteFile(filepath.Join(directory, "shutdown.mjs"), []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	observer := &activityRecorder{}
	service, err := NewService(&Config{
		Functions: map[string]Function{"event": {Runtime: "node", Command: []string{node}, Handler: "shutdown.mjs#handler", Timeout: 10 * time.Second, Environment: map[string]string{"DIRECTORY": directory}}},
		DevAsync:  &DevAsyncConfig{Workers: 2, LogWriter: io.Discard}, DevActivity: observer,
	}, directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close(context.Background()) })
	return service, directory, observer
}
func shutdownAdmit(t *testing.T, service *Service, payload string) Admission {
	t.Helper()
	admission, err := service.Admit(context.Background(), InvokeInput{FunctionName: "event", Payload: []byte(payload)})
	if err != nil {
		t.Fatal(err)
	}
	return admission
}
func shutdownStartedPID(t *testing.T, directory, label string) int {
	t.Helper()
	name := filepath.Join(directory, label+"-started")
	waitForFile(t, name)
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(string(data))
	if err != nil {
		t.Fatal(err)
	}
	return pid
}
func shutdownRelease(t *testing.T, directory, label string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(directory, label+"-release"), nil, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestDevAsyncShutdownRealFutureRetryAndOverlappingClaimedAttempts(t *testing.T) {
	service, directory, observer := shutdownNativeService(t)
	failed := shutdownAdmit(t, service, `{"label":"retry","fail":true}`)
	retry := waitAsyncState(t, service, failed.RequestID, "retrying")
	if retry.Attempts != 1 || retry.NextAttemptAt.Sub(retry.UpdatedAt) < 50*time.Second {
		t.Fatalf("native retry default changed:%+v", retry)
	}
	retryPID := shutdownStartedPID(t, directory, "retry")
	success := shutdownAdmit(t, service, `{"label":"success","gate":true}`)
	successPID := shutdownStartedPID(t, directory, "success")
	failure := shutdownAdmit(t, service, `{"label":"failure","gate":true,"fail":true}`)
	failurePID := shutdownStartedPID(t, directory, "failure")
	defer shutdownRelease(t, directory, "success")
	defer shutdownRelease(t, directory, "failure")
	pending := shutdownAdmit(t, service, `{"label":"queued"}`)
	waitAsyncState(t, service, pending.RequestID, "queued")
	service.DevDiscardPendingAsync()
	service.DevDiscardPendingAsync()
	if _, err := service.Admit(context.Background(), InvokeInput{FunctionName: "event", Payload: []byte("{}")}); err == nil {
		t.Fatal("discard admitted new Event")
	} else {
		assertServiceUnavailable(t, err)
	}
	// The immediate fence prevents future claims. Cancellation evidence advances
	// on existing workers; both active attempts still own their running work.
	if _, err := os.Stat(filepath.Join(directory, "queued-started")); !os.IsNotExist(err) {
		t.Fatal("queued handler launched")
	}
	result, err := service.Execute(context.Background(), InvokeInput{FunctionName: "event", Payload: []byte(`{"label":"callback"}`)})
	if err != nil || result.FunctionError {
		t.Fatalf("policy disabled synchronous callbacks:%s %v", result.Payload, err)
	}
	drained := make(chan error, 1)
	go func() { drained <- service.DrainAsync(context.Background()) }()
	shutdownRelease(t, directory, "success")
	waitAsyncState(t, service, success.RequestID, "succeeded")
	requireShutdownDiscard(t, waitAsyncState(t, service, failed.RequestID, "canceled"), 1)
	requireShutdownDiscard(t, waitAsyncState(t, service, pending.RequestID, "canceled"), 0)
	select {
	case err := <-drained:
		t.Fatalf("drain skipped active failure:%v", err)
	default:
	}
	if !processAlive(failurePID) {
		t.Fatal("discard canceled active attempt")
	}
	shutdownRelease(t, directory, "failure")
	select {
	case err := <-drained:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("discard waited on unusable native retry")
	}
	requireShutdownDiscard(t, waitAsyncState(t, service, failure.RequestID, "canceled"), 1)
	if service.DevDiscardedAsyncCount() != 3 {
		t.Fatalf("count%d", service.DevDiscardedAsyncCount())
	}
	for _, pid := range []int{retryPID, successPID, failurePID} {
		if processAlive(pid) {
			t.Fatalf("owned child%d remains", pid)
		}
	}
	if active, _, _ := observer.snapshot(); active != 0 {
		t.Fatalf("active leases%d", active)
	}
	if err := service.Close(context.Background()); err != nil {
		t.Fatalf("intentional discard poisoned Close:%v", err)
	}
}

func TestDevAsyncShutdownDefaultDrainStillWaitsFutureRetryAndRetainsDeadline(t *testing.T) {
	service, directory, _ := shutdownNativeService(t)
	admission := shutdownAdmit(t, service, `{"label":"retry","fail":true}`)
	record := waitAsyncState(t, service, admission.RequestID, "retrying")
	if record.NextAttemptAt.Sub(record.UpdatedAt) < 50*time.Second {
		t.Fatal("default retry interval changed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := service.DrainAsync(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("default drain:%v", err)
	}
	record = waitAsyncState(t, service, admission.RequestID, "canceled")
	if record.ErrorType != "ServiceShutdown" || service.DevDiscardedAsyncCount() != 0 {
		t.Fatalf("default selected dev discard:%+v", record)
	}
	if err := service.DrainAsync(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline erased:%v", err)
	}
	if err := service.Close(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close erased default deadline:%v", err)
	}
	if processAlive(shutdownStartedPID(t, directory, "retry")) {
		t.Fatal("default native worker retained")
	}
}
func TestDevAsyncShutdownDeadlineStillAbortsAndJoinsClaimedAttempt(t *testing.T) {
	service, directory, _ := shutdownNativeService(t)
	admission := shutdownAdmit(t, service, `{"label":"held","gate":true}`)
	pid := shutdownStartedPID(t, directory, "held")
	service.DevDiscardPendingAsync()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := service.DrainAsync(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline abort:%v", err)
	}
	record := waitAsyncState(t, service, admission.RequestID, "canceled")
	if record.ErrorType != "ServiceShutdown" || service.DevDiscardedAsyncCount() != 0 || processAlive(pid) {
		t.Fatalf("active abort misreported:%+v", record)
	}
	if err := service.Close(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("discard erased active deadline:%v", err)
	}
}

func TestDevAsyncShutdownAdmissionReservationSettlesAfterBlockedCapture(t *testing.T) {
	for _, captureFailure := range []bool{false, true} {
		writer := &shutdownCaptureGate{state: "queued", entered: make(chan struct{}), proceed: make(chan struct{})}
		var once sync.Once
		open := func() { once.Do(func() { close(writer.proceed) }) }
		defer open()
		if captureFailure {
			writer.err = errors.New("queued capture unavailable")
		}
		directory := t.TempDir()
		output := filepath.Join(directory, "must-not-run")
		service := newAsyncTestService(t, map[string]Function{"event": asyncCommand(t, output, "", "", "success")}, directory, &DevAsyncConfig{Workers: 1, LogWriter: writer})
		type admitted struct {
			value Admission
			err   error
		}
		done := make(chan admitted, 1)
		go func() {
			value, err := service.Admit(context.Background(), InvokeInput{FunctionName: "event", Payload: []byte("{}")})
			done <- admitted{value, err}
		}()
		<-writer.entered
		begin := make(chan struct{})
		go func() { service.DevDiscardPendingAsync(); close(begin) }()
		select {
		case <-begin:
		case <-time.After(time.Second):
			t.Fatal("begin blocked behind accepted queued capture")
		}
		if len(service.AsyncSnapshot()) != 0 || service.DevDiscardedAsyncCount() != 0 {
			t.Fatal("reservation published before capture")
		}
		open()
		result := <-done
		if captureFailure {
			if result.err == nil || result.value.RequestID != "" {
				t.Fatal("failed queued capture falsely accepted")
			}
			if err := service.DrainAsync(context.Background()); err == nil {
				t.Fatal("capture failure erased")
			}
			if service.DevDiscardedAsyncCount() != 0 {
				t.Fatal("unaccepted reservation counted as discarded")
			}
		} else {
			if result.err != nil || result.value.RequestID == "" {
				t.Fatalf("accepted reservation lost:%+v", result)
			}
			if err := service.DrainAsync(context.Background()); err != nil {
				t.Fatal(err)
			}
			requireShutdownDiscard(t, service.AsyncSnapshot()[0], 0)
			if service.DevDiscardedAsyncCount() != 1 {
				t.Fatal("accepted reservation not counted")
			}
		}
		if _, err := os.Stat(output); !os.IsNotExist(err) {
			t.Fatal("reservation launched after discard fence")
		}
	}
}
func TestDevAsyncShutdownClaimBeforeFenceRunsAfterBlockedRunningCapture(t *testing.T) {
	writer := &shutdownCaptureGate{state: "running", entered: make(chan struct{}), proceed: make(chan struct{})}
	var once sync.Once
	defer once.Do(func() { close(writer.proceed) })
	directory := t.TempDir()
	output := filepath.Join(directory, "claimed-result")
	service := newAsyncTestService(t, map[string]Function{"event": asyncCommand(t, output, "", "", "success")}, directory, &DevAsyncConfig{Workers: 1, LogWriter: writer})
	admission := shutdownAdmit(t, service, "{}")
	<-writer.entered
	service.DevDiscardPendingAsync()
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		t.Fatal("fixture launched before held running capture")
	}
	once.Do(func() { close(writer.proceed) })
	if err := service.DrainAsync(context.Background()); err != nil {
		t.Fatal(err)
	}
	record := waitAsyncState(t, service, admission.RequestID, "succeeded")
	if record.Attempts != 1 || service.DevDiscardedAsyncCount() != 0 {
		t.Fatalf("claimed task canceled:%+v", record)
	}
	if _, err := os.Stat(output); err != nil {
		t.Fatal("claim-winning handler never executed")
	}
}
func TestDevAsyncShutdownOwnershipFailureStaysSticky(t *testing.T) {
	service, directory, _ := shutdownNativeService(t)
	uncertain := errors.New("joined cleanup evidence unavailable")
	service.processCleanup = func(command *exec.Cmd) error { return errors.Join(localexec.Cleanup(command), uncertain) }
	admission := shutdownAdmit(t, service, `{"label":"uncertain","gate":true,"fail":true}`)
	shutdownStartedPID(t, directory, "uncertain")
	defer shutdownRelease(t, directory, "uncertain")
	service.DevDiscardPendingAsync()
	shutdownRelease(t, directory, "uncertain")
	if err := service.DrainAsync(context.Background()); !errors.Is(err, uncertain) {
		t.Fatalf("discard erased ownership uncertainty:%v", err)
	}
	requireShutdownDiscard(t, waitAsyncState(t, service, admission.RequestID, "canceled"), 1)
	if err := service.Close(context.Background()); !errors.Is(err, uncertain) {
		t.Fatalf("Close erased ownership uncertainty:%v", err)
	}
}
