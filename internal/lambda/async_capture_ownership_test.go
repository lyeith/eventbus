//go:build linux || darwin

package lambda

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/localexec"
)

// This borrowed writer blocks an actual async capture transition. Its gate is
// independent of Lambda locks, and tests release it before service cleanup.
type asyncOwnershipCapture struct {
	state, function string
	entered         chan AsyncRecord
	gate            chan struct{}
	blockOnce       sync.Once
	openOnce        sync.Once
	mu              sync.Mutex
	records         []AsyncRecord
	failure         error
	panicValue      any
}

func newAsyncOwnershipCapture(state, function string) *asyncOwnershipCapture {
	return &asyncOwnershipCapture{state: state, function: function, entered: make(chan AsyncRecord, 1), gate: make(chan struct{})}
}

func (writer *asyncOwnershipCapture) Write(data []byte) (int, error) {
	var record AsyncRecord
	if err := json.Unmarshal(bytes.TrimSpace(data), &record); err != nil {
		return 0, err
	}
	if record.State == writer.state && (writer.function == "" || record.FunctionName == writer.function) {
		writer.blockOnce.Do(func() {
			writer.entered <- record
			<-writer.gate
			if writer.panicValue != nil {
				panic(writer.panicValue)
			}
		})
		if writer.failure != nil {
			return 0, writer.failure
		}
	}
	writer.mu.Lock()
	writer.records = append(writer.records, record)
	writer.mu.Unlock()
	return len(data), nil
}

func (writer *asyncOwnershipCapture) open() {
	writer.openOnce.Do(func() { close(writer.gate) })
}

func (writer *asyncOwnershipCapture) snapshot() []AsyncRecord {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return append([]AsyncRecord(nil), writer.records...)
}

func asyncOwnershipService(t *testing.T, functions map[string]Function, directory string, observer DevActivity, writer *asyncOwnershipCapture, workers, capacity int) *Service {
	t.Helper()
	service, err := NewService(&Config{Functions: functions, DevActivity: observer, DevAsync: &DevAsyncConfig{
		Workers: workers, Capacity: capacity, LogWriter: writer, RetryDelays: []time.Duration{0, 0},
	}}, directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = service.Close(ctx)
	})
	t.Cleanup(writer.open)
	return service
}

func awaitAsyncOwnership[T any](t *testing.T, channel <-chan T, what string) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not finish", what)
		var zero T
		return zero
	}
}

type asyncOwnershipAdmission struct {
	admission Admission
	err       error
}

func startAsyncOwnershipAdmission(service *Service, function string) <-chan asyncOwnershipAdmission {
	done := make(chan asyncOwnershipAdmission, 1)
	go func() {
		admission, err := service.Admit(context.Background(), InvokeInput{FunctionName: function, Payload: []byte(`{"id":"owned"}`)})
		done <- asyncOwnershipAdmission{admission, err}
	}()
	return done
}

func assertAsyncOwnershipCapture(t *testing.T, writer *asyncOwnershipCapture, id string, states ...string) {
	t.Helper()
	var selected []AsyncRecord
	for _, record := range writer.snapshot() {
		if record.RequestID == id {
			selected = append(selected, record)
		}
	}
	if len(selected) != len(states) {
		t.Fatalf("capture count for %s: %#v; want %v", id, selected, states)
	}
	for index, state := range states {
		if selected[index].State != state || selected[index].SchemaVersion != "eventbus.lambda.async.v1" || selected[index].RequestID == "" {
			t.Fatalf("transition %d: %#v; want %s", index, selected[index], state)
		}
		if index == len(states)-1 && selected[index].CompletedAt.IsZero() {
			t.Fatalf("terminal capture lacks completion: %#v", selected[index])
		}
	}
}

func TestAsyncCaptureLeavesNativeInvocationAndMetadataAvailable(t *testing.T) {
	for _, state := range []string{"queued", "running", "succeeded"} {
		t.Run(state, func(t *testing.T) {
			directory := t.TempDir()
			eventOutput := filepath.Join(directory, "event")
			syncOutput := filepath.Join(directory, "sync")
			writer := newAsyncOwnershipCapture(state, "event")
			observer := &activityRecorder{}
			service := asyncOwnershipService(t, map[string]Function{
				"event": asyncCommand(t, eventOutput, "", "", "success"),
				"sync":  asyncCommand(t, syncOutput, "", "", "success"),
			}, directory, observer, writer, 1, 1)
			admissionDone := startAsyncOwnershipAdmission(service, "event")
			blocked := awaitAsyncOwnership(t, writer.entered, "blocked capture")
			metadataDone := make(chan error, 1)
			go func() {
				_, err := service.DescribeTarget("sync", "")
				metadataDone <- err
			}()
			if err := awaitAsyncOwnership(t, metadataDone, "unrelated target metadata"); err != nil {
				t.Fatal(err)
			}
			syncDone := make(chan error, 1)
			go func() {
				output, err := service.Execute(context.Background(), InvokeInput{FunctionName: "sync", Payload: []byte(`{"id":"sync"}`)})
				if err == nil && (output.FunctionError || output.RequestID == "" || !json.Valid(output.Payload)) {
					err = fmt.Errorf("native synchronous result: %#v", output)
				}
				syncDone <- err
			}()
			if err := awaitAsyncOwnership(t, syncDone, "actual synchronous invocation"); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(syncOutput); err != nil {
				t.Fatalf("synchronous child did not execute: %v", err)
			}
			records := service.AsyncSnapshot()
			if state == "queued" {
				if len(records) != 0 {
					t.Fatalf("unpublished reservation entered snapshot: %#v", records)
				}
				select {
				case admitted := <-admissionDone:
					t.Fatalf("admission preceded queued capture: %#v", admitted)
				default:
				}
			} else {
				wantState, wantAttempts := "queued", 0
				if state == "succeeded" {
					wantState, wantAttempts = "running", 1
				}
				if len(records) != 1 || records[0].RequestID != blocked.RequestID || records[0].State != wantState || records[0].Attempts != wantAttempts {
					t.Fatalf("transition published before capture: %#v", records)
				}
			}
			service.mu.Lock()
			outstanding := service.asyncOutstanding
			service.mu.Unlock()
			active, _, completed := observer.snapshot()
			if outstanding != 1 || active != 1 || len(completed) != 1 || completed[0].kind != "lambda_invoke" {
				t.Fatalf("blocked async owner released: outstanding=%d active=%d completed=%#v", outstanding, active, completed)
			}
			if err := service.DevEvidence(); err != nil {
				t.Fatalf("live capture mistaken for terminal uncertainty: %v", err)
			}
			writer.open()
			admitted := awaitAsyncOwnership(t, admissionDone, "native async admission")
			if admitted.err != nil || admitted.admission.RequestID != blocked.RequestID {
				t.Fatalf("admission identity: %#v, blocked=%#v", admitted, blocked)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := service.DrainAsync(ctx); err != nil {
				t.Fatal(err)
			}
			assertAsyncOwnershipCapture(t, writer, blocked.RequestID, "queued", "running", "succeeded")
			active, _, completed = observer.snapshot()
			if active != 0 || len(completed) != 2 || completed[1].id != blocked.RequestID || completed[1].kind != "lambda_async" || completed[1].err != nil {
				t.Fatalf("terminal release: active=%d completed=%#v", active, completed)
			}
		})
	}
}

func TestAsyncPendingAdmissionRemainsOwnedAcrossClose(t *testing.T) {
	for _, mode := range []string{"healthy", "abort", "capture_failure", "capture_panic"} {
		t.Run(mode, func(t *testing.T) {
			directory := t.TempDir()
			output := filepath.Join(directory, "event")
			writer := newAsyncOwnershipCapture("queued", "event")
			failure := errors.New("fixture capture denial")
			if mode == "capture_failure" {
				writer.failure = failure
			}
			if mode == "capture_panic" {
				writer.panicValue = "fixture borrowed writer panic"
			}
			observer := &activityRecorder{}
			service := asyncOwnershipService(t, map[string]Function{"event": asyncCommand(t, output, "", "", "success")}, directory, observer, writer, 1, 1)
			admissionDone := make(chan asyncOwnershipAdmission, 1)
			panicDone := make(chan any, 1)
			go func() {
				defer func() { panicDone <- recover() }()
				admission, err := service.Admit(context.Background(), InvokeInput{FunctionName: "event", Payload: []byte(`{}`)})
				admissionDone <- asyncOwnershipAdmission{admission, err}
			}()
			blocked := awaitAsyncOwnership(t, writer.entered, "pending admission capture")
			closeCtx, cancelClose := context.WithCancel(context.Background())
			defer cancelClose()
			closeDone := make(chan error, 1)
			go func() { closeDone <- service.Close(closeCtx) }()
			// Reading the service state must remain possible before capture is
			// released. The fence wake is published under the same mutex.
			fenced := make(chan struct{})
			go func() {
				for {
					service.mu.Lock()
					closed, wake := service.closed, service.asyncWake
					service.mu.Unlock()
					if closed {
						close(fenced)
						return
					}
					<-wake
				}
			}()
			awaitAsyncOwnership(t, fenced, "Close admission fence")
			select {
			case err := <-closeDone:
				t.Fatalf("Close passed an unresolved reservation: %v", err)
			default:
			}
			if mode == "abort" {
				cancelClose()
				aborted := make(chan struct{})
				go func() {
					for {
						service.mu.Lock()
						isAborted, wake := service.asyncAborted, service.asyncWake
						service.mu.Unlock()
						if isAborted {
							close(aborted)
							return
						}
						<-wake
					}
				}()
				awaitAsyncOwnership(t, aborted, "Close irreversible abort")
			}
			writer.open()
			panicValue := awaitAsyncOwnership(t, panicDone, "admission completion")
			if mode == "capture_panic" {
				if panicValue != writer.panicValue {
					t.Fatalf("writer panic was obscured: %v", panicValue)
				}
			} else {
				if panicValue != nil {
					t.Fatalf("unexpected panic: %v", panicValue)
				}
				admitted := awaitAsyncOwnership(t, admissionDone, "admission result")
				if mode == "capture_failure" {
					var native *InvokeError
					if !errors.As(admitted.err, &native) || native.Status != http.StatusInternalServerError || admitted.admission.RequestID != "" {
						t.Fatalf("failed capture claimed admission: %#v", admitted)
					}
				} else if admitted.err != nil || admitted.admission.RequestID != blocked.RequestID {
					t.Fatalf("reserved native admission was lost: %#v", admitted)
				}
			}
			closeErr := awaitAsyncOwnership(t, closeDone, "Close joined reservation")
			if mode == "healthy" {
				if closeErr != nil {
					t.Fatal(closeErr)
				}
				assertAsyncOwnershipCapture(t, writer, blocked.RequestID, "queued", "running", "succeeded")
				if _, err := os.Stat(output); err != nil {
					t.Fatalf("reserved event was not executed during healthy Close: %v", err)
				}
			} else {
				if closeErr == nil {
					t.Fatal("aborted or uncertain admission reported healthy Close")
				}
				if mode == "abort" {
					if !errors.Is(closeErr, context.Canceled) {
						t.Fatalf("abort lost caller cause: %v", closeErr)
					}
					assertAsyncOwnershipCapture(t, writer, blocked.RequestID, "queued", "canceled")
				} else {
					if len(service.AsyncSnapshot()) != 0 || len(writer.snapshot()) != 0 || service.DevEvidence() == nil {
						t.Fatal("failed unpublished reservation became successful evidence")
					}
					if mode == "capture_failure" && !errors.Is(closeErr, failure) {
						t.Fatalf("capture failure was lost: %v", closeErr)
					}
				}
				if _, err := os.Stat(output); !os.IsNotExist(err) {
					t.Fatalf("non-executable reservation launched a child: %v", err)
				}
			}
			active, begun, completed := observer.snapshot()
			if active != 0 || len(begun) != 1 || len(completed) != 1 || begun[0].id != blocked.RequestID || completed[0].id != blocked.RequestID {
				t.Fatalf("reservation owner not joined exactly once: %d %#v %#v", active, begun, completed)
			}
			if mode == "capture_failure" || mode == "capture_panic" {
				if completed[0].err == nil {
					t.Fatal("interrupted capture released healthy activity")
				}
			} else if completed[0].err != nil {
				t.Fatalf("joined shutdown/business cancellation was mistaken for evidence uncertainty: %v", completed[0].err)
			}
			service.mu.Lock()
			outstanding, queued, tasks := service.asyncOutstanding, len(service.asyncQueue), len(service.asyncTasks)
			service.mu.Unlock()
			if outstanding != 0 || queued != 0 || tasks != 0 {
				t.Fatalf("orphan reservation: outstanding=%d queued=%d tasks=%d", outstanding, queued, tasks)
			}
		})
	}
}

func TestAsyncBlockedTerminalCaptureCannotDelayAnotherChildCancellation(t *testing.T) {
	directory := t.TempDir()
	slowStarted := filepath.Join(directory, "slow-started")
	slowOutput := filepath.Join(directory, "slow-output")
	quickOutput := filepath.Join(directory, "quick-output")
	writer := newAsyncOwnershipCapture("succeeded", "quick")
	observer := &activityRecorder{}
	service := asyncOwnershipService(t, map[string]Function{
		"slow":  asyncCommand(t, slowOutput, filepath.Join(directory, "never-opened"), slowStarted, "success"),
		"quick": asyncCommand(t, quickOutput, "", "", "success"),
	}, directory, observer, writer, 2, 2)
	type joinedChild struct {
		requestID string
		pid       int
		waited    bool
		err       error
	}
	cleanupDone := make(chan joinedChild, 2)
	service.processCleanup = func(command *exec.Cmd) error {
		err := localexec.Cleanup(command)
		id := ""
		for _, value := range command.Env {
			if bytes.HasPrefix([]byte(value), []byte("EVENTBUS_LAMBDA_REQUEST_ID=")) {
				id = value[len("EVENTBUS_LAMBDA_REQUEST_ID="):]
			}
		}
		cleanupDone <- joinedChild{id, command.Process.Pid, command.ProcessState != nil, err}
		return err
	}
	slow, err := service.Admit(context.Background(), InvokeInput{FunctionName: "slow", Payload: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	waitForFile(t, slowStarted)
	quick, err := service.Admit(context.Background(), InvokeInput{FunctionName: "quick", Payload: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	blocked := awaitAsyncOwnership(t, writer.entered, "quick terminal capture")
	if blocked.RequestID != quick.RequestID {
		t.Fatalf("terminal capture identity: %#v, want %s", blocked, quick.RequestID)
	}
	quickChild := awaitAsyncOwnership(t, cleanupDone, "quick native child join")
	if quickChild.requestID != quick.RequestID || !quickChild.waited || quickChild.err != nil || processAlive(quickChild.pid) {
		t.Fatalf("terminal capture preceded actual native join: %#v", quickChild)
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	closeDone := make(chan error, 1)
	go func() { closeDone <- service.Close(closeCtx) }()
	// This child must be killed and Wait-reaped while another worker's writer
	// remains blocked. Service-wide cancellation cannot wait on transitionMu.
	slowChild := awaitAsyncOwnership(t, cleanupDone, "slow native cancellation and join")
	if slowChild.requestID != slow.RequestID || !slowChild.waited || slowChild.err != nil || processAlive(slowChild.pid) {
		t.Fatalf("owned child did not cancel promptly: %#v", slowChild)
	}
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before terminal capture: %v", err)
	default:
	}
	active, _, completed := observer.snapshot()
	if active != 2 || len(completed) != 0 {
		t.Fatalf("native joins incorrectly released capture lifetimes: active=%d completed=%#v", active, completed)
	}
	writer.open()
	if err := awaitAsyncOwnership(t, closeDone, "joined Close"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close deadline attribution: %v", err)
	}
	assertAsyncOwnershipCapture(t, writer, quick.RequestID, "queued", "running", "succeeded")
	assertAsyncOwnershipCapture(t, writer, slow.RequestID, "queued", "running", "canceled")
	active, _, completed = observer.snapshot()
	if active != 0 || len(completed) != 2 || completed[0].err != nil || completed[1].err != nil {
		t.Fatalf("joined terminal owners: active=%d completed=%#v", active, completed)
	}
	if _, err := os.Stat(slowOutput); !os.IsNotExist(err) {
		t.Fatalf("canceled child completed its handler: %v", err)
	}
}

// Keep compile-time writer ownership explicit: this fixture borrows a writer,
// while the real durable-file policy remains exercised by the performance lane.
var _ io.Writer = (*asyncOwnershipCapture)(nil)
