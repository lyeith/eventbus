package lambda

import (
	"encoding/json"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lyeith/eventbus/internal/devcapture"
)

type shutdownCaptureGate struct {
	state   string
	entered chan struct{}
	proceed chan struct{}
	once    sync.Once
	mu      sync.Mutex
	records []AsyncRecord
	err     error
}

func (capture *shutdownCaptureGate) Write(data []byte) (int, error) {
	var record AsyncRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return 0, err
	}
	if record.State == capture.state {
		capture.once.Do(func() { close(capture.entered); <-capture.proceed })
	}
	capture.mu.Lock()
	capture.records = append(capture.records, record)
	capture.mu.Unlock()
	if capture.err != nil {
		return 0, capture.err
	}
	return len(data), nil
}
func shutdownOwner(t *testing.T, state string, attempts int, ready time.Time, writer io.Writer) (*Service, *asyncTask) {
	t.Helper()
	if writer == nil {
		writer = io.Discard
	}
	id := uuid.NewString()
	task := &asyncTask{record: AsyncRecord{SchemaVersion: "eventbus.lambda.async.v1", RequestID: id, FunctionName: "event", State: state, Attempts: attempts, QueuedAt: time.Now()}, readyAt: ready}
	if state == "retrying" {
		task.record.NextAttemptAt = ready
	}
	sink := devcapture.NewWriter(writer, "shutdown owner")
	t.Cleanup(func() { _ = sink.Close() })
	service := &Service{asyncTasks: map[string]*asyncTask{id: task}, asyncOutstanding: 1, asyncHistoryLimit: 2, asyncWake: make(chan struct{}), asyncCapture: sink, asyncRetryDelays: [2]time.Duration{time.Minute, 2 * time.Minute}}
	if state == "queued" || state == "retrying" {
		service.asyncQueue = []*asyncTask{task}
	}
	return service, task
}
func requireShutdownDiscard(t *testing.T, record AsyncRecord, attempts int) {
	t.Helper()
	if record.State != "canceled" || record.ErrorType != devAsyncShutdownDiscarded || record.Attempts != attempts || !record.NextAttemptAt.IsZero() || record.CompletedAt.IsZero() {
		t.Fatalf("discard record:%+v", record)
	}
}
func TestDevAsyncShutdownCancelsEligibleAndFuturePendingOnce(t *testing.T) {
	for _, state := range []string{"queued", "retrying"} {
		for _, delay := range []time.Duration{0, time.Hour} {
			service, task := shutdownOwner(t, state, 1, time.Now().Add(delay), nil)
			releases := 0
			task.release = func(err error) {
				if err != nil {
					t.Errorf("intentional discard dirtied ownership:%v", err)
				}
				releases++
			}
			service.DevDiscardPendingAsync()
			service.DevDiscardPendingAsync()
			claimed, wake, _, stopped := service.pollAsync()
			if claimed != nil || wake != nil || stopped {
				t.Fatal("pending discard became a claimed execution")
			}
			requireShutdownDiscard(t, service.AsyncSnapshot()[0], 1)
			if service.DevDiscardedAsyncCount() != 1 || releases != 1 || service.asyncOutstanding != 0 {
				t.Fatal("discard did not settle/count exactly once")
			}
			_, _, _, stopped = service.pollAsync()
			if !stopped || service.DevDiscardedAsyncCount() != 1 {
				t.Fatal("repeated policy changed final count")
			}
		}
	}
	var absent *Service
	absent.DevDiscardPendingAsync()
	if absent.DevDiscardedAsyncCount() != 0 {
		t.Fatal("nil owner counted work")
	}
}
func TestDevAsyncShutdownActiveOutcomesPreserveNativeBudgetAndErrors(t *testing.T) {
	uncertain := errors.New("fixture ownership uncertainty")
	for _, test := range []struct {
		name                string
		attempts            int
		result              invocationResult
		err                 error
		state, errorType    string
		wantAttempts, count int
		ownership           error
	}{
		{"success", 1, invocationResult{}, nil, "succeeded", "", 1, 0, nil},
		{"retry-remaining", 1, failure("Error", "actual failure"), nil, "canceled", devAsyncShutdownDiscarded, 1, 1, nil},
		{"budget-exhausted", 3, failure("Error", "actual final failure"), nil, "failed", "FunctionError", 3, 0, nil},
		{"throttled", 1, invocationResult{}, invocationError(429, "TooManyRequestsException", "saturated"), "canceled", devAsyncShutdownDiscarded, 0, 1, nil},
		{"uncertain", 1, invocationResult{functionError: true, ownershipErr: uncertain}, nil, "canceled", devAsyncShutdownDiscarded, 1, 1, uncertain},
	} {
		t.Run(test.name, func(t *testing.T) {
			service, task := shutdownOwner(t, "running", test.attempts, time.Time{}, nil)
			service.DevDiscardPendingAsync()
			service.completeAsync(task, test.result, test.err)
			record := service.AsyncSnapshot()[0]
			if record.State != test.state || record.ErrorType != test.errorType || record.Attempts != test.wantAttempts || service.DevDiscardedAsyncCount() != test.count || len(service.asyncQueue) != 0 {
				t.Fatalf("active terminal:%+v count%d", record, service.DevDiscardedAsyncCount())
			}
			if !errors.Is(service.asyncDrainError(), test.ownership) {
				t.Fatalf("ownership evidence changed:%v", service.asyncDrainError())
			}
		})
	}
}
func TestDevAsyncShutdownRetryCaptureCannotRequeueAcrossFence(t *testing.T) {
	for _, throttle := range []bool{false, true} {
		writer := &shutdownCaptureGate{state: "retrying", entered: make(chan struct{}), proceed: make(chan struct{})}
		var once sync.Once
		open := func() { once.Do(func() { close(writer.proceed) }) }
		defer open()
		service, task := shutdownOwner(t, "running", 1, time.Time{}, writer)
		done := make(chan struct{})
		go func() {
			result, err := failure("Error", "actual failure"), error(nil)
			if throttle {
				result = invocationResult{}
				err = invocationError(429, "TooManyRequestsException", "saturated")
			}
			service.completeAsync(task, result, err)
			close(done)
		}()
		<-writer.entered
		begin := make(chan struct{})
		go func() { service.DevDiscardPendingAsync(); close(begin) }()
		select {
		case <-begin:
		case <-time.After(time.Second):
			t.Fatal("begin waited on retry transition/capture")
		}
		if service.AsyncSnapshot()[0].State != "running" || service.DevDiscardedAsyncCount() != 0 {
			t.Fatal("uncaptured retry/cancellation published")
		}
		open()
		<-done
		attempts := 1
		if throttle {
			attempts = 0
		}
		requireShutdownDiscard(t, service.AsyncSnapshot()[0], attempts)
		if service.DevDiscardedAsyncCount() != 1 || len(service.asyncQueue) != 0 {
			t.Fatal("capture spanning fence requeued work")
		}
	}
}
func TestDevAsyncShutdownCountsOnlyPublishedTerminalAndRetainsCaptureFailure(t *testing.T) {
	writer := &shutdownCaptureGate{state: "canceled", entered: make(chan struct{}), proceed: make(chan struct{})}
	var once sync.Once
	defer once.Do(func() { close(writer.proceed) })
	service, _ := shutdownOwner(t, "queued", 0, time.Now(), writer)
	service.DevDiscardPendingAsync()
	done := make(chan struct{})
	go func() { service.pollAsync(); close(done) }()
	<-writer.entered
	service.DevDiscardPendingAsync()
	if service.DevDiscardedAsyncCount() != 0 || service.AsyncSnapshot()[0].State != "queued" {
		t.Fatal("terminal count preceded evidence capture")
	}
	once.Do(func() { close(writer.proceed) })
	<-done
	if service.DevDiscardedAsyncCount() != 1 {
		t.Fatal("terminal publication not counted")
	}
	failure := errors.New("fixture capture unavailable")
	failing := &shutdownCaptureGate{err: failure}
	service, _ = shutdownOwner(t, "queued", 0, time.Now(), failing)
	service.DevDiscardPendingAsync()
	service.pollAsync()
	if !errors.Is(service.asyncDrainError(), failure) || service.DevDiscardedAsyncCount() != 1 {
		t.Fatalf("failed capture erased cancellation/evidence:%v", service.asyncDrainError())
	}
}
