package lambda

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lyeith/eventbus/internal/devcapture"
	"github.com/stretchr/testify/require"
)

// These tests exercise owner and queue transactions without recreating runtime
// execution. SDK acceptance owns proof of actual handler effects and replay.
func completionLossOwner(t *testing.T, state string, attempts int) (*Service, *asyncTask, *bytes.Buffer) {
	t.Helper()
	capture := new(bytes.Buffer)
	sink := devcapture.NewWriter(capture, "completion loss owner")
	t.Cleanup(func() { require.NoError(t, sink.Close()) })
	entry := executableFunction{name: "processor:live"}
	id := uuid.NewString()
	task := &asyncTask{
		entry: entry,
		input: invocation{name: entry.name, requestID: id, attempt: attempts, payload: []byte(" { \"native\" : true } \n")},
		record: AsyncRecord{
			SchemaVersion: "eventbus.lambda.async.v1", RequestID: id, FunctionName: entry.name,
			State: state, Attempts: attempts, QueuedAt: time.Now().UTC(),
		},
	}
	service := &Service{
		functions: map[string]executableFunction{
			entry.name: entry, "processor": {name: "processor"}, "other:live": {name: "other:live"},
		},
		asyncCompletionLossEnabled: true,
		asyncTasks:                 map[string]*asyncTask{id: task}, asyncOutstanding: 1,
		asyncHistoryLimit: 8, asyncCapture: sink, asyncWake: make(chan struct{}),
	}
	return service, task, capture
}

func requireCompletionLossError(t *testing.T, err error, status int, code string) {
	t.Helper()
	var control *DevAsyncControlError
	require.ErrorAs(t, err, &control)
	require.Equal(t, status, control.Status)
	require.Equal(t, code, control.Code)
}

func completionLossCaptured(t *testing.T, capture *bytes.Buffer) []AsyncRecord {
	t.Helper()
	var records []AsyncRecord
	decoder := json.NewDecoder(bytes.NewReader(capture.Bytes()))
	for {
		var record AsyncRecord
		err := decoder.Decode(&record)
		if errors.Is(err, io.EOF) {
			return records
		}
		require.NoError(t, err)
		records = append(records, record)
	}
}

func pollCompletionLossRetry(t *testing.T, service *Service, task *asyncTask) {
	t.Helper()
	selected, wake, _, stopped := service.pollAsync()
	require.Same(t, task, selected)
	require.Nil(t, wake)
	require.False(t, stopped)
	require.Empty(t, service.asyncQueue)
	task.input.attempt = task.record.Attempts
}

func TestDevAsyncCompletionLossDisabledByDefault(t *testing.T) {
	for _, config := range []*DevAsyncConfig{nil, {LogWriter: io.Discard}} {
		service, err := NewService(&Config{Functions: map[string]Function{"event": providedFunction(t, "echo")}, DevAsync: config}, t.TempDir())
		require.NoError(t, err)
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			require.NoError(t, service.Close(ctx))
		})
		require.False(t, service.asyncCompletionLossEnabled)
		_, disabledErr := service.DevAdmitAsyncCompletionLoss(t.Context(), InvokeInput{FunctionName: "event", Payload: []byte("{}")})
		requireCompletionLossError(t, disabledErr, 404, "completion_loss_disabled")
		_, err = service.DevArmAsyncCompletionLoss(t.Context(), "event", uuid.NewString(), 1)
		requireCompletionLossError(t, err, 404, "completion_loss_disabled")
		_, err = service.DevClearAsyncCompletionLoss(t.Context(), "event", uuid.NewString(), 1)
		requireCompletionLossError(t, err, 404, "completion_loss_disabled")
	}
	var absent *Service
	_, err := absent.DevArmAsyncCompletionLoss(t.Context(), "event", uuid.NewString(), 1)
	requireCompletionLossError(t, err, 404, "completion_loss_disabled")
}

func TestDevAsyncCompletionLossSelectsExactLiveAttempt(t *testing.T) {
	for _, scenario := range []struct {
		state            string
		attempts, target int
	}{{"queued", 0, 1}, {"running", 1, 1}, {"retrying", 1, 2}, {"retrying", 2, 3}} {
		t.Run(fmt.Sprintf("%s%d", scenario.state, scenario.attempts), func(t *testing.T) {
			service, task, _ := completionLossOwner(t, scenario.state, scenario.attempts)
			same, different := &asyncTask{record: task.record}, &asyncTask{record: task.record}
			same.record.RequestID = uuid.NewString()
			different.record.RequestID, different.record.FunctionName = uuid.NewString(), "other:live"
			service.asyncTasks[same.record.RequestID], service.asyncTasks[different.record.RequestID] = same, different
			before, payload := task.record, append([]byte(nil), task.input.payload...)
			arn := "arn:aws:lambda:us-east-1:000000000000:function:processor:live"
			armed, err := service.DevArmAsyncCompletionLoss(t.Context(), arn, task.record.RequestID, scenario.target)
			require.NoError(t, err)
			require.Equal(t, DevAsyncCompletionLoss{RequestID: task.record.RequestID, FunctionName: "processor:live", Attempt: scenario.target, Armed: true}, armed)
			repeated, err := service.DevArmAsyncCompletionLoss(t.Context(), "processor:live", task.record.RequestID, scenario.target)
			require.NoError(t, err)
			require.Equal(t, armed, repeated)
			_, err = service.DevArmAsyncCompletionLoss(t.Context(), "processor", task.record.RequestID, scenario.target)
			requireCompletionLossError(t, err, 404, "async_event_not_found")
			_, err = service.DevArmAsyncCompletionLoss(t.Context(), "other:live", task.record.RequestID, scenario.target)
			requireCompletionLossError(t, err, 404, "async_event_not_found")
			_, err = service.DevArmAsyncCompletionLoss(t.Context(), "processor:live", uuid.NewString(), scenario.target)
			requireCompletionLossError(t, err, 404, "async_event_not_found")
			wrong := scenario.target%3 + 1
			_, err = service.DevArmAsyncCompletionLoss(t.Context(), "processor:live", task.record.RequestID, wrong)
			requireCompletionLossError(t, err, 409, "attempt_not_armable")
			_, err = service.DevClearAsyncCompletionLoss(t.Context(), "processor:live", task.record.RequestID, wrong)
			requireCompletionLossError(t, err, 409, "different_attempt_armed")
			require.Equal(t, scenario.target, task.completionLossAttempt)
			require.Zero(t, same.completionLossAttempt)
			require.Zero(t, different.completionLossAttempt)
			require.Equal(t, before, task.record)
			require.Equal(t, payload, task.input.payload)
			for index := 0; index < 2; index++ {
				cleared, err := service.DevClearAsyncCompletionLoss(t.Context(), "processor:live", task.record.RequestID, scenario.target)
				require.NoError(t, err)
				require.False(t, cleared.Armed)
				require.Zero(t, task.completionLossAttempt)
			}
		})
	}
}

func TestDevAsyncCompletionLossDecisionConsumesOneShot(t *testing.T) {
	for _, scenario := range []struct {
		name               string
		succeeded, aborted bool
	}{{"successful", true, false}, {"handler failure", false, false}, {"canceled", false, true}, {"shutdown after success", true, true}} {
		t.Run(scenario.name, func(t *testing.T) {
			service, task, capture := completionLossOwner(t, "running", 1)
			_, err := service.DevArmAsyncCompletionLoss(t.Context(), "processor:live", task.record.RequestID, 1)
			require.NoError(t, err)
			service.asyncAborted = scenario.aborted
			injected := service.devCompleteAsyncAttempt(task.input, scenario.succeeded)
			require.Equal(t, scenario.succeeded && !scenario.aborted, injected)
			require.Zero(t, task.completionLossAttempt)
			require.Equal(t, 1, task.completionDecidedAttempt)
			require.False(t, service.devCompleteAsyncAttempt(task.input, scenario.succeeded))
			_, err = service.DevArmAsyncCompletionLoss(t.Context(), "processor:live", task.record.RequestID, 1)
			requireCompletionLossError(t, err, 409, "attempt_not_armable")
			if scenario.aborted || !scenario.succeeded {
				result := invocationResult{state: InvocationFailed, functionError: true, payload: []byte(`{"errorType":"ApplicationFailure","errorMessage":"private"}`)}
				var executionErr error
				if scenario.aborted {
					result.state, executionErr = InvocationCanceled, context.Canceled
				}
				service.completeAsync(task, result, executionErr)
				require.False(t, task.record.InjectedCompletionLoss)
				require.False(t, completionLossCaptured(t, capture)[0].InjectedCompletionLoss)
				if scenario.aborted {
					require.Equal(t, "canceled", task.record.State)
					require.Empty(t, service.asyncTasks)
				} else {
					require.Equal(t, "retrying", task.record.State)
					require.Equal(t, "FunctionError", task.record.ErrorType)
					pollCompletionLossRetry(t, service, task)
					require.False(t, service.devCompleteAsyncAttempt(task.input, true), "a failed native attempt consumed its rule")
				}
			}
		})
	}
}

func TestDevAsyncCompletionLossLateArmAndCloseFence(t *testing.T) {
	service, task, _ := completionLossOwner(t, "running", 1)
	require.False(t, service.devCompleteAsyncAttempt(task.input, true), "completion has no armed rule")
	// The native completion decision precedes private diagnostic capture and
	// async state publication: the task is still running but it is too late.
	require.Equal(t, "running", task.record.State)
	_, err := service.DevArmAsyncCompletionLoss(t.Context(), "processor:live", task.record.RequestID, 1)
	requireCompletionLossError(t, err, 409, "attempt_not_armable")
	for _, closeService := range []bool{false, true} {
		service, task, _ := completionLossOwner(t, "running", 1)
		_, err := service.DevArmAsyncCompletionLoss(t.Context(), "processor:live", task.record.RequestID, 1)
		require.NoError(t, err)
		service.closed, service.asyncClosed = closeService, !closeService
		_, err = service.DevArmAsyncCompletionLoss(t.Context(), "processor:live", task.record.RequestID, 1)
		requireCompletionLossError(t, err, 409, "async_service_closing")
		cleared, err := service.DevClearAsyncCompletionLoss(t.Context(), "processor:live", task.record.RequestID, 1)
		require.NoError(t, err, "admission fencing still permits clearing an active rule")
		require.False(t, cleared.Armed)
	}
}

func TestDevAsyncCompletionLossNormalRetryBudgetAndNativeResult(t *testing.T) {
	service, task, capture := completionLossOwner(t, "running", 1)
	releases := 0
	task.release = func(err error) { require.NoError(t, err); releases++ }
	payload := append([]byte(nil), task.input.payload...)
	result := invocationResult{state: InvocationSucceeded, payload: []byte(`{"committed":true}`), logs: []byte("original log")}
	for attempt := 1; attempt <= 3; attempt++ {
		require.Equal(t, attempt, task.record.Attempts)
		_, err := service.DevArmAsyncCompletionLoss(t.Context(), "processor:live", task.record.RequestID, attempt)
		require.NoError(t, err)
		result.devCompletionLoss = service.devCompleteAsyncAttempt(task.input, true)
		require.True(t, result.devCompletionLoss)
		before := result
		service.completeAsync(task, result, nil)
		require.Equal(t, before, result, "the native successful result is preserved")
		require.JSONEq(t, `{"committed":true}`, string(result.payload))
		require.Equal(t, "original log", string(result.logs))
		require.False(t, result.functionError)
		require.Equal(t, payload, task.input.payload)
		require.True(t, task.record.InjectedCompletionLoss)
		require.Equal(t, devCompletionLostError, task.record.ErrorType)
		if attempt < 3 {
			require.Equal(t, "retrying", task.record.State)
			require.Equal(t, 1, service.asyncOutstanding)
			require.Same(t, task, service.asyncTasks[task.record.RequestID])
			pollCompletionLossRetry(t, service, task)
			require.False(t, task.record.InjectedCompletionLoss)
		} else {
			require.Equal(t, "failed", task.record.State)
			require.Empty(t, service.asyncTasks)
			require.Zero(t, service.asyncOutstanding)
			require.Equal(t, 1, releases)
			_, err = service.DevArmAsyncCompletionLoss(t.Context(), "processor:live", task.record.RequestID, 3)
			requireCompletionLossError(t, err, 404, "async_event_not_found")
		}
	}
	records := completionLossCaptured(t, capture)
	require.Len(t, records, 5)
	require.Equal(t, "failed", records[4].State)
	require.True(t, records[4].InjectedCompletionLoss)
}

func TestDevAsyncCompletionLossSubsequentSuccessClearsFlag(t *testing.T) {
	service, task, capture := completionLossOwner(t, "running", 1)
	_, err := service.DevArmAsyncCompletionLoss(t.Context(), "processor:live", task.record.RequestID, 1)
	require.NoError(t, err)
	lost := service.devCompleteAsyncAttempt(task.input, true)
	service.completeAsync(task, invocationResult{state: InvocationSucceeded, devCompletionLoss: lost}, nil)
	pollCompletionLossRetry(t, service, task)
	require.False(t, service.devCompleteAsyncAttempt(task.input, true))
	service.completeAsync(task, invocationResult{state: InvocationSucceeded}, nil)
	require.Equal(t, "succeeded", task.record.State)
	require.False(t, task.record.InjectedCompletionLoss)
	require.Empty(t, task.record.ErrorType)
	require.Equal(t, 2, task.record.Attempts)
	records := completionLossCaptured(t, capture)
	require.Len(t, records, 3)
	require.True(t, records[0].InjectedCompletionLoss)
	require.False(t, records[1].InjectedCompletionLoss)
	require.False(t, records[2].InjectedCompletionLoss)
}

func TestDevAsyncCompletionLossConcurrentArmCompletionCutoff(t *testing.T) {
	for index := 0; index < 100; index++ {
		service, task, _ := completionLossOwner(t, "running", 1)
		start := make(chan struct{})
		var joined sync.WaitGroup
		joined.Add(2)
		var armErr error
		var injected bool
		go func() {
			defer joined.Done()
			<-start
			_, armErr = service.DevArmAsyncCompletionLoss(t.Context(), "processor:live", task.record.RequestID, 1)
		}()
		go func() {
			defer joined.Done()
			<-start
			injected = service.devCompleteAsyncAttempt(task.input, true)
		}()
		close(start)
		joined.Wait()
		if armErr == nil {
			require.True(t, injected, "an acknowledged live arm must win the completion decision")
		} else {
			requireCompletionLossError(t, armErr, 409, "attempt_not_armable")
			require.False(t, injected)
		}
		require.Zero(t, task.completionLossAttempt)
		require.Equal(t, 1, task.completionDecidedAttempt)
	}
}

func TestAsyncThrottleRetainsEventAttemptAndPendingCompletionLoss(t *testing.T) {
	for _, attempt := range []int{1, 2} {
		t.Run(fmt.Sprint(attempt), func(t *testing.T) {
			service, task, capture := completionLossOwner(t, "running", attempt)
			_, err := service.DevArmAsyncCompletionLoss(t.Context(), "processor:live", task.record.RequestID, attempt)
			require.NoError(t, err)
			payload := append([]byte(nil), task.input.payload...)
			throttled := invocationError(429, "TooManyRequestsException", "ConcurrentInvocationLimitExceeded")
			delays := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 32 * time.Second, time.Minute, time.Minute}
			for _, delay := range delays {
				before := time.Now()
				service.completeAsync(task, invocationResult{state: InvocationNotStarted}, throttled)
				after := time.Now()
				require.Equal(t, attempt-1, task.record.Attempts, "nonlaunch throttles consume no handler attempt")
				require.Equal(t, "retrying", task.record.State)
				require.Equal(t, "TooManyRequestsException", task.record.ErrorType)
				require.False(t, task.record.InjectedCompletionLoss)
				require.Equal(t, attempt, task.completionLossAttempt)
				require.Zero(t, task.completionDecidedAttempt)
				require.Equal(t, 1, service.asyncOutstanding)
				require.Len(t, service.asyncQueue, 1)
				require.Same(t, task, service.asyncTasks[task.record.RequestID])
				require.False(t, task.readyAt.Before(before.Add(delay)))
				require.False(t, task.readyAt.After(after.Add(delay)))
				require.Equal(t, task.readyAt.UTC(), task.record.NextAttemptAt)
				require.Equal(t, payload, task.input.payload)
				// Advance queue eligibility without waiting for wall-clock backoff.
				task.readyAt = time.Now().Add(-time.Second)
				pollCompletionLossRetry(t, service, task)
				require.Equal(t, attempt, task.record.Attempts)
			}
			require.Equal(t, len(delays), task.throttleRetries)
			releases := 0
			task.release = func(err error) { require.NoError(t, err); releases++ }
			service.completeAsync(task, invocationResult{state: InvocationNotStarted}, throttled)
			task.readyAt = time.Now().Add(-time.Second)
			task.record.QueuedAt = time.Now().Add(-maxEventAge - time.Second)
			selected, wake, _, stopped := service.pollAsync()
			require.Nil(t, selected)
			require.Nil(t, wake)
			require.False(t, stopped)
			require.Equal(t, "failed", task.record.State)
			require.Equal(t, "EventAgeExceeded", task.record.ErrorType)
			require.Equal(t, attempt-1, task.record.Attempts)
			require.False(t, task.record.InjectedCompletionLoss)
			require.Empty(t, service.asyncTasks)
			require.Empty(t, service.asyncQueue)
			require.Zero(t, service.asyncOutstanding)
			require.Equal(t, 1, releases)
			last := completionLossCaptured(t, capture)
			require.Equal(t, "EventAgeExceeded", last[len(last)-1].ErrorType)
		})
	}
}

type completionLossBrokenReader struct{}

func (completionLossBrokenReader) Read([]byte) (int, error) {
	return 0, errors.New("private read failure")
}

func TestDevAsyncCompletionLossHTTPStrictRequests(t *testing.T) {
	valid := `{"function_name":"processor:live","attempt":1}`
	for _, scenario := range []struct {
		name, body, code string
		status           int
	}{
		{"valid", valid, "", 200},
		{"exact limit", valid + strings.Repeat(" ", 4096-len(valid)), "", 200},
		{"over limit", valid + strings.Repeat(" ", 4097-len(valid)), "invalid_completion_loss_request", 400},
		{"unknown field", `{"function_name":"processor:live","attempt":1,"loss":true}`, "invalid_completion_loss_request", 400},
		{"extra document", valid + `{}`, "invalid_completion_loss_request", 400},
		{"wrong function type", `{"function_name":7,"attempt":1}`, "invalid_completion_loss_request", 400},
		{"wrong attempt type", `{"function_name":"processor:live","attempt":"1"}`, "invalid_completion_loss_request", 400},
		{"fraction attempt", `{"function_name":"processor:live","attempt":1.5}`, "invalid_completion_loss_request", 400},
		{"array", `[]`, "invalid_completion_loss_request", 400},
		{"empty", "", "invalid_completion_loss_request", 400},
		{"null", `null`, "invalid_completion_loss_target", 400},
		{"missing target", `{}`, "invalid_completion_loss_target", 400},
		{"zero attempt", `{"function_name":"processor:live","attempt":0}`, "invalid_completion_loss_target", 400},
		{"fourth attempt", `{"function_name":"processor:live","attempt":4}`, "invalid_completion_loss_target", 400},
		{"wrong live attempt", `{"function_name":"processor:live","attempt":2}`, "attempt_not_armable", 409},
		{"unknown function", `{"function_name":"missing","attempt":1}`, "function_not_found", 404},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			service, task, _ := completionLossOwner(t, "running", 1)
			request := httptest.NewRequest(http.MethodPut, devAsyncPrefix+"events/"+task.record.RequestID+"/completion-loss", strings.NewReader(scenario.body))
			result := httptest.NewRecorder()
			NewDevAsyncHandler(service).ServeHTTP(result, request)
			require.Equal(t, scenario.status, result.Code, result.Body.String())
			require.Equal(t, "application/json", result.Header().Get("Content-Type"))
			require.Empty(t, result.Header().Get("X-Amzn-RequestId"))
			if scenario.status == 200 {
				var armed DevAsyncCompletionLoss
				require.NoError(t, json.Unmarshal(result.Body.Bytes(), &armed))
				require.Equal(t, task.record.RequestID, armed.RequestID)
				require.True(t, armed.Armed)
			} else {
				var failure map[string]string
				require.NoError(t, json.Unmarshal(result.Body.Bytes(), &failure))
				require.Equal(t, map[string]string{"error": scenario.code}, failure)
				require.Zero(t, task.completionLossAttempt)
			}
		})
	}
}

func TestDevAsyncCompletionLossHTTPRoutesMethodsAndErrors(t *testing.T) {
	service, task, _ := completionLossOwner(t, "running", 1)
	route := devAsyncPrefix + "events/" + task.record.RequestID + "/completion-loss"
	for _, scenario := range []struct {
		method, path, allow, code string
		status                    int
	}{
		{"POST", devAsyncPrefix + "events", "GET", "async_snapshot_requires_get", 405},
		{"GET", route, "PUT, DELETE", "completion_loss_requires_put_or_delete", 405},
		{"PUT", devAsyncPrefix + "events//completion-loss", "", "async_control_route_not_found", 404},
		{"PUT", devAsyncPrefix + "events/nested/id/completion-loss", "", "async_control_route_not_found", 404},
		{"PUT", "/2015-03-31/functions/processor/invocations", "", "async_control_route_not_found", 404},
		{"PUT", devAsyncPrefix + "events/not-a-uuid/completion-loss", "", "invalid_async_request_id", 400},
		{"PUT", devAsyncPrefix + "events/" + uuid.NewString() + "/completion-loss", "", "async_event_not_found", 404},
	} {
		result := httptest.NewRecorder()
		NewDevAsyncHandler(service).ServeHTTP(result, httptest.NewRequest(scenario.method, scenario.path, strings.NewReader(`{"function_name":"processor:live","attempt":1}`)))
		require.Equal(t, scenario.status, result.Code, result.Body.String())
		require.Equal(t, scenario.allow, result.Header().Get("Allow"))
		require.Contains(t, result.Body.String(), scenario.code)
	}
	for _, disabled := range []*Service{nil, {}} {
		result := httptest.NewRecorder()
		NewDevAsyncHandler(disabled).ServeHTTP(result, httptest.NewRequest("GET", devAsyncPrefix+"events", nil))
		require.Equal(t, 404, result.Code)
		require.Contains(t, result.Body.String(), "completion_loss_disabled")
	}
	request := httptest.NewRequest("PUT", route, nil)
	request.Body = io.NopCloser(completionLossBrokenReader{})
	result := httptest.NewRecorder()
	NewDevAsyncHandler(service).ServeHTTP(result, request)
	require.Equal(t, 400, result.Code)
	require.NotContains(t, result.Body.String(), "private")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	request = httptest.NewRequest("PUT", route, strings.NewReader(`{"function_name":"processor:live","attempt":1}`)).WithContext(ctx)
	result = httptest.NewRecorder()
	NewDevAsyncHandler(service).ServeHTTP(result, request)
	require.Equal(t, 503, result.Code)
	require.Contains(t, result.Body.String(), "async_control_interrupted")
	status, code := devAsyncHTTPError(errors.New("private detail"))
	require.Equal(t, 500, status)
	require.Equal(t, "async_control_failed", code)
	status, code = devAsyncHTTPError(context.DeadlineExceeded)
	require.Equal(t, 503, status)
	require.Equal(t, "async_control_interrupted", code)
}

func TestDevAsyncCompletionLossHTTPSnapshotAndClear(t *testing.T) {
	service, task, _ := completionLossOwner(t, "running", 1)
	task.input.payload = []byte(`{"private-token":"not-in-evidence"}`)
	result := httptest.NewRecorder()
	handler := NewDevAsyncHandler(service)
	handler.ServeHTTP(result, httptest.NewRequest("GET", devAsyncPrefix+"events", nil))
	require.Equal(t, 200, result.Code)
	var snapshot struct {
		Events []AsyncRecord `json:"events"`
	}
	require.NoError(t, json.Unmarshal(result.Body.Bytes(), &snapshot))
	require.Equal(t, []AsyncRecord{task.record}, snapshot.Events)
	require.NotContains(t, result.Body.String(), "not-in-evidence")
	_, err := service.DevArmAsyncCompletionLoss(t.Context(), "processor:live", task.record.RequestID, 1)
	require.NoError(t, err)
	for index := 0; index < 2; index++ {
		result = httptest.NewRecorder()
		handler.ServeHTTP(result, httptest.NewRequest("DELETE", devAsyncPrefix+"events/"+task.record.RequestID+"/completion-loss",
			strings.NewReader(`{"function_name":"processor:live","attempt":1}`)))
		require.Equal(t, 200, result.Code)
		var cleared DevAsyncCompletionLoss
		require.NoError(t, json.Unmarshal(result.Body.Bytes(), &cleared))
		require.Equal(t, DevAsyncCompletionLoss{RequestID: task.record.RequestID, FunctionName: "processor:live", Attempt: 1}, cleared)
	}
}

func TestDevAsyncCompletionLossAtomicAdmissionRunsUngatedHandler(t *testing.T) {
	output := filepath.Join(t.TempDir(), "effects.jsonl")
	service := newAsyncTestService(t, map[string]Function{"processor:live": asyncCommand(t, output, "", "", "success")}, t.TempDir(),
		&DevAsyncConfig{CompletionLoss: true, Workers: 1, RetryDelays: []time.Duration{0, 0}, LogWriter: io.Discard})
	payload := []byte(" { \"id\" : \"real-ungated\" } \n")
	original := append([]byte(nil), payload...)
	admission, err := service.DevAdmitAsyncCompletionLoss(t.Context(), InvokeInput{FunctionName: "processor:live", Payload: payload})
	require.NoError(t, err)
	completed := waitAsyncState(t, service, admission.RequestID, "succeeded")
	require.Equal(t, 2, completed.Attempts)
	require.False(t, completed.InjectedCompletionLoss)
	require.Equal(t, original, payload)
	data, err := os.ReadFile(output)
	require.NoError(t, err)
	lines := bytes.Split(bytes.TrimSpace(data), []byte("\n"))
	require.Len(t, lines, 2, "the unchanged fast handler actually committed both side effects")
	for _, line := range lines {
		var effect struct{ ID, Payload string }
		require.NoError(t, json.Unmarshal(line, &effect))
		require.Equal(t, "real-ungated", effect.ID)
		decoded, err := base64.StdEncoding.DecodeString(effect.Payload)
		require.NoError(t, err)
		require.Equal(t, original, decoded)
	}
	ordinary, err := service.Admit(t.Context(), InvokeInput{FunctionName: "processor:live", Payload: []byte(`{"id":"ordinary"}`)})
	require.NoError(t, err)
	require.Equal(t, 1, waitAsyncState(t, service, ordinary.RequestID, "succeeded").Attempts)
}

func TestDevAsyncCompletionLossHTTPAtomicAdmission(t *testing.T) {
	for _, scenario := range []struct {
		name, body, code string
		capacity, status int
	}{
		{"native qualified target", `{"function_name":"processor","qualifier":"live","payload":{ "native" : true }}`, "", 4, 202},
		{"native exact payload limit", `{"function_name":"processor:live","payload":"` + strings.Repeat("x", maxEventPayload-2) + `"}`, "", 4, 202},
		{"native oversized payload", `{"function_name":"processor:live","payload":"` + strings.Repeat("x", maxEventPayload-1) + `"}`, "RequestTooLargeException", 4, 413},
		{"same native capacity", `{"function_name":"processor:live","payload":{}}`, "TooManyRequestsException", 1, 429},
		{"unknown field", `{"function_name":"processor:live","payload":{},"loss":true}`, "invalid_completion_loss_request", 4, 400},
		{"extra document", `{"function_name":"processor:live","payload":{}} {}`, "invalid_completion_loss_request", 4, 400},
		{"missing payload", `{"function_name":"processor:live"}`, "InvalidRequestContentException", 4, 400},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			service, existing, _ := completionLossOwner(t, "running", 1)
			service.asyncCapacity = scenario.capacity
			before := existing.record
			result := httptest.NewRecorder()
			NewDevAsyncHandler(service).ServeHTTP(result, httptest.NewRequest("POST", devAsyncPrefix+"events/completion-loss", strings.NewReader(scenario.body)))
			require.Equal(t, scenario.status, result.Code, result.Body.String())
			require.Equal(t, before, existing.record)
			require.Zero(t, existing.completionLossAttempt)
			if result.Code == 202 {
				var reply struct {
					RequestID string `json:"request_id"`
					Armed     bool   `json:"armed"`
				}
				require.NoError(t, json.Unmarshal(result.Body.Bytes(), &reply))
				require.True(t, reply.Armed)
				require.NotEqual(t, existing.record.RequestID, reply.RequestID)
				admitted := service.asyncTasks[reply.RequestID]
				require.NotNil(t, admitted)
				require.Equal(t, 1, admitted.completionLossAttempt)
				require.Equal(t, "queued", admitted.record.State)
				require.Zero(t, admitted.record.Attempts)
				require.Equal(t, "processor:live", admitted.record.FunctionName)
				require.Len(t, service.asyncQueue, 1)
				require.Equal(t, 2, service.asyncOutstanding)
				if scenario.name == "native qualified target" {
					require.Equal(t, []byte(`{ "native" : true }`), admitted.input.payload)
				}
			} else {
				require.Contains(t, result.Body.String(), scenario.code)
				require.Len(t, service.asyncTasks, 1)
				require.Empty(t, service.asyncQueue)
				require.Equal(t, 1, service.asyncOutstanding)
				if result.Code == 429 {
					require.Equal(t, "1", result.Header().Get("Retry-After"))
				}
			}
		})
	}
	service, _, _ := completionLossOwner(t, "running", 1)
	result := httptest.NewRecorder()
	NewDevAsyncHandler(service).ServeHTTP(result, httptest.NewRequest("PUT", devAsyncPrefix+"events/completion-loss", nil))
	require.Equal(t, 405, result.Code)
	require.Equal(t, "POST", result.Header().Get("Allow"))
	require.Contains(t, result.Body.String(), "completion_loss_admission_requires_post")
}

type completionLossDiagnosticGate struct {
	entered                chan invocationDiagnosticRecord
	release                chan struct{}
	blockOnce, releaseOnce sync.Once
	mu                     sync.Mutex
	capture                bytes.Buffer
}

func (writer *completionLossDiagnosticGate) Write(data []byte) (int, error) {
	var record invocationDiagnosticRecord
	if err := json.Unmarshal(bytes.TrimSpace(data), &record); err != nil {
		return 0, err
	}
	writer.blockOnce.Do(func() {
		writer.entered <- record
		<-writer.release
	})
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.capture.Write(data)
}

func (writer *completionLossDiagnosticGate) open() {
	writer.releaseOnce.Do(func() { close(writer.release) })
}

func TestDevAsyncCompletionLossNativeDecisionPrecedesBlockedDiagnostics(t *testing.T) {
	service := newAsyncTestService(t, map[string]Function{"processor:live": providedFunction(t, "echo")}, t.TempDir(),
		&DevAsyncConfig{CompletionLoss: true, Workers: 1, RetryDelays: []time.Duration{0, 0}, LogWriter: io.Discard})
	writer := &completionLossDiagnosticGate{entered: make(chan invocationDiagnosticRecord, 1), release: make(chan struct{})}
	service.diagnosticCapture = devcapture.NewWriter(writer, "gated private invocation diagnostics")
	t.Cleanup(writer.open) // Release before the service's joined cleanup.
	admission, err := service.Admit(t.Context(), InvokeInput{FunctionName: "processor:live", Payload: []byte(`{"native":true}`)})
	require.NoError(t, err)
	select {
	case completed := <-writer.entered:
		require.Equal(t, admission.RequestID, completed.RequestID)
		require.Equal(t, InvocationSucceeded, completed.State)
		require.False(t, completed.FunctionError)
		require.True(t, completed.OwnershipConfirmed)
	case <-time.After(10 * time.Second):
		t.Fatal("actual handler did not reach its private diagnostic writer")
	}
	snapshot := service.AsyncSnapshot()
	require.Len(t, snapshot, 1)
	require.Equal(t, "running", snapshot[0].State, "async state publication is still blocked by private capture")
	_, err = service.DevArmAsyncCompletionLoss(t.Context(), "processor:live", admission.RequestID, 1)
	requireCompletionLossError(t, err, 409, "attempt_not_armable")
	writer.open()
	completed := waitAsyncState(t, service, admission.RequestID, "succeeded")
	require.Equal(t, 1, completed.Attempts)
	require.False(t, completed.InjectedCompletionLoss)
	writer.mu.Lock()
	data := append([]byte(nil), writer.capture.Bytes()...)
	writer.mu.Unlock()
	var diagnostic invocationDiagnosticRecord
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(data), &diagnostic))
	require.False(t, diagnostic.InjectedCompletionLoss)
	require.False(t, diagnostic.FunctionError)
}

func TestAsyncThrottleActualWarmCapacityPreservesAtomicCompletionLoss(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("Node unavailable")
	}
	directory := t.TempDir()
	started, gate, effects := filepath.Join(directory, "started"), filepath.Join(directory, "release"), filepath.Join(directory, "effects.jsonl")
	writeFixture(t, directory, "capacity.mjs", `import fs from 'node:fs';
export async function holder(event) {
  fs.writeFileSync(process.env.STARTED, 'running');
  while (!fs.existsSync(process.env.RELEASE)) await new Promise(resolve=>setTimeout(resolve,5));
  return {released:true};
}
export function processor(event, context) {
  fs.appendFileSync(process.env.EFFECTS, JSON.stringify({event,request_id:context.awsRequestId})+'\n');
  return {committed:true};
}
`)
	function := func(export string) Function {
		return Function{Runtime: "node", Command: []string{node}, Handler: "capacity.mjs#" + export, Timeout: 12 * time.Second,
			Environment: map[string]string{"STARTED": started, "RELEASE": gate, "EFFECTS": effects}}
	}
	service, err := NewService(&Config{
		Functions: map[string]Function{"holder": function("holder"), "processor:live": function("processor")},
		DevWarm:   &DevWarmConfig{MaxWorkers: 1, MaxConcurrency: 1},
		DevAsync:  &DevAsyncConfig{CompletionLoss: true, Workers: 1, Capacity: 4, RetryDelays: []time.Duration{0, 0}, LogWriter: io.Discard},
	}, directory)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = os.WriteFile(gate, nil, 0600)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		require.NoError(t, service.Close(ctx))
	})
	type executed struct {
		output InvokeOutput
		err    error
	}
	holder := make(chan executed, 1)
	go func() {
		output, err := service.Execute(context.Background(), InvokeInput{FunctionName: "holder", Payload: []byte("{}")})
		holder <- executed{output, err}
	}()
	waitForFile(t, started)
	payload := []byte(`{ "id" : "real-nonlaunch-throttle", "unchanged" : true }`)
	admission, err := service.DevAdmitAsyncCompletionLoss(t.Context(), InvokeInput{FunctionName: "processor:live", Payload: payload})
	require.NoError(t, err)
	retrying := waitAsyncState(t, service, admission.RequestID, "retrying")
	require.Zero(t, retrying.Attempts)
	require.Equal(t, "TooManyRequestsException", retrying.ErrorType)
	require.False(t, retrying.InjectedCompletionLoss)
	service.mu.Lock()
	task := service.asyncTasks[admission.RequestID]
	armed, decided := task.completionLossAttempt, task.completionDecidedAttempt
	service.mu.Unlock()
	require.Equal(t, 1, armed)
	require.Zero(t, decided, "not-started execution has no completion decision to consume the rule")
	_, statErr := os.Stat(effects)
	require.True(t, os.IsNotExist(statErr), "throttled handler actually did not launch")
	require.NoError(t, os.WriteFile(gate, nil, 0600))
	select {
	case result := <-holder:
		require.NoError(t, result.err)
		require.False(t, result.output.FunctionError)
	case <-time.After(10 * time.Second):
		t.Fatal("occupied native execution did not join after releasing its handler")
	}
	completed := waitAsyncState(t, service, admission.RequestID, "succeeded")
	require.Equal(t, 2, completed.Attempts, "throttle plus one lost completion still uses only two handler attempts")
	require.False(t, completed.InjectedCompletionLoss)
	data, err := os.ReadFile(effects)
	require.NoError(t, err)
	lines := bytes.Split(bytes.TrimSpace(data), []byte("\n"))
	require.Len(t, lines, 2, "the pending one-shot survived actual nonlaunch throttling")
	for _, line := range lines {
		var effect struct {
			Event     json.RawMessage `json:"event"`
			RequestID string          `json:"request_id"`
		}
		require.NoError(t, json.Unmarshal(line, &effect))
		require.Equal(t, admission.RequestID, effect.RequestID)
		require.JSONEq(t, string(payload), string(effect.Event))
	}
	require.NoError(t, service.DevEvidence())
}
