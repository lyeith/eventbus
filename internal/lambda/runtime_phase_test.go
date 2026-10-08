//go:build linux || darwin

package lambda

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type providedPhaseExecution struct {
	outcome InvocationOutcome
	err     error
}

type providedPhasePoint struct {
	PID       int    `json:"pid"`
	Runtime   string `json:"runtime"`
	RequestID string `json:"request_id"`
}

type providedPhaseReply struct {
	Input        json.RawMessage `json:"input"`
	RequestID    string          `json:"requestId"`
	InvocationID string          `json:"invocationId"`
	Deadline     string          `json:"deadline"`
	ReceivedAt   int64           `json:"receivedAt"`
	RemainingMS  int64           `json:"remainingMs"`
	PID          int             `json:"pid"`
	Runtime      string          `json:"runtime"`
}

func readProvidedPhasePoint(t *testing.T, path string) providedPhasePoint {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var point providedPhasePoint
	require.NoError(t, json.Unmarshal(data, &point))
	require.Greater(t, point.PID, 0)
	require.NotEmpty(t, point.Runtime)
	return point
}

func assertProvidedPhaseJoined(t *testing.T, point providedPhasePoint) {
	t.Helper()
	require.False(t, processAlive(point.PID), "provided runtime child must join before completion")
	connection, err := net.DialTimeout("tcp", point.Runtime, 100*time.Millisecond)
	if err == nil {
		_ = connection.Close()
		t.Fatalf("provided Runtime API listener remained open: %s", point.Runtime)
	}
}

func TestProvidedDelayedInitPreservesShortHandlerBudgetAndOriginalContext(t *testing.T) {
	directory := t.TempDir()
	function := providedFunction(t, "phase-echo")
	function.Timeout = 150 * time.Millisecond
	function.Environment["EVENTBUS_LAMBDA_TEST_INIT_DELAY"] = "300ms"
	function.Environment["EVENTBUS_LAMBDA_TEST_INVOKE_DELAY"] = "30ms"
	service, path := newDiagnosticTestService(t, map[string]Function{"cold:2": function}, directory, nil, nil)
	var admitted InvocationMetadata
	outcome, err := service.ExecuteObserved(context.Background(), InvokeInput{FunctionName: "cold:2", Payload: []byte(`{"value":"original-event"}`)}, func(metadata InvocationMetadata) error { admitted = metadata; return nil })
	require.NoError(t, err)
	require.Equal(t, InvocationSucceeded, outcome.State)
	require.False(t, outcome.Output.FunctionError)
	require.Nil(t, outcome.OwnershipErr)
	require.Equal(t, admitted, outcome.Metadata)
	require.Equal(t, admitted.RequestID, outcome.Output.RequestID)
	require.Equal(t, "2", outcome.Output.ExecutedVersion)
	var reply providedPhaseReply
	require.NoError(t, json.Unmarshal(outcome.Output.Payload, &reply))
	require.JSONEq(t, `{"value":"original-event"}`, string(reply.Input))
	require.Equal(t, admitted.RequestID, reply.RequestID)
	require.Equal(t, admitted.RequestID, reply.InvocationID)
	require.GreaterOrEqual(t, reply.RemainingMS, int64(75), "Init cannot spend the configured handler budget")
	require.LessOrEqual(t, reply.RemainingMS, int64(150), "header must expose Invoke deadline rather than Init deadline")
	assertProvidedPhaseJoined(t, providedPhasePoint{PID: reply.PID, Runtime: reply.Runtime})
	records := readDiagnosticRecords(t, path)
	require.Len(t, records, 1)
	require.Equal(t, admitted.RequestID, records[0].RequestID)
	require.Equal(t, 1, records[0].Attempt)
	require.Equal(t, InvocationSucceeded, records[0].State)
	require.True(t, records[0].OwnershipConfirmed)
	require.Equal(t, int64(150), records[0].ConfiguredTimeoutMS)
	require.GreaterOrEqual(t, records[0].ElapsedMS, float64(300))
	require.Empty(t, records[0].TerminationCause)
	require.Len(t, records[0].ExecutionPhases, 1)
	phase := records[0].ExecutionPhases[0]
	require.Equal(t, 1, phase.InitAttempt)
	require.Equal(t, "initial", phase.Mode)
	require.Equal(t, "succeeded", phase.InitState)
	require.Equal(t, "succeeded", phase.InvokeState)
	require.GreaterOrEqual(t, phase.InitMS, float64(300))
	require.GreaterOrEqual(t, phase.InvokeMS, float64(30))
}

func TestProvidedActualHandlerTimeoutBeginsAfterFirstNext(t *testing.T) {
	directory := t.TempDir()
	initMarker := filepath.Join(directory, "init")
	invokeMarker := filepath.Join(directory, "invoke")
	function := providedFunction(t, "post-next-wait")
	function.Timeout = 100 * time.Millisecond
	function.Environment["EVENTBUS_LAMBDA_TEST_INIT_DELAY"] = "300ms"
	function.Environment["EVENTBUS_LAMBDA_TEST_INIT_PID"] = initMarker
	function.Environment["EVENTBUS_LAMBDA_TEST_PID"] = invokeMarker
	service, path := newDiagnosticTestService(t, map[string]Function{"wait": function}, directory, nil, nil)
	outcome, err := service.ExecuteObserved(context.Background(), InvokeInput{FunctionName: "wait", Payload: []byte(`{}`)}, nil)
	require.NoError(t, err)
	require.Equal(t, InvocationTimedOut, outcome.State)
	require.True(t, outcome.Output.FunctionError)
	require.Contains(t, string(outcome.Output.Payload), "Sandbox.Timedout")
	require.Nil(t, outcome.OwnershipErr)
	_, err = os.Stat(invokeMarker)
	require.NoError(t, err, "actual runtime must receive the event before handler budget expires")
	point := readProvidedPhasePoint(t, initMarker)
	require.Equal(t, outcome.Metadata.RequestID, point.RequestID)
	assertProvidedPhaseJoined(t, point)
	records := readDiagnosticRecords(t, path)
	require.Len(t, records, 1)
	require.Equal(t, "function_timeout", records[0].TerminationCause)
	require.Equal(t, int64(100), records[0].ConfiguredTimeoutMS)
	require.GreaterOrEqual(t, records[0].ElapsedMS, float64(400))
	require.True(t, records[0].OwnershipConfirmed)
	require.Len(t, records[0].ExecutionPhases, 1)
	phase := records[0].ExecutionPhases[0]
	require.Equal(t, "succeeded", phase.InitState)
	require.Equal(t, "timed_out", phase.InvokeState)
	require.GreaterOrEqual(t, phase.InitMS, float64(300))
	require.GreaterOrEqual(t, phase.InvokeMS, float64(100))
}

func TestProvidedCancellationAndCloseOwnInitAndInvokePhases(t *testing.T) {
	for _, phase := range []string{"init", "invoke"} {
		for _, boundary := range []string{"caller", "close"} {
			t.Run(phase+"/"+boundary, func(t *testing.T) {
				directory := t.TempDir()
				initMarker, invokeMarker := filepath.Join(directory, "init"), filepath.Join(directory, "invoke")
				function := providedFunction(t, "init-wait")
				function.Timeout = 2 * time.Second
				function.Environment["EVENTBUS_LAMBDA_TEST_INIT_PID"] = initMarker
				marker := initMarker
				if phase == "invoke" {
					function.Environment["EVENTBUS_LAMBDA_TEST_MODE"] = "post-next-wait"
					function.Environment["EVENTBUS_LAMBDA_TEST_PID"] = invokeMarker
					marker = invokeMarker
				}
				service, path := newDiagnosticTestService(t, map[string]Function{"wait": function}, directory, nil, nil)
				ctx, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)
				finished := make(chan providedPhaseExecution, 1)
				go func() {
					outcome, err := service.ExecuteObserved(ctx, InvokeInput{FunctionName: "wait", Payload: []byte(`{}`)}, nil)
					finished <- providedPhaseExecution{outcome: outcome, err: err}
				}()
				waitDiagnosticMarker(t, marker)
				point := readProvidedPhasePoint(t, initMarker)
				require.True(t, processAlive(point.PID))
				wantCause := "caller_canceled"
				if boundary == "caller" {
					cancel()
				} else {
					wantCause = "service_canceled"
					require.NoError(t, service.Close(context.Background()))
				}
				var result providedPhaseExecution
				select {
				case result = <-finished:
				case <-time.After(5 * time.Second):
					t.Fatal("provided phase cancellation failed to join")
				}
				if boundary == "caller" {
					require.ErrorIs(t, result.err, context.Canceled)
				} else {
					require.NoError(t, result.err)
				}
				require.Equal(t, InvocationCanceled, result.outcome.State)
				require.Nil(t, result.outcome.OwnershipErr)
				require.Equal(t, point.RequestID, result.outcome.Metadata.RequestID)
				assertProvidedPhaseJoined(t, point)
				records := readDiagnosticRecords(t, path)
				require.Len(t, records, 1)
				require.Equal(t, wantCause, records[0].TerminationCause)
				require.True(t, records[0].OwnershipConfirmed)
				require.Len(t, records[0].ExecutionPhases, 1)
				phaseRecord := records[0].ExecutionPhases[0]
				if phase == "init" {
					require.Equal(t, "canceled", phaseRecord.InitState)
					require.Empty(t, phaseRecord.InvokeState)
					require.Zero(t, phaseRecord.InvokeMS)
				} else {
					require.Equal(t, "succeeded", phaseRecord.InitState)
					require.Equal(t, "canceled", phaseRecord.InvokeState)
				}
			})
		}
	}
}

func TestProvidedInitTimeoutFallbackKeepsRemainingBudgetAndJoinsBothRuntimes(t *testing.T) {
	directory := t.TempDir()
	initLog := filepath.Join(directory, "init.jsonl")
	function := providedFunction(t, "phase-echo")
	function.Timeout = 450 * time.Millisecond
	function.Environment["EVENTBUS_LAMBDA_TEST_INIT_DELAY"] = "150ms"
	function.Environment["EVENTBUS_LAMBDA_TEST_INVOKE_DELAY"] = "30ms"
	function.Environment["EVENTBUS_LAMBDA_TEST_INIT_LOG"] = initLog
	service, path := newDiagnosticTestService(t, map[string]Function{"fallback": function}, directory, nil, nil)
	service.initTimeout = 100 * time.Millisecond // Private test policy; native default remains 10s.
	outcome, err := service.ExecuteObserved(context.Background(), InvokeInput{FunctionName: "fallback", Payload: []byte(`{"original":true}`)}, nil)
	require.NoError(t, err)
	require.Equal(t, InvocationSucceeded, outcome.State)
	require.False(t, outcome.Output.FunctionError)
	require.Nil(t, outcome.OwnershipErr)
	var reply providedPhaseReply
	require.NoError(t, json.Unmarshal(outcome.Output.Payload, &reply))
	require.Equal(t, outcome.Metadata.RequestID, reply.RequestID)
	require.JSONEq(t, `{"original":true}`, string(reply.Input))
	require.Greater(t, reply.RemainingMS, int64(100))
	require.Less(t, reply.RemainingMS, int64(400), "fallback Init and Invoke must share the already-running configured budget")
	data, err := os.ReadFile(initLog)
	require.NoError(t, err)
	var points []providedPhasePoint
	decoder := json.NewDecoder(bytes.NewReader(data))
	for {
		var point providedPhasePoint
		err := decoder.Decode(&point)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		points = append(points, point)
	}
	require.Len(t, points, 2, "one ordinary Init expiry permits one joined fallback initialization")
	require.NotEqual(t, points[0].PID, points[1].PID)
	for _, point := range points {
		require.Equal(t, outcome.Metadata.RequestID, point.RequestID, "Init retry cannot substitute the native request identity")
		assertProvidedPhaseJoined(t, point)
	}
	records := readDiagnosticRecords(t, path)
	require.Len(t, records, 1, "internal Init retry must remain one native invocation")
	require.Equal(t, outcome.Metadata.RequestID, records[0].RequestID)
	require.Equal(t, 1, records[0].Attempt)
	require.True(t, records[0].OwnershipConfirmed)
	require.Len(t, records[0].ExecutionPhases, 2)
	initial, fallback := records[0].ExecutionPhases[0], records[0].ExecutionPhases[1]
	require.Equal(t, 1, initial.InitAttempt)
	require.Equal(t, "initial", initial.Mode)
	require.Equal(t, "timed_out", initial.InitState)
	require.Empty(t, initial.InvokeState)
	require.Zero(t, initial.InvokeMS)
	require.Equal(t, 2, fallback.InitAttempt)
	require.Equal(t, "fallback", fallback.Mode)
	require.Equal(t, "succeeded", fallback.InitState)
	require.Equal(t, "succeeded", fallback.InvokeState)
	require.GreaterOrEqual(t, fallback.InitMS, float64(150))
	require.GreaterOrEqual(t, fallback.InvokeMS, float64(30))
}

func TestProvidedInitErrorBodiesRemainNativeFailuresWithoutEventInjection(t *testing.T) {
	for _, mode := range []string{"init", "init-empty"} {
		t.Run(mode, func(t *testing.T) {
			service := newTestService(t, map[string]Function{"error": providedFunction(t, mode)}, t.TempDir())
			output, err := service.Execute(context.Background(), InvokeInput{FunctionName: "error", Payload: []byte(`{"event":"must-not-enter-init-error"}`)})
			require.NoError(t, err)
			require.True(t, output.FunctionError)
			require.NotContains(t, string(output.Payload), "must-not-enter-init-error")
			var failure struct {
				ErrorType string `json:"errorType"`
			}
			require.NoError(t, json.Unmarshal(output.Payload, &failure))
			want := "ImportError"
			if mode == "init-empty" {
				want = "Runtime.Unknown"
			}
			require.Equal(t, want, failure.ErrorType)
		})
	}
}

func TestRuntimeInitErrorIgnoresInvocationAttemptHeader(t *testing.T) {
	for _, body := range []string{"", `{"errorType":"ImportError","errorMessage":"original-init-error"}`} {
		runtime := &runtimeInvocation{ctx: context.Background(), input: invocation{requestID: "request-id", payload: []byte(`{"secret":"event"}`)}, result: make(chan invocationResult, 1)}
		request := httptest.NewRequest(http.MethodPost, runtimePrefix+"init/error", strings.NewReader(body))
		request.Header.Set("Lambda-Runtime-Invocation-Id", "belongs-to-no-invocation")
		writer := httptest.NewRecorder()
		runtime.ServeHTTP(writer, request)
		require.Equal(t, http.StatusAccepted, writer.Code)
		result := <-runtime.result
		require.True(t, result.functionError)
		require.False(t, runtime.delivered)
		if body != "" {
			require.Equal(t, body, string(result.payload))
		} else {
			require.Contains(t, string(result.payload), `"errorType":"Runtime.Unknown"`)
			require.NotContains(t, string(result.payload), "secret")
		}
	}
}

type runtimeInitGatedBody struct {
	reader  *strings.Reader
	started chan struct{}
	gate    <-chan struct{}
	once    sync.Once
}

func (body *runtimeInitGatedBody) Read(data []byte) (int, error) {
	body.once.Do(func() { close(body.started) })
	<-body.gate
	return body.reader.Read(data)
}
func (*runtimeInitGatedBody) Close() error { return nil }

func TestRuntimeFirstNextFencesAnInitErrorStillReadingItsBody(t *testing.T) {
	runtime := &runtimeInvocation{ctx: context.Background(), input: invocation{requestID: "request-id", payload: []byte(`{"original":true}`), deadline: time.Now().Add(time.Second)}, result: make(chan invocationResult, 1)}
	started, gate, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	request := httptest.NewRequest(http.MethodPost, runtimePrefix+"init/error", nil)
	request.Body = &runtimeInitGatedBody{reader: strings.NewReader(`{"errorType":"ImportError"}`), started: started, gate: gate}
	initWriter := httptest.NewRecorder()
	go func() { runtime.ServeHTTP(initWriter, request); close(finished) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("Init error never entered actual body I/O")
	}
	nextWriter := httptest.NewRecorder()
	runtime.ServeHTTP(nextWriter, httptest.NewRequest(http.MethodGet, runtimePrefix+"invocation/next", nil))
	require.Equal(t, http.StatusOK, nextWriter.Code)
	require.JSONEq(t, `{"original":true}`, nextWriter.Body.String())
	release()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("Init body did not join")
	}
	require.Equal(t, http.StatusConflict, initWriter.Code)
	select {
	case result := <-runtime.result:
		t.Fatalf("late Init error replaced the actual invocation: %s", result.payload)
	default:
	}
	responseWriter := httptest.NewRecorder()
	runtime.ServeHTTP(responseWriter, httptest.NewRequest(http.MethodPost, runtimePrefix+"invocation/request-id/response", strings.NewReader(`{"ok":true}`)))
	require.Equal(t, http.StatusAccepted, responseWriter.Code)
	result := <-runtime.result
	require.False(t, result.functionError)
	require.JSONEq(t, `{"ok":true}`, string(result.payload))
	// Every admitted protocol request, including the rejected Init request, joined.
	runtime.requests.Wait()
}
