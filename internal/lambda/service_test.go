package lambda

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestRuntimeProcess is an application fixture running in the isolated child
// environment. It speaks the Runtime API using only the Go standard library.
func TestRuntimeProcess(t *testing.T) {
	mode := os.Getenv("EVENTBUS_LAMBDA_TEST_MODE")
	if mode == "" {
		return
	}
	if mode == "command" {
		input, _ := io.ReadAll(os.Stdin)
		fmt.Fprintln(os.Stderr, "command diagnostic")
		fmt.Printf(`{"input":%s,"secret":%q}`, input, os.Getenv("EVENTBUS_LAMBDA_PARENT_SECRET"))
		os.Exit(0)
	}
	if mode == "exit" {
		os.Exit(8)
	}
	if mode == "wait" {
		if marker := os.Getenv("EVENTBUS_LAMBDA_TEST_PID"); marker != "" {
			_ = os.WriteFile(marker, []byte(fmt.Sprint(os.Getpid())), 0600)
		}
		for {
			time.Sleep(time.Hour)
		}
	}
	client := &http.Client{Timeout: 5 * time.Second}
	base := "http://" + os.Getenv("AWS_LAMBDA_RUNTIME_API") + runtimePrefix
	if mode == "init" {
		response, err := client.Post(base+"init/error", "application/json", strings.NewReader(`{"errorType":"ImportError","errorMessage":"fixture import failure"}`))
		if err != nil || response.StatusCode != 202 {
			os.Exit(3)
		}
		_ = response.Body.Close()
		os.Exit(0)
	}
	response, err := client.Get(base + "invocation/next")
	if err != nil {
		os.Exit(2)
	}
	input, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	id := response.Header.Get("Lambda-Runtime-Aws-Request-Id")
	path := "invocation/" + id + "/response"
	var payload []byte
	switch mode {
	case "error":
		path = "invocation/" + id + "/error"
		payload = []byte(`{"errorType":"Error","errorMessage":"Unauthorized"}`)
	case "bad":
		payload = []byte("not JSON")
	case "big":
		payload = bytes.Repeat([]byte(" "), maxPayload+1)
	default:
		payload, _ = json.Marshal(map[string]any{
			"input":        json.RawMessage(input),
			"requestId":    id,
			"invocationId": response.Header.Get("Lambda-Runtime-Invocation-Id"),
			"deadline":     response.Header.Get("Lambda-Runtime-Deadline-Ms"),
			"arn":          response.Header.Get("Lambda-Runtime-Invoked-Function-Arn"),
			"client":       response.Header.Get("Lambda-Runtime-Client-Context"),
			"runtime":      os.Getenv("AWS_LAMBDA_RUNTIME_API"),
			"secret":       os.Getenv("EVENTBUS_LAMBDA_PARENT_SECRET"),
		})
	}
	fmt.Fprintln(os.Stdout, "Go application stdout")
	fmt.Fprintln(os.Stderr, "Go application stderr")
	post, _ := http.NewRequest(http.MethodPost, base+path, bytes.NewReader(payload))
	post.Header.Set("Content-Type", "application/json")
	post.Header.Set("Lambda-Runtime-Invocation-Id", response.Header.Get("Lambda-Runtime-Invocation-Id"))
	posted, err := client.Do(post)
	if err != nil {
		os.Exit(4)
	}
	_ = posted.Body.Close()
	if mode == "big" && posted.StatusCode != 413 {
		os.Exit(5)
	}
	// Native AWS runtimes long-poll again after posting a response.
	_, _ = client.Get(base + "invocation/next")
	os.Exit(0)
}

func providedFunction(t *testing.T, mode string) Function {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return Function{Runtime: "provided", Command: []string{executable, "-test.run=^TestRuntimeProcess$"}, Environment: map[string]string{"EVENTBUS_LAMBDA_TEST_MODE": mode}, Timeout: 5 * time.Second}
}

func newTestService(t *testing.T, functions map[string]Function, directory string) *Service {
	t.Helper()
	service, err := NewService(&Config{Functions: functions}, directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := service.Close(ctx); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return service
}

func requestInvoke(service *Service, name, payload string, headers map[string]string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, invokePrefix+name+invokeSuffix, strings.NewReader(payload))
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	recorder := httptest.NewRecorder()
	service.ServeHTTP(recorder, request)
	return recorder
}

func TestProvidedRuntimeInvokeContract(t *testing.T) {
	t.Setenv("EVENTBUS_LAMBDA_PARENT_SECRET", "must not enter handler")
	service := newTestService(t, map[string]Function{"echo": providedFunction(t, "echo"), "echo:2": providedFunction(t, "echo")}, t.TempDir())
	clientContext := `{"custom":{"client":"sdk"}}`
	response := requestInvoke(service, "arn:aws:lambda:eu-west-1:123456789012:function:echo:2", `{"token":"value"}`, map[string]string{"X-Amz-Log-Type": "Tail", "X-Amz-Client-Context": base64.StdEncoding.EncodeToString([]byte(clientContext))})
	if response.Code != 200 || response.Header().Get("X-Amz-Function-Error") != "" {
		t.Fatalf("response: %d %s", response.Code, response.Body.String())
	}
	var output map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &output); err != nil {
		t.Fatal(err)
	}
	if output["arn"] != "arn:aws:lambda:eu-west-1:123456789012:function:echo:2" || output["secret"] != "" || output["client"] != clientContext || output["requestId"] == "" || output["requestId"] != output["invocationId"] || output["runtime"] == "" || output["deadline"] == "" {
		t.Fatalf("Runtime API contract: %#v", output)
	}
	if response.Header().Get("X-Amz-Executed-Version") != "2" {
		t.Fatal("missing executed version")
	}
	logs, err := base64.StdEncoding.DecodeString(response.Header().Get("X-Amz-Log-Result"))
	if err != nil || !strings.Contains(string(logs), "Go application stdout") || !strings.Contains(string(logs), "Go application stderr") {
		t.Fatalf("Tail logs: %q %v", logs, err)
	}
	// A qualifier selects an explicitly registered fixture rather than silently
	// executing a differently named base function.
	response = requestInvoke(service, "echo", `{}`, nil)
	if response.Code != 200 {
		t.Fatalf("base function: %d %s", response.Code, response.Body.String())
	}
	response = requestInvoke(service, "123456789012:function:echo:2", `{}`, nil)
	if err := json.Unmarshal(response.Body.Bytes(), &output); err != nil {
		t.Fatal(err)
	}
	if output["arn"] != "arn:aws:lambda:us-east-1:123456789012:function:echo:2" {
		t.Fatalf("partial ARN context: %#v", output)
	}
}

func TestFunctionErrorsRemainInvokeSuccess(t *testing.T) {
	functions := map[string]Function{}
	for _, mode := range []string{"error", "init", "exit", "bad", "big"} {
		functions[mode] = providedFunction(t, mode)
	}
	service := newTestService(t, functions, t.TempDir())
	for _, test := range []struct{ name, kind, message string }{
		{"error", "Error", "Unauthorized"}, {"init", "ImportError", "fixture import failure"},
		{"exit", "Runtime.ExitError", ""}, {"bad", "Runtime.InvalidResponse", ""}, {"big", "Function.ResponseSizeTooLarge", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := requestInvoke(service, test.name, `{}`, nil)
			if response.Code != 200 || response.Header().Get("X-Amz-Function-Error") != "Unhandled" {
				t.Fatalf("%d %s", response.Code, response.Body.String())
			}
			var failure map[string]string
			if err := json.Unmarshal(response.Body.Bytes(), &failure); err != nil {
				t.Fatal(err)
			}
			if failure["errorType"] != test.kind || (test.message != "" && failure["errorMessage"] != test.message) {
				t.Fatalf("failure: %#v", failure)
			}
		})
	}
}

func TestInvokeValidationAndDryRun(t *testing.T) {
	service := newTestService(t, map[string]Function{"echo": providedFunction(t, "exit")}, t.TempDir())
	for _, test := range []struct {
		name, payload string
		headers       map[string]string
		status        int
		kind          string
	}{
		{"absent", `{}`, nil, 404, "ResourceNotFoundException"},
		{"echo", `bad JSON`, nil, 400, "InvalidRequestContentException"},
		{"echo", `{}`, map[string]string{"X-Amz-Invocation-Type": "Event"}, 400, "InvalidParameterValueException"},
		{"echo", `{}`, map[string]string{"X-Amz-Log-Type": "wrong"}, 400, "InvalidParameterValueException"},
		{"echo", `{}`, map[string]string{"X-Amz-Client-Context": "invalid"}, 400, "InvalidParameterValueException"},
		{"echo:missing", `{}`, nil, 404, "ResourceNotFoundException"},
		{"echo", strings.Repeat(" ", maxPayload+1), nil, 413, "RequestTooLargeException"},
	} {
		response := requestInvoke(service, test.name, test.payload, test.headers)
		if response.Code != test.status || response.Header().Get("X-Amzn-ErrorType") != test.kind {
			t.Fatalf("validation %s: %d %s", test.kind, response.Code, response.Body.String())
		}
	}
	response := requestInvoke(service, "echo", "", map[string]string{"X-Amz-Invocation-Type": "DryRun"})
	if response.Code != 204 || response.Body.Len() != 0 {
		t.Fatalf("DryRun ran a function: %d %s", response.Code, response.Body.String())
	}
	request := httptest.NewRequest(http.MethodGet, invokePrefix+"echo"+invokeSuffix, nil)
	recorder := httptest.NewRecorder()
	service.ServeHTTP(recorder, request)
	if recorder.Code != 405 {
		t.Fatalf("method: %d", recorder.Code)
	}
}

func TestCommandJSONContract(t *testing.T) {
	t.Setenv("EVENTBUS_LAMBDA_PARENT_SECRET", "must not be inherited")
	function := providedFunction(t, "command")
	function.Runtime = "command"
	service := newTestService(t, map[string]Function{"echo": function}, t.TempDir())
	response := requestInvoke(service, "echo", `[1,true,"json"]`, map[string]string{"X-Amz-Log-Type": "Tail"})
	if response.Code != 200 || response.Header().Get("X-Amz-Function-Error") != "" || response.Body.String() != `{"input":[1,true,"json"],"secret":""}` {
		t.Fatalf("command: %d %s", response.Code, response.Body.String())
	}
}

func TestNodeHandlersAndPrivateReply(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("Node not installed")
	}
	t.Setenv("EVENTBUS_LAMBDA_PARENT_SECRET", "must not be inherited")
	directory := t.TempDir()
	writeFixture(t, directory, "handler.mjs", `
export async function handler(event, context) {
  console.log('{"forged":"stdout-is-not-a-result"}');
  console.error('node stderr');
  if (event.fail) throw new Error('Unauthorized');
  if (event.big) return 'x'.repeat(8388608);
  if (event.hugeLogs) console.log('L'.repeat(200000));
  return {event, requestId:context.awsRequestId, remaining:context.getRemainingTimeInMillis(), arn:context.invokedFunctionArn, secret:process.env.EVENTBUS_LAMBDA_PARENT_SECRET ?? '', fixture:process.env.FIXTURE_VALUE, client:context.clientContext?.custom?.client};
}
`)
	writeFixture(t, directory, "handler.cjs", `
exports.callback = (event, context, done) => { setTimeout(() => done(null, {callback:event}), 5); };
exports.legacy = (event, context) => { setTimeout(() => context.succeed({legacy:event}), 5); };
exports.mixed = async (event, context, callback) => { callback(null,{callbackWins:true}); return {promiseWins:true}; };
`)
	functions := map[string]Function{
		"esm":    {Runtime: "node", Handler: "handler.mjs.handler", Environment: map[string]string{"FIXTURE_VALUE": "declared"}},
		"cjs":    {Runtime: "node", Handler: "handler.cjs.callback"},
		"legacy": {Runtime: "node", Handler: "handler.cjs.legacy"},
		"mixed":  {Runtime: "node", Handler: "handler.cjs.mixed"},
	}
	service := newTestService(t, functions, directory)
	response := requestInvoke(service, "esm", `{"value":42,"hugeLogs":true}`, map[string]string{"X-Amz-Log-Type": "Tail", "X-Amz-Client-Context": base64.StdEncoding.EncodeToString([]byte(`{"custom":{"client":"node"}}`))})
	if response.Header().Get("X-Amz-Function-Error") != "" {
		t.Fatalf("Node: %s", response.Body.String())
	}
	var output map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &output); err != nil {
		t.Fatal(err)
	}
	if output["fixture"] != "declared" || output["secret"] != "" || output["requestId"] == "" || output["remaining"].(float64) <= 0 || output["client"] != "node" || output["forged"] != nil {
		t.Fatalf("Node context/result: %#v", output)
	}
	logs, _ := base64.StdEncoding.DecodeString(response.Header().Get("X-Amz-Log-Result"))
	if len(logs) != 4096 {
		t.Fatalf("bounded log tail: %d", len(logs))
	}
	response = requestInvoke(service, "cjs", `{"value":42}`, nil)
	if response.Body.String() != `{"callback":{"value":42}}` {
		t.Fatalf("CJS callback: %s", response.Body.String())
	}
	response = requestInvoke(service, "legacy", `{"value":42}`, nil)
	if response.Body.String() != `{"legacy":{"value":42}}` {
		t.Fatalf("legacy context.succeed: %s", response.Body.String())
	}
	response = requestInvoke(service, "mixed", `{}`, nil)
	if response.Body.String() != `{"callbackWins":true}` {
		t.Fatalf("Promise/callback must settle once: %s", response.Body.String())
	}
	response = requestInvoke(service, "esm", `{"fail":true}`, nil)
	if response.Header().Get("X-Amz-Function-Error") != "Unhandled" || !strings.Contains(response.Body.String(), `"errorMessage":"Unauthorized"`) {
		t.Fatalf("Node error: %s", response.Body.String())
	}
	response = requestInvoke(service, "esm", `{"big":true}`, nil)
	if !strings.Contains(response.Body.String(), "Function.ResponseSizeTooLarge") {
		t.Fatalf("Node size: %s", response.Body.String())
	}
}

func TestNodePendingPromiseTimesOut(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("Node not installed")
	}
	directory := t.TempDir()
	writeFixture(t, directory, "pending.mjs", `export async function handler() { return await new Promise(()=>{}); }`)
	service := newTestService(t, map[string]Function{"pending": {Runtime: "node", Handler: "pending.mjs.handler", Timeout: 250 * time.Millisecond}}, directory)
	response := requestInvoke(service, "pending", `{}`, nil)
	if response.Header().Get("X-Amz-Function-Error") != "Unhandled" || !strings.Contains(response.Body.String(), "Sandbox.Timedout") {
		t.Fatalf("pending Promise: %s", response.Body.String())
	}
}

func pythonCommand(t *testing.T) ([]string, map[string]string) {
	t.Helper()
	uv, err := exec.LookPath("uv")
	if err != nil {
		t.Skip("uv not installed")
	}
	environment := map[string]string{"PYTHONDONTWRITEBYTECODE": "1"}
	// SSD resource-owner markers are explicitly declared test fixture values.
	// They are not inherited by application functions or the Service itself.
	for _, key := range []string{"SSD_DEV_RUN_ID", "SSD_DEV_RECEIPT", "SSD_DEV_SCOPE", "INVOCATION_ID", "TMPDIR", "GOTMPDIR", "GOCACHE", "UV_CACHE_DIR", "XDG_RUNTIME_DIR", "DBUS_SESSION_BUS_ADDRESS"} {
		if value, found := os.LookupEnv(key); found {
			environment[key] = value
		}
	}
	return []string{uv, "run", "--offline", "--no-project", "python"}, environment
}

func TestPythonHandlerContextAndPrivateReply(t *testing.T) {
	t.Setenv("EVENTBUS_LAMBDA_PARENT_SECRET", "must not be inherited")
	directory := t.TempDir()
	writeFixture(t, directory, "handler.py", `
import os
def handler(event, context):
    print('{"forged":"stdout-is-not-a-result"}')
    if event.get('fail'):
        raise Exception('Unauthorized')
    return {'event': event, 'requestId': context.aws_request_id, 'remaining': context.get_remaining_time_in_millis(), 'arn': context.invoked_function_arn, 'secret': os.environ.get('EVENTBUS_LAMBDA_PARENT_SECRET', ''), 'fixture': os.environ['FIXTURE_VALUE'], 'client': context.client_context.custom['client']}
`)
	command, environment := pythonCommand(t)
	environment["FIXTURE_VALUE"] = "declared"
	service := newTestService(t, map[string]Function{"python": {Runtime: "python", Command: command, Handler: "handler.handler", Environment: environment}}, directory)
	headers := map[string]string{"X-Amz-Log-Type": "Tail", "X-Amz-Client-Context": base64.StdEncoding.EncodeToString([]byte(`{"custom":{"client":"python"}}`))}
	response := requestInvoke(service, "python", `{"value":42}`, headers)
	if response.Header().Get("X-Amz-Function-Error") != "" {
		t.Fatalf("Python: %s logs:%s", response.Body.String(), response.Header().Get("X-Amz-Log-Result"))
	}
	var output map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &output); err != nil {
		t.Fatal(err)
	}
	if output["fixture"] != "declared" || output["secret"] != "" || output["requestId"] == "" || output["remaining"].(float64) <= 0 || output["client"] != "python" || output["forged"] != nil {
		t.Fatalf("Python context/result: %#v", output)
	}
	response = requestInvoke(service, "python", `{"fail":true}`, headers)
	if response.Header().Get("X-Amz-Function-Error") != "Unhandled" || !strings.Contains(response.Body.String(), `"errorMessage":"Unauthorized"`) {
		t.Fatalf("Python error: %s", response.Body.String())
	}
}

func writeFixture(t *testing.T, directory, name, source string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(directory, name), []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestCloseCancelsAndRejectsInvocations(t *testing.T) {
	function := providedFunction(t, "wait")
	function.Timeout = time.Minute
	directory := t.TempDir()
	marker := filepath.Join(directory, "pid")
	function.Environment["EVENTBUS_LAMBDA_TEST_PID"] = marker
	service := newTestService(t, map[string]Function{"wait": function}, directory)
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- requestInvoke(service, "wait", `{}`, nil) }()
	waitForFile(t, marker)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := service.Close(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("Close did not join invocation")
	}
	response := requestInvoke(service, "wait", `{}`, nil)
	if response.Code != 503 {
		t.Fatalf("closed service: %d %s", response.Code, response.Body.String())
	}
}

func TestTimeoutAndRequestCancellationJoinChildren(t *testing.T) {
	for _, cancelRequest := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelRequest), func(t *testing.T) {
			directory := t.TempDir()
			function := providedFunction(t, "wait")
			function.Timeout = 150 * time.Millisecond
			marker := filepath.Join(directory, "pid")
			function.Environment["EVENTBUS_LAMBDA_TEST_PID"] = marker
			if cancelRequest {
				function.Timeout = time.Minute
			}
			service := newTestService(t, map[string]Function{"wait": function}, directory)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			request := httptest.NewRequest(http.MethodPost, invokePrefix+"wait"+invokeSuffix, strings.NewReader(`{}`)).WithContext(ctx)
			recorder := httptest.NewRecorder()
			done := make(chan struct{})
			go func() { service.ServeHTTP(recorder, request); close(done) }()
			waitForFile(t, marker)
			if cancelRequest {
				cancel()
			}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("invocation failed to terminate")
			}
			if !cancelRequest && (recorder.Code != 200 || recorder.Header().Get("X-Amz-Function-Error") != "Unhandled" || !strings.Contains(recorder.Body.String(), "Sandbox.Timedout")) {
				t.Fatalf("timeout: %d %s", recorder.Code, recorder.Body.String())
			}
		})
	}
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("function did not start")
}
