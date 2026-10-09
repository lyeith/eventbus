package lambda

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/localexec"
)

const warmPythonHandler = `import json, os, sys, time
count = 0
prior = None
print('init-' + str(os.getpid()))
def handler(event, context):
    global count, prior
    count += 1
    print('out-' + event.get('label', ''), flush=True)
    print('err-' + event.get('label', ''), file=sys.stderr, flush=True)
    if event.get('started'):
        with open(event['started'], 'w') as f: f.write(str(os.getpid()))
    if event.get('sleep'): time.sleep(event['sleep'])
    if event.get('fail'): raise ValueError('actual handler failure')
    result = {'pid': os.getpid(), 'count': count, 'id': context.aws_request_id,
              'prior_id': prior.aws_request_id if prior else None,
              'client': context.client_context.custom if context.client_context else None,
              'auth': event.get('auth'), 'remaining': context.get_remaining_time_in_millis(),
              'setting': os.environ.get('SETTING'), 'trace': os.environ.get('_X_AMZN_TRACE_ID')}
    prior = context
    if event.get('record'):
        with open(event['record'], 'a') as f: f.write(json.dumps(result) + '\n')
    return result
`
const warmNodeHandler = `import fs from 'node:fs';
let count = 0, prior;
console.log('init-' + process.pid);
export async function handler(event, context) {
  count++;
  console.log('out-' + (event.label ?? ''));
  console.error('err-' + (event.label ?? ''));
  if (event.started) fs.writeFileSync(event.started, String(process.pid));
  if (event.sleep) await new Promise(resolve => setTimeout(resolve, event.sleep * 1000));
  if (event.fail) throw new Error('actual handler failure');
  const result = {pid:process.pid, count, id:context.awsRequestId, prior_id:prior?.awsRequestId ?? null,
    client:context.clientContext?.custom ?? null, auth:event.auth ?? null,
    remaining:context.getRemainingTimeInMillis(), setting:process.env.SETTING, trace:process.env._X_AMZN_TRACE_ID};
  prior = context;
  if (event.record) fs.appendFileSync(event.record, JSON.stringify(result) + '\n');
  return result;
}
`

func warmTestFunction(t *testing.T, runtime string, directory string) Function {
	t.Helper()
	command := runtime
	if runtime == "python" {
		command = "python3"
	}
	executable, err := exec.LookPath(command)
	if err != nil {
		t.Skipf("%s runtime unavailable: %v", runtime, err)
	}
	source, filename := warmNodeHandler, "handler.mjs"
	if runtime == "python" {
		source, filename = warmPythonHandler, "handler.py"
	}
	if err := os.WriteFile(filepath.Join(directory, filename), []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	return Function{Runtime: runtime, Command: []string{executable}, Handler: filename + "#handler", Environment: map[string]string{"SETTING": "old"}, Timeout: time.Second}
}
func warmTestService(t *testing.T, runtime string, limit int) (*Service, Function, string) {
	t.Helper()
	directory := t.TempDir()
	function := warmTestFunction(t, runtime, directory)
	service, err := NewService(&Config{Functions: map[string]Function{"warm": function}, DevWarm: &DevWarmConfig{MaxWorkers: limit}, DevAsync: &DevAsyncConfig{LogWriter: io.Discard, RetryDelays: []time.Duration{0, 0}}}, directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := service.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return service, function, directory
}
func warmTestExecute(t *testing.T, service *Service, event string) map[string]any {
	t.Helper()
	output, err := service.Execute(context.Background(), InvokeInput{FunctionName: "warm", Payload: []byte(event)})
	if err != nil || output.FunctionError {
		t.Fatalf("execute: %s, %v", output.Payload, err)
	}
	var result map[string]any
	if err := json.Unmarshal(output.Payload, &result); err != nil {
		t.Fatal(err)
	}
	return result
}
func warmWaitFile(t *testing.T, filename string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filename); err == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("handler did not reach owned marker")
}
func warmWaitRetired(t *testing.T, service *Service) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		service.warm.mu.Lock()
		count := len(service.warm.workers)
		service.warm.mu.Unlock()
		if count == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("warm workers did not join")
}

func TestWarmWorkersReuseImportsIndependentContextsAndLogs(t *testing.T) {
	for _, runtime := range []string{"python", "node"} {
		t.Run(runtime, func(t *testing.T) {
			service, _, _ := warmTestService(t, runtime, 1)
			var bodies []map[string]any
			for index, label := range []string{"one", "two"} {
				request := httptest.NewRequest(http.MethodPost, invokePrefix+"warm"+invokeSuffix, strings.NewReader(fmt.Sprintf(`{"label":%q,"auth":%q}`, label, label+"-token")))
				request.Header.Set("X-Amz-Log-Type", "Tail")
				request.Header.Set("X-Amz-Client-Context", base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf(`{"custom":{"label":%q}}`, label))))
				request.Header.Set("X-Amzn-Trace-Id", label+"-trace")
				response := httptest.NewRecorder()
				service.ServeHTTP(response, request)
				if response.Code != 200 || response.Header().Get("X-Amz-Function-Error") != "" {
					t.Fatalf("response %d %s", response.Code, response.Body.String())
				}
				var body map[string]any
				if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
					t.Fatal(err)
				}
				bodies = append(bodies, body)
				logs, err := base64.StdEncoding.DecodeString(response.Header().Get("X-Amz-Log-Result"))
				if err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(string(logs), "out-"+label) || !strings.Contains(string(logs), "err-"+label) || strings.Contains(string(logs), "eventbus-warm:") {
					t.Fatalf("logs %q", logs)
				}
				if index == 0 && !strings.Contains(string(logs), "init-") || index == 1 && (strings.Contains(string(logs), "init-") || strings.Contains(string(logs), "out-one")) {
					t.Fatalf("wrong invocation log boundary %q", logs)
				}
				if body["count"] != float64(index+1) || body["auth"] != label+"-token" || body["trace"] != label+"-trace" || body["remaining"].(float64) <= 0 || body["client"].(map[string]any)["label"] != label {
					t.Fatalf("context/event %v", body)
				}
			}
			if bodies[0]["pid"] != bodies[1]["pid"] || bodies[0]["id"] == bodies[1]["id"] || bodies[1]["prior_id"] != bodies[0]["id"] {
				t.Fatalf("reuse/context identity %v", bodies)
			}
		})
	}
}

func TestWarmWorkersGlobalLimitIncludesRetiredGenerationAndDrainFresh(t *testing.T) {
	service, function, directory := warmTestService(t, "python", 1)
	started := filepath.Join(directory, "started")
	firstDone := make(chan error, 1)
	go func() {
		_, err := service.Execute(context.Background(), InvokeInput{FunctionName: "warm", Payload: []byte(fmt.Sprintf(`{"started":%q,"sleep":0.3}`, started))})
		firstDone <- err
	}()
	warmWaitFile(t, started)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	changed := function
	changed.Environment = maps.Clone(function.Environment)
	changed.Environment["SETTING"] = "new"
	if err := service.RegisterFunction(ctx, "warm", changed); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("registration publishes then bounds old join: %v", err)
	}
	service.DevBeginWarmDrain()
	secondStarted := filepath.Join(directory, "second-started")
	secondDone := make(chan error, 1)
	go func() {
		_, err := service.Execute(context.Background(), InvokeInput{FunctionName: "warm", Payload: []byte(fmt.Sprintf(`{"started":%q}`, secondStarted))})
		secondDone <- err
	}()
	select {
	case err := <-secondDone:
		t.Fatalf("fallback bypassed occupied global slot: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	if _, err := os.Stat(secondStarted); !os.IsNotExist(err) {
		t.Fatalf("second handler launched before old joined: %v", err)
	}
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	warmWaitRetired(t, service)
	if err := service.DevResumeWarm(); err != nil {
		t.Fatal(err)
	}
	body := warmTestExecute(t, service, `{}`)
	if body["setting"] != "new" || body["count"] != float64(1) {
		t.Fatalf("new generation %v", body)
	}
}

func TestWarmWorkersReloadEnvironmentAndSource(t *testing.T) {
	for _, runtime := range []string{"python", "node"} {
		t.Run(runtime, func(t *testing.T) {
			service, function, directory := warmTestService(t, runtime, 1)
			first := warmTestExecute(t, service, `{}`)
			if err := service.ReloadFunction(context.Background(), "warm:$LATEST"); err != nil {
				t.Fatal(err)
			}
			second := warmTestExecute(t, service, `{}`)
			if first["pid"] == second["pid"] || second["count"] != float64(1) {
				t.Fatalf("reload did not reset module %v %v", first, second)
			}
			function.Environment["SETTING"] = "new"
			if err := service.RegisterFunction(context.Background(), "warm", function); err != nil {
				t.Fatal(err)
			}
			function.Environment["SETTING"] = "mutated-after-register"
			third := warmTestExecute(t, service, `{}`)
			if third["setting"] != "new" || third["pid"] == second["pid"] {
				t.Fatalf("environment snapshot %v", third)
			}
			source, filename := warmNodeHandler, "handler.mjs"
			if runtime == "python" {
				source, filename = warmPythonHandler, "handler.py"
			}
			source = strings.Replace(source, "actual handler failure", "changed source failure", 1)
			if err := os.WriteFile(filepath.Join(directory, filename), []byte(source), 0600); err != nil {
				t.Fatal(err)
			}
			if err := service.ReloadFunction(context.Background(), "warm"); err != nil {
				t.Fatal(err)
			}
			out, err := service.Execute(context.Background(), InvokeInput{FunctionName: "warm", Payload: []byte(`{"fail":true}`)})
			if err != nil || !out.FunctionError || !strings.Contains(string(out.Payload), "changed source failure") {
				t.Fatalf("source reload %s %v", out.Payload, err)
			}
		})
	}
}

func TestWarmWorkersErrorsTimeoutAndCloseJoin(t *testing.T) {
	for _, runtime := range []string{"python", "node"} {
		t.Run(runtime, func(t *testing.T) {
			service, function, directory := warmTestService(t, runtime, 1)
			first := warmTestExecute(t, service, `{}`)
			out, err := service.Execute(context.Background(), InvokeInput{FunctionName: "warm", Payload: []byte(`{"fail":true}`)})
			if err != nil || !out.FunctionError || !strings.Contains(string(out.Payload), "actual handler failure") {
				t.Fatalf("handler error %s %v", out.Payload, err)
			}
			warmWaitRetired(t, service)
			second := warmTestExecute(t, service, `{}`)
			if first["pid"] == second["pid"] || second["count"] != float64(1) {
				t.Fatalf("failed worker reused %v", second)
			}
			function.Timeout = 50 * time.Millisecond
			if err := service.RegisterFunction(context.Background(), "warm", function); err != nil {
				t.Fatal(err)
			}
			out, err = service.Execute(context.Background(), InvokeInput{FunctionName: "warm", Payload: []byte(`{"sleep":5}`)})
			if err != nil || !out.FunctionError || !strings.Contains(string(out.Payload), "Sandbox.Timedout") {
				t.Fatalf("timeout %s %v", out.Payload, err)
			}
			warmWaitRetired(t, service)
			function.Timeout = time.Second
			if err := service.RegisterFunction(context.Background(), "warm", function); err != nil {
				t.Fatal(err)
			}
			started := filepath.Join(directory, "closing")
			done := make(chan InvocationOutcome, 1)
			go func() {
				result, _ := service.ExecuteObserved(context.Background(), InvokeInput{FunctionName: "warm", Payload: []byte(fmt.Sprintf(`{"sleep":5,"started":%q}`, started))}, nil)
				done <- result
			}()
			warmWaitFile(t, started)
			if err := service.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if result := <-done; result.State != InvocationCanceled || result.OwnershipErr != nil {
				t.Fatalf("close outcome %+v", result)
			}
			warmWaitRetired(t, service)
		})
	}
}

func TestWarmWorkersStickyRetirementFaultFencesReuse(t *testing.T) {
	directory := t.TempDir()
	function := warmTestFunction(t, "python", directory)
	service, err := NewService(&Config{Functions: map[string]Function{"warm": function}, DevWarm: &DevWarmConfig{MaxWorkers: 1}, DevAsync: &DevAsyncConfig{LogWriter: io.Discard}}, directory)
	if err != nil {
		t.Fatal(err)
	}
	fault := errors.New("injected joined cleanup uncertainty")
	service.processCleanup = func(command *exec.Cmd) error { return errors.Join(localexec.Cleanup(command), fault) }
	warmTestExecute(t, service, `{}`)
	service.DevBeginWarmDrain()
	warmWaitRetired(t, service)
	if err := service.DevResumeWarm(); !errors.Is(err, fault) {
		t.Fatalf("resume erased fault %v", err)
	}
	result, err := service.ExecuteObserved(context.Background(), InvokeInput{FunctionName: "warm", Payload: []byte(`{}`)}, nil)
	if err != nil || result.State != InvocationNotStarted || !errors.Is(result.OwnershipErr, fault) {
		t.Fatalf("fault fence %+v %v", result, err)
	}
	if err := service.Close(context.Background()); !errors.Is(err, fault) {
		t.Fatalf("close erased fault %v", err)
	}
}

func TestWarmRegistryConcurrentImmutableResolution(t *testing.T) {
	service, function, _ := warmTestService(t, "python", 1)
	var group sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	for index := 0; index < 4; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for ctx.Err() == nil {
				entry, _, err := service.resolveTarget("warm", "")
				if err != nil {
					t.Error(err)
					return
				}
				_ = entry.environment["SETTING"]
				_ = entry.command[0]
			}
		}()
	}
	for index := 0; index < 20; index++ {
		function.Environment = map[string]string{"SETTING": fmt.Sprint(index)}
		if err := service.RegisterFunction(context.Background(), "warm", function); err != nil {
			t.Fatal(err)
		}
		function.Environment["SETTING"] = "caller mutation"
	}
	cancel()
	group.Wait()
}
