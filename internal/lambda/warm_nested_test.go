package lambda

import (
	"context"
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

	"github.com/lyeith/eventbus/internal/localexec"
)

const nestedWarmNode = `import process from 'node:process';
let count = 0;
export async function parent(event) {
  count++;
  await fetch(process.env.BARRIER + '/parents');
  const response = await fetch(process.env.ENDPOINT + '/2015-03-31/functions/child/invocations', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body:JSON.stringify({wait:true, auth:event.auth})
  });
  const child = await response.json();
  await fetch(process.env.BARRIER + '/completed');
  return {pid:process.pid, count, status:response.status, child};
}
export async function child(event) {
  count++;
  if (event.wait) await fetch(process.env.BARRIER + '/children');
  return {pid:process.pid, count, auth:event.auth ?? null};
}
`

const nestedWarmPython = `import json, os, urllib.request
count = 0
def parent(event, context):
    global count
    count += 1
    urllib.request.urlopen(os.environ['BARRIER'] + '/parents', timeout=3).read()
    request = urllib.request.Request(os.environ['ENDPOINT'] + '/2015-03-31/functions/child/invocations',
        data=json.dumps({'wait': True, 'auth': event.get('auth')}).encode(),
        headers={'Content-Type': 'application/json'}, method='POST')
    try:
        response = urllib.request.urlopen(request, timeout=3)
    except urllib.error.HTTPError as error:
        response = error
    status, child = response.status, json.load(response)
    response.close()
    urllib.request.urlopen(os.environ['BARRIER'] + '/completed', timeout=3).read()
    return {'pid': os.getpid(), 'count': count, 'status': status, 'child': child}
def child(event, context):
    global count
    count += 1
    if event.get('wait'):
        urllib.request.urlopen(os.environ['BARRIER'] + '/children', timeout=3).read()
    return {'pid': os.getpid(), 'count': count, 'auth': event.get('auth')}
`

func TestWarmNestedNativeInvocationsDoNotQueueBehindTheirParents(t *testing.T) {
	for _, scenario := range []struct {
		name, parentRuntime, childRuntime string
		concurrency                       int
		throttled                         bool
	}{
		{"node-to-python-default-execution-cap", "node", "python", 0, false},
		{"python-to-node-explicit-execution-cap", "python", "node", 4, false},
		{"execution-saturation-rejects-child-without-starvation", "node", "python", 2, true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			directory := t.TempDir()
			parentReady, completed, childrenReady, releaseChildren := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblockChildren := func() { releaseOnce.Do(func() { close(releaseChildren) }) }
			defer unblockChildren()
			var mu sync.Mutex
			var parents, completions, children int
			barrier := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				mu.Lock()
				var gate <-chan struct{}
				switch request.URL.Path {
				case "/parents":
					parents++
					if parents == 2 {
						close(parentReady)
					}
					gate = parentReady
				case "/completed":
					completions++
					if completions == 2 {
						close(completed)
					}
					gate = completed
				case "/children":
					children++
					if children == 2 {
						close(childrenReady)
					}
					gate = releaseChildren
				default:
					mu.Unlock()
					writer.WriteHeader(http.StatusNotFound)
					return
				}
				mu.Unlock()
				select {
				case <-gate:
					writer.WriteHeader(http.StatusNoContent)
				case <-request.Context().Done():
				}
			}))
			defer barrier.Close()
			var service *Service
			endpoint := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				service.ServeHTTP(writer, request)
			}))
			defer endpoint.Close()
			function := func(runtime, export string) Function {
				t.Helper()
				executable, filename, source := "node", "nested.mjs", nestedWarmNode
				if runtime == "python" {
					executable, filename, source = "python3", "nested.py", nestedWarmPython
				}
				command, err := exec.LookPath(executable)
				if err != nil {
					t.Skipf("%s runtime unavailable: %v", runtime, err)
				}
				if err := os.WriteFile(filepath.Join(directory, filename), []byte(source), 0600); err != nil {
					t.Fatal(err)
				}
				return Function{Runtime: runtime, Command: []string{command}, Handler: filename + "#" + export,
					Timeout: 4 * time.Second, Environment: map[string]string{"BARRIER": barrier.URL, "ENDPOINT": endpoint.URL}}
			}
			var err error
			service, err = NewService(&Config{
				Functions: map[string]Function{"parent": function(scenario.parentRuntime, "parent"), "child": function(scenario.childRuntime, "child")},
				DevWarm:   &DevWarmConfig{MaxWorkers: 2, MaxConcurrency: scenario.concurrency},
				DevAsync:  &DevAsyncConfig{LogWriter: io.Discard},
			}, directory)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				unblockChildren()
				if err := service.Close(context.Background()); err != nil {
					t.Error(err)
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			type reply struct {
				body map[string]any
				err  error
			}
			results := make(chan reply, 2)
			for index := 0; index < 2; index++ {
				go func() {
					request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.URL+invokePrefix+"parent"+invokeSuffix, strings.NewReader(`{"auth":"original-auth-value"}`))
					if err != nil {
						results <- reply{err: err}
						return
					}
					response, err := http.DefaultClient.Do(request)
					if err != nil {
						results <- reply{err: err}
						return
					}
					defer response.Body.Close()
					var body map[string]any
					if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
						results <- reply{err: err}
					} else if response.StatusCode != http.StatusOK || response.Header.Get("X-Amz-Function-Error") != "" {
						results <- reply{err: fmt.Errorf("parent native response: %d %v %v", response.StatusCode, response.Header, body)}
					} else {
						results <- reply{body: body}
					}
				}()
			}
			if !scenario.throttled {
				select {
				case <-childrenReady:
				case <-ctx.Done():
					t.Fatal("cap-saturating parents starved their native child invocations")
				}
				// Both actual parent and child handlers are executing simultaneously.
				// The old max_workers=2 pool cannot reach this joined barrier.
				warmAssertCounts(t, service.warm, 4, 0)
				unblockChildren()
			}
			childPIDs := make(map[float64]bool)
			parentPIDs := make(map[float64]bool)
			for index := 0; index < 2; index++ {
				select {
				case result := <-results:
					if result.err != nil {
						t.Fatal(result.err)
					}
					child := result.body["child"].(map[string]any)
					parentPIDs[result.body["pid"].(float64)] = true
					if scenario.throttled {
						if result.body["status"] != float64(http.StatusTooManyRequests) || !strings.Contains(child["message"].(string), "ConcurrentInvocationLimitExceeded") {
							t.Fatalf("saturated native child response %v", result.body)
						}
					} else {
						if result.body["status"] != float64(http.StatusOK) || child["auth"] != "original-auth-value" || child["count"] != float64(1) {
							t.Fatalf("native nested response %v", result.body)
						}
						childPIDs[child["pid"].(float64)] = true
					}
				case <-ctx.Done():
					t.Fatal("nested native request exceeded its caller deadline")
				}
			}
			if len(parentPIDs) != 2 {
				t.Fatalf("parent environments=%v", parentPIDs)
			}
			warmAssertCounts(t, service.warm, 0, 2)
			if scenario.throttled {
				mu.Lock()
				reached := children
				mu.Unlock()
				if reached != 0 {
					t.Fatal("throttled child launched")
				}
				return
			}
			if len(childPIDs) != 2 {
				t.Fatalf("child environments=%v", childPIDs)
			}
			for pid := range childPIDs {
				if parentPIDs[pid] {
					t.Fatal("parent and child executed in the same busy environment")
				}
			}
			// Parent completions are more recent than the nested child
			// completions, so the bounded LRU may retire those original children.
			// A later child enters the cache and must reuse its own environment.
			var reused map[string]any
			var out InvokeOutput
			var previousPID float64
			for invocation := 1; invocation <= 2; invocation++ {
				out, err = service.Execute(context.Background(), InvokeInput{FunctionName: "child", Payload: []byte(`{}`)})
				if err != nil || out.FunctionError {
					t.Fatalf("subsequent child reuse: %s %v", out.Payload, err)
				}
				if err := json.Unmarshal(out.Payload, &reused); err != nil {
					t.Fatal(err)
				}
				if reused["count"] != float64(invocation) || invocation == 2 && reused["pid"] != previousPID {
					t.Fatalf("healthy child was not retained/reused: %v", reused)
				}
				previousPID = reused["pid"].(float64)
			}
			if err := service.ReloadFunction(context.Background(), "child"); err != nil {
				t.Fatal(err)
			}
			warmAssertCounts(t, service.warm, 0, 1)
			out, err = service.Execute(context.Background(), InvokeInput{FunctionName: "child", Payload: []byte(`{}`)})
			if err != nil || out.FunctionError || json.Unmarshal(out.Payload, &reused) != nil || reused["pid"] == previousPID || reused["count"] != float64(1) {
				t.Fatalf("reload did not join/reset nested child state: %s %v", out.Payload, err)
			}
		})
	}
}

func TestWarmCanceledExecutionKeepsLimitUntilRealCleanupJoins(t *testing.T) {
	service, _, directory := warmConfiguredTestService(t, "node", 1, 1)
	started, cleanupReached, allowCleanup := filepath.Join(directory, "started"), make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	defer unblock.Do(func() { close(allowCleanup) })
	service.processCleanup = func(command *exec.Cmd) error {
		err := localexec.Cleanup(command)
		close(cleanupReached)
		<-allowCleanup
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		_, err := service.Execute(ctx, InvokeInput{FunctionName: "warm", Payload: []byte(fmt.Sprintf(`{"started":%q,"sleep":5}`, started))})
		finished <- err
	}()
	warmWaitFile(t, started)
	cancel()
	select {
	case <-cleanupReached:
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation did not reach actual native cleanup")
	}
	warmAssertCounts(t, service.warm, 1, 0)
	outcome, err := service.ExecuteObserved(context.Background(), InvokeInput{FunctionName: "warm", Payload: []byte(`{}`)}, nil)
	warmAssertThrottled(t, err)
	if outcome.State != InvocationNotStarted || outcome.OwnershipErr != nil {
		t.Fatalf("rejected execution acquired dirty ownership: %+v", outcome)
	}
	select {
	case err := <-finished:
		t.Fatalf("canceled execution returned before owned cleanup joined: %v", err)
	default:
	}
	unblock.Do(func() { close(allowCleanup) })
	if err := <-finished; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled execution: %v", err)
	}
	warmWaitRetired(t, service)
	// Future cleanup uses the original owner; the held cleanup fixture was one-shot.
	service.processCleanup = localexec.Cleanup
	body := warmTestExecute(t, service, `{}`)
	if body["count"] != float64(1) {
		t.Fatalf("canceled environment was reused: %v", body)
	}
}
