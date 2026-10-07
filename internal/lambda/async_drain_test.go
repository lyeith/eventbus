package lambda

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func waitAsyncAdmissionClosed(t *testing.T, service *Service) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		service.mu.Lock()
		closed := service.asyncClosed
		service.mu.Unlock()
		if closed {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("async admission did not stop")
}

func TestDrainAsyncPreservesSynchronousNestedInvoke(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("Node unavailable")
	}
	directory := t.TempDir()
	gate, started, output := filepath.Join(directory, "gate"), filepath.Join(directory, "started"), filepath.Join(directory, "nested.json")
	// Reserve an owned listener before configuring the caller's immutable env.
	serving := httptest.NewUnstartedServer(http.NotFoundHandler())
	t.Cleanup(serving.Close)
	writeFixture(t, directory, "nested.mjs", `import fs from 'node:fs';
export async function handler(event) {
 fs.writeFileSync(process.env.STARTED,'ready');
 while (!fs.existsSync(process.env.GATE)) await new Promise(r=>setTimeout(r,5));
 const response=await fetch(process.env.INVOKE_URL,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(event)});
 const result=await response.json();
 fs.writeFileSync(process.env.OUTPUT,JSON.stringify({status:response.status,result}));
 if (!response.ok || response.headers.has('X-Amz-Function-Error')) throw new Error('nested invocation failed');
 return result;
}`)
	service := newAsyncTestService(t, map[string]Function{
		"inner": providedFunction(t, "echo"),
		"outer": {Runtime: "node", Handler: "nested.mjs.handler", Timeout: 10 * time.Second, Environment: map[string]string{
			"STARTED": started, "GATE": gate, "OUTPUT": output,
			"INVOKE_URL": "http://" + serving.Listener.Addr().String() + invokePrefix + "inner" + invokeSuffix,
		}},
	}, directory, &DevAsyncConfig{Workers: 1, Capacity: 2, RetryDelays: []time.Duration{0, 0}})
	serving.Config.Handler = service
	serving.Start()
	admission, err := service.Admit(context.Background(), InvokeInput{FunctionName: "outer", Payload: []byte(`{"id":"nested"}`)})
	if err != nil {
		t.Fatal(err)
	}
	waitForFile(t, started)
	drainCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	drained := make(chan error, 1)
	go func() { drained <- service.DrainAsync(drainCtx) }()
	waitAsyncAdmissionClosed(t, service)
	response := requestInvoke(service, "outer", `{}`, map[string]string{"X-Amz-Invocation-Type": "Event"})
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("new Event admitted during drain: %d %s", response.Code, response.Body.String())
	}
	if err := service.ValidateTarget("inner", ""); err != nil {
		t.Fatalf("drain disabled synchronous target: %v", err)
	}
	result, err := service.Execute(drainCtx, InvokeInput{FunctionName: "inner", Payload: []byte(`{"id":"direct"}`)})
	if err != nil || result.FunctionError {
		t.Fatalf("synchronous Execute during drain: %#v %v", result, err)
	}
	select {
	case err := <-drained:
		t.Fatalf("drain returned before accepted handler settled: %v", err)
	default:
	}
	if err := os.WriteFile(gate, nil, 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-drained:
		if err != nil {
			t.Fatalf("healthy drain: %v", err)
		}
	case <-drainCtx.Done():
		t.Fatal("accepted nested invocation did not drain")
	}
	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	var nested struct {
		Status int `json:"status"`
		Result struct {
			Input map[string]string `json:"input"`
		} `json:"result"`
	}
	if json.Unmarshal(data, &nested) != nil || nested.Status != 200 || nested.Result.Input["id"] != "nested" {
		t.Fatalf("nested HTTP invocation failed during drain: %s", data)
	}
	record := waitAsyncState(t, service, admission.RequestID, "succeeded")
	if record.Attempts != 1 || record.CompletedAt.IsZero() {
		t.Fatalf("unsettled accepted event: %#v", record)
	}
	if err := service.DrainAsync(context.Background()); err != nil {
		t.Fatalf("repeated healthy drain: %v", err)
	}
	result, err = service.Execute(drainCtx, InvokeInput{FunctionName: "inner", Payload: []byte(`{}`)})
	if err != nil || result.FunctionError {
		t.Fatalf("synchronous Execute after drain: %#v %v", result, err)
	}
}

func TestDrainAsyncRetainsEvidenceFailure(t *testing.T) {
	directory := t.TempDir()
	writer := &failAsyncEvidenceWriter{}
	service := newAsyncTestService(t, map[string]Function{"event": providedFunction(t, "echo")}, directory, &DevAsyncConfig{LogWriter: writer, RetryDelays: []time.Duration{0, 0}})
	admission, err := service.Admit(context.Background(), InvokeInput{FunctionName: "event", Payload: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	waitAsyncState(t, service, admission.RequestID, "succeeded")
	if err := service.DrainAsync(context.Background()); err == nil {
		t.Fatal("drain reported healthy completion after evidence failure")
	}
	if err := service.DrainAsync(context.Background()); err == nil {
		t.Fatal("repeated drain erased evidence failure")
	}
	result, err := service.Execute(context.Background(), InvokeInput{FunctionName: "event", Payload: []byte(`{}`)})
	if err != nil || result.FunctionError {
		t.Fatalf("evidence failure disabled synchronous Execute: %#v %v", result, err)
	}
	if err := service.Close(context.Background()); err == nil {
		t.Fatal("final Close erased evidence failure")
	}
}

func assertServiceUnavailable(t *testing.T, err error) {
	t.Helper()
	var rejected *InvokeError
	if !errors.As(err, &rejected) || rejected.Status != http.StatusServiceUnavailable || rejected.Code != "ServiceException" {
		t.Fatalf("expected closed Event admission: %v", err)
	}
}
