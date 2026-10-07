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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The subprocess is a real command fixture, retaining the exact request bytes.
func TestAsyncCommandProcess(t *testing.T) {
	mode := os.Getenv("EVENTBUS_ASYNC_FIXTURE")
	if mode == "" {
		return
	}
	payload, _ := io.ReadAll(os.Stdin)
	var event struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(payload, &event)
	if gate := os.Getenv("EVENTBUS_ASYNC_GATE"); gate != "" {
		_ = os.WriteFile(os.Getenv("EVENTBUS_ASYNC_STARTED"), []byte("started"), 0600)
		for {
			if _, err := os.Stat(gate); err == nil {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	encoded, _ := json.Marshal(map[string]string{"id": event.ID, "payload": base64.StdEncoding.EncodeToString(payload)})
	previous, _ := os.ReadFile(os.Getenv("EVENTBUS_ASYNC_OUTPUT"))
	file, err := os.OpenFile(os.Getenv("EVENTBUS_ASYNC_OUTPUT"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		os.Exit(5)
	}
	_, err = file.Write(append(encoded, '\n'))
	_ = file.Close()
	if err != nil {
		os.Exit(6)
	}
	if mode == "fail" || mode == "retry" && len(previous) == 0 {
		os.Exit(7)
	}
	fmt.Print(`{"ok":true}`)
	os.Exit(0)
}

func newAsyncTestService(t *testing.T, functions map[string]Function, directory string, dev *DevAsyncConfig) *Service {
	t.Helper()
	if dev == nil {
		dev = &DevAsyncConfig{}
	}
	if dev.LogPath == "" && dev.LogWriter == nil {
		dev.LogWriter = io.Discard
	}
	service, err := NewService(&Config{Functions: functions, DevAsync: dev}, directory)
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

func waitAsyncState(t *testing.T, service *Service, id, state string) AsyncRecord {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, record := range service.AsyncSnapshot() {
			if record.RequestID == id && record.State == state {
				return record
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("async request %s did not reach %s: %#v", id, state, service.AsyncSnapshot())
	return AsyncRecord{}
}
func asyncCommand(t *testing.T, output, gate, started, mode string) Function {
	t.Helper()
	command, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return Function{Runtime: "command", Command: []string{command, "-test.run=^TestAsyncCommandProcess$"}, Timeout: 8 * time.Second, Environment: map[string]string{"EVENTBUS_ASYNC_FIXTURE": mode, "EVENTBUS_ASYNC_OUTPUT": output, "EVENTBUS_ASYNC_GATE": gate, "EVENTBUS_ASYNC_STARTED": started}}
}

func TestEventInvokeHTTP202AliasLimitAndDelayedHandler(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("Node unavailable")
	}
	directory := t.TempDir()
	marker := filepath.Join(directory, "result")
	gate := filepath.Join(directory, "gate")
	writeFixture(t, directory, "event.mjs", `import fs from 'node:fs';
export async function base(event) { await new Promise(r=>setTimeout(r,100)); fs.writeFileSync(process.env.MARKER,JSON.stringify({selected:'base',event})); return {}; }
export async function alias(event) { if (event.id==='accepted') while (!fs.existsSync(process.env.GATE)) await new Promise(r=>setTimeout(r,5)); fs.writeFileSync(process.env.MARKER,JSON.stringify({selected:'alias',event,client:process.env.EVENTBUS_LAMBDA_CLIENT_CONTEXT})); return {}; }`)
	base := Function{Runtime: "node", Handler: "event.mjs.base", Environment: map[string]string{"MARKER": marker, "GATE": gate}, Timeout: 3 * time.Second}
	alias := base
	alias.Handler = "event.mjs.alias"
	service := newAsyncTestService(t, map[string]Function{"event": base, "event:live": alias}, directory, &DevAsyncConfig{RetryDelays: []time.Duration{0, 0}})
	response := requestInvoke(service, "event:live", `{"id":"accepted"}`, map[string]string{"X-Amz-Invocation-Type": "Event", "X-Amz-Log-Type": "Tail", "X-Amz-Client-Context": base64.StdEncoding.EncodeToString([]byte(`{"custom":{"ignored":true}}`))})
	if response.Code != 202 || response.Body.Len() != 0 || response.Header().Get("X-Amz-Function-Error") != "" || response.Header().Get("X-Amz-Log-Result") != "" {
		t.Fatalf("admission: %d %s %#v", response.Code, response.Body.String(), response.Header())
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("handler completed before admission returned: %v", err)
	}
	if err := os.WriteFile(gate, nil, 0600); err != nil {
		t.Fatal(err)
	}
	waitForFile(t, marker)
	data, _ := os.ReadFile(marker)
	var sideEffect struct {
		Selected, Client string
		Event            map[string]string
	}
	if json.Unmarshal(data, &sideEffect) != nil || sideEffect.Selected != "alias" || sideEffect.Event["id"] != "accepted" || sideEffect.Client != "" {
		t.Fatalf("handler side effect: %s", data)
	}
	unknown := requestInvoke(service, "event:absent", `{}`, map[string]string{"X-Amz-Invocation-Type": "Event"})
	if unknown.Code != 404 {
		t.Fatalf("unknown alias: %d %s", unknown.Code, unknown.Body.String())
	}
	oversized := requestInvoke(service, "event", `"`+strings.Repeat("x", maxEventPayload-1)+`"`, map[string]string{"X-Amz-Invocation-Type": "Event"})
	if oversized.Code != 413 {
		t.Fatalf("oversize: %d %s", oversized.Code, oversized.Body.String())
	}
	exact := requestInvoke(service, "event", `"`+strings.Repeat("x", maxEventPayload-2)+`"`, map[string]string{"X-Amz-Invocation-Type": "Event"})
	if exact.Code != 202 || exact.Body.Len() != 0 {
		t.Fatalf("exact limit: %d %s", exact.Code, exact.Body.String())
	}
}

func TestEventBoundedAdmissionCopiesPayloadAndHealthyCloseDrains(t *testing.T) {
	directory := t.TempDir()
	output := filepath.Join(directory, "events.jsonl")
	gate := filepath.Join(directory, "gate")
	started := filepath.Join(directory, "started")
	service := newAsyncTestService(t, map[string]Function{"event": asyncCommand(t, output, gate, started, "success")}, directory, &DevAsyncConfig{Workers: 1, Capacity: 2, RetryDelays: []time.Duration{0, 0}})
	payload := []byte(" { \"id\" : \"one\" } \n")
	original := append([]byte(nil), payload...)
	ctx, cancel := context.WithCancel(context.Background())
	first, err := service.Admit(ctx, InvokeInput{FunctionName: "event", Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	copy(payload, bytes.Repeat([]byte{' '}, len(payload)))
	waitForFile(t, started)
	second, err := service.Admit(context.Background(), InvokeInput{FunctionName: "event", Payload: []byte(`{"id":"two"}`)})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Admit(context.Background(), InvokeInput{FunctionName: "event", Payload: []byte(`{"id":"overflow"}`)})
	var rejected *InvokeError
	if !errors.As(err, &rejected) || rejected.Status != 429 || rejected.Code != "TooManyRequestsException" {
		t.Fatalf("overflow: %v", err)
	}
	if err := os.WriteFile(gate, nil, 0600); err != nil {
		t.Fatal(err)
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer closeCancel()
	if err := service.Close(closeCtx); err != nil {
		t.Fatalf("healthy drain: %v", err)
	}
	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSpace(data), []byte{'\n'})
	if len(lines) != 2 {
		t.Fatalf("lost accepted events: %s", data)
	}
	found := map[string]string{}
	for _, line := range lines {
		var value map[string]string
		if json.Unmarshal(line, &value) != nil {
			t.Fatal("invalid side effect")
		}
		found[value["id"]] = value["payload"]
	}
	if found["one"] != base64.StdEncoding.EncodeToString(original) || found["two"] == "" || found["overflow"] != "" {
		t.Fatalf("owned bytes: %#v", found)
	}
	for _, id := range []string{first.RequestID, second.RequestID} {
		record := waitAsyncState(t, service, id, "succeeded")
		if record.Attempts != 1 || record.CompletedAt.IsZero() {
			t.Fatalf("premature completion: %#v", record)
		}
	}
	if err := service.ValidateTarget("event", ""); err == nil {
		t.Fatal("closed service validated target")
	}
}

func TestEventRetriesTerminalFailureAndEvidenceRedaction(t *testing.T) {
	directory := t.TempDir()
	output := filepath.Join(directory, "attempts.jsonl")
	var evidence bytes.Buffer
	service := newAsyncTestService(t, map[string]Function{"event": asyncCommand(t, output, "", "", "fail")}, directory, &DevAsyncConfig{Workers: 1, RetryDelays: []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}, LogWriter: &evidence})
	admission, err := service.Admit(context.Background(), InvokeInput{FunctionName: "event", Payload: []byte(`{"id":"secret-payload-value"}`)})
	if err != nil {
		t.Fatal(err)
	}
	record := waitAsyncState(t, service, admission.RequestID, "failed")
	if record.Attempts != 3 || record.ErrorType != "Runtime.ExitError" {
		t.Fatalf("retry settlement: %#v", record)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := service.Close(ctx); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if len(bytes.Split(bytes.TrimSpace(data), []byte{'\n'})) != 3 {
		t.Fatalf("attempts: %s", data)
	}
	if strings.Contains(evidence.String(), "secret-payload-value") {
		t.Fatal("payload entered execution evidence")
	}
	for _, line := range bytes.Split(bytes.TrimSpace(evidence.Bytes()), []byte{'\n'}) {
		var value AsyncRecord
		if json.Unmarshal(line, &value) != nil || value.SchemaVersion != "eventbus.lambda.async.v1" {
			t.Fatalf("evidence: %s", line)
		}
	}
}

func TestEventTimeoutRetriesAndNativeDefaults(t *testing.T) {
	function := providedFunction(t, "wait")
	function.Timeout = 50 * time.Millisecond
	service := newAsyncTestService(t, map[string]Function{"wait": function}, t.TempDir(), &DevAsyncConfig{RetryDelays: []time.Duration{0, 0}})
	admission, err := service.Admit(context.Background(), InvokeInput{FunctionName: "wait", Payload: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	record := waitAsyncState(t, service, admission.RequestID, "failed")
	if record.Attempts != 3 || record.ErrorType != "Sandbox.Timedout" {
		t.Fatalf("timeout retries: %#v", record)
	}
	defaults := newAsyncTestService(t, map[string]Function{"echo": providedFunction(t, "echo")}, t.TempDir(), nil)
	if defaults.asyncRetryDelays != [2]time.Duration{time.Minute, 2 * time.Minute} || defaults.asyncCapacity != 64 {
		t.Fatal("native retry defaults changed")
	}
}

func TestEventRetryWaitingCountsCapacityAndSuccessSettlesOnce(t *testing.T) {
	directory := t.TempDir()
	output := filepath.Join(directory, "attempts")
	service := newAsyncTestService(t, map[string]Function{"event": asyncCommand(t, output, "", "", "retry")}, directory, &DevAsyncConfig{Workers: 1, Capacity: 1, HistoryLimit: 1, RetryDelays: []time.Duration{10 * time.Second, 0}})
	admission, err := service.Admit(context.Background(), InvokeInput{FunctionName: "event", Payload: []byte(`{"id":"retry"}`)})
	if err != nil {
		t.Fatal(err)
	}
	waitAsyncState(t, service, admission.RequestID, "retrying")
	_, err = service.Admit(context.Background(), InvokeInput{FunctionName: "event", Payload: []byte(`{"id":"must-refuse"}`)})
	var capacity *InvokeError
	if !errors.As(err, &capacity) || capacity.Status != 429 {
		t.Fatalf("retry released admitted capacity: %v", err)
	}
	// Advance this isolated test's pending retry rather than waiting ten seconds.
	service.mu.Lock()
	service.asyncTasks[admission.RequestID].readyAt = time.Now()
	service.wakeAsyncLocked()
	service.mu.Unlock()
	terminal := waitAsyncState(t, service, admission.RequestID, "succeeded")
	if terminal.Attempts != 2 {
		t.Fatalf("retry success: %#v", terminal)
	}
	next, err := service.Admit(context.Background(), InvokeInput{FunctionName: "event", Payload: []byte(`{"id":"next"}`)})
	if err != nil {
		t.Fatal(err)
	}
	waitAsyncState(t, service, next.RequestID, "succeeded")
	records := service.AsyncSnapshot()
	if len(records) != 1 || records[0].RequestID != next.RequestID {
		t.Fatalf("terminal history not bounded: %#v", records)
	}
}

func TestEventAgeExpirationHasTerminalEvidenceWithoutExecution(t *testing.T) {
	directory := t.TempDir()
	output := filepath.Join(directory, "events")
	gate := filepath.Join(directory, "gate")
	started := filepath.Join(directory, "started")
	service := newAsyncTestService(t, map[string]Function{"event": asyncCommand(t, output, gate, started, "success")}, directory, &DevAsyncConfig{Workers: 1, Capacity: 2, RetryDelays: []time.Duration{0, 0}})
	_, err := service.Admit(context.Background(), InvokeInput{FunctionName: "event", Payload: []byte(`{"id":"running"}`)})
	if err != nil {
		t.Fatal(err)
	}
	waitForFile(t, started)
	expired, err := service.Admit(context.Background(), InvokeInput{FunctionName: "event", Payload: []byte(`{"id":"expired"}`)})
	if err != nil {
		t.Fatal(err)
	}
	service.mu.Lock()
	service.asyncTasks[expired.RequestID].record.QueuedAt = time.Now().Add(-maxEventAge - time.Second)
	service.mu.Unlock()
	if err := os.WriteFile(gate, nil, 0600); err != nil {
		t.Fatal(err)
	}
	record := waitAsyncState(t, service, expired.RequestID, "failed")
	if record.Attempts != 0 || record.ErrorType != "EventAgeExceeded" {
		t.Fatalf("expired event: %#v", record)
	}
	data, err := os.ReadFile(output)
	if err != nil || bytes.Contains(data, []byte(`"id":"expired"`)) {
		t.Fatalf("expired handler executed: %s %v", data, err)
	}
}

type failAsyncEvidenceWriter struct{ calls int }

func (writer *failAsyncEvidenceWriter) Write(data []byte) (int, error) {
	writer.calls++
	if writer.calls == 1 {
		return len(data), nil
	}
	return 0, errors.New("fixture evidence failure")
}
func TestEventEvidenceFailurePreservesAcceptedExecutionAndFailsClose(t *testing.T) {
	writer := &failAsyncEvidenceWriter{}
	service := newAsyncTestService(t, map[string]Function{"echo": providedFunction(t, "echo")}, t.TempDir(), &DevAsyncConfig{LogWriter: writer, RetryDelays: []time.Duration{0, 0}})
	accepted, err := service.Admit(context.Background(), InvokeInput{FunctionName: "echo", Payload: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	waitAsyncState(t, service, accepted.RequestID, "succeeded")
	_, err = service.Admit(context.Background(), InvokeInput{FunctionName: "echo", Payload: []byte(`{}`)})
	var rejected *InvokeError
	if !errors.As(err, &rejected) || rejected.Status != 500 {
		t.Fatalf("terminal capture did not stop admission: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := service.Close(ctx); err == nil {
		t.Fatal("evidence failure reported healthy drain")
	}
}

func TestTypedExecutionPreflightAndFunctionResult(t *testing.T) {
	service := newAsyncTestService(t, map[string]Function{"echo:2": providedFunction(t, "echo"), "error": providedFunction(t, "error")}, t.TempDir(), nil)
	if err := service.ValidateTarget("arn:aws:lambda:us-east-1:000000000000:function:echo:missing", ""); err == nil {
		t.Fatal("unknown alias passed preflight")
	}
	output, err := service.Execute(context.Background(), InvokeInput{FunctionName: "123456789012:function:echo", Qualifier: "2", Payload: []byte(`{"native":true}`)})
	if err != nil || output.FunctionError || output.RequestID == "" || output.ExecutedVersion != "2" {
		t.Fatalf("execute: %#v %v", output, err)
	}
	failed, err := service.Execute(context.Background(), InvokeInput{FunctionName: "error", Payload: []byte(`{}`)})
	if err != nil || !failed.FunctionError || !bytes.Contains(failed.Payload, []byte("Unauthorized")) {
		t.Fatalf("handler failure changed: %#v %v", failed, err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := service.Execute(canceled, InvokeInput{FunctionName: "error", Payload: []byte(`{}`)}); !errors.Is(err, context.Canceled) {
		t.Fatalf("caller cancellation: %v", err)
	}
	// DryRun remains validation-only, independent of invalid JSON content.
	response := requestInvoke(service, "error", "not JSON", map[string]string{"X-Amz-Invocation-Type": "DryRun"})
	if response.Code != http.StatusNoContent || response.Body.Len() != 0 {
		t.Fatal("DryRun contract changed")
	}
}
