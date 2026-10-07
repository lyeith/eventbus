//go:build linux || darwin

package lambda

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/devcapture"
	"github.com/lyeith/eventbus/internal/localexec"
)

func newDiagnosticTestService(t *testing.T, functions map[string]Function, directory string, activity DevActivity, async *DevAsyncConfig) (*Service, string) {
	t.Helper()
	if async == nil {
		async = &DevAsyncConfig{Workers: 1, LogWriter: io.Discard, RetryDelays: []time.Duration{0, 0}}
	}
	path := filepath.Join(directory, "private", "invocations.jsonl")
	service, err := NewService(&Config{Functions: functions, DevActivity: activity, DevAsync: async, DevDiagnostics: &DevDiagnosticsConfig{LogPath: path}}, directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = service.Close(ctx)
	})
	return service, path
}

func readDiagnosticRecords(t *testing.T, path string) []invocationDiagnosticRecord {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	var records []invocationDiagnosticRecord
	for {
		var record invocationDiagnosticRecord
		if err := decoder.Decode(&record); errors.Is(err, io.EOF) {
			return records
		} else if err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
	}
}

func diagnosticData(t *testing.T, output diagnosticOutput) []byte {
	t.Helper()
	switch output.Encoding {
	case "utf8":
		return []byte(output.Data)
	case "base64":
		data, err := base64.StdEncoding.DecodeString(output.Data)
		if err != nil {
			t.Fatal(err)
		}
		return data
	default:
		t.Fatalf("unknown diagnostic encoding %q", output.Encoding)
		return nil
	}
}

func requireDiagnosticNode(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("Node unavailable")
	}
}

func waitDiagnosticMarker(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("handler did not reach marker %s", path)
}

func TestPrivateDiagnosticsCaughtErrorPreservesNativeSuccessAndTail(t *testing.T) {
	requireDiagnosticNode(t)
	directory := t.TempDir()
	writeFixture(t, directory, "caught.mjs", `
export async function handler(event, context) {
  console.log('private stdout ' + context.awsRequestId);
  try { throw new Error('caught business failure'); }
  catch (error) { console.error(error.message); }
  return {requestId: context.awsRequestId, arn: context.invokedFunctionArn, response: 'successful-payload-must-stay-private-to-response'};
}
`)
	function := Function{Runtime: "node", Handler: "caught.mjs.handler", Timeout: 5 * time.Second}
	service, path := newDiagnosticTestService(t, map[string]Function{"caught:live": function}, directory, nil, nil)
	arn := "arn:aws:lambda:eu-west-1:123456789012:function:caught:live"
	response := requestInvoke(service, arn, `{"secret":"input-must-not-enter-diagnostics"}`, map[string]string{"X-Amz-Log-Type": "Tail"})
	if response.Code != 200 || response.Header().Get("X-Amz-Function-Error") != "" {
		t.Fatalf("native success changed: %d %s", response.Code, response.Body.String())
	}
	var output struct {
		RequestID string `json:"requestId"`
		ARN       string `json:"arn"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &output); err != nil {
		t.Fatal(err)
	}
	records := readDiagnosticRecords(t, path)
	if len(records) != 1 {
		t.Fatalf("attempt records: %#v", records)
	}
	record := records[0]
	if record.SchemaVersion != "eventbus.lambda.invocation-diagnostic.v1" || record.RequestID == "" || record.RequestID != output.RequestID || record.FunctionARN != arn || output.ARN != arn || record.FunctionName != "caught:live" || record.Runtime != "node" || record.InvocationType != "RequestResponse" || record.Attempt != 1 || record.State != InvocationSucceeded || record.FunctionError || !record.OwnershipConfirmed || record.FunctionDiagnostic != nil || record.StartedAt.IsZero() || record.CompletedAt.Before(record.StartedAt) {
		t.Fatalf("native identity/result: %#v", record)
	}
	if record.Stdout == nil || !strings.Contains(string(diagnosticData(t, *record.Stdout)), "private stdout "+output.RequestID) || !strings.Contains(string(diagnosticData(t, record.Stderr)), "caught business failure") {
		t.Fatalf("separate handler streams: %#v", record)
	}
	nativeTail, err := base64.StdEncoding.DecodeString(response.Header().Get("X-Amz-Log-Result"))
	if err != nil || !bytes.Equal(nativeTail, diagnosticData(t, record.Tail)) {
		t.Fatalf("native Tail changed: %q %v", nativeTail, err)
	}
	captured, _ := os.ReadFile(path)
	if bytes.Contains(captured, []byte("input-must-not-enter-diagnostics")) || bytes.Contains(captured, []byte("successful-payload-must-stay-private-to-response")) {
		t.Fatal("diagnostics copied successful input or response")
	}
	if service.DevDiagnosticsPath() != path || service.DevEvidence() != nil {
		t.Fatalf("healthy capture: %s %v", service.DevDiagnosticsPath(), service.DevEvidence())
	}
}

func TestObservedExecutionUsesRealIdentityInsideAdmittedLifetime(t *testing.T) {
	observer := &activityRecorder{}
	service, path := newDiagnosticTestService(t, map[string]Function{"echo:2": providedFunction(t, "echo")}, t.TempDir(), observer, nil)
	arn := "arn:aws:lambda:eu-west-1:123456789012:function:echo:2"
	var admitted InvocationMetadata
	outcome, err := service.ExecuteObserved(context.Background(), InvokeInput{FunctionName: arn, Payload: []byte("{}")}, func(metadata InvocationMetadata) error {
		// DescribeTarget takes mu: this must work without a lock inversion.
		if _, err := service.DescribeTarget(arn, ""); err != nil {
			return err
		}
		active, _, completed := observer.snapshot()
		if active != 1 || len(completed) != 0 || metadata.RequestID == "" || metadata.FunctionARN != arn || metadata.FunctionName != "echo:2" || metadata.Attempt != 1 {
			return errors.New("observer ran outside its actual admitted lifetime")
		}
		admitted = metadata
		return nil
	})
	if err != nil || outcome.State != InvocationSucceeded || outcome.OwnershipErr != nil || outcome.Metadata != admitted || outcome.Output.RequestID != admitted.RequestID || outcome.Output.FunctionError || outcome.Output.ExecutedVersion != "2" {
		t.Fatalf("observed outcome: %#v %v", outcome, err)
	}
	var payload struct {
		RequestID string `json:"requestId"`
		ARN       string `json:"arn"`
	}
	if err := json.Unmarshal(outcome.Output.Payload, &payload); err != nil || payload.RequestID != admitted.RequestID || payload.ARN != admitted.FunctionARN {
		t.Fatalf("runner identity was replaced: %#v %v", payload, err)
	}
	assertActivityFinished(t, observer, "lambda_invoke", nil)
	records := readDiagnosticRecords(t, path)
	if len(records) != 1 || records[0].RequestID != admitted.RequestID || records[0].State != InvocationSucceeded || !records[0].OwnershipConfirmed {
		t.Fatalf("joined attempt: %#v", records)
	}
}

func TestObservedRefusalAndAbnormalObserverNeverLaunchOrAttestSuccess(t *testing.T) {
	for _, mode := range []string{"error", "panic", "goexit"} {
		t.Run(mode, func(t *testing.T) {
			directory := t.TempDir()
			marker := filepath.Join(directory, "child-pid")
			function := providedFunction(t, "wait")
			function.Environment["EVENTBUS_LAMBDA_TEST_PID"] = marker
			observer := &activityRecorder{}
			service, path := newDiagnosticTestService(t, map[string]Function{"wait": function}, directory, observer, nil)
			refused := errors.New("private admission capture refused")
			var metadata InvocationMetadata
			callback := func(admitted InvocationMetadata) error {
				metadata = admitted
				switch mode {
				case "panic":
					panic(refused)
				case "goexit":
					runtime.Goexit()
				}
				return refused
			}
			switch mode {
			case "error":
				outcome, err := service.ExecuteObserved(context.Background(), InvokeInput{FunctionName: "wait", Payload: []byte("{}")}, callback)
				if !errors.Is(err, refused) || !errors.Is(outcome.OwnershipErr, refused) || outcome.Metadata != metadata || outcome.State != InvocationNotStarted {
					t.Fatalf("refused attempt: %#v %v", outcome, err)
				}
			case "panic":
				func() {
					defer func() {
						if recover() != refused {
							t.Error("observer panic was swallowed or replaced")
						}
					}()
					_, _ = service.ExecuteObserved(context.Background(), InvokeInput{FunctionName: "wait", Payload: []byte("{}")}, callback)
					t.Error("panic returned normally")
				}()
			case "goexit":
				done := make(chan struct{})
				go func() {
					defer close(done)
					_, _ = service.ExecuteObserved(context.Background(), InvokeInput{FunctionName: "wait", Payload: []byte("{}")}, callback)
				}()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("Goexit did not retire the admitted owner")
				}
			}
			if _, err := os.Stat(marker); !os.IsNotExist(err) {
				t.Fatalf("untracked child launched: %v", err)
			}
			active, begun, completed := observer.snapshot()
			if metadata.RequestID == "" || active != 0 || len(begun) != 1 || len(completed) != 1 || completed[0].err == nil || service.DevEvidence() == nil {
				t.Fatalf("abnormal observer released healthy: active=%d completed=%#v evidence=%v", active, completed, service.DevEvidence())
			}
			records := readDiagnosticRecords(t, path)
			if len(records) != 1 || records[0].RequestID != metadata.RequestID || records[0].State != InvocationNotStarted || records[0].OwnershipConfirmed || records[0].OwnershipError == "" {
				t.Fatalf("false terminal attestation: %#v", records)
			}
			if err := service.DrainAsync(context.Background()); err != nil {
				t.Fatalf("synchronous observer uncertainty poisoned async drain: %v", err)
			}
			if err := service.Close(context.Background()); err == nil || service.DevEvidence() == nil {
				t.Fatalf("strict owner closure hid observer uncertainty: %v %v", err, service.DevEvidence())
			}
		})
	}
}

func TestPrivateDiagnosticsAndLeaseWaitForActualOwnedCleanup(t *testing.T) {
	observer := &activityRecorder{}
	service, path := newDiagnosticTestService(t, map[string]Function{"echo": providedFunction(t, "echo")}, t.TempDir(), observer, nil)
	entered, gate := make(chan struct{}), make(chan struct{})
	var gateOnce sync.Once
	openGate := func() { gateOnce.Do(func() { close(gate) }) }
	t.Cleanup(openGate)
	service.processCleanup = func(command *exec.Cmd) error {
		err := localexec.Cleanup(command)
		close(entered)
		<-gate
		return err
	}
	type completion struct {
		outcome InvocationOutcome
		err     error
	}
	done := make(chan completion, 1)
	go func() {
		outcome, err := service.ExecuteObserved(context.Background(), InvokeInput{FunctionName: "echo", Payload: []byte("{}")}, nil)
		done <- completion{outcome, err}
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("runner never reached cleanup")
	}
	if records := readDiagnosticRecords(t, path); len(records) != 0 {
		t.Fatalf("diagnostics published before cleanup returned: %#v", records)
	}
	if active, _, completed := observer.snapshot(); active != 1 || len(completed) != 0 {
		t.Fatalf("cleanup owner falsely quiescent: active=%d completed=%#v", active, completed)
	}
	select {
	case result := <-done:
		t.Fatalf("execution returned before owner joined: %#v", result)
	default:
	}
	openGate()
	result := <-done
	if result.err != nil || result.outcome.State != InvocationSucceeded || result.outcome.OwnershipErr != nil {
		t.Fatalf("joined completion: %#v", result)
	}
	assertActivityFinished(t, observer, "lambda_invoke", nil)
	if records := readDiagnosticRecords(t, path); len(records) != 1 || !records[0].OwnershipConfirmed {
		t.Fatalf("missing joined diagnostic: %#v", records)
	}
}

func TestPrivateDiagnosticsAsyncRetryUsesOriginalIDAndNativeErrorPolicy(t *testing.T) {
	requireDiagnosticNode(t)
	directory := t.TempDir()
	writeFixture(t, directory, "retry.mjs", `
import fs from 'node:fs';
export async function handler(event, context) {
  const count = fs.existsSync(process.env.COUNT) ? Number(fs.readFileSync(process.env.COUNT, 'utf8')) + 1 : 1;
  fs.writeFileSync(process.env.COUNT, String(count));
  console.log('attempt ' + count + ' ' + context.awsRequestId);
  console.error('private retry detail ' + count);
  if (count === 1) throw new Error('native first-attempt failure');
  return {response: 'successful async response must not be captured'};
}
`)
	var redacted bytes.Buffer
	observer := &activityRecorder{}
	function := Function{Runtime: "node", Handler: "retry.mjs.handler", Timeout: 5 * time.Second, Environment: map[string]string{"COUNT": filepath.Join(directory, "count")}}
	service, path := newDiagnosticTestService(t, map[string]Function{"retry:live": function}, directory, observer, &DevAsyncConfig{Workers: 1, LogWriter: &redacted, RetryDelays: []time.Duration{0, 0}})
	admission, err := service.Admit(context.Background(), InvokeInput{FunctionName: "arn:aws:lambda:us-east-1:123456789012:function:retry:live", Payload: []byte(`{"secret":"async-input-not-captured"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if terminal := waitAsyncState(t, service, admission.RequestID, "succeeded"); terminal.Attempts != 2 || terminal.ErrorType != "" {
		t.Fatalf("native retry changed: %#v", terminal)
	}
	assertActivityFinished(t, observer, "lambda_async", nil)
	records := readDiagnosticRecords(t, path)
	if len(records) != 2 {
		t.Fatalf("actual attempts: %#v", records)
	}
	for index, record := range records {
		if record.RequestID != admission.RequestID || record.Attempt != index+1 || record.InvocationType != "Event" || !record.OwnershipConfirmed || record.FunctionARN != "arn:aws:lambda:us-east-1:123456789012:function:retry:live" {
			t.Fatalf("attempt identity: %#v", record)
		}
	}
	if records[0].State != InvocationFailed || !records[0].FunctionError || records[0].FunctionDiagnostic == nil || !strings.Contains(string(diagnosticData(t, *records[0].FunctionDiagnostic)), "native first-attempt failure") || records[1].State != InvocationSucceeded || records[1].FunctionError || records[1].FunctionDiagnostic != nil {
		t.Fatalf("native attempt classification: %#v", records)
	}
	for _, private := range []string{"private retry detail", "native first-attempt failure", "async-input-not-captured", "successful async response must not be captured"} {
		if strings.Contains(redacted.String(), private) {
			t.Fatalf("AsyncRecord leaked private detail %q", private)
		}
	}
	captured, _ := os.ReadFile(path)
	if bytes.Contains(captured, []byte("async-input-not-captured")) || bytes.Contains(captured, []byte("successful async response must not be captured")) {
		t.Fatal("private diagnostic copied input or successful response")
	}
}

func TestPrivateDiagnosticsCancelAndAsyncTimeoutAfterRunnerJoin(t *testing.T) {
	requireDiagnosticNode(t)
	for _, mode := range []string{"cancel", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			directory := t.TempDir()
			marker := filepath.Join(directory, "started")
			writeFixture(t, directory, "wait.mjs", `
import fs from 'node:fs';
export async function handler() {
  console.log('waiting stdout');
  console.error('waiting stderr');
  fs.writeFileSync(process.env.MARKER, 'started');
  await new Promise(() => {});
}
`)
			function := Function{Runtime: "node", Handler: "wait.mjs.handler", Timeout: time.Second, Environment: map[string]string{"MARKER": marker}}
			observer := &activityRecorder{}
			service, path := newDiagnosticTestService(t, map[string]Function{"wait": function}, directory, observer, nil)
			var id string
			if mode == "cancel" {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				type completion struct {
					outcome InvocationOutcome
					err     error
				}
				done := make(chan completion, 1)
				go func() {
					outcome, err := service.ExecuteObserved(ctx, InvokeInput{FunctionName: "wait", Payload: []byte("{}")}, nil)
					done <- completion{outcome, err}
				}()
				waitDiagnosticMarker(t, marker)
				cancel()
				result := <-done
				id = result.outcome.Metadata.RequestID
				if !errors.Is(result.err, context.Canceled) || result.outcome.State != InvocationCanceled || !result.outcome.Output.FunctionError || result.outcome.Output.RequestID != id || result.outcome.OwnershipErr != nil {
					t.Fatalf("joined observed cancellation: %#v", result)
				}
				assertActivityFinished(t, observer, "lambda_invoke", nil)
			} else {
				admission, err := service.Admit(context.Background(), InvokeInput{FunctionName: "wait", Payload: []byte("{}")})
				if err != nil {
					t.Fatal(err)
				}
				id = admission.RequestID
				if terminal := waitAsyncState(t, service, id, "failed"); terminal.Attempts != 3 || terminal.ErrorType != "Sandbox.Timedout" {
					t.Fatalf("native timeout retries changed: %#v", terminal)
				}
				assertActivityFinished(t, observer, "lambda_async", nil)
			}
			records := readDiagnosticRecords(t, path)
			wantCount, wantState, contextError := 1, InvocationCanceled, "canceled"
			if mode == "timeout" {
				wantCount, wantState, contextError = 3, InvocationTimedOut, "deadline_exceeded"
			}
			if len(records) != wantCount {
				t.Fatalf("joined attempts: %#v", records)
			}
			for index, record := range records {
				if record.RequestID != id || record.Attempt != index+1 || record.State != wantState || !record.FunctionError || !record.OwnershipConfirmed || record.ContextError != contextError || record.FunctionDiagnostic == nil || !strings.Contains(string(diagnosticData(t, record.Stderr)), "waiting stderr") {
					t.Fatalf("joined canceled/timeout diagnostics: %#v", record)
				}
			}
		})
	}
}

func TestPrivateDiagnosticsBoundsPreserveBinaryStreamsAndNativeTail(t *testing.T) {
	requireDiagnosticNode(t)
	directory := t.TempDir()
	writeFixture(t, directory, "bounded.mjs", `
import fs from 'node:fs';
export async function handler() {
  fs.writeSync(1, Buffer.alloc(65536 + 17, 255));
  fs.writeSync(1, Buffer.from('stdout-end'));
  fs.writeSync(2, Buffer.alloc(65536 + 23, 122));
  fs.writeSync(2, Buffer.from('stderr-end'));
  return {response: 'not-a-diagnostic'};
}
export async function failed() { throw new Error('f'.repeat(70 * 1024)); }
`)
	service, path := newDiagnosticTestService(t, map[string]Function{
		"bounded": {Runtime: "node", Handler: "bounded.mjs.handler", Timeout: 5 * time.Second},
		"failed":  {Runtime: "node", Handler: "bounded.mjs.failed", Timeout: 5 * time.Second},
	}, directory, nil, nil)
	response := requestInvoke(service, "bounded", "{}", map[string]string{"X-Amz-Log-Type": "Tail"})
	if response.Code != 200 || response.Header().Get("X-Amz-Function-Error") != "" {
		t.Fatalf("native bounded response: %d %s", response.Code, response.Body.String())
	}
	records := readDiagnosticRecords(t, path)
	if len(records) != 1 || records[0].Stdout == nil {
		t.Fatalf("bounded diagnostic: %#v", records)
	}
	record := records[0]
	stdout, stderr := diagnosticData(t, *record.Stdout), diagnosticData(t, record.Stderr)
	if len(stdout) != maxLogs || record.Stdout.Encoding != "base64" || record.Stdout.Bytes != maxLogs+17+int64(len("stdout-end")) || !record.Stdout.Truncated || !bytes.HasSuffix(stdout, []byte("stdout-end")) || len(stderr) != maxLogs || record.Stderr.Encoding != "utf8" || record.Stderr.Bytes != maxLogs+23+int64(len("stderr-end")) || !record.Stderr.Truncated || !bytes.HasSuffix(stderr, []byte("stderr-end")) {
		t.Fatalf("stream bounds: stdout=%d/%#v stderr=%d/%#v", len(stdout), *record.Stdout, len(stderr), record.Stderr)
	}
	nativeTail, err := base64.StdEncoding.DecodeString(response.Header().Get("X-Amz-Log-Result"))
	if err != nil || len(nativeTail) != 4096 || !bytes.Equal(nativeTail, diagnosticData(t, record.Tail)) || !record.Tail.Truncated || record.Tail.Bytes != record.Stdout.Bytes+record.Stderr.Bytes {
		t.Fatalf("tail bounds: %d %#v %v", len(nativeTail), record.Tail, err)
	}
	outcome, err := service.ExecuteObserved(context.Background(), InvokeInput{FunctionName: "failed", Payload: []byte("{}")}, nil)
	if err != nil || !outcome.Output.FunctionError || outcome.State != InvocationFailed {
		t.Fatalf("native large error: %#v %v", outcome, err)
	}
	records = readDiagnosticRecords(t, path)
	if len(records) != 2 {
		t.Fatalf("missing actual failure diagnostic: %#v", records)
	}
	failure := records[1].FunctionDiagnostic
	if failure == nil || len(diagnosticData(t, *failure)) != maxDiagnosticFailure || !failure.Truncated || failure.Bytes != int64(len(outcome.Output.Payload)) {
		t.Fatalf("function failure bounds: %#v", failure)
	}
}

func TestPrivateDiagnosticsCommandStdoutRemainsOnlyNativeResponse(t *testing.T) {
	function := providedFunction(t, "command")
	function.Runtime = "command"
	service, path := newDiagnosticTestService(t, map[string]Function{"command": function}, t.TempDir(), nil, nil)
	outcome, err := service.ExecuteObserved(context.Background(), InvokeInput{FunctionName: "command", Payload: []byte(`{"response_secret":"successful-command-payload"}`)}, nil)
	if err != nil || outcome.State != InvocationSucceeded || !bytes.Contains(outcome.Output.Payload, []byte("successful-command-payload")) {
		t.Fatalf("native command response changed: %#v %v", outcome, err)
	}
	records := readDiagnosticRecords(t, path)
	if len(records) != 1 || records[0].Stdout != nil || !records[0].StdoutIsResponse || !strings.Contains(string(diagnosticData(t, records[0].Stderr)), "command diagnostic") {
		t.Fatalf("command stream policy: %#v", records)
	}
	captured, _ := os.ReadFile(path)
	if bytes.Contains(captured, []byte("successful-command-payload")) {
		t.Fatal("command response stdout leaked into private diagnostics")
	}
	plain := newTestService(t, map[string]Function{"command": function}, t.TempDir())
	if plain.DevDiagnosticsPath() != "" || plain.diagnosticCapture != nil || plain.DevEvidence() != nil {
		t.Fatal("diagnostics enabled without explicit configuration")
	}
	output, err := plain.Execute(context.Background(), InvokeInput{FunctionName: "command", Payload: []byte("{}")})
	if err != nil || output.FunctionError || !json.Valid(output.Payload) {
		t.Fatalf("default native Execute: %#v %v", output, err)
	}
}

func TestPrivateDiagnosticsRejectAliasedAsyncCaptureBeforeWriting(t *testing.T) {
	for _, mode := range []string{"same", "missing-parent-alias", "hardlink"} {
		t.Run(mode, func(t *testing.T) {
			directory := t.TempDir()
			privatePath := filepath.Join(directory, "private.jsonl")
			asyncPath := privatePath
			if mode == "missing-parent-alias" {
				realParent := filepath.Join(directory, "real")
				if err := os.Mkdir(realParent, 0700); err != nil {
					t.Fatal(err)
				}
				aliasParent := filepath.Join(directory, "alias")
				if err := os.Symlink(realParent, aliasParent); err != nil {
					t.Fatal(err)
				}
				privatePath = filepath.Join(realParent, "private.jsonl")
				asyncPath = filepath.Join(aliasParent, "private.jsonl")
			} else if mode == "hardlink" {
				if err := os.WriteFile(privatePath, nil, 0600); err != nil {
					t.Fatal(err)
				}
				asyncPath = filepath.Join(directory, "async.jsonl")
				if err := os.Link(privatePath, asyncPath); err != nil {
					t.Fatal(err)
				}
			}
			service, err := NewService(&Config{Functions: map[string]Function{"echo": providedFunction(t, "echo")}, DevDiagnostics: &DevDiagnosticsConfig{LogPath: privatePath}, DevAsync: &DevAsyncConfig{LogPath: asyncPath}}, directory)
			if err == nil || service != nil || !strings.Contains(err.Error(), "separate files") {
				t.Fatalf("aliased captures accepted: %v %v", service, err)
			}
			if data, err := os.ReadFile(privatePath); err == nil && len(data) != 0 {
				t.Fatalf("constructor wrote mixed capture data: %q", data)
			} else if err != nil && !os.IsNotExist(err) {
				t.Fatal(err)
			}
		})
	}
	for _, path := range []string{"", "-"} {
		config := &Config{Functions: map[string]Function{"echo": providedFunction(t, "echo")}, DevDiagnostics: &DevDiagnosticsConfig{LogPath: path}}
		if err := config.Validate(); err == nil {
			t.Fatalf("non-private diagnostics path accepted: %q", path)
		}
	}
}

type unavailableDiagnosticWriter struct{ err error }

func (writer unavailableDiagnosticWriter) Write([]byte) (int, error) { return 0, writer.err }

func TestPrivateDiagnosticCaptureFailureIsStickyWithoutChangingNativeRetries(t *testing.T) {
	observer := &activityRecorder{}
	service, _ := newDiagnosticTestService(t, map[string]Function{"echo": providedFunction(t, "echo")}, t.TempDir(), observer, nil)
	if err := service.diagnosticCapture.Close(); err != nil {
		t.Fatal(err)
	}
	unavailable := errors.New("private fixture storage unavailable")
	service.diagnosticCapture = devcapture.NewWriter(unavailableDiagnosticWriter{unavailable}, "private diagnostic fixture")
	outcome, err := service.ExecuteObserved(context.Background(), InvokeInput{FunctionName: "echo", Payload: []byte("{}")}, nil)
	if err != nil || outcome.State != InvocationSucceeded || outcome.Output.FunctionError || !errors.Is(outcome.OwnershipErr, unavailable) || !errors.Is(service.DevEvidence(), unavailable) {
		t.Fatalf("capture fault changed native execution or hid uncertainty: %#v %v %v", outcome, err, service.DevEvidence())
	}
	admission, err := service.Admit(context.Background(), InvokeInput{FunctionName: "echo", Payload: []byte("{}")})
	if err != nil {
		t.Fatalf("private capture fault changed native async admission: %v", err)
	}
	if terminal := waitAsyncState(t, service, admission.RequestID, "succeeded"); terminal.Attempts != 1 {
		t.Fatalf("private capture fault retried successful handler: %#v", terminal)
	}
	active, begun, completed := observer.snapshot()
	if active != 0 || len(begun) != 2 || len(completed) != 2 || !errors.Is(completed[0].err, unavailable) || !errors.Is(completed[1].err, unavailable) {
		t.Fatalf("capture fault released healthy owner: %d %#v", active, completed)
	}
	if err := service.DrainAsync(context.Background()); !errors.Is(err, unavailable) {
		t.Fatalf("async private capture uncertainty was lost on drain: %v", err)
	}
	closeErr := service.Close(context.Background())
	if !errors.Is(closeErr, unavailable) || !errors.Is(service.DevEvidence(), unavailable) {
		t.Fatalf("owned sink close lost sticky failure: %v %v", closeErr, service.DevEvidence())
	}
	if again := service.Close(context.Background()); again == nil || again.Error() != closeErr.Error() {
		t.Fatalf("repeated close aggregated failure again: %v; first %v", again, closeErr)
	}
}

func TestObservedAdmissionCallbackRemainsOwnedUntilCloseJoins(t *testing.T) {
	directory := t.TempDir()
	marker := filepath.Join(directory, "child-pid")
	function := providedFunction(t, "wait")
	function.Environment["EVENTBUS_LAMBDA_TEST_PID"] = marker
	observer := &activityRecorder{}
	service, path := newDiagnosticTestService(t, map[string]Function{"wait": function}, directory, observer, nil)
	entered, gate := make(chan struct{}), make(chan struct{})
	var gateOnce sync.Once
	openGate := func() { gateOnce.Do(func() { close(gate) }) }
	t.Cleanup(openGate)
	var admitted InvocationMetadata
	type completion struct {
		outcome InvocationOutcome
		err     error
	}
	executed := make(chan completion, 1)
	go func() {
		outcome, err := service.ExecuteObserved(context.Background(), InvokeInput{FunctionName: "wait", Payload: []byte("{}")}, func(metadata InvocationMetadata) error {
			admitted = metadata
			close(entered)
			<-gate
			return nil
		})
		executed <- completion{outcome, err}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("observer was never admitted")
	}
	closed := make(chan error, 1)
	go func() { closed <- service.Close(context.Background()) }()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		service.mu.Lock()
		closing := service.closed
		service.mu.Unlock()
		if closing {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	service.mu.Lock()
	closing := service.closed
	service.mu.Unlock()
	if !closing {
		t.Fatal("Close never stopped admission")
	}
	select {
	case err := <-closed:
		t.Fatalf("Close left an owned admission callback running: %v", err)
	default:
	}
	if records := readDiagnosticRecords(t, path); len(records) != 0 {
		t.Fatalf("observer still running but terminal diagnostic published: %#v", records)
	}
	openGate()
	result := <-executed
	if err := <-closed; err != nil {
		t.Fatalf("joined Close: %v", err)
	}
	// Service-driven cancellation retains real identity and the native failure
	// output while the caller's uncanceled context preserves ordinary Execute.
	if result.err != nil || result.outcome.Metadata != admitted || result.outcome.State != InvocationCanceled || !result.outcome.Output.FunctionError || result.outcome.OwnershipErr != nil {
		t.Fatalf("joined admission result: %#v", result)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("child launched after owner cancellation: %v", err)
	}
	assertActivityFinished(t, observer, "lambda_invoke", nil)
	if records := readDiagnosticRecords(t, path); len(records) != 1 || records[0].RequestID != admitted.RequestID || records[0].State != InvocationCanceled || !records[0].OwnershipConfirmed {
		t.Fatalf("joined canceled admission: %#v", records)
	}
}

func TestPrivateDiagnosticsConstructorClosesSinkWhenAsyncOpenFails(t *testing.T) {
	directory := t.TempDir()
	privatePath := filepath.Join(directory, "private.jsonl")
	asyncPath := filepath.Join(directory, "directory")
	if err := os.Mkdir(asyncPath, 0700); err != nil {
		t.Fatal(err)
	}
	service, err := NewService(&Config{
		Functions:      map[string]Function{"echo": providedFunction(t, "echo")},
		DevDiagnostics: &DevDiagnosticsConfig{LogPath: privatePath},
		DevAsync:       &DevAsyncConfig{LogPath: asyncPath},
	}, directory)
	if err == nil || service != nil {
		t.Fatalf("invalid async sink accepted: %v %v", service, err)
	}
	data, err := os.ReadFile(privatePath)
	if err != nil || len(data) != 0 {
		t.Fatalf("failed constructor emitted a private record: %q %v", data, err)
	}
	// On Linux, prove that the constructor returned no retained private fd.
	// The assertion is limited to this fixture's inode, independent of other
	// concurrently opened resources or the process's global descriptor count.
	if runtime.GOOS == "linux" {
		info, err := os.Stat(privatePath)
		if err != nil {
			t.Fatal(err)
		}
		descriptors, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			t.Fatal(err)
		}
		for _, descriptor := range descriptors {
			if opened, err := os.Stat(filepath.Join("/proc/self/fd", descriptor.Name())); err == nil && os.SameFile(info, opened) {
				t.Fatalf("failed constructor retained private sink descriptor %s", descriptor.Name())
			}
		}
	}
}

func TestPrivateDiagnosticHealthDistinguishesCompletedOwnedClosure(t *testing.T) {
	service, _ := newDiagnosticTestService(t, map[string]Function{"echo": providedFunction(t, "echo")}, t.TempDir(), nil, nil)
	if _, err := service.Execute(context.Background(), InvokeInput{FunctionName: "echo", Payload: []byte("{}")}); err != nil {
		t.Fatal(err)
	}
	stop, checked := make(chan struct{}), make(chan error, 1)
	go func() {
		for {
			select {
			case <-stop:
				checked <- nil
				return
			default:
				if err := service.DevEvidence(); err != nil {
					checked <- err
					return
				}
			}
		}
	}()
	closeErr := service.Close(context.Background())
	close(stop)
	if err := <-checked; err != nil || closeErr != nil || service.DevEvidence() != nil {
		t.Fatalf("completed healthy close reported uncertainty: close=%v concurrent=%v after=%v", closeErr, err, service.DevEvidence())
	}
	if err := service.Close(context.Background()); err != nil || service.DevEvidence() != nil {
		t.Fatalf("healthy closure was not stable: %v %v", err, service.DevEvidence())
	}
}

func TestPrivateDiagnosticOwnedCloseFailureRemainsEvidence(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("owned descriptor close fault uses Linux /proc")
	}
	service, path := newDiagnosticTestService(t, map[string]Function{"echo": providedFunction(t, "echo")}, t.TempDir(), nil, nil)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	descriptors, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	invalidated := false
	for _, descriptor := range descriptors {
		opened, err := os.Stat(filepath.Join("/proc/self/fd", descriptor.Name()))
		if err != nil || !os.SameFile(info, opened) {
			continue
		}
		number, err := strconv.Atoi(descriptor.Name())
		if err != nil {
			t.Fatal(err)
		}
		// This fixture owns the private file exclusively. Invalidate that sole
		// fd immediately before actual owner Close to inject a close-only fault;
		// no handler, append or other fixture uses it.
		if err := os.NewFile(uintptr(number), "private close fault fixture").Close(); err != nil {
			t.Fatal(err)
		}
		invalidated = true
		break
	}
	if !invalidated {
		t.Fatal("owned private file descriptor not found")
	}
	closeErr := service.Close(context.Background())
	if closeErr == nil || service.diagnosticCloseErr == nil || !errors.Is(service.DevEvidence(), service.diagnosticCloseErr) {
		t.Fatalf("close-only evidence was lost: close=%v private=%v evidence=%v", closeErr, service.diagnosticCloseErr, service.DevEvidence())
	}
	if again := service.Close(context.Background()); again == nil || again.Error() != closeErr.Error() {
		t.Fatalf("repeated close changed retained evidence: %v; first %v", again, closeErr)
	}
}
