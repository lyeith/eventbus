//go:build linux || darwin

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

func diagnosticModeService(t *testing.T, functions map[string]Function, directory string, mode DevDiagnosticsMode, warm bool, async *DevAsyncConfig) (*Service, string) {
	t.Helper()
	if async == nil {
		async = &DevAsyncConfig{Workers: 1, LogWriter: io.Discard, RetryDelays: []time.Duration{0, 0}}
	}
	path := filepath.Join(directory, "private", "mode.jsonl")
	config := &Config{Functions: functions, DevDiagnostics: &DevDiagnosticsConfig{LogPath: path, Mode: mode}, DevAsync: async}
	if warm {
		config.DevWarm = &DevWarmConfig{MaxWorkers: 1, MaxConcurrency: 1}
	}
	service, err := NewService(config, directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := service.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return service, path
}
func diagnosticModeFunction(t *testing.T, runtime, directory string) Function {
	t.Helper()
	command, filename := "node", "mode.mjs"
	source := `import fs from 'node:fs';
let count=0;
export async function handler(event,context) {
 count++;
 console.log('mode stdout '+context.awsRequestId);
 console.error('mode stderr '+context.awsRequestId);
 if(event.marker) fs.writeFileSync(event.marker,String(process.pid));
 while(event.wait&&!fs.existsSync(event.release)) await new Promise(r=>setTimeout(r,2));
 if(event.fail) throw new Error('retained native failure detail');
 if(event.caught) { try { throw new Error('caught business failure detail'); } catch(error) {console.error(error.message);} }
 return {requestId:context.awsRequestId,pid:process.pid,count,ok:true};
}`
	if runtime == "python" {
		command, filename = "python3", "mode.py"
		source = `import os,sys,time
count=0
def handler(event,context):
 global count
 count+=1
 print('mode stdout '+context.aws_request_id,flush=True)
 print('mode stderr '+context.aws_request_id,file=sys.stderr,flush=True)
 if event.get('marker'):
  with open(event['marker'],'w') as output: output.write(str(os.getpid()))
 while event.get('wait') and not os.path.exists(event['release']): time.sleep(.002)
 if event.get('fail'): raise RuntimeError('retained native failure detail')
 if event.get('caught'):
  try: raise RuntimeError('caught business failure detail')
  except RuntimeError as error: print(str(error),file=sys.stderr,flush=True)
 return {'requestId':context.aws_request_id,'pid':os.getpid(),'count':count,'ok':True}
`
	}
	executable, err := exec.LookPath(command)
	if err != nil {
		t.Skipf("%s unavailable", command)
	}
	if err := os.WriteFile(filepath.Join(directory, filename), []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	return Function{Runtime: runtime, Command: []string{executable}, Handler: filename + "#handler", Timeout: 5 * time.Second}
}

func TestDiagnosticFailuresModeNativeSuccessCaughtErrorAndWarmReuse(t *testing.T) {
	for _, scenario := range []struct {
		runtime string
		warm    bool
	}{{"node", false}, {"python", true}} {
		t.Run(scenario.runtime, func(t *testing.T) {
			directory := t.TempDir()
			function := diagnosticModeFunction(t, scenario.runtime, directory)
			service, path := diagnosticModeService(t, map[string]Function{"processor:live": function}, directory, DevDiagnosticsFailures, scenario.warm, nil)
			var priorPID float64
			for index, body := range []string{"{}", `{"caught":true}`} {
				response := requestInvoke(service, "processor:live", body, map[string]string{"X-Amz-Log-Type": "Tail"})
				if response.Code != http.StatusOK || response.Header().Get("X-Amz-Function-Error") != "" {
					t.Fatalf("native success:%d %s", response.Code, response.Body.String())
				}
				var output map[string]any
				if err := json.Unmarshal(response.Body.Bytes(), &output); err != nil {
					t.Fatal(err)
				}
				tail, err := base64.StdEncoding.DecodeString(response.Header().Get("X-Amz-Log-Result"))
				if err != nil || !bytes.Contains(tail, []byte("mode stdout "+output["requestId"].(string))) {
					t.Fatalf("native Tail changed:%s %v", tail, err)
				}
				if index == 1 && !bytes.Contains(tail, []byte("caught business failure detail")) {
					t.Fatal("caught business error lost native Tail")
				}
				if scenario.warm && index == 1 && (output["pid"] != priorPID || output["count"] != float64(2)) {
					t.Fatalf("filter changed retained imports:%v", output)
				}
				priorPID = output["pid"].(float64)
				if records := readDiagnosticRecords(t, path); len(records) != 0 {
					t.Fatalf("native success generated failure record:%+v", records)
				}
			}
			outcome, err := service.ExecuteObserved(context.Background(), InvokeInput{FunctionName: "processor:live", Payload: []byte(`{"fail":true}`)}, nil)
			if err != nil || outcome.State != InvocationFailed || !outcome.Output.FunctionError || outcome.OwnershipErr != nil {
				t.Fatalf("native failure changed:%+v %v", outcome, err)
			}
			records := readDiagnosticRecords(t, path)
			if len(records) != 1 {
				t.Fatalf("failure records:%+v", records)
			}
			record := records[0]
			if record.RequestID != outcome.Metadata.RequestID || record.FunctionName != "processor:live" || record.State != InvocationFailed || !record.OwnershipConfirmed || record.Stdout == nil || record.FunctionDiagnostic == nil || !strings.Contains(string(diagnosticData(t, *record.FunctionDiagnostic)), "retained native failure detail") || !strings.Contains(string(diagnosticData(t, record.Stderr)), "mode stderr") {
				t.Fatalf("retained failure evidence:%+v", record)
			}
			if err := service.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if processAlive(int(priorPID)) {
				t.Fatal("mode left owned runtime alive")
			}
		})
	}
}

func TestDiagnosticAllModeStillCapturesCaughtSuccess(t *testing.T) {
	directory := t.TempDir()
	function := diagnosticModeFunction(t, "node", directory)
	service, path := diagnosticModeService(t, map[string]Function{"processor": function}, directory, DevDiagnosticsAll, false, nil)
	response := requestInvoke(service, "processor", `{"caught":true}`, map[string]string{"X-Amz-Log-Type": "Tail"})
	if response.Code != 200 || response.Header().Get("X-Amz-Function-Error") != "" {
		t.Fatalf("native all-mode success:%s", response.Body.String())
	}
	records := readDiagnosticRecords(t, path)
	if len(records) != 1 || records[0].State != InvocationSucceeded || !strings.Contains(string(diagnosticData(t, records[0].Stderr)), "caught business failure detail") {
		t.Fatalf("all compatibility:%+v", records)
	}
}

func TestDiagnosticFailuresModeProvidedNativeSuccessAndError(t *testing.T) {
	service, path := diagnosticModeService(t, map[string]Function{"echo": providedFunction(t, "echo"), "failed": providedFunction(t, "error")}, t.TempDir(), DevDiagnosticsFailures, false, nil)
	outcome, err := service.ExecuteObserved(context.Background(), InvokeInput{FunctionName: "echo", Payload: []byte("{}")}, nil)
	if err != nil || outcome.State != InvocationSucceeded || outcome.Output.FunctionError || outcome.OwnershipErr != nil {
		t.Fatalf("native provided success:%+v %v", outcome, err)
	}
	// A successful native provided runtime is intentionally stopped after its
	// accepted response. Its exit/process diagnostics are not a failed attempt.
	if records := readDiagnosticRecords(t, path); len(records) != 0 {
		t.Fatalf("successful runtime cleanup counted as failure:%+v", records)
	}
	failed, err := service.ExecuteObserved(context.Background(), InvokeInput{FunctionName: "failed", Payload: []byte("{}")}, nil)
	if err != nil || !failed.Output.FunctionError || failed.State != InvocationFailed {
		t.Fatalf("provided native error:%+v %v", failed, err)
	}
	records := readDiagnosticRecords(t, path)
	if len(records) != 1 || records[0].RequestID != failed.Metadata.RequestID || records[0].FunctionDiagnostic == nil || !strings.Contains(string(diagnosticData(t, *records[0].FunctionDiagnostic)), "Unauthorized") {
		t.Fatalf("native response error omitted:%+v", records)
	}
}

func TestDiagnosticFailuresModeRetainsFailedAsyncAttemptBeforeSuccessfulRetry(t *testing.T) {
	requireDiagnosticNode(t)
	directory := t.TempDir()
	writeFixture(t, directory, "retry.mjs", `import fs from 'node:fs';
export async function handler(event,context) {
 const count=fs.existsSync(process.env.COUNT)?Number(fs.readFileSync(process.env.COUNT,'utf8'))+1:1;
 fs.writeFileSync(process.env.COUNT,String(count));
 console.log('actual attempt '+count+' '+context.awsRequestId);
 console.error('private retry detail '+count);
 if(count===1) throw new Error('native first attempt failure detail');
 return {successfulResponse:'must not be captured'};
}`)
	var redacted bytes.Buffer
	function := Function{Runtime: "node", Handler: "retry.mjs#handler", Timeout: 5 * time.Second, Environment: map[string]string{"COUNT": filepath.Join(directory, "count")}}
	service, path := diagnosticModeService(t, map[string]Function{"retry:live": function}, directory, DevDiagnosticsFailures, false, &DevAsyncConfig{Workers: 1, LogWriter: &redacted, RetryDelays: []time.Duration{0, 0}})
	admission, err := service.Admit(context.Background(), InvokeInput{FunctionName: "arn:aws:lambda:us-east-1:123456789012:function:retry:live", Payload: []byte(`{"secret":"request-must-not-be-captured"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.DrainAsync(context.Background()); err != nil {
		t.Fatal(err)
	}
	record := waitAsyncState(t, service, admission.RequestID, "succeeded")
	if record.Attempts != 2 || record.ErrorType != "" {
		t.Fatalf("native retry changed:%+v", record)
	}
	records := readDiagnosticRecords(t, path)
	if len(records) != 1 || records[0].RequestID != admission.RequestID || records[0].Attempt != 1 || records[0].InvocationType != "Event" || records[0].State != InvocationFailed || records[0].FunctionDiagnostic == nil || !strings.Contains(string(diagnosticData(t, *records[0].FunctionDiagnostic)), "native first attempt failure detail") {
		t.Fatalf("original failed native attempt:%+v", records)
	}
	if !strings.Contains(redacted.String(), "\"state\":\"retrying\"") || !strings.Contains(redacted.String(), "\"state\":\"succeeded\"") || strings.Contains(redacted.String(), "private retry detail") {
		t.Fatalf("redacted async evidence changed:%s", redacted.String())
	}
	captured, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"request-must-not-be-captured", "successfulResponse", "private retry detail 2"} {
		if bytes.Contains(captured, []byte(secret)) {
			t.Fatalf("omitted successful attempt/input leaked:%s", secret)
		}
	}
}

func TestDiagnosticFailuresModeRetainsTimeoutAndCallerCancellation(t *testing.T) {
	for _, cancelCaller := range []bool{false, true} {
		t.Run(fmt.Sprintf("caller%v", cancelCaller), func(t *testing.T) {
			directory := t.TempDir()
			function := diagnosticModeFunction(t, "node", directory)
			if !cancelCaller {
				function.Timeout = 150 * time.Millisecond
			}
			service, path := diagnosticModeService(t, map[string]Function{"wait": function}, directory, DevDiagnosticsFailures, false, nil)
			marker, release := filepath.Join(directory, "started"), filepath.Join(directory, "release")
			body, _ := json.Marshal(map[string]any{"wait": true, "marker": marker, "release": release})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			type result struct {
				outcome InvocationOutcome
				err     error
			}
			done := make(chan result, 1)
			go func() {
				outcome, err := service.ExecuteObserved(ctx, InvokeInput{FunctionName: "wait", Payload: body}, nil)
				done <- result{outcome, err}
			}()
			waitDiagnosticMarker(t, marker)
			if cancelCaller {
				cancel()
			}
			actual := <-done
			wantState, wantCause := InvocationTimedOut, "function_timeout"
			if cancelCaller {
				wantState, wantCause = InvocationCanceled, "caller_canceled"
				if !errors.Is(actual.err, context.Canceled) {
					t.Fatal(actual.err)
				}
			} else if actual.err != nil {
				t.Fatal(actual.err)
			}
			if actual.outcome.State != wantState || !actual.outcome.Output.FunctionError || actual.outcome.OwnershipErr != nil {
				t.Fatalf("joined non-success:%+v", actual)
			}
			records := readDiagnosticRecords(t, path)
			if len(records) != 1 || records[0].State != wantState || records[0].TerminationCause != wantCause || records[0].RequestID != actual.outcome.Metadata.RequestID || !records[0].OwnershipConfirmed || records[0].Stdout == nil || !strings.Contains(string(diagnosticData(t, *records[0].Stdout)), "mode stdout") {
				t.Fatalf("non-success omitted:%+v", records)
			}
		})
	}
}

func TestDiagnosticFailuresModeRetainsInitializationAndLaunchFailure(t *testing.T) {
	for _, mode := range []string{"import_failure", "init_cancel", "launch_failure"} {
		t.Run(mode, func(t *testing.T) {
			directory := t.TempDir()
			var function Function
			if mode == "launch_failure" {
				launcher := filepath.Join(directory, "launcher")
				if err := os.WriteFile(launcher, []byte("#!/bin/sh\nprintf '{}'\n"), 0700); err != nil {
					t.Fatal(err)
				}
				function = Function{Runtime: "command", Command: []string{launcher}, Timeout: time.Second}
			} else {
				function = managedPhaseFunction(t, "node", directory, mode, false)
			}
			service, path := diagnosticModeService(t, map[string]Function{"phase": function}, directory, DevDiagnosticsFailures, false, nil)
			if mode == "launch_failure" {
				if err := os.Remove(function.Command[0]); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			type result struct {
				outcome InvocationOutcome
				err     error
			}
			done := make(chan result, 1)
			go func() {
				outcome, err := service.ExecuteObserved(ctx, InvokeInput{FunctionName: "phase", Payload: []byte("{}")}, nil)
				done <- result{outcome, err}
			}()
			if mode == "init_cancel" {
				waitDiagnosticMarker(t, filepath.Join(directory, "launch-1.json"))
				cancel()
			}
			actual := <-done
			if mode == "init_cancel" {
				if !errors.Is(actual.err, context.Canceled) || actual.outcome.State != InvocationCanceled {
					t.Fatalf("Init cancellation:%+v", actual)
				}
			} else if actual.err != nil || !actual.outcome.Output.FunctionError {
				t.Fatalf("Init/start failure:%+v", actual)
			}
			records := readDiagnosticRecords(t, path)
			if len(records) != 1 || records[0].RequestID != actual.outcome.Metadata.RequestID || !records[0].OwnershipConfirmed {
				t.Fatalf("owned init/start failure omitted:%+v", records)
			}
			if mode == "launch_failure" {
				if records[0].State != InvocationNotStarted || records[0].ProcessError == "" {
					t.Fatalf("launch failure lost detail:%+v", records[0])
				}
			} else if len(records[0].ExecutionPhases) != 1 || records[0].ExecutionPhases[0].InvokeState != "" || records[0].ExecutionPhases[0].InitState == "" {
				t.Fatalf("Init attribution lost:%+v", records[0])
			}
		})
	}
}

func TestDiagnosticFailuresModeRetainsActualConcurrencyRejection(t *testing.T) {
	directory := t.TempDir()
	function := diagnosticModeFunction(t, "node", directory)
	service, path := diagnosticModeService(t, map[string]Function{"held": function}, directory, DevDiagnosticsFailures, true, nil)
	marker, release := filepath.Join(directory, "started"), filepath.Join(directory, "release")
	body, _ := json.Marshal(map[string]any{"wait": true, "marker": marker, "release": release})
	done := make(chan error, 1)
	go func() {
		output, err := service.Execute(context.Background(), InvokeInput{FunctionName: "held", Payload: body})
		if err == nil && output.FunctionError {
			err = fmt.Errorf("held native failure:%s", output.Payload)
		}
		done <- err
	}()
	waitDiagnosticMarker(t, marker)
	defer os.WriteFile(release, nil, 0600)
	rejected, err := service.ExecuteObserved(context.Background(), InvokeInput{FunctionName: "held", Payload: []byte("{}")}, nil)
	warmAssertThrottled(t, err)
	if rejected.State != InvocationNotStarted || rejected.Metadata.RequestID == "" {
		t.Fatalf("native rejection changed:%+v", rejected)
	}
	records := readDiagnosticRecords(t, path)
	if len(records) != 1 || records[0].RequestID != rejected.Metadata.RequestID || records[0].State != InvocationNotStarted || !records[0].OwnershipConfirmed {
		t.Fatalf("owned refusal omitted:%+v", records)
	}
	if err := os.WriteFile(release, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if records = readDiagnosticRecords(t, path); len(records) != 1 {
		t.Fatalf("successful held invocation captured:%+v", records)
	}
}
