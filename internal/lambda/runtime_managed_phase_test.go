//go:build linux || darwin

package lambda

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// These handlers have real module initialization before the wrapper's READY.
// Markers are fixture-owned observations, never runtime control.
const managedFallbackInitBudget = time.Second
const managedFallbackInvokeBudget = 2 * time.Second
const managedFallbackCleanupMargin = 750 * time.Millisecond

const managedPhasePythonSource = `
import glob
import json
import os
import time

directory = os.environ["PHASE_DIRECTORY"]
mode = os.environ["PHASE_MODE"]
launch = len(glob.glob(os.path.join(directory, "launch-*.json"))) + 1

def marker(name, value):
    path = os.path.join(directory, name)
    with open(path + ".tmp", "w") as output:
        json.dump(value, output)
    os.replace(path + ".tmp", path)

marker("launch-%d.json" % launch, {
    "pid": os.getpid(),
    "request_id": os.environ["EVENTBUS_LAMBDA_REQUEST_ID"],
    "previous_joined": launch == 1 or os.path.exists(os.path.join(directory, "joined-%d" % (launch - 1))),
})
if mode in ("init_cancel", "caller_limit"):
    while not os.path.exists(os.path.join(directory, "release-init")):
        time.sleep(0.005)
delay = 700
if mode.startswith("fallback"):
    delay = int(os.environ["PHASE_FIRST_INIT_DELAY_MS"]) if launch == 1 else (900 if mode == "fallback_exhausted" else 60)
if mode == "import_failure":
    raise RuntimeError("controlled module import failure")
if mode not in ("init_cancel", "invoke_cancel", "caller_limit"):
    time.sleep(delay / 1000)

def handler(event, context):
    remaining = context.get_remaining_time_in_millis()
    marker("invoke.json", {
        "request_id": context.aws_request_id,
        "arn": context.invoked_function_arn,
        "remaining": remaining,
        "event": event,
        "launch": launch,
    })
    if mode == "invoke_cancel":
        while True:
            time.sleep(10)
    if mode == "fallback_process_failure":
        os._exit(17)
    if mode == "fallback_handler_failure":
        raise RuntimeError("controlled fallback handler failure")
    handler_delay = int(os.environ["PHASE_EXHAUSTED_HANDLER_DELAY_MS"]) if mode == "fallback_exhausted" else (700 if mode == "handler_timeout" else 50)
    time.sleep(handler_delay / 1000)
    return {"event": event, "requestId": context.aws_request_id,
            "arn": context.invoked_function_arn, "remaining": remaining}
`

const managedPhaseNodeSource = `
import fs from 'node:fs';
import path from 'node:path';
const directory = process.env.PHASE_DIRECTORY;
const mode = process.env.PHASE_MODE;
const launch = fs.readdirSync(directory).filter(name => /^launch-[0-9]+\.json$/.test(name)).length + 1;
const sleep = milliseconds => new Promise(resolve => setTimeout(resolve, milliseconds));
function marker(name, value) {
  const target = path.join(directory, name);
  fs.writeFileSync(target + '.tmp', JSON.stringify(value));
  fs.renameSync(target + '.tmp', target);
}
marker('launch-' + launch + '.json', {
  pid: process.pid,
  request_id: process.env.EVENTBUS_LAMBDA_REQUEST_ID,
  previous_joined: launch === 1 || fs.existsSync(path.join(directory, 'joined-' + (launch - 1))),
});
if (mode === 'init_cancel' || mode === 'caller_limit') {
  while (!fs.existsSync(path.join(directory, 'release-init'))) await sleep(5);
}
let delay = 700;
if (mode.startsWith('fallback')) delay = launch === 1 ? Number(process.env.PHASE_FIRST_INIT_DELAY_MS) : (mode === 'fallback_exhausted' ? 900 : 60);
if (mode === 'import_failure') throw new Error('controlled module import failure');
if (!['init_cancel', 'invoke_cancel', 'caller_limit'].includes(mode)) await sleep(delay);
export async function handler(event, context) {
  const remaining = context.getRemainingTimeInMillis();
  marker('invoke.json', {request_id: context.awsRequestId, arn: context.invokedFunctionArn, remaining, event, launch});
  if (mode === 'invoke_cancel') await new Promise(() => {});
  if (mode === 'fallback_process_failure') process.exit(17);
  if (mode === 'fallback_handler_failure') throw new Error('controlled fallback handler failure');
  await sleep(mode === 'fallback_exhausted' ? Number(process.env.PHASE_EXHAUSTED_HANDLER_DELAY_MS) : (mode === 'handler_timeout' ? 700 : 50));
  return {event, requestId: context.awsRequestId, arn: context.invokedFunctionArn, remaining};
}
`

func managedPhaseFunction(t *testing.T, runtime, directory, mode string, frozenPython bool) Function {
	t.Helper()
	function := Function{Runtime: runtime, Timeout: 450 * time.Millisecond, Environment: map[string]string{
		"PHASE_DIRECTORY": directory,
		"PHASE_MODE":      mode,
	}}
	if strings.HasPrefix(mode, "fallback") {
		// Let a real managed interpreter reach the first marker under race/load.
		// Deliberate module/handler delays, not bootstrap speed, select the phase.
		function.Timeout = managedFallbackInvokeBudget
		function.Environment["PHASE_FIRST_INIT_DELAY_MS"] = "2000"
		function.Environment["PHASE_EXHAUSTED_HANDLER_DELAY_MS"] = "2300"
	}
	switch runtime {
	case "node":
		requireDiagnosticNode(t)
		writeFixture(t, directory, "phase.mjs", managedPhaseNodeSource)
		function.Handler = "phase.mjs.handler"
	case "python":
		writeFixture(t, directory, "phase.py", managedPhasePythonSource)
		command, environment := pythonCommand(t)
		if frozenPython {
			// No environment is created. Short Init limits exercise module
			// initialization rather than only uv's real launcher startup.
			interpreter := os.Getenv("EVENTBUS_SMOKE_PYTHON")
			if interpreter == "" {
				interpreter = filepath.Join("..", "..", ".venv", "bin", "python")
			}
			interpreter, err := filepath.Abs(interpreter)
			if err != nil {
				t.Fatal(err)
			}
			if info, err := os.Stat(interpreter); err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
				t.Skip("existing frozen Python interpreter unavailable")
			}
			command = []string{interpreter}
		}
		function.Command = command
		for key, value := range environment {
			function.Environment[key] = value
		}
		function.Handler = "phase.handler"
	default:
		t.Fatalf("unsupported test runtime %s", runtime)
	}
	return function
}

func managedPhaseService(t *testing.T, directory string, function Function) (*Service, string) {
	t.Helper()
	service, path := newDiagnosticTestService(t, map[string]Function{"phase:live": function}, directory, nil, nil)
	cleanup := service.processCleanup
	var mu sync.Mutex
	joins := 0
	service.processCleanup = func(command *exec.Cmd) error {
		err := cleanup(command)
		// Native cleanup runs first; a marker observed by the next actual
		// import proves fallback cannot overlap its original child's cleanup.
		if command.ProcessState == nil {
			err = errors.Join(err, errors.New("child cleanup preceded process wait"))
		}
		if err != nil {
			return err
		}
		mu.Lock()
		joins++
		index := joins
		mu.Unlock()
		return os.WriteFile(filepath.Join(directory, fmt.Sprintf("joined-%d", index)), []byte("joined"), 0600)
	}
	return service, path
}

func managedPhaseRecord(t *testing.T, service *Service, path, directory string, metadata InvocationMetadata, ownershipErr error, launches int) invocationDiagnosticRecord {
	t.Helper()
	records := readDiagnosticRecords(t, path)
	if len(records) != 1 {
		t.Fatalf("native terminal diagnostics: %#v", records)
	}
	record := records[0]
	for _, phase := range record.ExecutionPhases {
		if !phase.OwnershipConfirmed || phase.OwnershipError != "" {
			t.Fatalf("native launch ownership differs from actual joined fixture: %#v", phase)
		}
	}
	if len(record.ExecutionPhases) > 0 {
		final := record.ExecutionPhases[len(record.ExecutionPhases)-1]
		if record.ProcessError != final.ProcessError || record.ContextError != final.ContextError || record.TerminationCause != final.TerminationCause {
			t.Fatalf("invocation error attribution differs from final launch: record=%#v final=%#v", record, final)
		}
	}
	if metadata.RequestID == "" || record.RequestID != metadata.RequestID ||
		record.FunctionARN != metadata.FunctionARN || record.FunctionName != "phase:live" ||
		record.Attempt != 1 || metadata.Attempt != 1 || !record.OwnershipConfirmed ||
		record.OwnershipError != "" || ownershipErr != nil || service.DevEvidence() != nil {
		t.Fatalf("native identity or joined ownership: metadata=%#v record=%#v evidence=%v", metadata, record, service.DevEvidence())
	}
	files, err := filepath.Glob(filepath.Join(directory, "launch-*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != launches {
		t.Fatalf("actual module launches=%d, want %d: %v", len(files), launches, files)
	}
	for index := 1; index <= launches; index++ {
		var launch struct {
			PID            int    `json:"pid"`
			RequestID      string `json:"request_id"`
			PreviousJoined bool   `json:"previous_joined"`
		}
		data, err := os.ReadFile(filepath.Join(directory, fmt.Sprintf("launch-%d.json", index)))
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(data, &launch); err != nil {
			t.Fatal(err)
		}
		if launch.PID <= 0 || launch.RequestID != record.RequestID || !launch.PreviousJoined {
			t.Fatalf("native launch identity/prior child join: %#v", launch)
		}
		if processAlive(launch.PID) {
			t.Fatalf("native launch %d process %d still lives after joined completion", index, launch.PID)
		}
		if _, err := os.Stat(filepath.Join(directory, fmt.Sprintf("joined-%d", index))); err != nil {
			t.Fatalf("native launch %d did not join: %v", index, err)
		}
	}
	service.mu.Lock()
	active := len(service.active)
	service.mu.Unlock()
	if active != 0 {
		t.Fatalf("terminal invocation still owns %d children", active)
	}
	return record
}

func managedPhasePayload(t *testing.T, outcome InvocationOutcome, lowerRemaining, upperRemaining int64) {
	t.Helper()
	var payload struct {
		Event     map[string]int `json:"event"`
		RequestID string         `json:"requestId"`
		ARN       string         `json:"arn"`
		Remaining int64          `json:"remaining"`
	}
	if err := json.Unmarshal(outcome.Output.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Event["unchanged"] != 42 || payload.RequestID != outcome.Metadata.RequestID ||
		payload.ARN != outcome.Metadata.FunctionARN || payload.Remaining < lowerRemaining || payload.Remaining > upperRemaining {
		t.Fatalf("real managed handler budget/identity/payload: %#v", payload)
	}
}

func TestManagedRuntimeInitializationHasIndependentBudget(t *testing.T) {
	for _, runtime := range []string{"python", "node"} {
		t.Run(runtime, func(t *testing.T) {
			for _, mode := range []string{"delayed_success", "handler_timeout", "import_failure"} {
				t.Run(mode, func(t *testing.T) {
					directory := t.TempDir()
					function := managedPhaseFunction(t, runtime, directory, mode, false)
					service, path := managedPhaseService(t, directory, function)
					outcome, err := service.ExecuteObserved(context.Background(), InvokeInput{
						FunctionName: "arn:aws:lambda:eu-west-1:123456789012:function:phase:live",
						Payload:      []byte(`{"unchanged":42}`),
					}, nil)
					if err != nil {
						t.Fatal(err)
					}
					record := managedPhaseRecord(t, service, path, directory, outcome.Metadata, outcome.OwnershipErr, 1)
					if len(record.ExecutionPhases) != 1 {
						t.Fatalf("ordinary Init unexpectedly retried: %#v", record.ExecutionPhases)
					}
					phase := record.ExecutionPhases[0]
					if phase.InitAttempt != 1 || phase.Mode != "initial" {
						t.Fatalf("ordinary managed phase: %#v", phase)
					}
					switch mode {
					case "delayed_success":
						if outcome.State != InvocationSucceeded || outcome.Output.FunctionError || record.TerminationCause != "" ||
							phase.InitState != "succeeded" || phase.InvokeState != "succeeded" || phase.InitMS < 650 || phase.InvokeMS >= 450 {
							t.Fatalf("module Init spent the handler budget: outcome=%#v record=%#v", outcome, record)
						}
						managedPhasePayload(t, outcome, 350, 450)
					case "handler_timeout":
						assertLegacyDiagnosticTimeout(t, outcome.Output, function.Timeout)
						if outcome.State != InvocationTimedOut || record.TerminationCause != "function_timeout" ||
							phase.InitState != "succeeded" || phase.InvokeState != "timed_out" || phase.InitMS < 650 || phase.InvokeMS < 440 {
							t.Fatalf("actual handler deadline: outcome=%#v record=%#v", outcome, record)
						}
					case "import_failure":
						if outcome.State != InvocationFailed || !outcome.Output.FunctionError || record.TerminationCause != "" ||
							!strings.Contains(string(outcome.Output.Payload), "controlled module import failure") ||
							phase.InitState != "failed" || phase.InvokeState != "" || phase.InvokeMS != 0 {
							t.Fatalf("module failure was retried or relabeled: outcome=%#v record=%#v", outcome, record)
						}
						if _, err := os.Stat(filepath.Join(directory, "invoke.json")); !errors.Is(err, os.ErrNotExist) {
							t.Fatalf("handler ran after import failure: %v", err)
						}
					}
					if err := service.Close(context.Background()); err != nil {
						t.Fatalf("native managed teardown: %v", err)
					}
				})
			}
		})
	}
}

func TestManagedRuntimeCancellationJoinsEachPhase(t *testing.T) {
	for _, runtime := range []string{"python", "node"} {
		t.Run(runtime, func(t *testing.T) {
			for _, mode := range []string{"init_cancel", "invoke_cancel"} {
				t.Run(mode, func(t *testing.T) {
					directory := t.TempDir()
					function := managedPhaseFunction(t, runtime, directory, mode, false)
					service, path := managedPhaseService(t, directory, function)
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					type completion struct {
						outcome InvocationOutcome
						err     error
					}
					done := make(chan completion, 1)
					go func() {
						outcome, err := service.ExecuteObserved(ctx, InvokeInput{FunctionName: "phase:live", Payload: []byte("{}")}, nil)
						done <- completion{outcome, err}
					}()
					marker := "launch-1.json"
					if mode == "invoke_cancel" {
						marker = "invoke.json"
					}
					waitDiagnosticMarker(t, filepath.Join(directory, marker))
					cancel()
					var completed completion
					select {
					case completed = <-done:
					case <-time.After(5 * time.Second):
						t.Fatal("canceled managed invocation did not join")
					}
					if !errors.Is(completed.err, context.Canceled) || completed.outcome.State != InvocationCanceled {
						t.Fatalf("caller cancellation: %#v %v", completed.outcome, completed.err)
					}
					record := managedPhaseRecord(t, service, path, directory, completed.outcome.Metadata, completed.outcome.OwnershipErr, 1)
					if record.TerminationCause != "caller_canceled" || len(record.ExecutionPhases) != 1 {
						t.Fatalf("caller cancellation phase/cause: %#v", record)
					}
					phase := record.ExecutionPhases[0]
					if phase.ProcessError == "" || phase.ContextError != "canceled" || phase.TerminationCause != "caller_canceled" {
						t.Fatalf("caller cancellation lost actual final-launch error/cause: %#v", phase)
					}
					if mode == "init_cancel" {
						if phase.InitState != "canceled" || phase.InvokeState != "" || phase.InvokeMS != 0 {
							t.Fatalf("canceled Init ran a handler: %#v", phase)
						}
						if _, err := os.Stat(filepath.Join(directory, "invoke.json")); !errors.Is(err, os.ErrNotExist) {
							t.Fatalf("handler ran after Init cancellation: %v", err)
						}
					} else if phase.InitState != "succeeded" || phase.InvokeState != "canceled" {
						t.Fatalf("caller cancel lost the Invoke boundary: %#v", phase)
					}
					if err := service.Close(context.Background()); err != nil {
						t.Fatalf("canceled native child teardown: %v", err)
					}
				})
			}
		})
	}
}

func TestManagedRuntimeInitTimeoutHasOneJoinedFallback(t *testing.T) {
	for _, runtime := range []string{"python", "node"} {
		t.Run(runtime, func(t *testing.T) {
			for _, mode := range []string{"fallback_success", "fallback_exhausted"} {
				t.Run(mode, func(t *testing.T) {
					directory := t.TempDir()
					function := managedPhaseFunction(t, runtime, directory, mode, true)
					service, path := managedPhaseService(t, directory, function)
					// Only ordinary Init's 10s limit is shortened. Fallback shares
					// the configured budget across real bootstrap, imports and Invoke.
					service.initTimeout = managedFallbackInitBudget
					outcome, err := service.ExecuteObserved(context.Background(), InvokeInput{
						FunctionName: "phase:live", Payload: []byte(`{"unchanged":42}`),
					}, nil)
					if err != nil {
						t.Fatal(err)
					}
					record := managedPhaseRecord(t, service, path, directory, outcome.Metadata, outcome.OwnershipErr, 2)
					if len(record.ExecutionPhases) != 2 {
						t.Fatalf("expected exactly one Init fallback: %#v", record)
					}
					initial, fallback := record.ExecutionPhases[0], record.ExecutionPhases[1]
					if initial.ProcessError == "" || initial.ContextError != "deadline_exceeded" || initial.TerminationCause != "initialization_timeout" {
						t.Fatalf("retired Init lost its attributed process failure: %#v", initial)
					}
					if mode == "fallback_success" && (record.ProcessError != "" || fallback.ProcessError != "") {
						t.Fatalf("successful fallback retained a retired process error: %#v", record)
					}
					if initial.InitAttempt != 1 || initial.Mode != "initial" || initial.InitState != "timed_out" ||
						initial.InvokeState != "" || initial.InvokeMS != 0 || initial.InitMS < float64(managedFallbackInitBudget.Milliseconds()-10) ||
						fallback.InitAttempt != 2 || fallback.Mode != "fallback" || fallback.InitState != "succeeded" {
						t.Fatalf("native Init fallback phases: %#v", record.ExecutionPhases)
					}
					if mode == "fallback_success" {
						if outcome.State != InvocationSucceeded || outcome.Output.FunctionError || record.TerminationCause != "" || fallback.InvokeState != "succeeded" {
							t.Fatalf("bounded fallback success changed: outcome=%#v record=%#v", outcome, record)
						}
						managedPhasePayload(t, outcome, 1, function.Timeout.Milliseconds()-int64(fallback.InitMS)+20)
					} else {
						assertLegacyDiagnosticTimeout(t, outcome.Output, function.Timeout)
						if outcome.State != InvocationTimedOut || record.TerminationCause != "function_timeout" ||
							fallback.InvokeState != "timed_out" || fallback.InitMS < 890 ||
							fallback.InitMS+fallback.InvokeMS < float64(function.Timeout.Milliseconds()-20) ||
							fallback.InitMS+fallback.InvokeMS > float64((function.Timeout+managedFallbackCleanupMargin).Milliseconds()) {
							t.Fatalf("fallback renewed its configured budget after Init: outcome=%#v record=%#v", outcome, record)
						}
						var invocation struct {
							Remaining int64 `json:"remaining"`
						}
						data, err := os.ReadFile(filepath.Join(directory, "invoke.json"))
						if err != nil {
							t.Fatal(err)
						}
						if err := json.Unmarshal(data, &invocation); err != nil {
							t.Fatal(err)
						}
						// Actual remaining time independently detects an Invoke renewal.
						// The duration bound above allows joined cleanup, not another budget.
						if invocation.Remaining <= 0 || invocation.Remaining > function.Timeout.Milliseconds()-int64(fallback.InitMS)+20 {
							t.Fatalf("fallback handler lost the shared Init+Invoke remainder: %#v", invocation)
						}
					}
					if err := service.Close(context.Background()); err != nil {
						t.Fatalf("fallback native teardown: %v", err)
					}
				})
			}
		})
	}
}

func TestManagedRuntimeInvokeDeadlineIsCappedByCaller(t *testing.T) {
	for _, runtime := range []string{"python", "node"} {
		t.Run(runtime, func(t *testing.T) {
			directory := t.TempDir()
			function := managedPhaseFunction(t, runtime, directory, "caller_limit", true)
			service, path := managedPhaseService(t, directory, function)
			deadline := time.Now().Add(2 * time.Second)
			ctx, cancel := context.WithDeadline(context.Background(), deadline)
			defer cancel()
			type completion struct {
				outcome InvocationOutcome
				err     error
			}
			done := make(chan completion, 1)
			go func() {
				outcome, err := service.ExecuteObserved(ctx, InvokeInput{FunctionName: "phase:live", Payload: []byte(`{"unchanged":42}`)}, nil)
				done <- completion{outcome, err}
			}()
			waitDiagnosticMarker(t, filepath.Join(directory, "launch-1.json"))
			time.Sleep(time.Until(deadline.Add(-200 * time.Millisecond)))
			if err := os.WriteFile(filepath.Join(directory, "release-init"), []byte("ready"), 0600); err != nil {
				t.Fatal(err)
			}
			var completed completion
			select {
			case completed = <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("caller-capped invocation did not join")
			}
			if completed.err != nil || completed.outcome.State != InvocationSucceeded || completed.outcome.Output.FunctionError {
				t.Fatalf("handler within caller deadline: %#v %v", completed.outcome, completed.err)
			}
			managedPhasePayload(t, completed.outcome, 100, 200)
			record := managedPhaseRecord(t, service, path, directory, completed.outcome.Metadata, completed.outcome.OwnershipErr, 1)
			if len(record.ExecutionPhases) != 1 || record.ExecutionPhases[0].InitMS < 1700 ||
				record.ExecutionPhases[0].InitState != "succeeded" || record.ExecutionPhases[0].InvokeState != "succeeded" {
				t.Fatalf("caller deadline phase transition: %#v", record.ExecutionPhases)
			}
			if err := service.Close(context.Background()); err != nil {
				t.Fatalf("caller-capped native teardown: %v", err)
			}
		})
	}
}

func TestManagedRuntimeEventInitFallbackKeepsOneNativeAttempt(t *testing.T) {
	for _, runtime := range []string{"python", "node"} {
		t.Run(runtime, func(t *testing.T) {
			directory := t.TempDir()
			function := managedPhaseFunction(t, runtime, directory, "fallback_success", true)
			service, path := managedPhaseService(t, directory, function)
			service.initTimeout = managedFallbackInitBudget
			const arn = "arn:aws:lambda:eu-west-1:123456789012:function:phase:live"
			payload := []byte(`{"unchanged":42}`)
			admission, err := service.Admit(context.Background(), InvokeInput{FunctionName: arn, Payload: payload})
			if err != nil {
				t.Fatal(err)
			}
			// Admission owns a copy, independent of the caller's buffer, while
			// the single async task keeps custody across both joined launches.
			copy(payload, []byte(`{"unchanged":99}`))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := service.Close(ctx); err != nil {
				t.Fatalf("healthy Close failed to join accepted Event fallback: %v", err)
			}
			history := service.AsyncSnapshot()
			if len(history) != 1 || history[0].RequestID != admission.RequestID || history[0].FunctionName != "phase:live" ||
				history[0].State != "succeeded" || history[0].Attempts != 1 || history[0].ErrorType != "" || history[0].CompletedAt.IsZero() {
				t.Fatalf("initialization fallback became an async retry: %#v", history)
			}
			metadata := InvocationMetadata{RequestID: admission.RequestID, FunctionARN: arn, FunctionName: "phase:live", Attempt: 1}
			record := managedPhaseRecord(t, service, path, directory, metadata, nil, 2)
			if record.InvocationType != "Event" || record.State != InvocationSucceeded || record.FunctionError ||
				record.TerminationCause != "" || len(record.ExecutionPhases) != 2 {
				t.Fatalf("native Event terminal evidence: %#v", record)
			}
			initial, fallback := record.ExecutionPhases[0], record.ExecutionPhases[1]
			if record.ProcessError != "" || initial.ProcessError == "" || fallback.ProcessError != "" ||
				initial.ContextError != "deadline_exceeded" || initial.TerminationCause != "initialization_timeout" {
				t.Fatalf("successful Event fallback has ambiguous process attribution: %#v", record)
			}
			if initial.InitAttempt != 1 || initial.Mode != "initial" || initial.InitState != "timed_out" ||
				initial.InvokeState != "" || fallback.InitAttempt != 2 || fallback.Mode != "fallback" ||
				fallback.InitState != "succeeded" || fallback.InvokeState != "succeeded" {
				t.Fatalf("Event's internal initialization phases: %#v", record.ExecutionPhases)
			}
			var invoked struct {
				RequestID string         `json:"request_id"`
				ARN       string         `json:"arn"`
				Event     map[string]int `json:"event"`
				Launch    int            `json:"launch"`
			}
			data, err := os.ReadFile(filepath.Join(directory, "invoke.json"))
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(data, &invoked); err != nil {
				t.Fatal(err)
			}
			if invoked.RequestID != admission.RequestID || invoked.ARN != arn || invoked.Event["unchanged"] != 42 || invoked.Launch != 2 {
				t.Fatalf("accepted native Event payload/identity changed across fallback: %#v", invoked)
			}
		})
	}
}

func TestManagedRuntimeFallbackFinalFailuresHaveExactLaunchAttribution(t *testing.T) {
	for _, runtime := range []string{"python", "node"} {
		for _, mode := range []string{"fallback_process_failure", "fallback_handler_failure"} {
			t.Run(runtime+"/"+mode, func(t *testing.T) {
				directory := t.TempDir()
				function := managedPhaseFunction(t, runtime, directory, mode, true)
				service, path := managedPhaseService(t, directory, function)
				service.initTimeout = managedFallbackInitBudget
				outcome, err := service.ExecuteObserved(context.Background(), InvokeInput{
					FunctionName: "phase:live", Payload: []byte(`{"unchanged":42}`),
				}, nil)
				if err != nil {
					t.Fatal(err)
				}
				record := managedPhaseRecord(t, service, path, directory, outcome.Metadata, outcome.OwnershipErr, 2)
				if outcome.State != InvocationFailed || !outcome.Output.FunctionError || record.State != InvocationFailed ||
					!record.FunctionError || record.TerminationCause != "" || len(record.ExecutionPhases) != 2 {
					t.Fatalf("fallback final native failure changed: outcome=%#v record=%#v", outcome, record)
				}
				initial, final := record.ExecutionPhases[0], record.ExecutionPhases[1]
				if initial.ProcessError == "" || initial.InitAttempt != 1 || initial.InitState != "timed_out" ||
					initial.TerminationCause != "initialization_timeout" || initial.ContextError != "deadline_exceeded" ||
					final.InitAttempt != 2 || final.Mode != "fallback" || final.InitState != "succeeded" || final.InvokeState != "failed" ||
					final.ContextError != "" || final.TerminationCause != "" {
					t.Fatalf("failed launches lost typed attribution: %#v", record.ExecutionPhases)
				}
				var native struct {
					ErrorType    string `json:"errorType"`
					ErrorMessage string `json:"errorMessage"`
				}
				if err := json.Unmarshal(outcome.Output.Payload, &native); err != nil {
					t.Fatal(err)
				}
				if mode == "fallback_process_failure" {
					if record.ProcessError != "exit status 17" || final.ProcessError != "exit status 17" || native.ErrorType != "Runtime.ExitError" {
						t.Fatalf("retired kill replaced final exit failure: native=%#v record=%#v", native, record)
					}
				} else {
					if record.ProcessError != "" || final.ProcessError != "" || native.ErrorMessage != "controlled fallback handler failure" {
						t.Fatalf("handler failure confused with retired process exit: native=%#v record=%#v", native, record)
					}
				}
				if err := service.Close(context.Background()); err != nil {
					t.Fatalf("ordinary final failure corrupted joined ownership: %v", err)
				}
			})
		}
	}
}
