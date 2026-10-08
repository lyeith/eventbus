//go:build linux || darwin

package lambda

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/gateway"
)

const diagnosticCausePythonSource = `
import json
import os
import sys
import time

def started(context):
    print("cause stdout " + context.aws_request_id, flush=True)
    print("cause stderr " + context.aws_request_id, file=sys.stderr, flush=True)
    marker = os.environ["CAUSE_MARKER"]
    with open(marker + ".tmp", "w") as output:
        json.dump({"request_id": context.aws_request_id, "arn": context.invoked_function_arn}, output)
    os.replace(marker + ".tmp", marker)

def waiting(event, context):
    started(context)
    while True:
        time.sleep(10)

def success(event, context):
    started(context)
    return {"requestId": context.aws_request_id, "arn": context.invoked_function_arn, "ok": True}

def failed(event, context):
    started(context)
    raise RuntimeError("controlled application failure")

def authorize(event, context):
    headers = {name.lower(): value for name, value in event["headers"].items()}
    effect = "Allow" if headers.get("authorization") == "Bearer owned" else "Deny"
    return {"principalId": "cause-fixture", "policyDocument": {"Version": "2012-10-17",
        "Statement": [{"Action": "execute-api:Invoke", "Effect": effect, "Resource": event["methodArn"]}]}}
`

func diagnosticCausePythonFunction(t *testing.T, directory, marker, handler string, timeout time.Duration) Function {
	t.Helper()
	writeFixture(t, directory, "cause.py", diagnosticCausePythonSource)
	command, environment := pythonCommand(t)
	environment["CAUSE_MARKER"] = marker
	return Function{Runtime: "python", Command: command, Handler: "cause." + handler, Environment: environment, Timeout: timeout}
}

func assertDiagnosticCauseIdentity(t *testing.T, record invocationDiagnosticRecord, metadata InvocationMetadata, marker string, timeout time.Duration) {
	t.Helper()
	var actual struct {
		RequestID string `json:"request_id"`
		ARN       string `json:"arn"`
	}
	data, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &actual); err != nil {
		t.Fatal(err)
	}
	if actual.RequestID == "" || actual.RequestID != metadata.RequestID || actual.ARN != metadata.FunctionARN ||
		record.SchemaVersion != "eventbus.lambda.invocation-diagnostic.v1" ||
		record.RequestID != actual.RequestID || record.FunctionARN != actual.ARN || record.FunctionName != metadata.FunctionName ||
		record.Runtime != "python" || record.InvocationType != "RequestResponse" || record.Attempt != 1 ||
		!record.OwnershipConfirmed || record.OwnershipError != "" || record.ConfiguredTimeoutMS != timeout.Milliseconds() {
		t.Fatalf("actual Python attempt identity/ownership: record=%#v metadata=%#v actual=%#v", record, metadata, actual)
	}
	if record.StartedAt.IsZero() || record.CompletedAt.Before(record.StartedAt) || record.ElapsedMS <= 0 ||
		math.IsNaN(record.ElapsedMS) || math.IsInf(record.ElapsedMS, 0) {
		t.Fatalf("joined invocation times: %#v", record)
	}
	wallElapsedMS := float64(record.CompletedAt.Sub(record.StartedAt)) / float64(time.Millisecond)
	if math.Abs(wallElapsedMS-record.ElapsedMS) > 10 {
		t.Fatalf("elapsed_ms does not describe actual joined lifetime: elapsed=%f timestamps=%f", record.ElapsedMS, wallElapsedMS)
	}
	if record.Stdout == nil || record.Stdout.Truncated || record.Stderr.Truncated || record.Tail.Truncated ||
		!strings.Contains(string(diagnosticData(t, *record.Stdout)), "cause stdout "+actual.RequestID) ||
		!strings.Contains(string(diagnosticData(t, record.Stderr)), "cause stderr "+actual.RequestID) ||
		!strings.Contains(string(diagnosticData(t, record.Tail)), actual.RequestID) ||
		record.Stdout.Bytes > maxLogs || record.Stderr.Bytes > maxLogs || record.Tail.Bytes > maxLogs {
		t.Fatalf("bounded Python streams lost actual identity: %#v", record)
	}
}

func assertLegacyDiagnosticTimeout(t *testing.T, output InvokeOutput, timeout time.Duration) {
	t.Helper()
	var native struct {
		ErrorType    string `json:"errorType"`
		ErrorMessage string `json:"errorMessage"`
	}
	if err := json.Unmarshal(output.Payload, &native); err != nil {
		t.Fatal(err)
	}
	if !output.FunctionError || native.ErrorType != "Sandbox.Timedout" ||
		native.ErrorMessage != fmt.Sprintf("Task timed out after %.2f seconds", timeout.Seconds()) {
		t.Fatalf("legacy native timeout response changed: %#v %s", output, output.Payload)
	}
}

func TestPrivateDiagnosticsPythonCancellationCausesPreserveNativeProjection(t *testing.T) {
	for _, mode := range []string{"caller_cancel", "caller_deadline", "service_close", "function_budget"} {
		t.Run(mode, func(t *testing.T) {
			directory := t.TempDir()
			marker := filepath.Join(directory, "started")
			budget := 6 * time.Second
			if mode == "function_budget" {
				budget = 2 * time.Second
			}
			function := diagnosticCausePythonFunction(t, directory, marker, "waiting", budget)
			observer := &activityRecorder{}
			service, path := newDiagnosticTestService(t, map[string]Function{"waiting:live": function}, directory, observer, nil)
			arn := "arn:aws:lambda:eu-west-1:123456789012:function:waiting:live"
			ctx := context.Background()
			cancel := func() {}
			wantErr := error(nil)
			wantState, wantCause, wantContext := InvocationCanceled, "caller_canceled", "canceled"
			switch mode {
			case "caller_cancel":
				ctx, cancel = context.WithCancel(ctx)
				wantErr = context.Canceled
			case "caller_deadline":
				ctx, cancel = context.WithTimeout(ctx, 2*time.Second)
				wantErr, wantState = context.DeadlineExceeded, InvocationTimedOut
				wantCause, wantContext = "caller_deadline", "deadline_exceeded"
			case "service_close":
				wantCause = "service_canceled"
			case "function_budget":
				wantState, wantCause, wantContext = InvocationTimedOut, "function_timeout", "deadline_exceeded"
			}
			defer cancel()
			type completion struct {
				outcome InvocationOutcome
				err     error
			}
			done := make(chan completion, 1)
			go func() {
				outcome, err := service.ExecuteObserved(ctx, InvokeInput{FunctionName: arn, Payload: []byte("{}")}, nil)
				done <- completion{outcome, err}
			}()
			waitDiagnosticMarker(t, marker)
			switch mode {
			case "caller_cancel":
				cancel()
			case "service_close":
				if err := service.Close(context.Background()); err != nil {
					t.Fatalf("joined service cancellation: %v", err)
				}
			}
			var result completion
			select {
			case result = <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("invocation did not join after cancellation")
			}
			if (wantErr == nil && result.err != nil) || (wantErr != nil && !errors.Is(result.err, wantErr)) ||
				result.outcome.State != wantState || result.outcome.OwnershipErr != nil ||
				result.outcome.Output.RequestID == "" || result.outcome.Output.RequestID != result.outcome.Metadata.RequestID {
				t.Fatalf("native observed result changed: %#v %v", result.outcome, result.err)
			}
			assertLegacyDiagnosticTimeout(t, result.outcome.Output, budget)
			assertActivityFinished(t, observer, "lambda_invoke", nil)
			records := readDiagnosticRecords(t, path)
			if len(records) != 1 {
				t.Fatalf("actual joined attempt count: %#v", records)
			}
			record := records[0]
			assertDiagnosticCauseIdentity(t, record, result.outcome.Metadata, marker, budget)
			if record.State != wantState || !record.FunctionError || record.ContextError != wantContext ||
				record.TerminationCause != wantCause || !record.NativeResponseSynthesized {
				t.Fatalf("private cause disagrees with actual cancellation: %#v", record)
			}
			if mode == "function_budget" {
				if record.FunctionDiagnostic == nil || !strings.Contains(string(diagnosticData(t, *record.FunctionDiagnostic)), "Sandbox.Timedout") ||
					record.ElapsedMS < float64(budget.Milliseconds())-100 {
					t.Fatalf("own budget lost explicitly synthetic diagnostic: %#v", record)
				}
			} else if record.FunctionDiagnostic != nil || record.ElapsedMS >= float64(budget.Milliseconds()) {
				t.Fatalf("external cancellation falsely claims configured timeout: %#v", record)
			}
			if err := service.Close(context.Background()); err != nil || service.DevEvidence() != nil {
				t.Fatalf("confirmed canceled attempt left uncertain owner: close=%v evidence=%v", err, service.DevEvidence())
			}
			// Closing/canceling after the joined attempt must not relabel its frozen cause.
			closedRecords := readDiagnosticRecords(t, path)
			if len(closedRecords) != 1 || closedRecords[0].TerminationCause != wantCause || closedRecords[0].ElapsedMS != record.ElapsedMS {
				t.Fatalf("completion cause/time changed after closure: %#v", closedRecords)
			}
		})
	}
}

func TestPrivateDiagnosticsPythonOrdinaryResultsDoNotInventCancellation(t *testing.T) {
	for _, handler := range []string{"success", "failed"} {
		t.Run(handler, func(t *testing.T) {
			directory := t.TempDir()
			marker := filepath.Join(directory, "started")
			budget := 6 * time.Second
			function := diagnosticCausePythonFunction(t, directory, marker, handler, budget)
			observer := &activityRecorder{}
			service, path := newDiagnosticTestService(t, map[string]Function{"ordinary:live": function}, directory, observer, nil)
			arn := "arn:aws:lambda:eu-west-1:123456789012:function:ordinary:live"
			outcome, err := service.ExecuteObserved(context.Background(), InvokeInput{FunctionName: arn, Payload: []byte("{}")}, nil)
			wantState, wantFunctionError := InvocationSucceeded, false
			if handler == "failed" {
				wantState, wantFunctionError = InvocationFailed, true
			}
			if err != nil || outcome.OwnershipErr != nil || outcome.State != wantState || outcome.Output.FunctionError != wantFunctionError {
				t.Fatalf("ordinary native outcome changed: %#v %v", outcome, err)
			}
			assertActivityFinished(t, observer, "lambda_invoke", nil)
			records := readDiagnosticRecords(t, path)
			if len(records) != 1 {
				t.Fatalf("ordinary attempt count: %#v", records)
			}
			record := records[0]
			assertDiagnosticCauseIdentity(t, record, outcome.Metadata, marker, budget)
			if record.State != wantState || record.FunctionError != wantFunctionError || record.TerminationCause != "" ||
				record.ContextError != "" || record.NativeResponseSynthesized {
				t.Fatalf("ordinary handler result mislabeled as cancellation: %#v", record)
			}
			if wantFunctionError {
				if record.FunctionDiagnostic == nil || !strings.Contains(string(diagnosticData(t, *record.FunctionDiagnostic)), "controlled application failure") ||
					!strings.Contains(string(outcome.Output.Payload), `"errorType":"RuntimeError"`) {
					t.Fatalf("genuine Python failure was replaced: %#v %s", record, outcome.Output.Payload)
				}
			} else {
				var native struct {
					RequestID string `json:"requestId"`
					ARN       string `json:"arn"`
					OK        bool   `json:"ok"`
				}
				if err := json.Unmarshal(outcome.Output.Payload, &native); err != nil {
					t.Fatal(err)
				}
				if record.FunctionDiagnostic != nil || native.RequestID != record.RequestID || native.ARN != arn || !native.OK {
					t.Fatalf("native successful response changed: %#v %#v", native, record)
				}
			}
			if err := service.Close(context.Background()); err != nil || service.DevEvidence() != nil {
				t.Fatalf("ordinary joined owner: close=%v evidence=%v", err, service.DevEvidence())
			}
		})
	}
}

func TestPrivateDiagnosticsPythonGatewayDeadlineRemainsNative504(t *testing.T) {
	directory := t.TempDir()
	marker := filepath.Join(directory, "integration-started")
	budget := 6 * time.Second
	function := diagnosticCausePythonFunction(t, directory, marker, "waiting", budget)
	authorizer := diagnosticCausePythonFunction(t, directory, filepath.Join(directory, "authorizer-unused"), "authorize", budget)
	observer := &activityRecorder{}
	path := filepath.Join(directory, "private", "invocations.jsonl")
	service, err := NewService(&Config{
		Functions:   map[string]Function{"waiting:live": function, "authorize": authorizer},
		DevActivity: observer,
		DevAsync:    &DevAsyncConfig{Workers: 1, LogWriter: io.Discard},
		DevDiagnostics: &DevDiagnosticsConfig{
			LogPath: path, PythonStacks: &DevPythonStacksConfig{SnapshotAfter: 1800 * time.Millisecond},
		},
	}, directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := service.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	aws := httptest.NewServer(service)
	t.Cleanup(aws.Close)
	arn := "arn:aws:lambda:eu-west-1:123456789012:function:waiting:live"
	zero := 0
	proxy, err := gateway.New(gateway.Config{
		Region: "eu-west-1", AccountID: "123456789012", APIID: "cause", Stage: "test",
		Authorizers: map[string]gateway.AuthorizerConfig{"native": {
			Type: "REQUEST", InvokeURL: aws.URL + invokePrefix + "authorize" + invokeSuffix, TTL: &zero, Timeout: 2 * time.Second,
		}},
		Routes: []gateway.RouteConfig{{
			Path: "/work", Method: http.MethodGet, Authorizer: "native",
			Integration: gateway.IntegrationConfig{Type: "AWS_PROXY", InvokeURL: aws.URL + invokePrefix + arn + invokeSuffix, PayloadFormatVersion: "1.0", Timeout: 2500 * time.Millisecond},
		}},
	}, gateway.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := proxy.Close(); err != nil {
			t.Error(err)
		}
	})
	request := httptest.NewRequest(http.MethodGet, "http://gateway.example/work", nil)
	request.Header.Set("Authorization", "Bearer owned")
	response := httptest.NewRecorder()
	proxy.ServeHTTP(response, request)
	gatewayCompletedAt := time.Now()
	if response.Code != http.StatusGatewayTimeout {
		t.Fatalf("native authorized integration timeout changed: %d %s", response.Code, response.Body.String())
	}
	waitDiagnosticMarker(t, marker)
	// The gateway's HTTP result may precede Lambda process cleanup and private
	// append. Inspect records only after both real admitted lifetimes retire.
	deadline := time.Now().Add(10 * time.Second)
	for {
		active, begun, completed := observer.snapshot()
		if active == 0 && len(completed) == 2 {
			if len(begun) != 2 {
				t.Fatalf("native authorizer/integration admissions: %#v", begun)
			}
			for _, joined := range completed {
				if joined.err != nil {
					t.Fatalf("gateway cancellation did not confirm process ownership: %#v", joined)
				}
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("gateway Lambda handlers did not join: active=%d begun=%#v completed=%#v", active, begun, completed)
		}
		time.Sleep(5 * time.Millisecond)
	}
	snapshots, records := readPythonStackEvidence(t, path)
	if len(records) != 2 {
		t.Fatalf("native authorizer/integration attempt records: %#v", records)
	}
	var canceled *invocationDiagnosticRecord
	for index := range records {
		record := &records[index]
		switch record.FunctionName {
		case "waiting:live":
			canceled = record
		case "authorize":
			if record.State != InvocationSucceeded || record.TerminationCause != "" || record.ContextError != "" || record.NativeResponseSynthesized || !record.OwnershipConfirmed {
				t.Fatalf("native authorization was replaced by a bypass: %#v", record)
			}
		default:
			t.Fatalf("unexpected registered function executed: %#v", record)
		}
	}
	if canceled == nil {
		t.Fatal("actual integration attempt missing")
	}
	assertDiagnosticCauseIdentity(t, *canceled, InvocationMetadata{RequestID: canceled.RequestID, FunctionARN: arn, FunctionName: "waiting:live", Attempt: 1}, marker, budget)
	if canceled.State != InvocationCanceled || canceled.TerminationCause != "caller_canceled" || canceled.ContextError != "canceled" ||
		!canceled.FunctionError || !canceled.NativeResponseSynthesized || canceled.FunctionDiagnostic != nil ||
		canceled.ElapsedMS >= float64(budget.Milliseconds()) {
		t.Fatalf("HTTP disconnect manufactured a configured-timeout exception: %#v", canceled)
	}
	var waitingSnapshots []pythonStackRecord
	for _, snapshot := range snapshots {
		if snapshot.RequestID == canceled.RequestID {
			waitingSnapshots = append(waitingSnapshots, snapshot)
		}
	}
	if len(waitingSnapshots) != 1 {
		t.Fatalf("actual integration stack attempts: %#v", waitingSnapshots)
	}
	snapshot := waitingSnapshots[0]
	if snapshot.FunctionName != canceled.FunctionName || snapshot.FunctionARN != arn || snapshot.Attempt != canceled.Attempt ||
		snapshot.Trigger != "snapshot_after" || snapshot.Status != "captured" ||
		snapshot.RequestedAt.IsZero() || snapshot.CapturedAt.Before(snapshot.RequestedAt) ||
		!snapshot.CapturedAt.Before(gatewayCompletedAt) || !snapshot.CapturedAt.Before(canceled.CompletedAt) {
		t.Fatalf("Python wait snapshot was not captured before actual HTTP cancellation: stack=%#v terminal=%#v", snapshot, canceled)
	}
	foundWaitingThread := false
	for _, thread := range snapshot.Threads {
		for _, frame := range thread.Frames {
			if frame.Function == "waiting" && filepath.Base(frame.File) == "cause.py" {
				foundWaitingThread = true
			}
		}
	}
	if !foundWaitingThread || canceled.PythonStack == nil || canceled.PythonStack.Status != "captured" || canceled.PythonStack.Trigger != "snapshot_after" {
		t.Fatalf("captured thread wait missing from correlated private evidence: stack=%#v terminal=%#v", snapshot, canceled)
	}
	if err := service.Close(context.Background()); err != nil || service.DevEvidence() != nil {
		t.Fatalf("gateway canceled owner: close=%v evidence=%v", err, service.DevEvidence())
	}
}
