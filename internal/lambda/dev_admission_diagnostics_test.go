//go:build linux || darwin

package lambda

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/devcapture"
	"github.com/stretchr/testify/require"
)

func TestPrivateAdmissionErrorContainsOnlyAllowlistedNativeFields(t *testing.T) {
	const secret = "request-authorization-environment-or-arbitrary-error-must-stay-out"
	native := &InvokeError{Code: "TooManyRequestsException", Status: 429, Message: strings.Repeat(secret, 2000)}
	for _, test := range []struct {
		name      string
		state     InvocationState
		rejection *InvokeError
		want      bool
	}{
		{"native-throttle", InvocationNotStarted, native, true},
		{"wrong-status", InvocationNotStarted, &InvokeError{Code: native.Code, Status: 500, Message: secret}, false},
		{"arbitrary-code", InvocationNotStarted, &InvokeError{Code: secret, Status: 429, Message: secret}, false},
		{"no-native-rejection", InvocationNotStarted, nil, false},
		{"accepted", InvocationSucceeded, native, false},
		{"runtime-failure", InvocationFailed, native, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			sink := devcapture.NewWriter(&output, "private admission contract")
			t.Cleanup(func() { require.NoError(t, sink.Close()) })
			service := &Service{diagnosticCapture: sink}
			now := time.Now()
			require.NoError(t, service.captureDiagnostics(
				executableFunction{name: "admission:live", runtime: "command", timeout: time.Second, environment: map[string]string{"PRIVATE": secret}},
				invocation{requestID: "native-owned-id", name: "admission:live", payload: []byte(secret), clientContext: secret},
				invocationResult{state: test.state, admissionError: test.rejection}, false, now, diagnosticCompletion{at: now},
			))
			var record map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(output.Bytes(), &record))
			field, present := record["admission_error"]
			require.Equal(t, test.want, present)
			if test.want {
				var admission map[string]any
				require.NoError(t, json.Unmarshal(field, &admission))
				require.Equal(t, map[string]any{"code": "TooManyRequestsException", "http_status": float64(http.StatusTooManyRequests)}, admission)
			}
			require.NotContains(t, output.String(), secret)
			require.Less(t, output.Len(), 2048, "an arbitrary native message must not expand admission evidence")
		})
	}
}

func TestPrivateAdmissionErrorDoesNotRelabelOtherNonstartedCauses(t *testing.T) {
	for _, cause := range []string{"observer-error", "observer-invoke-error", "caller-canceled", "launch-failed"} {
		t.Run(cause, func(t *testing.T) {
			directory := t.TempDir()
			function := providedFunction(t, "echo")
			var missing string
			if cause == "launch-failed" {
				missing = filepath.Join(directory, "owned-command")
				require.NoError(t, os.WriteFile(missing, []byte("#!/bin/sh\nexit 0\n"), 0700))
				function = Function{Runtime: "command", Command: []string{missing}, Timeout: time.Second}
			}
			service, path := newDiagnosticTestService(t, map[string]Function{"other": function}, directory, nil, nil)
			if missing != "" {
				require.NoError(t, os.Remove(missing))
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var observer func(InvocationMetadata) error
			switch cause {
			case "observer-error":
				observer = func(InvocationMetadata) error { return errors.New("owned observer unavailable") }
			case "observer-invoke-error":
				// An application/dev callback cannot manufacture a runtime admission fact.
				observer = func(InvocationMetadata) error {
					return &InvokeError{Code: "TooManyRequestsException", Status: 429, Message: "observer refusal"}
				}
			case "caller-canceled":
				observer = func(InvocationMetadata) error { cancel(); return ctx.Err() }
			}
			outcome, err := service.ExecuteObserved(ctx, InvokeInput{FunctionName: "other", Payload: []byte("{}")}, observer)
			require.Equal(t, InvocationNotStarted, outcome.State)
			if cause == "launch-failed" {
				require.NoError(t, err)
				require.True(t, outcome.Output.FunctionError)
				require.Nil(t, outcome.OwnershipErr)
			} else {
				require.Error(t, err)
				require.NotNil(t, outcome.OwnershipErr)
			}
			records := readDiagnosticRecords(t, path)
			require.Len(t, records, 1)
			require.Nil(t, records[0].AdmissionError)
			if cause == "caller-canceled" {
				require.Equal(t, "canceled", records[0].ContextError)
				require.Equal(t, "caller_canceled", records[0].TerminationCause)
			}
			if cause == "launch-failed" {
				require.NotEmpty(t, records[0].ProcessError)
				require.NotNil(t, records[0].FunctionDiagnostic)
			}
		})
	}
}

func TestPrivateAdmissionDecisionPrecedesOptionalEvidenceAndLateCancellation(t *testing.T) {
	directory := t.TempDir()
	writeFixture(t, directory, "admission.py", "def handler(event, context):\n    return {}\n")
	command, environment := pythonCommand(t)
	service, err := NewService(&Config{
		Functions:      map[string]Function{"admission": {Runtime: "python", Command: command, Environment: environment, Handler: "admission.handler", Timeout: time.Second}},
		DevWarm:        &DevWarmConfig{MaxWorkers: 1, MaxConcurrency: 1},
		DevAsync:       &DevAsyncConfig{Workers: 1, LogWriter: io.Discard},
		DevDiagnostics: &DevDiagnosticsConfig{LogPath: filepath.Join(directory, "private.jsonl"), PythonStacks: &DevPythonStacksConfig{}},
	}, directory)
	require.NoError(t, err)
	writer := &pythonStackPolicyWriter{entered: make(chan struct{}), release: make(chan struct{})}
	var unblock sync.Once
	release := func() { unblock.Do(func() { close(writer.release) }) }
	t.Cleanup(func() {
		release()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, service.Close(ctx))
	})
	require.NoError(t, service.diagnosticCapture.Close())
	service.diagnosticCapture = devcapture.NewWriter(writer, "held optional admission evidence")
	entry := service.functions["admission"]
	reservation, err := service.warm.acquire(context.Background(), entry, false)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type completion struct {
		outcome InvocationOutcome
		err     error
	}
	done := make(chan completion, 1)
	go func() {
		outcome, err := service.ExecuteObserved(ctx, InvokeInput{FunctionName: "admission", Payload: []byte("{}")}, nil)
		done <- completion{outcome, err}
	}()
	select {
	case <-writer.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("native admission never reached optional stack evidence")
	}
	// The actual pool has already refused this request. Only its optional private
	// append is held; canceling now must not replace the native admission cause.
	cancel()
	release()
	select {
	case result := <-done:
		var native *InvokeError
		require.ErrorAs(t, result.err, &native)
		require.Equal(t, 429, native.Status)
		require.Equal(t, InvocationNotStarted, result.outcome.State)
		require.Nil(t, result.outcome.OwnershipErr)
	case <-time.After(5 * time.Second):
		t.Fatal("native admission evidence did not join")
	}
	stacks, records := writer.records(t)
	require.Len(t, stacks, 1)
	require.Equal(t, "process_not_started", stacks[0].Reason)
	require.Len(t, records, 1)
	require.Equal(t, &diagnosticAdmissionError{Code: "TooManyRequestsException", HTTPStatus: 429}, records[0].AdmissionError)
	require.Empty(t, records[0].ContextError)
	require.Empty(t, records[0].TerminationCause)
	require.False(t, records[0].NativeResponseSynthesized)
	require.True(t, records[0].OwnershipConfirmed)
	require.Empty(t, records[0].ExecutionPhases)
	_, err = service.warm.release(reservation, false)
	require.NoError(t, err)
}
