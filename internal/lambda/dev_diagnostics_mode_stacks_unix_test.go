//go:build linux || darwin

package lambda

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/devcapture"
	"github.com/stretchr/testify/require"
)

const diagnosticModeStackHandler = `
import json
import os
from pathlib import Path
import sys
import time

def handler(event, context):
    marker = Path(os.environ['MODE_STACK_MARKER'])
    temporary = marker.with_suffix('.tmp')
    temporary.write_text(json.dumps({'request_id': context.aws_request_id, 'pid': os.getpid()}))
    temporary.replace(marker)
    print('mode stack stderr ' + context.aws_request_id, file=sys.stderr, flush=True)
    while not Path(os.environ['MODE_STACK_RELEASE']).exists():
        time.sleep(0.005)
    if event.get('fail'):
        raise RuntimeError('controlled mode stack failure')
    return {'request_id': context.aws_request_id, 'ok': True}
`

func newDiagnosticModeStackService(t *testing.T, wantCloseError error) (*Service, string, string, string) {
	t.Helper()
	directory := t.TempDir()
	writeFixture(t, directory, "mode_stack.py", diagnosticModeStackHandler)
	command, environment := pythonCommand(t)
	marker, release := filepath.Join(directory, "handler-ready"), filepath.Join(directory, "handler-release")
	environment["MODE_STACK_MARKER"], environment["MODE_STACK_RELEASE"] = marker, release
	path := filepath.Join(directory, "private", "mode.jsonl")
	service, err := NewService(&Config{
		Functions: map[string]Function{"stackwait": {Runtime: "python", Command: command, Environment: environment,
			Handler: "mode_stack.handler", Timeout: 10 * time.Second}},
		DevAsync: &DevAsyncConfig{Workers: 1, LogWriter: io.Discard},
		DevDiagnostics: &DevDiagnosticsConfig{LogPath: path, Mode: DevDiagnosticsFailures,
			PythonStacks: &DevPythonStacksConfig{}},
	}, directory)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = os.WriteFile(release, nil, 0600)
		err := service.Close(context.Background())
		if err != nil && (wantCloseError == nil || !errors.Is(err, wantCloseError)) {
			t.Errorf("unexpected private collector cleanup error: %v", err)
		}
	})
	return service, path, marker, release
}

// Wait for an actual collector response in its existing bounded session. Disk
// absence alone could otherwise pass because the snapshot never ran at all.
func collectedDiagnosticModeStack(t *testing.T, service *Service, metadata InvocationMetadata) (*pythonStackSession, pythonStackRecord) {
	t.Helper()
	service.mu.Lock()
	var session *pythonStackSession
	for _, owner := range service.active {
		if owner.metadata == metadata {
			session = owner.pythonStacks
		}
	}
	service.mu.Unlock()
	require.NotNil(t, session)
	select {
	case <-session.done:
	case <-time.After(3 * time.Second):
		t.Fatal("real Python collector did not finish its requested snapshot")
	}
	session.mu.Lock()
	record := session.record
	session.mu.Unlock()
	assertPythonStackSnapshot(t, record, metadata)
	require.Equal(t, "explicit", record.Trigger)
	return session, record
}

func assertDiagnosticModeStackJoined(t *testing.T, service *Service, session *pythonStackSession, pid int) {
	t.Helper()
	require.False(t, processAlive(pid), "real Python handler survived native completion")
	select {
	case <-session.done:
	default:
		t.Fatal("native invocation returned before collector join")
	}
	session.mu.Lock()
	stopped, reader, writer := session.stopped, session.reader, session.writer
	session.mu.Unlock()
	require.True(t, stopped)
	require.Nil(t, reader)
	require.Nil(t, writer)
	service.mu.Lock()
	active := len(service.active)
	service.mu.Unlock()
	require.Zero(t, active, "native invocation owner survived completion")
}

func TestFailureOnlyDiagnosticsDefersRealPythonStackUntilFinalOutcome(t *testing.T) {
	for _, result := range []string{"success", "handler-failure", "caller-cancel"} {
		t.Run(result, func(t *testing.T) {
			service, path, marker, release := newDiagnosticModeStackService(t, nil)
			metadata, cancel, finished := startPythonStackExecution(t, service, map[string]any{"fail": result == "handler-failure"}, nil)
			waitDiagnosticMarker(t, marker)
			pid := pythonStackProcessPID(t, marker, metadata)
			require.True(t, service.RequestPythonSnapshot(metadata))
			session, snapshot := collectedDiagnosticModeStack(t, service, metadata)
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Empty(t, data, "failure-only mode emitted a live stack before final selection")
			require.True(t, processAlive(pid), "fixture must still be executing at the early-file assertion")
			if result == "caller-cancel" {
				cancel()
			} else {
				require.NoError(t, os.WriteFile(release, nil, 0600))
			}
			native := finishPythonStackExecution(t, finished)
			require.Equal(t, metadata, native.outcome.Metadata)
			require.NoError(t, native.outcome.OwnershipErr)
			assertDiagnosticModeStackJoined(t, service, session, pid)
			if result == "success" {
				require.NoError(t, native.err)
				require.Equal(t, InvocationSucceeded, native.outcome.State)
				require.False(t, native.outcome.Output.FunctionError)
				var payload struct {
					RequestID string `json:"request_id"`
					OK        bool   `json:"ok"`
				}
				require.NoError(t, json.Unmarshal(native.outcome.Output.Payload, &payload))
				require.Equal(t, metadata.RequestID, payload.RequestID)
				require.True(t, payload.OK)
				data, err = os.ReadFile(path)
				require.NoError(t, err)
				require.Empty(t, data, "clean success retained a row from either private schema")
				require.NoError(t, service.DevEvidence())
				require.NoError(t, service.Close(context.Background()))
				return
			}
			wantState := InvocationFailed
			if result == "caller-cancel" {
				wantState = InvocationCanceled
				require.ErrorIs(t, native.err, context.Canceled)
			} else {
				require.NoError(t, native.err)
				require.Contains(t, string(native.outcome.Output.Payload), "controlled mode stack failure")
			}
			require.Equal(t, wantState, native.outcome.State)
			require.True(t, native.outcome.Output.FunctionError)
			assertPythonStackTerminal(t, path, metadata, wantState, snapshot)
			data, err = os.ReadFile(path)
			require.NoError(t, err)
			lines := bytes.Split(bytes.TrimSpace(data), []byte{'\n'})
			require.Len(t, lines, 2)
			require.Contains(t, string(lines[0]), "eventbus.lambda.python-stack.v1")
			require.Contains(t, string(lines[1]), "eventbus.lambda.invocation-diagnostic.v1")
			require.NoError(t, service.DevEvidence())
			require.NoError(t, service.Close(context.Background()))
		})
	}
}

type deferredDiagnosticModeStackFailure struct {
	writer  *pythonStackPolicyWriter
	failure error
}

func (writer deferredDiagnosticModeStackFailure) Write(data []byte) (int, error) {
	written, err := writer.writer.Write(data)
	if err == nil && bytes.Contains(data, []byte("eventbus.lambda.python-stack.v1")) {
		return 0, writer.failure
	}
	return written, err
}

func TestFailureOnlyDeferredPythonStackAppendRemainsJoinedAndSticky(t *testing.T) {
	failure := errors.New("controlled deferred stack append failure")
	service, _, marker, handlerRelease := newDiagnosticModeStackService(t, failure)
	writer := &pythonStackPolicyWriter{entered: make(chan struct{}), release: make(chan struct{})}
	require.NoError(t, service.diagnosticCapture.Close())
	service.diagnosticCapture = devcapture.NewWriter(deferredDiagnosticModeStackFailure{writer, failure}, "deferred stack fault")
	var once sync.Once
	unblock := func() { once.Do(func() { close(writer.release) }) }
	t.Cleanup(unblock)
	metadata, _, finished := startPythonStackExecution(t, service, map[string]any{"fail": true}, nil)
	waitDiagnosticMarker(t, marker)
	pid := pythonStackProcessPID(t, marker, metadata)
	require.True(t, service.RequestPythonSnapshot(metadata))
	session, _ := collectedDiagnosticModeStack(t, service, metadata)
	select {
	case <-writer.entered:
		t.Fatal("failure-only stack reached its sink before native outcome selection")
	default:
	}
	require.NoError(t, os.WriteFile(handlerRelease, nil, 0600))
	select {
	case <-writer.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("selected native failure did not append its deferred snapshot")
	}
	require.False(t, processAlive(pid), "deferred append must follow actual native cleanup")
	select {
	case result := <-finished:
		t.Fatalf("invocation crossed held deferred append: %+v", result)
	default:
	}
	closed := make(chan error, 1)
	go func() { closed <- service.Close(context.Background()) }()
	require.Eventually(t, func() bool {
		service.mu.Lock()
		defer service.mu.Unlock()
		return service.closed
	}, time.Second, time.Millisecond)
	select {
	case err := <-closed:
		t.Fatalf("Close crossed held deferred evidence: %v", err)
	default:
	}
	unblock()
	native := finishPythonStackExecution(t, finished)
	require.NoError(t, native.err, "private append failure must not alter the native error contract")
	require.Equal(t, InvocationFailed, native.outcome.State)
	require.True(t, native.outcome.Output.FunctionError)
	require.Contains(t, string(native.outcome.Output.Payload), "controlled mode stack failure")
	require.ErrorIs(t, native.outcome.OwnershipErr, failure)
	assertDiagnosticModeStackJoined(t, service, session, pid)
	select {
	case err := <-closed:
		require.ErrorIs(t, err, failure)
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not join released deferred append")
	}
	require.ErrorIs(t, service.DevEvidence(), failure)
	require.ErrorIs(t, service.Close(context.Background()), failure)
	stacks, terminals := writer.records(t)
	require.Len(t, stacks, 1, "borrowed writer saw the failed append")
	require.Equal(t, metadata.RequestID, stacks[0].RequestID)
	require.Empty(t, terminals, "a sticky sink failure cannot publish a false healthy terminal row")
}
