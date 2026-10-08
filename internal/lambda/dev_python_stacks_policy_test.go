//go:build linux || darwin

package lambda

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/devcapture"
	"github.com/stretchr/testify/require"
)

type pythonStackPolicyWriter struct {
	mu               sync.Mutex
	buffer           bytes.Buffer
	entered, release chan struct{}
	block            sync.Once
}

func (writer *pythonStackPolicyWriter) Write(data []byte) (int, error) {
	if bytes.Contains(data, []byte("eventbus.lambda.python-stack.v1")) {
		writer.block.Do(func() {
			close(writer.entered)
			<-writer.release
		})
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.buffer.Write(data)
}

func (writer *pythonStackPolicyWriter) records(t *testing.T) ([]pythonStackRecord, []invocationDiagnosticRecord) {
	t.Helper()
	writer.mu.Lock()
	data := append([]byte(nil), writer.buffer.Bytes()...)
	writer.mu.Unlock()
	decoder := json.NewDecoder(bytes.NewReader(data))
	var stacks []pythonStackRecord
	var terminals []invocationDiagnosticRecord
	for {
		var record json.RawMessage
		err := decoder.Decode(&record)
		if err == io.EOF {
			return stacks, terminals
		}
		require.NoError(t, err)
		var header struct {
			SchemaVersion string `json:"schema_version"`
		}
		require.NoError(t, json.Unmarshal(record, &header))
		switch header.SchemaVersion {
		case "eventbus.lambda.python-stack.v1":
			var stack pythonStackRecord
			require.NoError(t, json.Unmarshal(record, &stack))
			stacks = append(stacks, stack)
		case "eventbus.lambda.invocation-diagnostic.v1":
			var terminal invocationDiagnosticRecord
			require.NoError(t, json.Unmarshal(record, &terminal))
			terminals = append(terminals, terminal)
		default:
			t.Fatalf("unknown private evidence schema %q", header.SchemaVersion)
		}
	}
}

// Optional private collection joins inside the invocation owner, after the
// immutable native completion. Neither its latency nor a subsequent Close may
// manufacture a native timeout/cancellation for an already joined success.
func TestPythonStackOptionalJoinPreservesAlreadyJoinedNativeSuccess(t *testing.T) {
	for _, stop := range []string{"function-budget", "service-close"} {
		t.Run(stop, func(t *testing.T) {
			const budget = 3 * time.Second
			directory := t.TempDir()
			ready, handlerRelease := filepath.Join(directory, "ready"), filepath.Join(directory, "release-handler")
			writeFixture(t, directory, "policy.py", `
import os
from pathlib import Path
import time

def handler(event, context):
    Path(os.environ['STACK_POLICY_READY']).write_text(context.aws_request_id)
    while not Path(os.environ['STACK_POLICY_RELEASE']).exists():
        time.sleep(0.005)
    return {'unchanged': True}
`)
			command, environment := pythonCommand(t)
			environment["STACK_POLICY_READY"], environment["STACK_POLICY_RELEASE"] = ready, handlerRelease
			service, err := NewService(&Config{
				Functions: map[string]Function{"policy": {Runtime: "python", Command: command, Environment: environment,
					Handler: "policy.handler", Timeout: budget}},
				DevAsync: &DevAsyncConfig{Workers: 1, LogWriter: io.Discard},
				DevDiagnostics: &DevDiagnosticsConfig{LogPath: filepath.Join(directory, "private.jsonl"),
					PythonStacks: &DevPythonStacksConfig{}},
			}, directory)
			require.NoError(t, err)
			writer := &pythonStackPolicyWriter{entered: make(chan struct{}), release: make(chan struct{})}
			require.NoError(t, service.diagnosticCapture.Close())
			// Borrowed fault injection replaces only the private sink before any
			// invocation; native configuration, process and collector are real.
			service.diagnosticCapture = devcapture.NewWriter(writer, "private policy")
			cleanup := service.processCleanup
			nativeJoined := make(chan time.Time, 1)
			service.processCleanup = func(command *exec.Cmd) error {
				err := cleanup(command)
				if err == nil && command.ProcessState != nil && command.ProcessState.Success() {
					nativeJoined <- time.Now()
				}
				return err
			}
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(writer.release) }) }
			caller, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			finished := make(chan pythonStackExecution, 1)
			var closeResult chan error
			t.Cleanup(func() {
				_ = os.WriteFile(handlerRelease, nil, 0600)
				unblock()
				cancel()
				require.NoError(t, service.Close(context.Background()))
			})
			admitted := make(chan InvocationMetadata, 1)
			admissionAt := time.Now()
			go func() {
				outcome, err := service.ExecuteObserved(caller, InvokeInput{FunctionName: "policy", Payload: []byte("{}")},
					func(metadata InvocationMetadata) error {
						admitted <- metadata
						return nil
					})
				finished <- pythonStackExecution{outcome: outcome, err: err}
			}()
			var metadata InvocationMetadata
			select {
			case metadata = <-admitted:
			case <-time.After(5 * time.Second):
				t.Fatal("actual Python invocation did not admit")
			}
			require.Eventually(t, func() bool {
				data, err := os.ReadFile(ready)
				return err == nil && string(data) == metadata.RequestID
			}, 2*time.Second, time.Millisecond)
			require.True(t, service.RequestPythonSnapshot(metadata))
			select {
			case <-writer.entered:
			case <-time.After(time.Second):
				t.Fatal("actual collector never reached the gated private append")
			}
			require.NoError(t, os.WriteFile(handlerRelease, nil, 0600))
			select {
			case joinedAt := <-nativeJoined:
				require.Less(t, joinedAt.Sub(admissionAt), budget-200*time.Millisecond,
					"the original native child must already be successfully joined before the budget expires")
			case <-time.After(time.Second):
				t.Fatal("actual native child did not join before optional evidence")
			}
			service.mu.Lock()
			active := len(service.active)
			var session *pythonStackSession
			for _, owner := range service.active {
				session = owner.pythonStacks
			}
			service.mu.Unlock()
			require.Equal(t, 1, active, "optional collector still belongs to the admitted invocation")
			require.NotNil(t, session)
			require.Eventually(t, func() bool {
				session.mu.Lock()
				defer session.mu.Unlock()
				return session.stopped
			}, time.Second, time.Millisecond, "native completion reached its sole optional collector join")
			select {
			case returned := <-finished:
				t.Fatalf("invocation escaped its private collector join: %+v", returned)
			default:
			}
			if stop == "function-budget" {
				// This wait deliberately crosses the configured deadline after
				// the real child joined, independently of append scheduling.
				expired := time.NewTimer(time.Until(admissionAt.Add(budget + 100*time.Millisecond)))
				<-expired.C
			} else {
				closeResult = make(chan error, 1)
				go func() { closeResult <- service.Close(context.Background()) }()
				require.Eventually(t, func() bool {
					service.mu.Lock()
					defer service.mu.Unlock()
					return service.closed
				}, time.Second, time.Millisecond)
				select {
				case err := <-closeResult:
					t.Fatalf("Close returned before private collection joined: %v", err)
				default:
				}
			}
			unblock()
			var completed pythonStackExecution
			select {
			case completed = <-finished:
			case <-time.After(5 * time.Second):
				t.Fatal("invocation failed to join after private append release")
			}
			require.NoError(t, completed.err)
			require.Equal(t, metadata, completed.outcome.Metadata)
			require.Equal(t, InvocationSucceeded, completed.outcome.State)
			require.False(t, completed.outcome.Output.FunctionError)
			require.JSONEq(t, `{"unchanged":true}`, string(completed.outcome.Output.Payload))
			require.NoError(t, completed.outcome.OwnershipErr)
			if closeResult != nil {
				select {
				case err := <-closeResult:
					require.NoError(t, err)
				case <-time.After(time.Second):
					t.Fatal("Close did not join released native evidence")
				}
			}
			require.NoError(t, service.Close(context.Background()))
			stacks, terminals := writer.records(t)
			require.Len(t, stacks, 1)
			require.Equal(t, "captured", stacks[0].Status)
			require.Equal(t, metadata.RequestID, stacks[0].RequestID)
			require.Len(t, terminals, 1)
			terminal := terminals[0]
			require.Equal(t, metadata.RequestID, terminal.RequestID)
			require.Equal(t, metadata.Attempt, terminal.Attempt)
			require.Equal(t, InvocationSucceeded, terminal.State)
			require.False(t, terminal.FunctionError)
			require.Empty(t, terminal.TerminationCause)
			require.Empty(t, terminal.ContextError)
			require.False(t, terminal.NativeResponseSynthesized)
			require.Nil(t, terminal.FunctionDiagnostic)
			require.True(t, terminal.OwnershipConfirmed)
			if stop == "function-budget" {
				require.Greater(t, terminal.ElapsedMS, float64(budget/time.Millisecond),
					"terminal elapsed time includes the joined private collector without changing native cause")
			}
		})
	}
}
