//go:build linux || darwin

package lambda

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/devcapture"
	"github.com/stretchr/testify/require"
)

const warmBlockedInitSource = `
import json
import os
from pathlib import Path
import time

directory = Path(os.environ["PHASE_DIRECTORY"])
marker = directory / "launch-1.json"
temporary = directory / "launch-1.json.tmp"
with temporary.open("w") as output:
    json.dump({"pid": os.getpid(), "request_id": os.environ["EVENTBUS_LAMBDA_REQUEST_ID"]}, output)
temporary.replace(marker)
print("blocked-init-pid=%d" % os.getpid(), flush=True)
while True:
    time.sleep(0.005)

def handler(event, context):
    raise RuntimeError("timed-out module must never reach its handler")
`

func TestWarmInitFallbackReservationFailurePreservesActualLaunch(t *testing.T) {
	for _, scenario := range []string{"concurrency", "caller-cancellation", "async-attempt", "ownership-failure"} {
		t.Run(scenario, func(t *testing.T) {
			directory := t.TempDir()
			initial := managedPhaseFunction(t, "python", directory, "init_cancel", true)
			initial.Timeout = 2 * time.Second
			writeFixture(t, directory, "phase.py", warmBlockedInitSource)
			holderStarted, holderGate := filepath.Join(directory, "holder-started"), filepath.Join(directory, "release-holder")
			holder := asyncCommand(t, filepath.Join(directory, "holder-results.jsonl"), holderGate, holderStarted, "success")
			service, err := NewService(&Config{
				Functions: map[string]Function{"initial": initial, "holder": holder},
				DevWarm:   &DevWarmConfig{MaxWorkers: 1, MaxConcurrency: 1},
				DevAsync:  &DevAsyncConfig{Workers: 1, LogWriter: io.Discard, RetryDelays: []time.Duration{time.Hour, time.Hour}},
				DevDiagnostics: &DevDiagnosticsConfig{
					LogPath:      filepath.Join(directory, "private.jsonl"),
					PythonStacks: &DevPythonStacksConfig{DeadlineLead: 200 * time.Millisecond},
				},
			}, directory)
			require.NoError(t, err)
			service.initTimeout = time.Second
			// Reuse the existing private writer fixture. Its actual Python stack
			// append holds finish() between initial process retirement and retry
			// acquisition, without adding a production hook or changing capacity.
			writer := &pythonStackPolicyWriter{entered: make(chan struct{}), release: make(chan struct{})}
			require.NoError(t, service.diagnosticCapture.Close())
			service.diagnosticCapture = devcapture.NewWriter(writer, "held initial Python stack")
			var openOnce sync.Once
			openWriter := func() { openOnce.Do(func() { close(writer.release) }) }
			parent, cancelParent := context.WithCancel(context.Background())
			var holderCancel context.CancelFunc = func() {}
			var holderFinished <-chan pythonStackExecution
			var ownershipFault error
			t.Cleanup(func() {
				openWriter()
				cancelParent()
				holderCancel()
				closeContext, cancelClose := context.WithCancel(context.Background())
				cancelClose() // Abort the deliberately delayed accepted async retry.
				closeErr := service.Close(closeContext)
				if closeErr != nil && !errors.Is(closeErr, context.Canceled) && !errors.Is(closeErr, ownershipFault) {
					t.Error(closeErr)
				}
				if holderFinished != nil {
					select {
					case <-holderFinished:
					case <-time.After(5 * time.Second):
						t.Error("owned competing process did not join")
					}
				}
				if ownershipFault == nil {
					require.NoError(t, service.DevEvidence())
				} else {
					require.ErrorIs(t, service.DevEvidence(), ownershipFault)
				}
			})
			var admission Admission
			var originalFinished <-chan pythonStackExecution
			if scenario == "async-attempt" {
				admission, err = service.Admit(parent, InvokeInput{FunctionName: "initial", Payload: []byte(`{"native":true}`)})
				require.NoError(t, err)
			} else {
				finished := make(chan pythonStackExecution, 1)
				originalFinished = finished
				go func() {
					outcome, err := service.ExecuteObserved(parent, InvokeInput{FunctionName: "initial", Payload: []byte(`{"native":true}`)}, nil)
					finished <- pythonStackExecution{outcome, err}
				}()
			}
			warmWaitFile(t, filepath.Join(directory, "launch-1.json"))
			var launch struct {
				PID       int    `json:"pid"`
				RequestID string `json:"request_id"`
			}
			data, err := os.ReadFile(filepath.Join(directory, "launch-1.json"))
			require.NoError(t, err)
			require.NoError(t, json.Unmarshal(data, &launch))
			require.Positive(t, launch.PID)
			require.NotEmpty(t, launch.RequestID)
			select {
			case <-writer.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("initial native stack did not reach its private writer")
			}
			warmWaitRetired(t, service)
			require.False(t, processAlive(launch.PID), "initial process must actually join before its quota becomes available")
			holderContext, stopHolder := context.WithCancel(context.Background())
			holderCancel = stopHolder
			holderDone := make(chan pythonStackExecution, 1)
			holderFinished = holderDone
			go func() {
				outcome, err := service.ExecuteObserved(holderContext, InvokeInput{FunctionName: "holder", Payload: []byte(`{"id":"holder"}`)}, nil)
				holderDone <- pythonStackExecution{outcome, err}
			}()
			warmWaitFile(t, holderStarted)
			warmAssertCounts(t, service.warm, 1, 0)
			if scenario == "caller-cancellation" {
				cancelParent()
			} else if scenario == "ownership-failure" {
				ownershipFault = errors.New("controlled concurrent warm-owner uncertainty")
				service.warm.mu.Lock()
				service.warm.closeErr = ownershipFault
				service.warm.mu.Unlock()
			}
			openWriter()
			wantState, wantCause := InvocationTimedOut, "initialization_timeout"
			if scenario == "caller-cancellation" {
				wantState, wantCause = InvocationCanceled, "caller_canceled"
			}
			if scenario == "async-attempt" {
				record := waitAsyncState(t, service, admission.RequestID, "retrying")
				require.Equal(t, 1, record.Attempts, "actual failed Init cannot become an uncharged nonlaunch throttle")
				require.Equal(t, "Sandbox.Timedout", record.ErrorType)
				require.Equal(t, launch.RequestID, admission.RequestID)
			} else {
				select {
				case completed := <-originalFinished:
					if scenario == "caller-cancellation" {
						require.ErrorIs(t, completed.err, context.Canceled)
					} else {
						require.NoError(t, completed.err, "failed real Init remains a function result rather than a prelaunch 429")
					}
					require.Equal(t, wantState, completed.outcome.State)
					require.True(t, completed.outcome.Output.FunctionError)
					require.Contains(t, string(completed.outcome.Output.Payload), "Sandbox.Timedout")
					require.Equal(t, CompletionProcess, completed.outcome.CompletionScope)
					require.Equal(t, launch.RequestID, completed.outcome.Metadata.RequestID)
					if ownershipFault == nil {
						require.NoError(t, completed.outcome.OwnershipErr)
					} else {
						require.ErrorIs(t, completed.outcome.OwnershipErr, ownershipFault)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("fallback reservation decision did not return joined initial evidence")
				}
			}
			stacks, terminals := writer.records(t)
			require.Len(t, stacks, 1)
			require.Equal(t, launch.RequestID, stacks[0].RequestID)
			require.Len(t, terminals, 1)
			record := terminals[0]
			require.Equal(t, launch.RequestID, record.RequestID)
			require.Equal(t, "initial", record.FunctionName)
			require.Equal(t, 1, record.Attempt)
			require.Equal(t, wantState, record.State)
			require.True(t, record.FunctionError)
			require.Equal(t, ownershipFault == nil, record.OwnershipConfirmed)
			require.Equal(t, wantCause, record.TerminationCause)
			require.Contains(t, record.Tail.Data, fmt.Sprintf("blocked-init-pid=%d", launch.PID))
			require.NotEmpty(t, record.ProcessError)
			require.Len(t, record.ExecutionPhases, 1)
			phase := record.ExecutionPhases[0]
			require.Equal(t, 1, phase.InitAttempt)
			require.Equal(t, "initial", phase.Mode)
			require.Equal(t, "timed_out", phase.InitState)
			require.Empty(t, phase.InvokeState)
			require.Equal(t, "initialization_timeout", phase.TerminationCause)
			require.Equal(t, record.ProcessError, phase.ProcessError)
			require.True(t, phase.OwnershipConfirmed)
			require.Equal(t, CompletionProcess, phase.CompletionScope)
			require.NotContains(t, strings.ToLower(record.Tail.Data), "timed-out module must never reach")
			warmAssertCounts(t, service.warm, 1, 0)
		})
	}
}
