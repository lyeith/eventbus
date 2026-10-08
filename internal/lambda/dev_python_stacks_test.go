//go:build linux || darwin

package lambda

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// This stdlib fixture is an unchanged synchronous Lambda handler. Its ASGI
// application suspends on an actual body queue; socket modes perform actual
// blocking I/O against a listener owned by the test, without SDK/business mocks.
const pythonStackHandlerFixture = `
import asyncio
import json
import os
import pathlib
import socket


def ready(context):
    pathlib.Path(os.environ['PYTHON_STACK_READY']).write_text(json.dumps({'request_id': context.aws_request_id, 'pid': os.getpid()}))


async def app(scope, receive, send):
    secret_local = scope['headers']
    source_line_secret = 'SOURCE_LINE_SECRET_26'
    await receive()
    await send({'type': 'http.response.body', 'body': b''})


async def asgi_wait(event, context):
    body_queue = asyncio.Queue()
    async def receive():
        ready(context)
        return await body_queue.get()
    async def send(message):
        pass
    scope = {'type': 'http', 'headers': [(b'authorization', event.get('signed_header', '').encode())]}
    await app(scope, receive, send)


async def deep_wait(depth):
    if depth:
        return await deep_wait(depth - 1)
    await asyncio.Event().wait()


async def crowded_wait(event, context):
    tasks = [asyncio.create_task(deep_wait(80), name='TASK_NAME_SECRET_26-' + str(index)) for index in range(140)]
    await asyncio.sleep(0)
    ready(context)
    await asyncio.gather(*tasks)


def sync_socket_wait(event, context):
    host, port = event['address'].rsplit(':', 1)
    with socket.create_connection((host, int(port))) as connection:
        ready(context)
        connection.recv(1)


async def blocked_loop_wait(event, context):
    sync_socket_wait(event, context)


def handler(event, context):
    if event['mode'] == 'fast':
        return {'request_id': context.aws_request_id, 'echo': event}
    if event['mode'] == 'socket':
        sync_socket_wait(event, context)
    elif event['mode'] == 'blocked_loop':
        asyncio.run(blocked_loop_wait(event, context))
    elif event['mode'] == 'crowded':
        asyncio.run(crowded_wait(event, context))
    else:
        asyncio.run(asgi_wait(event, context))
    return {'completed': True}
`

func newPythonStackTestService(t *testing.T, stacks *DevPythonStacksConfig, timeout time.Duration) (*Service, string, string) {
	t.Helper()
	directory := t.TempDir()
	writeFixture(t, directory, "stack_handler.py", pythonStackHandlerFixture)
	command, environment := pythonCommand(t)
	marker := filepath.Join(directory, "handler-ready")
	environment["PYTHON_STACK_READY"] = marker
	path := filepath.Join(directory, "private", "python.jsonl")
	service, err := NewService(&Config{
		Functions:      map[string]Function{"stackwait": {Runtime: "python", Command: command, Environment: environment, Handler: "stack_handler.handler", Timeout: timeout}},
		DevAsync:       &DevAsyncConfig{Workers: 1, LogWriter: io.Discard, RetryDelays: []time.Duration{0, 0}},
		DevDiagnostics: &DevDiagnosticsConfig{LogPath: path, PythonStacks: stacks},
	}, directory)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, service.Close(context.Background())) })
	return service, path, marker
}

type pythonStackExecution struct {
	outcome InvocationOutcome
	err     error
}

func startPythonStackExecution(t *testing.T, service *Service, event map[string]any, onAdmission func(InvocationMetadata)) (InvocationMetadata, context.CancelFunc, <-chan pythonStackExecution) {
	t.Helper()
	return startPythonStackExecutionContext(t, context.Background(), service, event, onAdmission)
}

func startPythonStackExecutionContext(t *testing.T, parent context.Context, service *Service, event map[string]any, onAdmission func(InvocationMetadata)) (InvocationMetadata, context.CancelFunc, <-chan pythonStackExecution) {
	t.Helper()
	payload, err := json.Marshal(event)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(parent)
	t.Cleanup(cancel)
	admitted := make(chan InvocationMetadata, 1)
	finished := make(chan pythonStackExecution, 1)
	go func() {
		outcome, err := service.ExecuteObserved(ctx, InvokeInput{FunctionName: "stackwait", Payload: payload}, func(metadata InvocationMetadata) error {
			if onAdmission != nil {
				onAdmission(metadata)
			}
			admitted <- metadata
			return nil
		})
		finished <- pythonStackExecution{outcome: outcome, err: err}
	}()
	select {
	case metadata := <-admitted:
		return metadata, cancel, finished
	case result := <-finished:
		t.Fatalf("registered Python invocation never admitted: %#v %v", result.outcome, result.err)
	case <-time.After(5 * time.Second):
		t.Fatal("registered Python invocation did not admit")
	}
	return InvocationMetadata{}, cancel, finished
}

// Read only completed JSONL records while the actual invocation is still live.
// A concurrent file write's unfinished tail cannot be mistaken for a row.
func readPythonStackEvidence(t *testing.T, path string) ([]pythonStackRecord, []invocationDiagnosticRecord) {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	lines := bytes.Split(data, []byte{'\n'})
	var snapshots []pythonStackRecord
	var terminals []invocationDiagnosticRecord
	for _, line := range lines[:len(lines)-1] {
		if len(line) == 0 {
			continue
		}
		var header struct {
			SchemaVersion string `json:"schema_version"`
		}
		require.NoError(t, json.Unmarshal(line, &header))
		switch header.SchemaVersion {
		case "eventbus.lambda.python-stack.v1":
			var snapshot pythonStackRecord
			require.NoError(t, json.Unmarshal(line, &snapshot))
			snapshots = append(snapshots, snapshot)
		case "eventbus.lambda.invocation-diagnostic.v1":
			var terminal invocationDiagnosticRecord
			require.NoError(t, json.Unmarshal(line, &terminal))
			terminals = append(terminals, terminal)
		default:
			t.Fatalf("unexpected private schema %q", header.SchemaVersion)
		}
	}
	return snapshots, terminals
}

func waitPythonStackRecord(t *testing.T, path string, metadata InvocationMetadata) pythonStackRecord {
	t.Helper()
	var record pythonStackRecord
	require.Eventually(t, func() bool {
		snapshots, _ := readPythonStackEvidence(t, path)
		for _, snapshot := range snapshots {
			if snapshot.RequestID == metadata.RequestID && snapshot.Attempt == metadata.Attempt {
				record = snapshot
				return true
			}
		}
		return false
	}, 5*time.Second, 5*time.Millisecond, "live correlated Python snapshot was not retained")
	return record
}

func finishPythonStackExecution(t *testing.T, finished <-chan pythonStackExecution) pythonStackExecution {
	t.Helper()
	select {
	case result := <-finished:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("Python invocation did not join child/collector cleanup")
	}
	return pythonStackExecution{}
}

func pythonStackFrameFunctions(record pythonStackRecord) []string {
	var functions []string
	for _, thread := range record.Threads {
		for _, frame := range thread.Frames {
			functions = append(functions, frame.Function)
		}
	}
	for _, loop := range record.Loops {
		for _, task := range loop.Tasks {
			for _, frame := range task.Frames {
				functions = append(functions, frame.Function)
			}
		}
	}
	return functions
}

func assertPythonStackSnapshot(t *testing.T, record pythonStackRecord, metadata InvocationMetadata) {
	t.Helper()
	require.Equal(t, metadata.RequestID, record.RequestID)
	require.Equal(t, metadata.Attempt, record.Attempt)
	require.Equal(t, "captured", record.Status, "capture: %#v", record)
	require.False(t, record.RequestedAt.IsZero())
	require.False(t, record.CapturedAt.IsZero())
	require.False(t, record.CapturedAt.Before(record.RequestedAt))
	require.NotEmpty(t, record.Threads)
	encoded, err := json.Marshal(record)
	require.NoError(t, err)
	for _, secret := range []string{"SIGNED_HEADER_SECRET_26", "BODY_SECRET_26", "SOURCE_LINE_SECRET_26", "TASK_NAME_SECRET_26", "CREDENTIAL_SECRET_26"} {
		require.NotContains(t, string(encoded), secret, "snapshot must exclude locals, source lines, names and event values")
	}
}

func assertPythonStackTerminal(t *testing.T, path string, metadata InvocationMetadata, expected InvocationState, snapshot pythonStackRecord) {
	t.Helper()
	snapshots, terminals := readPythonStackEvidence(t, path)
	require.Len(t, snapshots, 1)
	require.Len(t, terminals, 1)
	terminal := terminals[0]
	require.Equal(t, metadata.RequestID, terminal.RequestID)
	require.Equal(t, metadata.Attempt, terminal.Attempt)
	require.Equal(t, expected, terminal.State)
	require.True(t, terminal.OwnershipConfirmed)
	require.NotNil(t, terminal.PythonStack)
	require.Equal(t, snapshot.Status, terminal.PythonStack.Status)
	require.Equal(t, snapshot.Trigger, terminal.PythonStack.Trigger)
	require.Equal(t, snapshot.Reason, terminal.PythonStack.Reason)
	require.False(t, terminal.CompletedAt.Before(snapshot.CapturedAt))
}

func pythonStackProcessPID(t *testing.T, marker string, metadata InvocationMetadata) int {
	t.Helper()
	data, err := os.ReadFile(marker)
	require.NoError(t, err)
	var process struct {
		RequestID string `json:"request_id"`
		PID       int    `json:"pid"`
	}
	require.NoError(t, json.Unmarshal(data, &process))
	require.Equal(t, metadata.RequestID, process.RequestID)
	require.Greater(t, process.PID, 0)
	return process.PID
}

func TestPythonStackExplicitASGIReceiveCapturesSafeAwaitChainBeforeCancellation(t *testing.T) {
	service, path, marker := newPythonStackTestService(t, &DevPythonStacksConfig{}, 5*time.Second)
	metadata, cancel, finished := startPythonStackExecution(t, service, map[string]any{"mode": "asgi", "signed_header": "SIGNED_HEADER_SECRET_26", "body": "BODY_SECRET_26", "credential": "CREDENTIAL_SECRET_26"}, nil)
	waitDiagnosticMarker(t, marker)
	pid := pythonStackProcessPID(t, marker, metadata)
	require.True(t, processAlive(pid))
	require.True(t, service.RequestPythonSnapshot(metadata))
	record := waitPythonStackRecord(t, path, metadata)
	assertPythonStackSnapshot(t, record, metadata)
	require.Equal(t, "explicit", record.Trigger)
	require.Contains(t, pythonStackFrameFunctions(record), "app")
	require.Contains(t, pythonStackFrameFunctions(record), "receive", "native await-chain traversal must expose the body wait")
	_, terminals := readPythonStackEvidence(t, path)
	require.Empty(t, terminals, "live snapshot cannot claim invocation completion")
	select {
	case result := <-finished:
		t.Fatalf("suspended ASGI handler completed before explicit cancel: %#v %v", result.outcome, result.err)
	default:
	}
	cancel()
	result := finishPythonStackExecution(t, finished)
	require.ErrorIs(t, result.err, context.Canceled)
	require.Equal(t, InvocationCanceled, result.outcome.State)
	require.Nil(t, result.outcome.OwnershipErr)
	require.False(t, processAlive(pid), "cancellation must join the actual captured Python child")
	assertPythonStackTerminal(t, path, metadata, InvocationCanceled, record)
	require.False(t, service.RequestPythonSnapshot(metadata), "completed identity cannot admit another snapshot")
}

func TestPythonStackExplicitAdmissionIsAvailableBeforeChildAndOneShot(t *testing.T) {
	service, path, _ := newPythonStackTestService(t, &DevPythonStacksConfig{}, 5*time.Second)
	var wrongAccepted, firstAccepted, repeatAccepted bool
	metadata, cancel, finished := startPythonStackExecution(t, service, map[string]any{"mode": "asgi"}, func(metadata InvocationMetadata) {
		wrong := metadata
		wrong.Attempt++
		wrongAccepted = service.RequestPythonSnapshot(wrong)
		firstAccepted = service.RequestPythonSnapshot(metadata)
		repeatAccepted = service.RequestPythonSnapshot(metadata)
	})
	require.False(t, wrongAccepted)
	require.True(t, firstAccepted, "actual admitted identity must be requestable before child launch")
	require.False(t, repeatAccepted)
	record := waitPythonStackRecord(t, path, metadata)
	require.Equal(t, "explicit", record.Trigger)
	if record.Status == "captured" {
		assertPythonStackSnapshot(t, record, metadata)
	} else {
		// The SSD's managed UV launcher can spend longer than the bounded
		// collection window before Python starts. Early acceptance is still
		// correlated evidence; it cannot promise a useful runtime snapshot.
		require.Equal(t, "capture_unavailable", record.Status, "capture: %#v", record)
		require.Equal(t, "collection_timeout", record.Reason, "capture: %#v", record)
		require.Equal(t, metadata.RequestID, record.RequestID)
		require.Equal(t, metadata.Attempt, record.Attempt)
	}
	cancel()
	result := finishPythonStackExecution(t, finished)
	require.ErrorIs(t, result.err, context.Canceled)
	require.Nil(t, result.outcome.OwnershipErr)
	assertPythonStackTerminal(t, path, metadata, InvocationCanceled, record)
}

func TestPythonStackConcurrentRequestsSelectOnlyOriginalAttempt(t *testing.T) {
	service, path, marker := newPythonStackTestService(t, &DevPythonStacksConfig{}, 5*time.Second)
	metadata, cancel, finished := startPythonStackExecution(t, service, map[string]any{"mode": "asgi"}, nil)
	waitDiagnosticMarker(t, marker)
	wrong := metadata
	wrong.Attempt++
	require.False(t, service.RequestPythonSnapshot(wrong))
	var accepted atomic.Int32
	var requests sync.WaitGroup
	for range 32 {
		requests.Add(1)
		go func() {
			defer requests.Done()
			if service.RequestPythonSnapshot(metadata) {
				accepted.Add(1)
			}
		}()
	}
	requests.Wait()
	require.Equal(t, int32(1), accepted.Load())
	record := waitPythonStackRecord(t, path, metadata)
	assertPythonStackSnapshot(t, record, metadata)
	cancel()
	require.ErrorIs(t, finishPythonStackExecution(t, finished).err, context.Canceled)
	assertPythonStackTerminal(t, path, metadata, InvocationCanceled, record)
}

func TestPythonStackScheduledAndConfiguredDeadlinePreserveHardStop(t *testing.T) {
	for _, deadlineTrigger := range []bool{false, true} {
		name, budget := "scheduled_after", 5*time.Second
		options := &DevPythonStacksConfig{SnapshotAfter: 2 * time.Second}
		if deadlineTrigger {
			name, budget = "configured_deadline", 2*time.Second
			options = &DevPythonStacksConfig{DeadlineLead: 300 * time.Millisecond}
		}
		t.Run(name, func(t *testing.T) {
			service, path, _ := newPythonStackTestService(t, options, budget)
			metadata, cancel, finished := startPythonStackExecution(t, service, map[string]any{"mode": "asgi"}, nil)
			record := waitPythonStackRecord(t, path, metadata)
			assertPythonStackSnapshot(t, record, metadata)
			if deadlineTrigger {
				require.Equal(t, "deadline", record.Trigger)
				result := finishPythonStackExecution(t, finished)
				require.NoError(t, result.err, "configured Lambda timeout retains its native function-error boundary")
				require.Equal(t, InvocationTimedOut, result.outcome.State)
				require.True(t, result.outcome.Output.FunctionError)
				require.Contains(t, string(result.outcome.Output.Payload), "Sandbox.Timedout")
				require.Nil(t, result.outcome.OwnershipErr)
				assertPythonStackTerminal(t, path, metadata, InvocationTimedOut, record)
				_, terminals := readPythonStackEvidence(t, path)
				require.Len(t, terminals, 1)
				require.Len(t, terminals[0].ExecutionPhases, 1)
				phase := terminals[0].ExecutionPhases[0]
				require.Equal(t, 1, phase.InitAttempt)
				require.Equal(t, "initial", phase.Mode)
				require.Equal(t, "succeeded", phase.InitState)
				require.Equal(t, "timed_out", phase.InvokeState)
				require.GreaterOrEqual(t, phase.InvokeMS, float64(budget.Milliseconds())-100, "Init must not spend the configured handler budget")
				require.Less(t, phase.InvokeMS, float64((budget + time.Second).Milliseconds()), "snapshot collection cannot extend the handler deadline")
			} else {
				require.Equal(t, "snapshot_after", record.Trigger)
				_, terminals := readPythonStackEvidence(t, path)
				require.Empty(t, terminals)
				cancel()
				require.ErrorIs(t, finishPythonStackExecution(t, finished).err, context.Canceled)
				assertPythonStackTerminal(t, path, metadata, InvocationCanceled, record)
			}
		})
	}
}

func TestPythonStackBlockingSocketExposesThreadAndUnavailableLoop(t *testing.T) {
	for _, mode := range []string{"socket", "blocked_loop"} {
		t.Run(mode, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			t.Cleanup(func() { _ = listener.Close() })
			// A connected TCP peer which never sends bytes is the real blocking
			// dependency. Closing it is joined after the Lambda process is stopped.
			accepted := make(chan net.Conn, 1)
			acceptDone := make(chan struct{})
			go func() {
				defer close(acceptDone)
				connection, err := listener.Accept()
				if err == nil {
					accepted <- connection
				}
			}()
			t.Cleanup(func() {
				_ = listener.Close()
				<-acceptDone
				select {
				case connection := <-accepted:
					_ = connection.Close()
				default:
				}
			})
			service, path, marker := newPythonStackTestService(t, &DevPythonStacksConfig{}, 5*time.Second)
			metadata, cancel, finished := startPythonStackExecution(t, service, map[string]any{"mode": mode, "address": listener.Addr().String()}, nil)
			waitDiagnosticMarker(t, marker)
			pid := pythonStackProcessPID(t, marker, metadata)
			require.True(t, processAlive(pid))
			require.True(t, service.RequestPythonSnapshot(metadata))
			record := waitPythonStackRecord(t, path, metadata)
			assertPythonStackSnapshot(t, record, metadata)
			require.Contains(t, pythonStackFrameFunctions(record), "sync_socket_wait")
			if mode == "blocked_loop" {
				require.NotEmpty(t, record.Loops)
				foundUnavailable := false
				for _, loop := range record.Loops {
					if loop.State == "unavailable" && loop.Reason == "callback_timeout" {
						foundUnavailable = true
					}
				}
				require.True(t, foundUnavailable, "blocked loop must not be represented as an empty healthy task list")
			}
			cancel()
			result := finishPythonStackExecution(t, finished)
			require.ErrorIs(t, result.err, context.Canceled)
			require.Nil(t, result.outcome.OwnershipErr)
			require.False(t, processAlive(pid), "blocked socket/loop child must join before returning")
			assertPythonStackTerminal(t, path, metadata, InvocationCanceled, record)
		})
	}
}

func TestPythonStackDeepAwaitAndTaskBoundsReportTruncation(t *testing.T) {
	service, path, marker := newPythonStackTestService(t, &DevPythonStacksConfig{}, 5*time.Second)
	metadata, cancel, finished := startPythonStackExecution(t, service, map[string]any{"mode": "crowded"}, nil)
	waitDiagnosticMarker(t, marker)
	require.True(t, service.RequestPythonSnapshot(metadata))
	record := waitPythonStackRecord(t, path, metadata)
	assertPythonStackSnapshot(t, record, metadata)
	require.True(t, record.Truncated)
	require.LessOrEqual(t, len(record.Threads), 32)
	require.LessOrEqual(t, len(record.Loops), 8)
	totalTasks, truncatedFrames := 0, false
	checkFrames := func(frames []pythonStackFrame, truncated bool) {
		require.LessOrEqual(t, len(frames), 32)
		truncatedFrames = truncatedFrames || truncated
		for _, frame := range frames {
			require.LessOrEqual(t, len(frame.Function), 256)
			require.LessOrEqual(t, len(frame.File), 256)
			require.Greater(t, frame.Line, 0)
		}
	}
	for _, thread := range record.Threads {
		checkFrames(thread.Frames, thread.Truncated)
	}
	for _, loop := range record.Loops {
		totalTasks += len(loop.Tasks)
		for _, task := range loop.Tasks {
			checkFrames(task.Frames, task.Truncated)
		}
	}
	require.Greater(t, totalTasks, 0)
	require.LessOrEqual(t, totalTasks, 64)
	require.True(t, truncatedFrames, "bounded native coroutine await chains must signal their discarded tail")
	cancel()
	require.ErrorIs(t, finishPythonStackExecution(t, finished).err, context.Canceled)
	assertPythonStackTerminal(t, path, metadata, InvocationCanceled, record)
}

func TestPythonStackDefaultOffAndFastSuccessPreserveNativePayload(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		name := "default_off"
		var stacks *DevPythonStacksConfig
		if enabled {
			name = "scheduled_after_completion"
			stacks = &DevPythonStacksConfig{SnapshotAfter: time.Minute}
		}
		t.Run(name, func(t *testing.T) {
			service, path, _ := newPythonStackTestService(t, stacks, 3*time.Second)
			if enabled {
				// Native execution must retain the startup copy, not a mutable
				// caller-owned diagnostics recipe.
				stacks.SnapshotAfter = time.Nanosecond
				stacks.DeadlineLead = 5 * time.Second
			}
			event := map[string]any{"mode": "fast", "value": 42, "signed_header": "SIGNED_HEADER_SECRET_26"}
			payload, err := json.Marshal(event)
			require.NoError(t, err)
			var metadata InvocationMetadata
			outcome, err := service.ExecuteObserved(context.Background(), InvokeInput{FunctionName: "stackwait", Payload: payload}, func(actual InvocationMetadata) error {
				metadata = actual
				if !enabled {
					require.False(t, service.RequestPythonSnapshot(actual), "default-off invocation must not allocate a snapshot session")
				}
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, InvocationSucceeded, outcome.State)
			require.False(t, outcome.Output.FunctionError)
			require.Nil(t, outcome.OwnershipErr)
			var result struct {
				RequestID string         `json:"request_id"`
				Echo      map[string]any `json:"echo"`
			}
			require.NoError(t, json.Unmarshal(outcome.Output.Payload, &result))
			require.Equal(t, metadata.RequestID, result.RequestID)
			require.Equal(t, "SIGNED_HEADER_SECRET_26", result.Echo["signed_header"], "private stack configuration cannot redact native payloads")
			require.Equal(t, float64(42), result.Echo["value"])
			require.False(t, service.RequestPythonSnapshot(metadata))
			snapshots, terminals := readPythonStackEvidence(t, path)
			if enabled {
				require.Len(t, snapshots, 1)
				require.Equal(t, "capture_unavailable", snapshots[0].Status)
				require.Equal(t, "process_completed", snapshots[0].Reason)
				require.Equal(t, "not_requested", snapshots[0].Trigger)
				require.True(t, snapshots[0].RequestedAt.IsZero())
			} else {
				require.Empty(t, snapshots)
			}
			require.Len(t, terminals, 1)
			require.Equal(t, InvocationSucceeded, terminals[0].State)
			if !enabled {
				require.Nil(t, terminals[0].PythonStack)
			} else {
				require.NotNil(t, terminals[0].PythonStack)
				require.Equal(t, "capture_unavailable", terminals[0].PythonStack.Status)
				require.Equal(t, "not_requested", terminals[0].PythonStack.Trigger)
			}
			require.NoError(t, service.Close(context.Background()))
			joinedSnapshots, _ := readPythonStackEvidence(t, path)
			require.Equal(t, snapshots, joinedSnapshots, "joined shutdown must stop the optional timer before capture closure")
		})
	}
}

func TestPythonStackEarlyScheduleRetainsExplicitAvailability(t *testing.T) {
	service, path, marker := newPythonStackTestService(t, &DevPythonStacksConfig{SnapshotAfter: 50 * time.Millisecond}, 5*time.Second)
	metadata, cancel, finished := startPythonStackExecution(t, service, map[string]any{"mode": "asgi"}, nil)
	record := waitPythonStackRecord(t, path, metadata)
	require.Equal(t, "snapshot_after", record.Trigger)
	if record.Status == "captured" {
		assertPythonStackSnapshot(t, record, metadata)
	} else {
		require.Equal(t, "capture_unavailable", record.Status, "capture: %#v", record)
		require.Equal(t, "collection_timeout", record.Reason, "capture: %#v", record)
		require.Equal(t, metadata.RequestID, record.RequestID)
		require.Equal(t, metadata.Attempt, record.Attempt)
	}
	waitDiagnosticMarker(t, marker)
	pid := pythonStackProcessPID(t, marker, metadata)
	require.True(t, processAlive(pid), "early diagnostic unavailability cannot stop the actual handler")
	_, terminals := readPythonStackEvidence(t, path)
	require.Empty(t, terminals)
	cancel()
	result := finishPythonStackExecution(t, finished)
	require.ErrorIs(t, result.err, context.Canceled)
	require.Nil(t, result.outcome.OwnershipErr)
	require.False(t, processAlive(pid))
	assertPythonStackTerminal(t, path, metadata, InvocationCanceled, record)
}

func TestPythonStackNativeAsyncTimeoutRetryRetainsOriginalAttemptIdentity(t *testing.T) {
	service, path, _ := newPythonStackTestService(t, &DevPythonStacksConfig{DeadlineLead: 300 * time.Millisecond}, 2*time.Second)
	admission, err := service.Admit(context.Background(), InvokeInput{FunctionName: "stackwait", Payload: []byte(`{"mode":"asgi"}`)})
	require.NoError(t, err)
	require.NotEmpty(t, admission.RequestID)
	require.Eventually(t, func() bool {
		records := service.AsyncSnapshot()
		return len(records) == 1 && records[0].State == "failed"
	}, 15*time.Second, 5*time.Millisecond, "native async timeout retries did not join")
	async := service.AsyncSnapshot()
	require.Len(t, async, 1)
	require.Equal(t, admission.RequestID, async[0].RequestID)
	require.Equal(t, 3, async[0].Attempts)
	require.Equal(t, "Sandbox.Timedout", async[0].ErrorType)
	require.NoError(t, service.Close(context.Background()))
	snapshots, terminals := readPythonStackEvidence(t, path)
	require.Len(t, snapshots, 3)
	require.Len(t, terminals, 3)
	for index := range snapshots {
		metadata := InvocationMetadata{RequestID: admission.RequestID, FunctionName: "stackwait", FunctionARN: snapshots[index].FunctionARN, Attempt: index + 1}
		assertPythonStackSnapshot(t, snapshots[index], metadata)
		require.Equal(t, "deadline", snapshots[index].Trigger)
		require.Contains(t, pythonStackFrameFunctions(snapshots[index]), "receive", "each original native retry must reach the real ASGI wait")
		require.Equal(t, admission.RequestID, terminals[index].RequestID)
		require.Equal(t, index+1, terminals[index].Attempt)
		require.Equal(t, InvocationTimedOut, terminals[index].State)
		require.Equal(t, "Event", terminals[index].InvocationType)
		require.Equal(t, "function_timeout", terminals[index].TerminationCause)
		require.True(t, terminals[index].FunctionError)
		require.True(t, terminals[index].OwnershipConfirmed)
		require.NotNil(t, terminals[index].PythonStack)
		require.Equal(t, "captured", terminals[index].PythonStack.Status)
		require.False(t, terminals[index].CompletedAt.Before(snapshots[index].CapturedAt))
		require.Len(t, terminals[index].ExecutionPhases, 1)
		phase := terminals[index].ExecutionPhases[0]
		require.Equal(t, 1, phase.InitAttempt)
		require.Equal(t, "initial", phase.Mode)
		require.Equal(t, "succeeded", phase.InitState)
		require.Equal(t, "timed_out", phase.InvokeState)
		require.GreaterOrEqual(t, phase.InvokeMS, float64(1900), "each original retry receives its configured 2s handler budget")
		require.Less(t, phase.InvokeMS, float64(3000), "snapshot collection cannot extend a retry handler deadline")
	}
}

func TestPythonStackInheritedCallerDeadlineSchedulesBeforeActualEarlierStop(t *testing.T) {
	service, path, marker := newPythonStackTestService(t, &DevPythonStacksConfig{DeadlineLead: 300 * time.Millisecond}, 6*time.Second)
	caller, cancelCaller := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancelCaller)
	metadata, _, finished := startPythonStackExecutionContext(t, caller, service, map[string]any{"mode": "asgi"}, nil)
	waitDiagnosticMarker(t, marker)
	pid := pythonStackProcessPID(t, marker, metadata)
	record := waitPythonStackRecord(t, path, metadata)
	assertPythonStackSnapshot(t, record, metadata)
	require.Equal(t, "deadline", record.Trigger)
	require.Contains(t, pythonStackFrameFunctions(record), "receive")
	result := finishPythonStackExecution(t, finished)
	require.ErrorIs(t, result.err, context.DeadlineExceeded)
	require.Equal(t, InvocationTimedOut, result.outcome.State)
	require.Nil(t, result.outcome.OwnershipErr)
	require.False(t, processAlive(pid), "earlier caller deadline cannot leave collector or Python work behind")
	assertPythonStackTerminal(t, path, metadata, InvocationTimedOut, record)
	_, terminals := readPythonStackEvidence(t, path)
	require.Equal(t, "caller_deadline", terminals[0].TerminationCause)
	require.Equal(t, int64(6000), terminals[0].ConfiguredTimeoutMS)
	require.Less(t, terminals[0].ElapsedMS, float64(6000), "diagnostic stop cannot claim the configured budget elapsed")
}
