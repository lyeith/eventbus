//go:build linux || darwin

package lambda

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const managedPhaseFaultEnvironment = "EVENTBUS_MANAGED_PHASE_FAULT"

type managedPhaseFaultPoint struct {
	PID       int    `json:"pid"`
	RequestID string `json:"request_id"`
}

type managedPhaseFaultReply struct {
	RequestID string `json:"request_id"`
	Forged    bool   `json:"forged"`
}

// This unique testbinary fixture replaces only the managed interpreter. Its
// native fd3 result is deliberately valid even when READY is malformed, so a
// missing protocol guard cannot pass merely because the response is invalid.
func TestManagedPhaseProtocolHelper(t *testing.T) {
	mode := os.Getenv(managedPhaseFaultEnvironment)
	if mode == "" {
		return
	}
	fail := func() { os.Exit(91) }
	point := managedPhaseFaultPoint{PID: os.Getpid(), RequestID: os.Getenv("EVENTBUS_LAMBDA_REQUEST_ID")}
	data, err := json.Marshal(point)
	pointPath := os.Getenv("EVENTBUS_MANAGED_PHASE_POINT")
	if err != nil || os.WriteFile(pointPath, data, 0600) != nil {
		fail()
	}
	launches, err := os.OpenFile(pointPath+".launches", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		fail()
	}
	_, writeErr := launches.Write(append(data, '\n'))
	closeErr := launches.Close()
	if writeErr != nil || closeErr != nil {
		fail()
	}
	if mode == "init_wait" {
		for {
			time.Sleep(time.Hour)
		}
	}
	result := os.NewFile(3, "native-result")
	ready := os.NewFile(6, "phase-ready")
	ack := os.NewFile(7, "phase-ack")
	if result == nil || ready == nil || ack == nil {
		fail()
	}
	if mode == "ready_eof_import_error" {
		_ = ready.Close()
		_, err := io.WriteString(result, `{"error":{"errorType":"ImportError","errorMessage":"original managed import failure"}}`)
		if err != nil {
			fail()
		}
		os.Exit(0)
	}
	if mode == "closed_ack" {
		if ack.Close() != nil {
			fail()
		}
	}
	frame := `{"version":1,"ready":true}` + "\n"
	switch mode {
	case "malformed":
		frame = "not JSON\n"
	case "oversized":
		frame = strings.Repeat("x", maxPhaseFrame+1) + "\n"
	case "unknown_field":
		frame = `{"version":1,"ready":true,"renew_timeout_ms":900000}` + "\n"
	case "invalid_version":
		frame = `{"version":2,"ready":true}` + "\n"
	case "not_ready":
		frame = `{"version":1,"ready":false}` + "\n"
	case "valid_retained_ready", "closed_ack":
	default:
		fail()
	}
	if _, err := io.WriteString(ready, frame); err != nil {
		fail()
	}
	// A malicious interpreter can forge its fd3 envelope before ACK. The host
	// must still reject invalid readiness instead of trusting this result.
	envelope, err := json.Marshal(map[string]any{"result": managedPhaseFaultReply{RequestID: point.RequestID, Forged: true}})
	if err != nil {
		fail()
	}
	if _, err := result.Write(envelope); err != nil {
		fail()
	}
	if mode == "closed_ack" {
		os.Exit(0)
	}
	// Retain fd6 until process exit, as a launcher retaining its duplicate
	// would. READY/ACK must use live frame boundaries, not EOF.
	frameBytes, err := bufio.NewReaderSize(ack, maxPhaseFrame+1).ReadSlice('\n')
	if err != nil {
		os.Exit(0)
	}
	var acknowledgement struct {
		Version    int   `json:"version"`
		DeadlineMS int64 `json:"deadline_ms"`
	}
	if json.Unmarshal(frameBytes, &acknowledgement) != nil || acknowledgement.Version != 1 || acknowledgement.DeadlineMS <= time.Now().UnixMilli() {
		fail()
	}
	// This marker is the only handler admission observation. Invalid frames
	// must never receive an ACK that permits execution or renews the budget.
	if os.WriteFile(os.Getenv("EVENTBUS_MANAGED_PHASE_HANDLER"), frameBytes, 0600) != nil {
		fail()
	}
	os.Exit(0)
}

func managedPhaseProtocolFixture(t *testing.T, mode string) (*Service, string, string, string, *bool) {
	t.Helper()
	directory := t.TempDir()
	pointPath, handlerPath := filepath.Join(directory, "process.json"), filepath.Join(directory, "handler.json")
	executable, err := os.Executable()
	require.NoError(t, err)
	writeFixture(t, directory, "phase_fault.py", "def handler(event, context):\n    return event\n")
	function := Function{
		Runtime: "python", Handler: "phase_fault.handler", Timeout: 2 * time.Second,
		Command: []string{executable, "-test.run=^TestManagedPhaseProtocolHelper$", "--"},
		Environment: map[string]string{
			managedPhaseFaultEnvironment:     mode,
			"EVENTBUS_MANAGED_PHASE_POINT":   pointPath,
			"EVENTBUS_MANAGED_PHASE_HANDLER": handlerPath,
		},
	}
	service, diagnostics := newDiagnosticTestService(t, map[string]Function{"protocol:live": function}, directory, nil, nil)
	// Keep a lost readiness frame bounded without spending the default 10s
	// Init, while leaving ample time for the actual testbinary startup.
	service.initTimeout = 3 * time.Second
	cleanup, joined := service.processCleanup, new(bool)
	service.processCleanup = func(command *exec.Cmd) error {
		err := cleanup(command)
		*joined = command.ProcessState != nil && !processAlive(command.Process.Pid)
		return err
	}
	return service, diagnostics, pointPath, handlerPath, joined
}

func assertManagedPhaseProtocolChildJoined(t *testing.T, pointPath string, metadata InvocationMetadata, joined bool) {
	t.Helper()
	data, err := os.ReadFile(pointPath)
	require.NoError(t, err, "the actual managed interpreter must have started")
	var point managedPhaseFaultPoint
	require.NoError(t, json.Unmarshal(data, &point))
	require.Greater(t, point.PID, 0)
	require.Equal(t, metadata.RequestID, point.RequestID)
	require.True(t, joined, "native cleanup must join before phase/result classification")
	require.False(t, processAlive(point.PID), "completion cannot leave the interpreter alive")
}

func TestManagedPhaseProtocolFaultsCannotBypassNativeAdmission(t *testing.T) {
	for _, mode := range []string{"malformed", "oversized", "unknown_field", "invalid_version", "not_ready", "closed_ack"} {
		t.Run(mode, func(t *testing.T) {
			service, path, point, handler, joined := managedPhaseProtocolFixture(t, mode)
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
			defer cancel()
			outcome, err := service.ExecuteObserved(ctx, InvokeInput{FunctionName: "protocol:live", Payload: []byte(`{}`)}, nil)
			require.NoError(t, err)
			require.Equal(t, InvocationFailed, outcome.State)
			require.True(t, outcome.Output.FunctionError)
			require.Nil(t, outcome.OwnershipErr)
			var failure struct {
				ErrorType string `json:"errorType"`
			}
			require.NoError(t, json.Unmarshal(outcome.Output.Payload, &failure))
			require.Equal(t, "Runtime.InvalidResponse", failure.ErrorType, "a forged fd3 success cannot bypass readiness")
			assertManagedPhaseProtocolChildJoined(t, point, outcome.Metadata, *joined)
			_, err = os.Stat(handler)
			require.ErrorIs(t, err, os.ErrNotExist, "invalid readiness cannot admit a handler")
			records := readDiagnosticRecords(t, path)
			require.Len(t, records, 1)
			require.Equal(t, outcome.Metadata.RequestID, records[0].RequestID)
			require.Equal(t, 1, records[0].Attempt)
			require.Equal(t, "runtime_protocol_error", records[0].TerminationCause)
			require.True(t, records[0].OwnershipConfirmed)
			require.Len(t, records[0].ExecutionPhases, 1, "protocol failures cannot trigger Init fallback")
			require.Equal(t, 1, records[0].ExecutionPhases[0].InitAttempt)
			if mode == "closed_ack" {
				require.Equal(t, "succeeded", records[0].ExecutionPhases[0].InitState)
				require.Equal(t, "failed", records[0].ExecutionPhases[0].InvokeState)
			} else {
				require.Equal(t, "failed", records[0].ExecutionPhases[0].InitState)
				require.Empty(t, records[0].ExecutionPhases[0].InvokeState)
			}
			require.NoError(t, service.DevEvidence(), "protocol rejection is a joined native failure")
		})
	}
}

func TestManagedPhaseProtocolFramesDoNotWaitForReadyEOF(t *testing.T) {
	service, path, point, handler, joined := managedPhaseProtocolFixture(t, "valid_retained_ready")
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	outcome, err := service.ExecuteObserved(ctx, InvokeInput{FunctionName: "protocol:live", Payload: []byte(`{}`)}, nil)
	require.NoError(t, err)
	require.Equal(t, InvocationSucceeded, outcome.State)
	require.False(t, outcome.Output.FunctionError)
	require.Nil(t, outcome.OwnershipErr)
	var reply managedPhaseFaultReply
	require.NoError(t, json.Unmarshal(outcome.Output.Payload, &reply))
	require.True(t, reply.Forged)
	require.Equal(t, outcome.Metadata.RequestID, reply.RequestID)
	assertManagedPhaseProtocolChildJoined(t, point, outcome.Metadata, *joined)
	data, err := os.ReadFile(handler)
	require.NoError(t, err, "live READY must admit without its writing descriptor closing")
	var acknowledgement struct {
		Version    int   `json:"version"`
		DeadlineMS int64 `json:"deadline_ms"`
	}
	require.NoError(t, json.Unmarshal(data, &acknowledgement))
	require.Equal(t, 1, acknowledgement.Version)
	records := readDiagnosticRecords(t, path)
	require.Len(t, records, 1)
	require.Equal(t, outcome.Metadata.RequestID, records[0].RequestID)
	require.Empty(t, records[0].TerminationCause)
	require.True(t, records[0].OwnershipConfirmed)
	require.Len(t, records[0].ExecutionPhases, 1)
	require.Equal(t, "succeeded", records[0].ExecutionPhases[0].InitState)
	require.Equal(t, "succeeded", records[0].ExecutionPhases[0].InvokeState)
}

func TestManagedPhaseReadyEOFPreservesOriginalImportFailure(t *testing.T) {
	service, path, point, handler, joined := managedPhaseProtocolFixture(t, "ready_eof_import_error")
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	outcome, err := service.ExecuteObserved(ctx, InvokeInput{FunctionName: "protocol:live", Payload: []byte(`{}`)}, nil)
	require.NoError(t, err)
	require.Equal(t, InvocationFailed, outcome.State)
	require.True(t, outcome.Output.FunctionError)
	require.Nil(t, outcome.OwnershipErr)
	require.JSONEq(t, `{"errorType":"ImportError","errorMessage":"original managed import failure"}`, string(outcome.Output.Payload))
	assertManagedPhaseProtocolChildJoined(t, point, outcome.Metadata, *joined)
	_, err = os.Stat(handler)
	require.ErrorIs(t, err, os.ErrNotExist)
	records := readDiagnosticRecords(t, path)
	require.Len(t, records, 1)
	require.Empty(t, records[0].TerminationCause)
	require.Len(t, records[0].ExecutionPhases, 1)
	require.Equal(t, "failed", records[0].ExecutionPhases[0].InitState)
	require.Empty(t, records[0].ExecutionPhases[0].InvokeState)
	require.True(t, records[0].OwnershipConfirmed)
	require.NoError(t, service.DevEvidence())
}

func TestManagedInitDirtyCleanupCannotLaunchFallback(t *testing.T) {
	service, path, point, handler, joined := managedPhaseProtocolFixture(t, "init_wait")
	service.initTimeout = 300 * time.Millisecond
	cleanup := service.processCleanup
	uncertain := errors.New("controlled native cleanup uncertainty after actual child join")
	cleanups := 0
	service.processCleanup = func(command *exec.Cmd) error {
		err := cleanup(command)
		cleanups++
		return errors.Join(err, uncertain)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	outcome, err := service.ExecuteObserved(ctx, InvokeInput{FunctionName: "protocol:live", Payload: []byte(`{}`)}, nil)
	require.NoError(t, err)
	require.Equal(t, InvocationTimedOut, outcome.State)
	require.True(t, outcome.Output.FunctionError)
	require.Contains(t, string(outcome.Output.Payload), "Sandbox.Timedout")
	require.ErrorIs(t, outcome.OwnershipErr, uncertain)
	require.Equal(t, 1, cleanups, "dirty native cleanup cannot be retried as another process")
	assertManagedPhaseProtocolChildJoined(t, point, outcome.Metadata, *joined)
	launches, err := os.ReadFile(point + ".launches")
	require.NoError(t, err)
	require.Equal(t, 1, bytes.Count(launches, []byte{'\n'}), "there must be only one actual interpreter launch")
	_, err = os.Stat(handler)
	require.ErrorIs(t, err, os.ErrNotExist)
	records := readDiagnosticRecords(t, path)
	require.Len(t, records, 1)
	require.Equal(t, outcome.Metadata.RequestID, records[0].RequestID)
	require.Equal(t, 1, records[0].Attempt)
	require.Equal(t, "initialization_timeout", records[0].TerminationCause)
	require.False(t, records[0].OwnershipConfirmed)
	require.Contains(t, records[0].OwnershipError, uncertain.Error())
	require.Len(t, records[0].ExecutionPhases, 1)
	require.Equal(t, 1, records[0].ExecutionPhases[0].InitAttempt)
	require.Equal(t, "initial", records[0].ExecutionPhases[0].Mode)
	require.Equal(t, "timed_out", records[0].ExecutionPhases[0].InitState)
	require.Empty(t, records[0].ExecutionPhases[0].InvokeState)
	phase := records[0].ExecutionPhases[0]
	require.NotEmpty(t, phase.ProcessError)
	require.Equal(t, records[0].ProcessError, phase.ProcessError)
	require.Equal(t, "deadline_exceeded", phase.ContextError)
	require.Equal(t, "initialization_timeout", phase.TerminationCause)
	require.False(t, phase.OwnershipConfirmed)
	require.Equal(t, records[0].OwnershipError, phase.OwnershipError)
	require.Contains(t, phase.OwnershipError, uncertain.Error())
	require.ErrorIs(t, service.DevEvidence(), uncertain)
	require.ErrorIs(t, service.Close(context.Background()), uncertain)
}
