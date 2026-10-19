package lambda

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDevDiagnosticsModeConfig(t *testing.T) {
	for _, mode := range []DevDiagnosticsMode{"", DevDiagnosticsAll, DevDiagnosticsFailures} {
		t.Run("valid-"+string(mode), func(t *testing.T) {
			directory := t.TempDir()
			source := "functions:\n  fixture:\n    runtime: command\n    command: [fixture]\ndev_diagnostics:\n  log_path: private/invocations.jsonl\n"
			if mode != "" {
				source += "  mode: " + string(mode) + "\n"
			}
			path := filepath.Join(directory, "functions.yaml")
			require.NoError(t, os.WriteFile(path, []byte(source), 0600))
			config, err := LoadConfig(path)
			require.NoError(t, err)
			require.Equal(t, mode, config.DevDiagnostics.Mode)
			require.NoError(t, validateDevDiagnostics(config.DevDiagnostics))
		})
	}
	require.NoError(t, validateDevDiagnostics(nil))
	for _, mode := range []DevDiagnosticsMode{"failure", "errors", "ALL", " failures", "unknown"} {
		t.Run("invalid-"+string(mode), func(t *testing.T) {
			directory := t.TempDir()
			capture := filepath.Join(directory, "private", "invocations.jsonl")
			_, err := NewService(&Config{
				Functions:      map[string]Function{"fixture": {Runtime: "command", Command: []string{"fixture"}}},
				DevDiagnostics: &DevDiagnosticsConfig{LogPath: capture, Mode: mode},
			}, directory)
			require.ErrorContains(t, err, "unknown dev_diagnostics.mode")
			_, err = os.Stat(filepath.Dir(capture))
			require.ErrorIs(t, err, os.ErrNotExist, "invalid mode must fail before opening private output")
		})
	}
	require.ErrorContains(t, validateDevDiagnostics(&DevDiagnosticsConfig{
		LogPath: "-", Mode: DevDiagnosticsFailures,
	}), "private owned file")
}

func TestDevDiagnosticsRetentionUsesFinalJoinedAttempt(t *testing.T) {
	clean := func() invocationResult { return invocationResult{state: InvocationSucceeded} }
	ownership := errors.New("actual cleanup uncertainty")
	for _, tc := range []struct {
		name       string
		result     invocationResult
		completion diagnosticCompletion
		failure    bool
	}{
		{name: "clean success", result: clean()},
		{name: "caught business error is still native success", result: invocationResult{
			state: InvocationSucceeded, diagnostics: invocationDiagnostics{stderr: []byte("caught failure")},
		}},
		{name: "successful provided runtime can have a process exit error", result: invocationResult{
			state: InvocationSucceeded, diagnostics: invocationDiagnostics{processError: "signal: killed"},
		}},
		{name: "successful fallback keeps historical init failure without retaining success", result: invocationResult{
			state: InvocationSucceeded, phases: []runtimePhaseRecord{{InitState: "timed_out"}, {InitState: "succeeded", InvokeState: "succeeded"}},
		}},
		{name: "handler failure", result: invocationResult{state: InvocationFailed, functionError: true}, failure: true},
		{name: "timeout", result: invocationResult{state: InvocationTimedOut}, failure: true},
		{name: "canceled", result: invocationResult{state: InvocationCanceled}, failure: true},
		{name: "not started rejection", result: invocationResult{state: InvocationNotStarted}, failure: true},
		{name: "interrupted outcome", result: invocationResult{}, failure: true},
		{name: "native success with unconfirmed cleanup", result: invocationResult{state: InvocationSucceeded, ownershipErr: ownership}, failure: true},
		{name: "native success with frozen cancellation", result: clean(), completion: diagnosticCompletion{contextError: "canceled"}, failure: true},
		{name: "completion loss selects native async retry", result: invocationResult{state: InvocationSucceeded, devCompletionLoss: true}, failure: true},
		{name: "worker crash cannot disappear behind a successful flag", result: invocationResult{state: InvocationSucceeded, devWorkerCrash: true}, failure: true},
		{name: "function failure cannot disappear behind a successful flag", result: invocationResult{state: InvocationSucceeded, functionError: true}, failure: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, mode := range []DevDiagnosticsMode{"", DevDiagnosticsAll, DevDiagnosticsFailures} {
				service := &Service{diagnosticMode: mode}
				want := mode != DevDiagnosticsFailures || tc.failure
				require.Equal(t, want, service.retainsDiagnostics(tc.result, tc.completion), "mode=%q", mode)
			}
		})
	}
}
