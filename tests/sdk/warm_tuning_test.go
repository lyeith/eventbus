//go:build sdksmoke

package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/messaging"
	"github.com/lyeith/eventbus/internal/server"
	"github.com/stretchr/testify/require"
)

func TestLambdaWarmTuningPythonSDKSmoke(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires owned worker signal checks")
	}
	python := sdkPython(t)
	directory := t.TempDir()
	output, gates := filepath.Join(directory, "effects.jsonl"), filepath.Join(directory, "gates")
	require.NoError(t, os.Mkdir(gates, 0700))
	functions, err := lambda.NewService(&lambda.Config{
		Functions: map[string]lambda.Function{"warm-tuning:live": {
			Runtime: "python", Command: []string{python, "-E", "-s"},
			Handler: fixturePath("python", "warm_tuning_smoke.py") + "#handler", Timeout: 25 * time.Second,
			Environment: map[string]string{"WARM_TUNING_OUTPUT": output, "WARM_TUNING_GATES": gates},
		}},
		DevWarm: &lambda.DevWarmConfig{MaxWorkers: 3, MaxConcurrency: 1},
		DevAsync: &lambda.DevAsyncConfig{Workers: 2, Capacity: 8, RetryDelays: []time.Duration{0, 0},
			LogPath: filepath.Join(directory, "async.jsonl")},
	}, directory)
	require.NoError(t, err)
	serving := httptest.NewUnstartedServer(nil)
	broker := messaging.NewBroker("us-east-1", "000000000000", serving.Listener.Addr().(*net.TCPAddr).Port)
	capture, err := messaging.OpenSNSCapture(filepath.Join(directory, "sns.jsonl"))
	require.NoError(t, err)
	broker.SetSNSCapture(capture)
	broker.SetLambdaDelivery(sdkSNSLambdaDelivery{functions: functions})
	serving.Config.Handler = server.New(server.Services{Messaging: messaging.NewHandler(broker), Lambda: functions,
		LambdaWarmDev: lambda.NewDevWarmHandler(functions)})
	serving.Start()
	t.Cleanup(func() {
		require.NoError(t, os.WriteFile(filepath.Join(gates, "cleanup"), []byte("finish"), 0600))
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		drainErr := functions.DrainAsync(ctx)
		serving.Close()
		require.NoError(t, errors.Join(drainErr, functions.Close(ctx), capture.Close()))
		data, readErr := os.ReadFile(output)
		if readErr == nil {
			pids := make(map[int]struct{})
			for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
				var record struct {
					PID int `json:"pid"`
				}
				require.NoError(t, json.Unmarshal([]byte(line), &record))
				pids[record.PID] = struct{}{}
			}
			for pid := range pids {
				require.Positive(t, pid)
				require.ErrorIs(t, syscall.Kill(pid, 0), syscall.ESRCH, "every observed worker must be joined")
			}
		}
	})
	environment := append(sdkEnvironment(t.TempDir(), "", "", ""),
		"WARM_TUNING_ENDPOINT="+serving.URL, "WARM_TUNING_OUTPUT="+output, "WARM_TUNING_GATES="+gates)
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	logs, err := runSDKProcess(ctx, python, fixturePath("python", "warm_tuning_smoke.py"), environment)
	t.Logf("real public warm tuning + native SNS/SQS/Lambda SDK proof:\n%s", logs)
	require.NoError(t, err)
	require.NoError(t, functions.DrainAsync(ctx))
	records := functions.AsyncSnapshot()
	require.Len(t, records, 2, "direct Event and SNS Event remain owned through live tuning")
	data, err := os.ReadFile(output)
	require.NoError(t, err)
	starts, finishes := make(map[string]string), make(map[string]string)
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var row struct {
			Phase     string `json:"phase"`
			ID        string `json:"id"`
			RequestID string `json:"request_id"`
		}
		require.NoError(t, json.Unmarshal([]byte(line), &row))
		if row.Phase == "start" {
			starts[row.ID] = row.RequestID
		} else {
			finishes[row.ID] = row.RequestID
		}
	}
	require.Equal(t, starts, finishes, "live tuning preserves actual invocation identity across completion")
	for _, record := range records {
		require.Contains(t, []string{starts["queued-event"], starts["queued-sns"]}, record.RequestID)
		require.Equal(t, "succeeded", record.State)
		require.Equal(t, 1, record.Attempts, "warm pressure does not consume handler retries")
	}
}
