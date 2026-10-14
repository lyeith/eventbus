//go:build sdksmoke

package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/events"
	"github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/server"
	"github.com/stretchr/testify/require"
)

func runEventBridgeJavascriptSDKProof(t *testing.T, recurring bool) {
	t.Helper()
	node := sdkNode(t)
	directory := t.TempDir()
	output, gate := filepath.Join(directory, "effects.jsonl"), filepath.Join(directory, "gate")
	functions, err := lambda.NewService(&lambda.Config{
		Functions: map[string]lambda.Function{"events:live": {
			Runtime: "node", Command: []string{node},
			Handler: fixturePath("javascript", "eventbridge_handler.mjs") + "#handler", Timeout: 95 * time.Second,
			Environment: map[string]string{"EVENTS_OUTPUT": output, "EVENTS_GATE": gate},
		}},
		DevAsync: &lambda.DevAsyncConfig{Workers: 8, Capacity: 32, RetryDelays: []time.Duration{0, 0},
			LogPath: filepath.Join(directory, "async.jsonl")},
	}, directory)
	require.NoError(t, err)
	rules, err := events.New(events.Options{Region: "us-east-1", AccountID: "000000000000"}, schedulerSDKTarget{functions})
	require.NoError(t, err)
	serving := httptest.NewServer(server.New(server.Services{Lambda: functions, Events: events.NewHandler(rules)}))
	t.Cleanup(func() {
		require.NoError(t, os.WriteFile(gate, []byte("cleanup"), 0600))
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		ruleErr := rules.Close(ctx)
		drainErr := functions.DrainAsync(ctx)
		serving.Close()
		require.NoError(t, errors.Join(ruleErr, drainErr, functions.Close(ctx)))
		data, readErr := os.ReadFile(output)
		if readErr == nil {
			for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
				var row struct {
					PID int `json:"pid"`
				}
				require.NoError(t, json.Unmarshal([]byte(line), &row))
				require.Positive(t, row.PID)
				require.ErrorIs(t, syscall.Kill(row.PID, 0), syscall.ESRCH, "every actual child must be joined")
			}
		}
	})
	environment := append(sdkEnvironment(t.TempDir(), "", "", ""),
		"EVENTS_ENDPOINT_URL="+serving.URL, "EVENTS_OUTPUT_FILE="+output, "EVENTS_GATE_FILE="+gate)
	if recurring {
		environment = append(environment, "EVENTS_RECURRING=1")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	logs, err := runJavascriptSDKProcess(ctx, node, fixturePath("javascript", "eventbridge.mjs"), nil, environment)
	t.Logf("real EventBridge JavaScript SDK proof (recurring=%v):\n%s", recurring, logs)
	require.NoError(t, err)
	require.NoError(t, functions.DrainAsync(ctx))
	records := functions.AsyncSnapshot()
	expected := 0
	if recurring {
		expected = 12
	}
	require.Len(t, records, expected)
	for _, record := range records {
		require.Equal(t, "succeeded", record.State)
		require.Equal(t, 1, record.Attempts)
		require.Equal(t, "events:live", record.FunctionName)
	}
}

func TestEventBridgeJavascriptSDKSmoke(t *testing.T) {
	runEventBridgeJavascriptSDKProof(t, false)
}

func TestEventBridgeRecurringJavascriptSDKSmoke(t *testing.T) {
	if os.Getenv("EVENTBUS_SMOKE_RECURRING_EVENTS") != "1" {
		t.Skip("set EVENTBUS_SMOKE_RECURRING_EVENTS=1 for the genuine minute-rate proof (~3 minutes)")
	}
	runEventBridgeJavascriptSDKProof(t, true)
}
