//go:build sdksmoke && performance && (linux || darwin)

package sdk

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Run unchanged for before/after attribution. The SDK driver includes real
// provisioning, SRP arithmetic, three auth chains and independent JWT checks;
// individual rows separate auth HTTP latency from client arithmetic/validation.
func TestPerformanceCognitoNativeSDKAuth(t *testing.T) {
	for _, warm := range []bool{false, true} {
		mode := map[bool]string{false: "fresh", true: "warm"}[warm]
		t.Run(mode, func(t *testing.T) {
			fixture := newJavascriptFixture(t, sdkNode(t))
			if warm {
				fixture.close(t)
				fixture.warm = true
				fixture.start(t, "define.mjs")
			}
			ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
			defer cancel()
			environment := append(sdkEnvironment(fixture.directory, fixture.endpoint, fixture.pool, ""),
				"SMOKE_RUN_ID="+fixture.runID,
				"SMOKE_STATE_PATH="+filepath.Join(fixture.directory, "state.json"),
				"SES_CAPTURE_PATH="+filepath.Join(fixture.directory, "emails.jsonl"),
			)
			started := time.Now()
			output, err := runJavascriptSDKProcess(ctx, fixture.node, fixturePath("javascript", "performance_auth.mjs"), nil, environment)
			joined := time.Now()
			t.Logf("native SDK auth timing fixture (%s):\n%s", mode, output)
			require.NoError(t, err)
			imports, auth := 0, 0
			var driver struct {
				PID                 int    `json:"pid"`
				Node                string `json:"node_executable"`
				NodeVersion         string `json:"node_version"`
				ModuleStartedUnixMS int64  `json:"module_started_unix_ms"`
				SDKImportNS         int64  `json:"sdk_import_ns"`
			}
			for _, line := range strings.Split(string(output), "\n") {
				if !strings.HasPrefix(line, "PERF ") {
					continue
				}
				data := []byte(strings.TrimPrefix(line, "PERF "))
				var row struct {
					SchemaVersion string `json:"schema_version"`
					Sample        int    `json:"sample"`
					NativeCalls   int    `json:"native_auth_http_calls"`
					CapturedCount int    `json:"captured_email_count"`
					NativeHTTPNS  int64  `json:"native_http_ns"`
				}
				require.NoError(t, json.Unmarshal(data, &row))
				// The SDK scenario is unchanged; only the Go host labels execution mode.
				var labelled map[string]any
				require.NoError(t, json.Unmarshal(data, &labelled))
				labelled["trigger_execution_mode"] = mode
				labelledData, labelErr := json.Marshal(labelled)
				require.NoError(t, labelErr)
				t.Logf("PERF %s", labelledData)
				switch row.SchemaVersion {
				case "eventbus.performance.sdk-import.v1":
					imports++
					require.NoError(t, json.Unmarshal(data, &driver))
				case "eventbus.performance.sdk-auth.v1":
					auth++
					require.Equal(t, auth, row.Sample)
					require.Equal(t, 3, row.NativeCalls)
					require.Equal(t, auth, row.CapturedCount)
					require.Positive(t, row.NativeHTTPNS)
				default:
					t.Fatalf("unexpected performance row schema %q", row.SchemaVersion)
				}
			}
			require.Equal(t, 1, imports)
			require.Equal(t, 3, auth)
			require.Greater(t, driver.PID, 1)
			require.Positive(t, driver.SDKImportNS)
			actualNode, err := filepath.EvalSymlinks(driver.Node)
			require.NoError(t, err)
			expectedNode, err := filepath.EvalSymlinks(fixture.node)
			require.NoError(t, err)
			require.Equal(t, expectedNode, actualNode)
			require.ErrorIs(t, syscall.Kill(driver.PID, 0), syscall.ESRCH, "native SDK driver must be reaped before timing ends")
			require.GreaterOrEqual(t, driver.ModuleStartedUnixMS, started.UnixMilli())
			// Each native Cognito response already waited for its registered runner;
			// Close additionally proves there is no accepted trigger work left to join.
			fixture.close(t)
			row := map[string]any{"schema_version": "eventbus.performance.sdk-driver.v1", "trigger_execution_mode": mode, "node_executable": driver.Node,
				"node_version": driver.NodeVersion, "auth_samples": auth, "total_joined_ns": joined.Sub(started).Nanoseconds(),
				"launch_to_module_ms": driver.ModuleStartedUnixMS - started.UnixMilli(), "sdk_import_ns": driver.SDKImportNS,
				"ownership_joined": true}
			encoded, err := json.Marshal(row)
			require.NoError(t, err)
			t.Logf("PERF %s", encoded)

		})
	}
}
