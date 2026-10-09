//go:build performance && (linux || darwin)

package cognitotrigger

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/server"
	"github.com/lyeith/eventbus/internal/ses"
	"github.com/stretchr/testify/require"
)

// Import and execute the existing real SDK fixtures; do not replace their
// challenge decisions or SES sending with timing-only handlers.
const performanceTriggerWrapper = `import {appendFileSync} from 'node:fs';
const moduleStartedUnixMS = Date.now();
const importStarted = process.hrtime.bigint();
const real = await import(process.env.PERF_REAL_MODULE);
const importNS = Number(process.hrtime.bigint() - importStarted);
export async function handler(event, context) {
  const handlerStartedUnixMS = Date.now();
  const started = process.hrtime.bigint();
  const result = await real.handler(event, context);
  const handlerNS = Number(process.hrtime.bigint() - started);
  appendFileSync(process.env.PERF_TRACE, JSON.stringify({
    pid: process.pid, node_executable: process.execPath, node_version: process.version,
    trigger: process.env.PERF_TRIGGER, module_started_unix_ms: moduleStartedUnixMS,
    handler_started_unix_ms: handlerStartedUnixMS, real_handler_import_ns: importNS,
    handler_ns: handlerNS, handler_completed_unix_ms: Date.now(),
  }) + '\n', {mode: 0o600});
  return result;
}
`

type performanceTriggerTrace struct {
	PID                    int    `json:"pid"`
	Node                   string `json:"node_executable"`
	NodeVersion            string `json:"node_version"`
	Trigger                string `json:"trigger"`
	ModuleStartedUnixMS    int64  `json:"module_started_unix_ms"`
	HandlerStartedUnixMS   int64  `json:"handler_started_unix_ms"`
	ImportNS               int64  `json:"real_handler_import_ns"`
	HandlerNS              int64  `json:"handler_ns"`
	HandlerCompletedUnixMS int64  `json:"handler_completed_unix_ms"`
}

func TestPerformanceCognitoRegisteredTriggerChain(t *testing.T) {
	node := os.Getenv("EVENTBUS_SMOKE_NODE")
	if node == "" {
		var err error
		node, err = exec.LookPath("node")
		require.NoError(t, err)
	}
	node, err := filepath.Abs(node)
	require.NoError(t, err)
	canonicalNode, err := filepath.EvalSymlinks(node)
	require.NoError(t, err)
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)
	javascript := filepath.Clean(filepath.Join(filepath.Dir(filename), "..", "..", "tests", "sdk", "javascript"))
	packageData, err := os.ReadFile(filepath.Join(javascript, "node_modules", "@aws-sdk", "client-sesv2", "package.json"))
	require.NoError(t, err, "provision existing pinned npm dependencies before the opt-in lane")
	var sdkPackage struct{ Version string }
	require.NoError(t, json.Unmarshal(packageData, &sdkPackage))
	require.Equal(t, "3.1146.0", sdkPackage.Version)
	directory := t.TempDir()
	tracePath := filepath.Join(directory, "trigger.jsonl")
	module := filepath.Join(directory, "timed.mjs")
	require.NoError(t, os.WriteFile(module, []byte(performanceTriggerWrapper), 0600))
	capturePath := filepath.Join(directory, "emails.jsonl")
	capture, err := ses.OpenSESCapture(capturePath)
	require.NoError(t, err)
	mail := ses.NewSESManager(ses.SESFixtures{}, capture)
	t.Cleanup(func() { require.NoError(t, mail.Close()) })
	serving := httptest.NewServer(server.New(server.Services{SES: ses.NewHandler(mail)}))
	t.Cleanup(serving.Close)
	entry := func(trigger, filename string) *Entry {
		realModule := (&url.URL{Scheme: "file", Path: filepath.Join(javascript, "triggers", filename)}).String()
		return &Entry{Handler: module, TimeoutSeconds: 5, Env: map[string]string{
			"PERF_REAL_MODULE": realModule, "PERF_TRACE": tracePath, "PERF_TRIGGER": trigger,
			"SES_ENDPOINT_URL": serving.URL,
		}}
	}
	runner, err := New(&Config{Node: node, Pools: map[string]Pool{"owned-pool": {
		DefineAuthChallenge: entry(DefineAuthChallenge, "define.mjs"), CreateAuthChallenge: entry(CreateAuthChallenge, "create.mjs"),
		VerifyAuthChallengeResponse: entry(VerifyAuthChallengeResponse, "verify.mjs"),
	}}}, directory)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runner.Close(context.Background())) })
	invocations := 0
	for sample := 1; sample <= 5; sample++ {
		chainStarted := time.Now()
		invoke := func(trigger string, event map[string]any) map[string]any {
			started := time.Now()
			result, err := runner.Invoke(t.Context(), "owned-pool", trigger, event)
			joined := time.Now()
			require.NoError(t, err)
			invocations++
			data, err := os.ReadFile(tracePath)
			require.NoError(t, err)
			lines := strings.Split(strings.TrimSpace(string(data)), "\n")
			require.Len(t, lines, invocations)
			var trace performanceTriggerTrace
			require.NoError(t, json.Unmarshal([]byte(lines[len(lines)-1]), &trace))
			require.Equal(t, trigger, trace.Trigger)
			actualNode, err := filepath.EvalSymlinks(trace.Node)
			require.NoError(t, err)
			require.Equal(t, canonicalNode, actualNode, "every trigger uses the same selected Node interpreter")
			require.ErrorIs(t, syscall.Kill(trace.PID, 0), syscall.ESRCH, "direct child must be reaped before timing ends")
			require.GreaterOrEqual(t, trace.ModuleStartedUnixMS, started.UnixMilli())
			require.GreaterOrEqual(t, joined.UnixMilli(), trace.HandlerCompletedUnixMS)
			row := map[string]any{"schema_version": "eventbus.performance.cognito-trigger.v1", "sample": sample, "trigger": trigger,
				"node_executable": trace.Node, "node_version": trace.NodeVersion, "total_joined_ns": joined.Sub(started).Nanoseconds(),
				"launch_to_module_ms": trace.ModuleStartedUnixMS - started.UnixMilli(), "real_handler_import_ns": trace.ImportNS,
				"handler_ns": trace.HandlerNS, "after_handler_to_join_ms": joined.UnixMilli() - trace.HandlerCompletedUnixMS,
				"ownership_joined": true}
			encoded, err := json.Marshal(row)
			require.NoError(t, err)
			t.Logf("PERF %s", encoded)
			return result
		}
		first := awsEvent("DefineAuthChallenge_Authentication")
		first["request"].(map[string]any)["session"] = []any{map[string]any{"challengeName": "SRP_A", "challengeResult": true}}
		result := invoke(DefineAuthChallenge, first)
		require.Equal(t, "PASSWORD_VERIFIER", result["response"].(map[string]any)["challengeName"])
		result = invoke(DefineAuthChallenge, awsEvent("DefineAuthChallenge_Authentication"))
		require.Equal(t, "CUSTOM_CHALLENGE", result["response"].(map[string]any)["challengeName"])
		create := awsEvent("CreateAuthChallenge_Authentication")
		create["request"].(map[string]any)["challengeName"] = "CUSTOM_CHALLENGE"
		result = invoke(CreateAuthChallenge, create)
		private := result["response"].(map[string]any)["privateChallengeParameters"].(map[string]any)
		verify := awsEvent("VerifyAuthChallengeResponse_Authentication")
		verify["request"].(map[string]any)["privateChallengeParameters"] = private
		verify["request"].(map[string]any)["challengeAnswer"] = private["answer"]
		result = invoke(VerifyAuthChallengeResponse, verify)
		require.Equal(t, true, result["response"].(map[string]any)["answerCorrect"])
		final := awsEvent("DefineAuthChallenge_Authentication")
		request := final["request"].(map[string]any)
		request["session"] = append(request["session"].([]any), map[string]any{"challengeName": "CUSTOM_CHALLENGE", "challengeResult": true})
		result = invoke(DefineAuthChallenge, final)
		require.Equal(t, true, result["response"].(map[string]any)["issueTokens"])
		data, err := os.ReadFile(capturePath)
		require.NoError(t, err)
		require.Len(t, strings.Split(strings.TrimSpace(string(data)), "\n"), sample, "each actual Create handler sends one captured email via the pinned SDK")
		t.Logf("PERF {\"schema_version\":\"eventbus.performance.cognito-chain.v1\",\"sample\":%d,\"cold_trigger_invocations\":5,\"total_joined_ns\":%d,\"ownership_joined\":true}", sample, time.Since(chainStarted).Nanoseconds())
	}
	require.NoError(t, runner.Close(t.Context()))
}
