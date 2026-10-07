//go:build sdksmoke

package sdk

import (
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/localexec"
	"github.com/stretchr/testify/require"
)

func TestLambdaEventSDKSmoke(t *testing.T) {
	python := sdkPython(t)
	directory := t.TempDir()
	output := filepath.Join(directory, "side-effects.jsonl")
	gate := filepath.Join(directory, "gate")
	handler := `import fs from 'node:fs';
async function execute(kind,event) { if (event.id==='delayed') while (!fs.existsSync(process.env.GATE)) await new Promise(r=>setTimeout(r,5)); fs.appendFileSync(process.env.OUTPUT,JSON.stringify({kind,event})+'\n'); return {kind,event}; }
export const base=(event)=>execute('base',event);
export const alias=(event)=>execute('alias',event);
`
	require.NoError(t, os.WriteFile(filepath.Join(directory, "handler.mjs"), []byte(handler), 0600))
	function := lambda.Function{Runtime: "node", Handler: "handler.mjs.base", Timeout: 5 * time.Second, Environment: map[string]string{"OUTPUT": output, "GATE": gate}}
	alias := function
	alias.Handler = "handler.mjs.alias"
	service, err := lambda.NewService(&lambda.Config{Functions: map[string]lambda.Function{"sdk-event": function, "sdk-event:live": alias}, DevAsync: &lambda.DevAsyncConfig{Workers: 2, Capacity: 32, RetryDelays: []time.Duration{0, 0}, LogPath: filepath.Join(directory, "execution.jsonl")}}, directory)
	require.NoError(t, err)
	serving := httptest.NewServer(service)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		drainErr := service.DrainAsync(ctx)
		serving.Close()
		closeErr := service.Close(ctx)
		if drainErr != nil || closeErr != nil {
			t.Errorf("owned Lambda cleanup: drain=%v close=%v", drainErr, closeErr)
		}
	})
	env := append(sdkEnvironment(t.TempDir(), "", "", ""), "LAMBDA_ENDPOINT_URL="+serving.URL, "LAMBDA_OUTPUT_FILE="+output, "LAMBDA_GATE_FILE="+gate,
		// SSD's managed uv wrapper resolves the real tool under the operation
		// owner's HOME. Explicit null AWS files retain SDK profile isolation.
		"HOME="+os.Getenv("HOME"), "AWS_CONFIG_FILE="+os.DevNull, "AWS_SHARED_CREDENTIALS_FILE="+os.DevNull)
	// These explicit fixture variables let uv join the already-owned SSD test
	// operation. They do not enter application handler environments.
	for _, key := range []string{"SSD_DEV_RUN_ID", "SSD_DEV_RECEIPT", "SSD_DEV_SCOPE", "TMPDIR", "GOTMPDIR", "GOCACHE", "UV_CACHE_DIR", "XDG_RUNTIME_DIR", "DBUS_SESSION_BUS_ADDRESS", "INVOCATION_ID"} {
		if value, ok := os.LookupEnv(key); ok {
			env = append(env, key+"="+value)
		}
	}
	uv, err := exec.LookPath("uv")
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Second)
	defer cancel()
	// Invoke the frozen interpreter through uv, preserving -E/-s assertion and
	// environment isolation without installing or resolving additional packages.
	command := exec.CommandContext(ctx, uv, "run", "--no-project", python, "-E", "-s", "-X", "utf8", "-u", fixturePath("python", "smoke_lambda_event.py"))
	command.Env = env
	command.WaitDelay = time.Second
	require.NoError(t, localexec.Configure(command))
	result, err := command.CombinedOutput()
	cleanupErr := localexec.Cleanup(command)
	t.Logf("real Lambda Event boto3 proof:\n%s", result)
	require.NoError(t, err)
	require.NoError(t, cleanupErr)
	passes := 0
	for _, line := range strings.Split(string(result), "\n") {
		if strings.TrimSpace(line) == "PASS" {
			passes++
		}
		require.NotEqual(t, "FAIL", strings.TrimSpace(line))
	}
	require.Equal(t, 1, passes, fmt.Sprintf("expected exactly one PASS: %s", result))
}
