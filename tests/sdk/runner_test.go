//go:build sdksmoke

package sdk

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func fixturePath(parts ...string) string {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		panic("SDK fixture source path unavailable")
	}
	return filepath.Join(append([]string{filepath.Dir(filename)}, parts...)...)
}

// Missing dependencies fail this opt-in lane; ordinary short tests do not need Python.
func sdkPython(t *testing.T) string {
	t.Helper()
	python := os.Getenv("EVENTBUS_SMOKE_PYTHON")
	require.True(t, filepath.IsAbs(python), "set EVENTBUS_SMOKE_PYTHON to the frozen environment's absolute executable")
	info, err := os.Stat(python)
	require.NoError(t, err)
	require.False(t, info.IsDir())
	return python
}

// Endpoints come only from the OS-owned listener. Do not inherit AWS profiles,
// proxy settings, shared test targets or Python options. Scripts use fake credentials.
func sdkEnvironment(home, endpoint, pool, client string) []string {
	env := []string{
		"HOME=" + home,
		"COGNITO_ENDPOINT_URL=" + endpoint,
		"COGNITO_ISSUER_BASE=" + endpoint,
		"COGNITO_USER_POOL_ID=" + pool,
		"COGNITO_CLIENT_ID=" + client,
		"SMOKE_EMAIL=" + uuid.NewString() + "@sdk-smoke.test",
		"AWS_EC2_METADATA_DISABLED=true",
		"NO_PROXY=*",
	}
	for _, name := range []string{"PATH", "SystemRoot", "TMPDIR", "TMP", "TEMP", "LD_LIBRARY_PATH"} {
		if value, ok := os.LookupEnv(name); ok {
			env = append(env, name+"="+value)
		}
	}
	return env
}

func runSDKProcess(ctx context.Context, python, script string, env []string) ([]byte, error) {
	// -E prevents PYTHONOPTIMIZE from disabling assertions. -s excludes user site
	// packages; script-local imports and the frozen virtualenv remain available.
	command := exec.CommandContext(ctx, python, "-E", "-s", "-X", "utf8", "-u", script)
	command.Env = env
	command.WaitDelay = time.Second
	output, err := command.CombinedOutput()
	if ctx.Err() != nil {
		return output, fmt.Errorf("SDK smoke deadline: %w", ctx.Err())
	}
	if err != nil {
		return output, fmt.Errorf("SDK smoke child: %w", err)
	}
	passes := 0
	for _, line := range strings.Split(string(output), "\n") {
		switch strings.TrimSpace(line) {
		case "PASS":
			passes++
		case "FAIL":
			return output, fmt.Errorf("SDK smoke reported FAIL")
		}
	}
	if passes != 1 {
		return output, fmt.Errorf("SDK smoke requires exactly one PASS marker, got %d", passes)
	}
	return output, nil
}

func TestSDKProcessControls(t *testing.T) {
	python := sdkPython(t)
	for _, scenario := range []struct {
		name, body string
		pass       bool
	}{
		{"success", "print('PASS')\n", true},
		{"empty", "pass\n", false},
		{"false-marker", "print('NOT PASS')\n", false},
		{"duplicate", "print('PASS\\nPASS')\n", false},
		{"nonzero", "print('PASS')\nraise SystemExit(7)\n", false},
		{"assertion", "print('PASS')\nassert False, 'assertions enabled'\n", false},
		{"conflicting-markers", "print('FAIL\\nPASS')\n", false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			script := filepath.Join(t.TempDir(), "probe.py")
			require.NoError(t, os.WriteFile(script, []byte(scenario.body), 0o600))
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			env := append(sdkEnvironment(t.TempDir(), "", "", ""), "PYTHONOPTIMIZE=2")
			_, err := runSDKProcess(ctx, python, script, env)
			if scenario.pass {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
	t.Run("deadline", func(t *testing.T) {
		script := filepath.Join(t.TempDir(), "probe.py")
		require.NoError(t, os.WriteFile(script, []byte("import time\ntime.sleep(30)\n"), 0o600))
		ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
		defer cancel()
		_, err := runSDKProcess(ctx, python, script, sdkEnvironment(t.TempDir(), "", "", ""))
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
	t.Run("missing-script", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		_, err := runSDKProcess(ctx, python, filepath.Join(t.TempDir(), "absent.py"), sdkEnvironment(t.TempDir(), "", "", ""))
		require.Error(t, err)
	})
	t.Run("environment-isolation", func(t *testing.T) {
		t.Setenv("COGNITO_ENDPOINT_URL", "inherited-shared-service")
		t.Setenv("AWS_PROFILE", "inherited-profile")
		t.Setenv("HTTPS_PROXY", "inherited-proxy")
		t.Setenv("PYTHONOPTIMIZE", "2")
		env := strings.Join(sdkEnvironment(t.TempDir(), "owned-listener", "owned-pool", "owned-client"), "\n")
		require.NotContains(t, env, "inherited")
		require.NotContains(t, env, "PYTHONOPTIMIZE")
		require.Contains(t, env, "COGNITO_ENDPOINT_URL=owned-listener")
	})
}
