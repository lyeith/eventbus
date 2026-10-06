//go:build linux || darwin

package cognitotrigger

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const spawnFixtureChild = `import fs from 'node:fs';
import {spawn} from 'node:child_process';
export async function handler(event) {
  const child=spawn(process.execPath,['-e','setInterval(()=>{},1000);setTimeout(()=>process.exit(0),5000)'],{stdio:'ignore'});
  child.unref();
  fs.writeFileSync(process.env.PID_FILE,String(child.pid));
  event.response.child=child.pid;
  FIXTURE_COMPLETION
}`

type fixtureChild struct {
	pid    int
	reaped bool
}

func childPID(t *testing.T, path string) *fixtureChild {
	t.Helper()
	var pid int
	require.Eventually(t, func() bool {
		data, err := os.ReadFile(path)
		if err != nil {
			return false
		}
		pid, err = strconv.Atoi(string(data))
		return err == nil && pid > 0
	}, 2*time.Second, 5*time.Millisecond, "fixture child did not start")
	child := &fixtureChild{pid: pid}
	t.Cleanup(func() {
		if !child.reaped {
			_ = syscall.Kill(child.pid, syscall.SIGKILL)
		}
	})
	return child
}

func requireChildReaped(t *testing.T, child *fixtureChild) {
	t.Helper()
	require.Eventually(t, func() bool {
		return syscall.Kill(child.pid, 0) == syscall.ESRCH
	}, 2*time.Second, 5*time.Millisecond, "trigger returned before its child was terminated and reaped")
	child.reaped = true
}

func TestInvokeCleansUpChildrenAfterSuccess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "child.pid")
	runner, _ := fixtureRunner(t, "handler.mjs", strings.Replace(spawnFixtureChild, "FIXTURE_COMPLETION", "return event;", 1), map[string]string{"PID_FILE": path})
	_, err := runner.Invoke(t.Context(), "owned-pool", DefineAuthChallenge, awsEvent("DefineAuthChallenge_Authentication"))
	require.NoError(t, err)
	requireChildReaped(t, childPID(t, path))
}

func TestInvokeTimeoutAndCloseCleanUpChildren(t *testing.T) {
	for _, mode := range []string{"cancel", "close"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "child.pid")
			runner, _ := fixtureRunner(t, "handler.mjs", strings.Replace(spawnFixtureChild, "FIXTURE_COMPLETION", "await new Promise(()=>{});", 1), map[string]string{"PID_FILE": path})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			result := make(chan error, 1)
			go func() {
				_, err := runner.Invoke(ctx, "owned-pool", DefineAuthChallenge, awsEvent("DefineAuthChallenge_Authentication"))
				result <- err
			}()
			pid := childPID(t, path)
			if mode == "cancel" {
				cancel()
			} else {
				require.NoError(t, runner.Close(t.Context()))
			}
			select {
			case err := <-result:
				require.Error(t, err)
			case <-time.After(2 * time.Second):
				t.Fatal("invocation did not join after cancellation")
			}
			requireChildReaped(t, pid)
		})
	}
}

func TestInheritedChildPipesCannotExtendInvocationIndefinitely(t *testing.T) {
	runner, _ := fixtureRunner(t, "handler.mjs", `import {spawn} from 'node:child_process';
export async function handler(event){
  const child=spawn(process.execPath,['-e','setInterval(()=>{},1000);setTimeout(()=>process.exit(0),5000)'],{stdio:'inherit'});
  child.unref();
  return event;
}`, nil)
	started := time.Now()
	_, err := runner.Invoke(t.Context(), "owned-pool", DefineAuthChallenge, awsEvent("DefineAuthChallenge_Authentication"))
	require.Error(t, err, "incomplete process output ownership must fail closed")
	require.Less(t, time.Since(started), 3*time.Second)
}
