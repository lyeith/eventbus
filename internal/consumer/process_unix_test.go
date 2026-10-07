//go:build linux || darwin

package consumer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/messaging"
	"github.com/stretchr/testify/require"
)

func TestConsumerOwnsRealHandlerDescendants(t *testing.T) {
	for _, mode := range []string{"success", "timeout", "retained-pipes"} {
		t.Run(mode, func(t *testing.T) {
			directory := t.TempDir()
			marker := filepath.Join(directory, "child.pid")
			cleanupConsumerChild(t, marker)
			handler := filepath.Join(directory, "handler")
			script := "#!/bin/sh\nsleep 60 >/dev/null 2>&1 &\necho \"$!\" > \"$EVENTBUS_CHILD_PID_FILE\"\nprintf '{}\\n'\n"
			if mode == "timeout" {
				script = "#!/bin/sh\nsleep 60 &\necho \"$!\" > \"$EVENTBUS_CHILD_PID_FILE\"\nwait\n"
			} else if mode == "retained-pipes" {
				script = "#!/bin/sh\nsleep 60 &\necho \"$!\" > \"$EVENTBUS_CHILD_PID_FILE\"\nprintf '{}\\n'\n"
			}
			require.NoError(t, os.WriteFile(handler, []byte(script), 0700))
			manager := NewConsumerManager(messaging.NewBroker("us-east-1", "000000000000", 0), directory)
			entry := ConsumerEntry{Name: "owned", Type: "go", Handler: handler, TimeoutSeconds: 10, Env: map[string]string{"EVENTBUS_CHILD_PID_FILE": marker}}
			budget := 5 * time.Second
			if mode == "timeout" {
				budget = time.Second
			}
			ctx, cancel := context.WithTimeout(t.Context(), budget)
			defer cancel()
			result := make(chan error, 1)
			go func() {
				_, err := manager.invokeHandlerResult(ctx, entry, buildLambdaEvent(nil))
				result <- err
			}()
			child := consumerChildPID(t, marker)
			var err error
			select {
			case err = <-result:
			case <-time.After(6 * time.Second):
				t.Fatal("handler or inherited output pipes survived their owner deadline")
			}
			switch mode {
			case "success":
				require.NoError(t, err)
			case "timeout":
				require.ErrorContains(t, err, "timed out")
			case "retained-pipes":
				// A pipe cutoff cannot prove a complete handler response. Keep
				// the batch for retry while still stopping every descendant.
				require.ErrorIs(t, err, exec.ErrWaitDelay)
			}
			require.Eventually(t, func() bool { return !consumerChildRunning(child) }, 3*time.Second, 10*time.Millisecond, "handler descendant is still running")
		})
	}
}

func TestPythonConsumerOwnsItsHandlerDescendants(t *testing.T) {
	if _, err := exec.LookPath("uv"); err != nil {
		t.Skip("uv is required for Python consumer execution")
	}
	directory := t.TempDir()
	marker := filepath.Join(directory, "child.pid")
	cleanupConsumerChild(t, marker)
	source := `import os
import subprocess

def handler(event, context):
    child = subprocess.Popen(["sleep", "60"], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    with open(os.environ["EVENTBUS_CHILD_PID_FILE"], "w") as marker:
        marker.write(str(child.pid))
    return {}
`
	require.NoError(t, os.WriteFile(filepath.Join(directory, "owned.py"), []byte(source), 0600))
	manager := NewConsumerManager(messaging.NewBroker("us-east-1", "000000000000", 0), directory)
	environment := fixtureToolEnvironment()
	environment["EVENTBUS_CHILD_PID_FILE"] = marker
	entry := ConsumerEntry{Name: "owned-python", Type: "python", Handler: "owned.handler", TimeoutSeconds: 10, Env: environment}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err := manager.invokeHandlerResult(ctx, entry, buildLambdaEvent(nil))
	require.NoError(t, err)
	child := consumerChildPID(t, marker)
	require.Eventually(t, func() bool { return !consumerChildRunning(child) }, 3*time.Second, 10*time.Millisecond, "Python handler descendant is still running")
}

func consumerChildPID(t *testing.T, marker string) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(marker)
		if err == nil {
			pid, parseErr := strconv.Atoi(strings.TrimSpace(string(data)))
			if parseErr == nil && pid > 0 {
				return pid
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("handler never created its child marker")
	return 0
}

func consumerChildRunning(pid int) bool {
	if errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) {
		return false
	}
	if data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid)); err == nil {
		fields := strings.Fields(string(data))
		if len(fields) > 2 && fields[2] == "Z" {
			return false
		}
	}
	return true
}

func cleanupConsumerChild(t *testing.T, marker string) {
	t.Helper()
	t.Cleanup(func() {
		data, err := os.ReadFile(marker)
		if err != nil {
			return
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
		if err == nil && pid > 0 {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	})
}
