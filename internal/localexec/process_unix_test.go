//go:build linux || darwin

package localexec

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
)

func TestProcessGroupOwnsDescendantsAndBoundsRetainedPipes(t *testing.T) {
	for _, mode := range []string{"success", "cancellation", "retained-pipes"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			marker := filepath.Join(t.TempDir(), "child.pid")
			script := `sleep 60 >/dev/null 2>&1 & echo "$!" > "$1"; exit 0`
			if mode == "cancellation" {
				script = `sleep 60 & echo "$!" > "$1"; wait`
			} else if mode == "retained-pipes" {
				script = `sleep 60 & echo "$!" > "$1"; exit 0`
			}
			command := exec.CommandContext(ctx, "/bin/sh", "-c", script, "owned-handler", marker)
			// Non-file outputs make os/exec own pipes which grandchildren can
			// retain. Their EOF must not keep Wait blocked indefinitely.
			command.Stdout, command.Stderr = new(strings.Builder), new(strings.Builder)
			if err := Configure(command); err != nil {
				t.Fatal(err)
			}
			if command.WaitDelay != time.Second {
				t.Fatalf("unbounded retained pipes: WaitDelay=%s", command.WaitDelay)
			}
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			wait := make(chan error, 1)
			go func() { wait <- command.Wait() }()
			t.Cleanup(func() { cancel(); _ = Cleanup(command) })
			child := readChildPID(t, marker)
			t.Cleanup(func() { _ = syscall.Kill(child, syscall.SIGKILL) })
			if mode == "cancellation" {
				cancel()
			}
			var err error
			select {
			case err = <-wait:
			case <-time.After(5 * time.Second):
				t.Fatal("Wait did not join the direct child and retained pipes")
			}
			if mode == "success" && err != nil {
				t.Fatalf("successful command: %v", err)
			}
			if mode == "retained-pipes" && !errors.Is(err, exec.ErrWaitDelay) {
				t.Fatalf("retained pipes: got %v, want ErrWaitDelay", err)
			}
			if mode == "cancellation" && err == nil {
				t.Fatal("canceled command returned success")
			}
			if err := Cleanup(command); err != nil {
				t.Fatalf("cleanup: %v", err)
			}
			if err := Cleanup(command); err != nil {
				t.Fatalf("repeated cleanup: %v", err)
			}
			waitChildStopped(t, child)
		})
	}
}

func readChildPID(t *testing.T, marker string) int {
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

func waitChildStopped(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if !childRunning(pid) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("handler descendant %d still runs", pid)
}

func childRunning(pid int) bool {
	if errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) {
		return false
	}
	// A Linux container may retain a killed, adopted child as a zombie. It no
	// longer executes or owns pipes and is waiting for the container's reaper.
	if data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid)); err == nil {
		fields := strings.Fields(string(data))
		if len(fields) > 2 && fields[2] == "Z" {
			return false
		}
	}
	return true
}
