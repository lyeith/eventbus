//go:build linux || darwin

package localexec

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestTrackedOutputDetectsRetainedPipeDespiteExitStatus(t *testing.T) {
	for _, stream := range []string{"stdout", "stderr"} {
		for _, status := range []int{0, 7} {
			t.Run(fmt.Sprintf("%s_exit%d", stream, status), func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				marker := filepath.Join(t.TempDir(), "child.pid")
				redirect := "2>/dev/null"
				if stream == "stderr" {
					redirect = ">/dev/null"
				}
				command := exec.CommandContext(ctx, "/bin/sh", "-c", fmt.Sprintf("sleep 60 %s & echo \"$!\" > \"$1\"; exit %d", redirect, status), "copy-fixture", marker)
				stdout, stderr := NewTrackedOutput(new(strings.Builder)), NewTrackedOutput(new(strings.Builder))
				command.Stdout, command.Stderr = stdout, stderr
				if err := Configure(command); err != nil {
					t.Fatal(err)
				}
				if err := command.Start(); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { cancel(); _ = Cleanup(command) })
				child := readChildPID(t, marker)
				err := command.Wait()
				if status == 7 {
					var exited *exec.ExitError
					if !errors.As(err, &exited) || exited.ExitCode() != 7 || errors.Is(err, exec.ErrWaitDelay) {
						t.Fatalf("native nonzero exit changed instead of masking copy failure: %v", err)
					}
				} else if !errors.Is(err, exec.ErrWaitDelay) {
					t.Fatalf("successful exit must retain native WaitDelay error: %v", err)
				}
				held, normal := stdout, stderr
				if stream == "stderr" {
					held, normal = stderr, stdout
				}
				if !errors.Is(held.Err(), exec.ErrWaitDelay) || normal.Err() != nil {
					t.Fatalf("independent stream evidence was lost: held=%v normal=%v", held.Err(), normal.Err())
				}
				if err := Cleanup(command); err != nil {
					t.Fatal(err)
				}
				waitChildStopped(t, child)
			})
		}
	}
}

func TestTrackedOutputsKeepSequentialMergedStreamOrdering(t *testing.T) {
	command := exec.CommandContext(t.Context(), "/bin/sh", "-c", "i=0; while [ \"$i\" -lt 1000 ]; do printf a; printf b >&2; i=$((i+1)); done")
	merged := new(strings.Builder)
	stdout, stderr := NewTrackedOutputs(merged, merged)
	command.Stdout, command.Stderr = stdout, stderr
	if err := Configure(command); err != nil {
		t.Fatal(err)
	}
	if err := command.Run(); err != nil {
		t.Fatal(err)
	}
	if err := Cleanup(command); err != nil {
		t.Fatal(err)
	}
	if merged.String() != strings.Repeat("ab", 1000) || stdout.Err() != nil || stderr.Err() != nil {
		t.Fatalf("native shared-pipe ordering changed: bytes=%d stdout=%v stderr=%v", merged.Len(), stdout.Err(), stderr.Err())
	}
}
