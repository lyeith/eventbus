//go:build darwin

package localexec

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestDarwinGroupSignalOnlyRetriesPermissionTransition(t *testing.T) {
	denied := fmt.Errorf("denied: %w", syscall.EPERM)
	for _, test := range []struct {
		name     string
		sequence []error
		want     error
		calls    int
	}{
		{"success", []error{nil}, nil, 1},
		{"absent", []error{syscall.ESRCH}, nil, 1},
		{"other", []error{syscall.EINVAL}, syscall.EINVAL, 1},
		{"permission then live then absent", []error{denied, nil, syscall.ESRCH}, nil, 3},
		{"permission then absent", []error{denied, fmt.Errorf("gone: %w", syscall.ESRCH)}, nil, 2},
		{"permission then other", []error{denied, syscall.EINVAL}, syscall.EINVAL, 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			err := signalProcessGroupWithBudget(4242, func(pid int, signal syscall.Signal) error {
				wantSignal := syscall.Signal(0)
				if calls == 0 {
					wantSignal = syscall.SIGKILL
				}
				if pid != -4242 || signal != wantSignal {
					t.Fatalf("signal pid=%d signal=%v, want %v", pid, signal, wantSignal)
				}
				if calls >= len(test.sequence) {
					t.Fatal("unexpected extra signal")
				}
				result := test.sequence[calls]
				calls++
				return result
			}, time.Second)
			if calls != test.calls || !errors.Is(err, test.want) {
				t.Fatalf("calls=%d err=%v, want calls=%d err=%v", calls, err, test.calls, test.want)
			}
		})
	}
}

func TestDarwinGroupSignalPersistentDenialRemainsDirty(t *testing.T) {
	denied := fmt.Errorf("permission denied: %w", syscall.EPERM)
	for _, probe := range []struct {
		name string
		err  error
	}{{"denied", syscall.EPERM}, {"live", nil}} {
		for _, budget := range []time.Duration{0, 20 * time.Millisecond} {
			t.Run(probe.name+"/"+budget.String(), func(t *testing.T) {
				calls := 0
				start := time.Now()
				err := signalProcessGroupWithBudget(4242, func(pid int, signal syscall.Signal) error {
					calls++
					if pid != -4242 || (calls == 1 && signal != syscall.SIGKILL) || (calls > 1 && signal != 0) {
						t.Fatalf("destructive retry: call=%d pid=%d signal=%v", calls, pid, signal)
					}
					if calls == 1 {
						return denied
					}
					return probe.err
				}, budget)
				elapsed := time.Since(start)
				if err != denied {
					t.Fatalf("permission denial replaced: %v", err)
				}
				if budget == 0 && calls != 1 {
					t.Fatalf("zero budget retried %d calls", calls)
				}
				if budget > 0 && (calls < 2 || elapsed < budget || elapsed > time.Second) {
					t.Fatalf("unbounded or missing retry: calls=%d elapsed=%v budget=%v", calls, elapsed, budget)
				}
			})
		}
	}
}

// This uses a real Darwin zombie, not an invented errno sequence. The child
// exits while its Wait is deliberately withheld, so its private group remains
// present with no runnable member. Darwin rejects SIGKILL for that group with
// EPERM. Releasing the actual Wait lets the same cleanup owner observe ESRCH.
func TestDarwinGroupCleanupReconcilesUnreapedChild(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	command := exec.CommandContext(ctx, "/bin/sh", "-c", "exit 0")
	if err := Configure(command); err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	joined := make(chan struct{})
	var releaseOnce sync.Once
	var waitErr error
	releaseWait := func() { releaseOnce.Do(func() { close(release) }) }
	go func() {
		<-release
		waitErr = command.Wait()
		close(joined)
	}()
	t.Cleanup(func() {
		releaseWait()
		_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		<-joined
	})

	deadline := time.Now().Add(3 * time.Second)
	for {
		err := syscall.Kill(-command.Process.Pid, 0)
		if errors.Is(err, syscall.EPERM) {
			break
		}
		if err != nil || time.Now().After(deadline) {
			t.Fatalf("unreaped private group never became a Darwin zombie: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	if err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL); !errors.Is(err, syscall.EPERM) {
		t.Fatalf("negative group signal for unreaped child: %v, want EPERM", err)
	}
	if err := syscall.Kill(command.Process.Pid, 0); err != nil {
		t.Fatalf("positive signal for the same unreaped child: %v", err)
	}
	// Keep the zombie present while the public cleanup owner enters its
	// reconciliation loop. The old one-shot Cleanup returns EPERM immediately;
	// the corrected owner waits for this actual reaping before returning clean.
	reaper := time.AfterFunc(50*time.Millisecond, releaseWait)
	defer reaper.Stop()
	cleanupErr := Cleanup(command)
	releaseWait()
	select {
	case <-joined:
	case <-time.After(2 * time.Second):
		t.Fatal("actual Wait did not reap the child")
	}
	if waitErr != nil {
		t.Fatalf("native child exit: %v", waitErr)
	}
	if cleanupErr != nil {
		t.Fatalf("cleanup failed before delayed actual Wait: %v", cleanupErr)
	}
	t.Log("native negative-PGID SIGKILL returned EPERM for an unreaped exited child; delayed Wait reaped it and public Cleanup reconciled actual absence")
	if err := Cleanup(command); err != nil {
		t.Fatalf("repeated public cleanup after Wait: %v", err)
	}
}
