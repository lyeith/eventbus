//go:build darwin

package localexec

import (
	"errors"
	"syscall"
	"time"
)

func signalProcessGroup(pid int, kill func(int, syscall.Signal) error) error {
	return signalProcessGroupWithBudget(pid, kill, time.Second)
}

// Darwin's killpg1 can return EPERM for a still-existing group with only
// unreaped zombies. Cancellation and explicit cleanup can race into this
// transition. Probe actual absence within the existing process/pipe cleanup
// budget, without issuing another destructive signal to a potentially reused
// group. Persistent permission denial remains an ownership failure. The caller
// owns Wait, which must run independently to reap the directly launched process.
func signalProcessGroupWithBudget(pid int, kill func(int, syscall.Signal) error, budget time.Duration) error {
	deadline := time.Now().Add(budget)
	denied := kill(-pid, syscall.SIGKILL)
	if errors.Is(denied, syscall.ESRCH) {
		return nil
	}
	if !errors.Is(denied, syscall.EPERM) {
		return denied
	}
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return denied
		}
		time.Sleep(min(10*time.Millisecond, remaining))
		err := kill(-pid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		if err != nil && !errors.Is(err, syscall.EPERM) {
			return errors.Join(denied, err)
		}
	}
}
