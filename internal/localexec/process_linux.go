//go:build linux

package localexec

import (
	"errors"
	"syscall"
)

func signalProcessGroup(pid int, kill func(int, syscall.Signal) error) error {
	err := kill(-pid, syscall.SIGKILL)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return err
}
