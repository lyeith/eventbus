//go:build linux || darwin

package cognitotrigger

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

func processGroupsSupported() error { return nil }

func ownProcessGroup(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error {
		if command.Process == nil {
			return os.ErrProcessDone
		}
		return stopProcessGroup(command)
	}
}

func stopProcessGroup(command *exec.Cmd) error {
	if command.Process == nil {
		return nil
	}
	err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return err
}
