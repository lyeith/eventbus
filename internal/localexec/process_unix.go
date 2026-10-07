//go:build linux || darwin

package localexec

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
	"time"
)

func Supported() error { return nil }

// Configure must run before Start on a CommandContext command. Cancellation stops the whole process group,
// and WaitDelay bounds pipes retained by descendants after the direct child exits.
func Configure(command *exec.Cmd) error {
	if command.SysProcAttr == nil {
		command.SysProcAttr = &syscall.SysProcAttr{}
	}
	command.SysProcAttr.Setpgid = true
	command.SysProcAttr.Pgid = 0
	command.WaitDelay = time.Second
	command.Cancel = func() error {
		if command.Process == nil {
			return os.ErrProcessDone
		}
		return Cleanup(command)
	}
	return nil
}

// Cleanup stops the process group, including descendants left by successful
// invocations. The caller must also reap the directly launched child with Wait.
func Cleanup(command *exec.Cmd) error {
	if command.Process == nil {
		return nil
	}
	err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return err
}
