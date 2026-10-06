//go:build !linux && !darwin

package lambda

import (
	"errors"
	"os/exec"
)

func processGroupsSupported() error {
	return errors.New("local Lambda runtimes require Linux or macOS process groups")
}
func ownProcessGroup(command *exec.Cmd) {}
func stopProcessGroup(command *exec.Cmd) error {
	if command.Process == nil {
		return nil
	}
	return command.Process.Kill()
}
