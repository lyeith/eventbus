//go:build !linux && !darwin

package cognitotrigger

import (
	"errors"
	"os/exec"
)

func processGroupsSupported() error {
	return errors.New("Cognito trigger execution requires Linux or macOS process groups")
}
func ownProcessGroup(command *exec.Cmd)        {}
func stopProcessGroup(command *exec.Cmd) error { return nil }
