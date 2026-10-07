//go:build !linux && !darwin

package localexec

import "os/exec"

func Supported() error { return ErrUnsupported }

func Configure(command *exec.Cmd) error { return ErrUnsupported }
func Cleanup(command *exec.Cmd) error   { return ErrUnsupported }
