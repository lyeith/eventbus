//go:build !linux && !darwin

package localexec

import (
	"context"
	"errors"
	"os/exec"
	"testing"
)

func TestUnsupportedPlatformRefusesProcessOwnership(t *testing.T) {
	if !errors.Is(Supported(), ErrUnsupported) {
		t.Fatal("unsupported process groups were admitted")
	}
	command := exec.CommandContext(context.Background(), "not-started")
	if !errors.Is(Configure(command), ErrUnsupported) {
		t.Fatal("unsupported command was configured")
	}
	if command.Process != nil {
		t.Fatal("refusal started a child")
	}
}
