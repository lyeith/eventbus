//go:build linux

package localexec

import (
	"errors"
	"fmt"
	"syscall"
	"testing"
)

func TestLinuxGroupSignalDoesNotRetry(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want error
	}{
		{"success", nil, nil},
		{"absent", fmt.Errorf("gone: %w", syscall.ESRCH), nil},
		{"permission", syscall.EPERM, syscall.EPERM},
		{"other", syscall.EINVAL, syscall.EINVAL},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			err := signalProcessGroup(4242, func(pid int, signal syscall.Signal) error {
				calls++
				if pid != -4242 || signal != syscall.SIGKILL {
					t.Fatalf("signal pid=%d signal=%v", pid, signal)
				}
				return test.err
			})
			if calls != 1 || !errors.Is(err, test.want) {
				t.Fatalf("calls=%d err=%v, want one call and %v", calls, err, test.want)
			}
		})
	}
}
