//go:build linux || darwin

package devcapture

import (
	"errors"
	"os"
	"syscall"
)

func privateOpenFlags() (int, error) {
	// No-follow applies atomically to the final configured path. Nonblocking
	// prevents a replaced FIFO from stalling before fd type validation.
	return os.O_CREATE | os.O_APPEND | os.O_RDWR | syscall.O_NOFOLLOW | syscall.O_NONBLOCK, nil
}

func checkPrivateOwner(info os.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return errors.New("file ownership cannot be confirmed")
	}
	return nil
}
