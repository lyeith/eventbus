package devcapture

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// OpenPrivate owns an explicitly configured private JSONL file. Unlike Open,
// it never borrows stdout or repairs an existing file's access permissions.
// Configured parent directories are trusted; only missing parents are created.
func OpenPrivate(path, name string) (*Sink, error) {
	if path == "" || path == "-" {
		return nil, fmt.Errorf("%s private log requires a configured file path", name)
	}
	flags, err := privateOpenFlags()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	// Refuse known unsafe paths before opening. The no-follow open and fd
	// validation also cover replacement between this check and opening.
	info, err := os.Lstat(path)
	if err == nil {
		if err := checkPrivateFile(info, name); err != nil {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	file, err := os.OpenFile(path, flags, 0600)
	if err != nil {
		return nil, err
	}
	info, err = file.Stat()
	if err == nil {
		err = checkPrivateFile(info, name)
	}
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	return newFileSink(file, info, name)
}

func checkPrivateFile(info os.FileInfo, name string) error {
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%s private log must be a regular file", name)
	}
	if info.Mode().Perm() != 0600 || info.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 {
		return fmt.Errorf("%s private log must have permissions 0600", name)
	}
	if err := checkPrivateOwner(info); err != nil {
		return fmt.Errorf("%s private log must be owned by the current user: %w", name, err)
	}
	return nil
}

// PathsConflict checks configured capture aliases without creating or changing
// either file. Empty paths and stdout are ordinary optional sinks, so they do
// not participate. Existing file identity catches hardlinks and symlink aliases;
// distinct missing paths are compared only by their absolute cleaned names.
func PathsConflict(first, second string) (bool, error) {
	if first == "" || first == "-" || second == "" || second == "-" {
		return false, nil
	}
	firstPath, err := filepath.Abs(first)
	if err != nil {
		return false, err
	}
	secondPath, err := filepath.Abs(second)
	if err != nil {
		return false, err
	}
	if firstPath == secondPath {
		return true, nil
	}
	firstInfo, err := os.Stat(firstPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	secondInfo, secondErr := os.Stat(secondPath)
	if secondErr != nil && !errors.Is(secondErr, os.ErrNotExist) {
		return false, secondErr
	}
	if err != nil || secondErr != nil {
		return false, nil
	}
	return os.SameFile(firstInfo, secondInfo), nil
}
