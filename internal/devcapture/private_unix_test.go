//go:build linux || darwin

package devcapture

import (
	"bytes"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOpenPrivateOwnsAppendOnlyFileAndCreatesPrivateParents(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "configured")
	require.NoError(t, os.Mkdir(parent, 0755))
	require.NoError(t, os.Chmod(parent, 0755))
	path := filepath.Join(parent, "one", "two", "diagnostics.jsonl")
	sink, err := OpenPrivate(path, "diagnostics")
	require.NoError(t, err)
	t.Cleanup(func() { _ = sink.Close() })
	require.NoError(t, sink.Append(map[string]any{"request": "first", "log": "caught\nprivate exception"}))
	original, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, 1, bytes.Count(original, []byte{'\n'}))
	for _, directory := range []string{filepath.Join(parent, "one"), filepath.Dir(path)} {
		info, err := os.Stat(directory)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0700), info.Mode().Perm())
	}
	info, err := os.Stat(parent)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0755), info.Mode().Perm(), "configured parents are not repaired")
	info, err = os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0600), info.Mode().Perm())
	require.NoError(t, sink.Err())
	file := sink.writer.(*os.File)
	require.NoError(t, sink.Close())
	_, err = file.WriteString("late")
	require.ErrorIs(t, err, os.ErrClosed)
	require.NoError(t, sink.Close())

	next, err := OpenPrivate(path, "diagnostics")
	require.NoError(t, err)
	t.Cleanup(func() { _ = next.Close() })
	require.NoError(t, next.Append(map[string]any{"request": "second"}))
	require.NoError(t, next.Close())
	final, err := os.ReadFile(path)
	require.NoError(t, err)
	require.True(t, bytes.HasPrefix(final, original), "reopening must not truncate")
	require.Equal(t, 2, bytes.Count(final, []byte{'\n'}))
}

func TestOpenPrivateRejectsUnconfiguredAndNonRegularFiles(t *testing.T) {
	for _, path := range []string{"", "-"} {
		_, err := OpenPrivate(path, "diagnostics")
		require.ErrorContains(t, err, "configured file path")
	}
	base := t.TempDir()
	target := filepath.Join(base, "target.jsonl")
	original := []byte("{\"private\":true}\n")
	require.NoError(t, os.WriteFile(target, original, 0600))
	link := filepath.Join(base, "symlink.jsonl")
	require.NoError(t, os.Symlink(target, link))
	dangling := filepath.Join(base, "dangling.jsonl")
	danglingTarget := filepath.Join(base, "absent.jsonl")
	require.NoError(t, os.Symlink(danglingTarget, dangling))
	for _, path := range []string{base, os.DevNull, link, dangling} {
		_, err := OpenPrivate(path, "diagnostics")
		require.ErrorContains(t, err, "regular file", path)
	}
	after, err := os.ReadFile(target)
	require.NoError(t, err)
	require.Equal(t, original, after)
	_, err = os.Lstat(danglingTarget)
	require.ErrorIs(t, err, os.ErrNotExist, "never create through a dangling symlink")
}

func TestOpenPrivateRefusesUnsafeModesWithoutRepairOrTruncation(t *testing.T) {
	for _, mode := range []os.FileMode{0644, 0620, 0601, 0700, 0400, 0200, 0000, 0600 | os.ModeSetuid, 0600 | os.ModeSetgid, 0600 | os.ModeSticky} {
		t.Run(mode.String(), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "existing.jsonl")
			original := []byte("{\"retained\":true}\n")
			require.NoError(t, os.WriteFile(path, original, 0600))
			require.NoError(t, os.Chmod(path, mode))
			before, err := os.Stat(path)
			require.NoError(t, err)
			if before.Mode().Perm() == 0600 && before.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) == 0 {
				t.Skip("filesystem does not preserve this special permission")
			}
			_, err = OpenPrivate(path, "diagnostics")
			require.ErrorContains(t, err, "permissions 0600")
			after, err := os.Stat(path)
			require.NoError(t, err)
			require.Equal(t, before.Mode(), after.Mode())
			require.Equal(t, before.Size(), after.Size())
			require.NoError(t, os.Chmod(path, 0600))
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, original, data)
		})
	}
}

func TestOpenPrivateRefusesIncompleteRecordWithoutTruncation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "partial.jsonl")
	original := []byte("{\"unfinished\":")
	require.NoError(t, os.WriteFile(path, original, 0600))
	_, err := OpenPrivate(path, "diagnostics")
	require.ErrorContains(t, err, "incomplete record")
	content, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, original, content)
}

type privateOwnerInfo struct {
	os.FileInfo
	stat any
}

func (info privateOwnerInfo) Sys() any { return info.stat }

func TestPrivateCaptureRejectsForeignOrUnprovableOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "owned.jsonl")
	require.NoError(t, os.WriteFile(path, nil, 0600))
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.NoError(t, checkPrivateFile(info, "diagnostics"))
	foreign := &syscall.Stat_t{Uid: uint32(os.Geteuid()) ^ 1}
	require.ErrorContains(t, checkPrivateFile(privateOwnerInfo{FileInfo: info, stat: foreign}, "diagnostics"), "owned by the current user")
	require.ErrorContains(t, checkPrivateFile(privateOwnerInfo{FileInfo: info}, "diagnostics"), "owned by the current user")
}

func TestPrivateOpenAtomicallyRejectsSymlinkAndDoesNotBlockOnFIFO(t *testing.T) {
	base := t.TempDir()
	target := filepath.Join(base, "target.jsonl")
	require.NoError(t, os.WriteFile(target, nil, 0600))
	link := filepath.Join(base, "replacement.jsonl")
	require.NoError(t, os.Symlink(target, link))
	flags, err := privateOpenFlags()
	require.NoError(t, err)
	file, err := os.OpenFile(link, flags, 0600)
	if file != nil {
		_ = file.Close()
	}
	require.Error(t, err, "the open must refuse a symlink without relying on Lstat")

	fifo := filepath.Join(base, "replacement.fifo")
	require.NoError(t, syscall.Mkfifo(fifo, 0600))
	done := make(chan error, 1)
	go func() {
		// Exercise the flags directly: the FIFO may replace a previously
		// validated path before opening, so this must not wait for a peer.
		file, err := os.OpenFile(fifo, flags, 0600)
		if err == nil {
			info, statErr := file.Stat()
			if statErr == nil {
				err = checkPrivateFile(info, "diagnostics")
			} else {
				err = statErr
			}
			_ = file.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		require.Error(t, err, "opening a FIFO must refuse it or fail its type validation")
	case <-time.After(time.Second):
		t.Fatal("private file open waited for a FIFO peer")
	}
}

func TestPathsConflictDetectsExistingHardlinkAndSymlinkAliases(t *testing.T) {
	base := t.TempDir()
	private := filepath.Join(base, "private.jsonl")
	require.NoError(t, os.WriteFile(private, nil, 0600))
	hardlink := filepath.Join(base, "public.jsonl")
	require.NoError(t, os.Link(private, hardlink))
	symlink := filepath.Join(base, "alias.jsonl")
	require.NoError(t, os.Symlink(private, symlink))
	for _, other := range []string{hardlink, symlink} {
		conflict, err := PathsConflict(private, other)
		require.NoError(t, err)
		require.True(t, conflict, other)
		conflict, err = PathsConflict(other, private)
		require.NoError(t, err)
		require.True(t, conflict, other)
	}
}

func TestPathsConflictDetectsParentSymlinkAliasAfterPrivateOpen(t *testing.T) {
	base := t.TempDir()
	parent := filepath.Join(base, "configured")
	require.NoError(t, os.Mkdir(parent, 0700))
	alias := filepath.Join(base, "parent-alias")
	require.NoError(t, os.Symlink(parent, alias))
	private, public := filepath.Join(parent, "capture.jsonl"), filepath.Join(alias, "capture.jsonl")
	conflict, err := PathsConflict(private, public)
	require.NoError(t, err)
	require.False(t, conflict, "distinct absent paths use only absolute clean identity")
	sink, err := OpenPrivate(private, "diagnostics")
	require.NoError(t, err)
	t.Cleanup(func() { _ = sink.Close() })
	conflict, err = PathsConflict(private, public)
	require.NoError(t, err)
	require.True(t, conflict, "consumers recheck aliases after the private file exists")
}
