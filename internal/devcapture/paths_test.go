package devcapture

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPathsConflictChecksCleanAbsoluteIdentityWithoutCreatingFiles(t *testing.T) {
	base := t.TempDir()
	path := filepath.Join(base, "missing.jsonl")
	working, err := os.Getwd()
	require.NoError(t, err)
	relative, err := filepath.Rel(working, path)
	require.NoError(t, err)
	for _, other := range []string{path, relative, base + string(os.PathSeparator) + "." + string(os.PathSeparator) + "missing.jsonl"} {
		conflict, err := PathsConflict(path, other)
		require.NoError(t, err)
		require.True(t, conflict, other)
	}
	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestPathsConflictIgnoresOptionalAndStdoutSinks(t *testing.T) {
	for _, optional := range []string{"", "-"} {
		// No filesystem access is required when one sink is unconfigured.
		for _, paths := range [][2]string{{optional, "\x00invalid"}, {"\x00invalid", optional}} {
			conflict, err := PathsConflict(paths[0], paths[1])
			require.NoError(t, err)
			require.False(t, conflict)
		}
	}
}

func TestPathsConflictDistinguishesFilesAndMissingPaths(t *testing.T) {
	base := t.TempDir()
	first, second, missing := filepath.Join(base, "first.jsonl"), filepath.Join(base, "second.jsonl"), filepath.Join(base, "missing.jsonl")
	require.NoError(t, os.WriteFile(first, nil, 0600))
	require.NoError(t, os.WriteFile(second, nil, 0600))
	for _, paths := range [][2]string{{first, second}, {first, missing}, {missing, first}, {missing, filepath.Join(base, "also-missing.jsonl")}} {
		conflict, err := PathsConflict(paths[0], paths[1])
		require.NoError(t, err)
		require.False(t, conflict)
	}
}

func TestPathsConflictPropagatesUninspectablePaths(t *testing.T) {
	path := filepath.Join(t.TempDir(), "capture.jsonl")
	for _, paths := range [][2]string{{path, "\x00invalid"}, {"\x00invalid", path}} {
		conflict, err := PathsConflict(paths[0], paths[1])
		require.Error(t, err)
		require.False(t, conflict)
	}
}
