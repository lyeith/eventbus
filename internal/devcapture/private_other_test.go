//go:build !linux && !darwin

package devcapture

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenPrivateFailsClosedOnUnsupportedPlatform(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing", "diagnostics.jsonl")
	_, err := OpenPrivate(path, "diagnostics")
	require.ErrorIs(t, err, errPrivateUnsupported)
	_, err = os.Stat(filepath.Dir(path))
	require.ErrorIs(t, err, os.ErrNotExist, "unsupported opening must not create parents")
}
