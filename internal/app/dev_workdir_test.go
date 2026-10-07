package app

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFindProjectRoot(t *testing.T) {
	// Create a temp dir structure with pyproject.toml
	root := t.TempDir()
	sub := filepath.Join(root, "tools", "eventbus")
	require.NoError(t, os.MkdirAll(sub, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "pyproject.toml"), []byte("[project]"), 0644))

	result := findProjectRoot(sub)
	assert.Equal(t, root, result)
}

func TestFindProjectRootFallback(t *testing.T) {
	// No pyproject.toml anywhere — falls back to start dir
	tmp := t.TempDir()
	sub := filepath.Join(tmp, "deep", "nested")
	require.NoError(t, os.MkdirAll(sub, 0755))

	result := findProjectRoot(sub)
	assert.Equal(t, sub, result)
}
