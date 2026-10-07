// Development harness application-root discovery for local handler recipes.
package app

import (
	"os"
	"path/filepath"
)

// findProjectRoot walks up from the given start directory looking for a pyproject.toml or go.work file.
func findProjectRoot(start string) string {
	dir, _ := filepath.Abs(start)
	for {
		if _, err := os.Stat(filepath.Join(dir, "pyproject.toml")); err == nil {
			return dir
		}
		if _, err := os.Stat(filepath.Join(dir, "go.work")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return start
		}
		dir = parent
	}
}
