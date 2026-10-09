package architecture_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const internalPrefix = "github.com/lyeith/eventbus/internal/"

// Classes express ownership, rather than recording today's full import graph.
// New state owners must be classified explicitly; wiring belongs in app.
var ownerClasses = map[string]string{
	"app":     "composition",
	"server":  "dispatcher",
	"cognito": "service", "messaging": "service", "ses": "service",
	"firehose": "service", "scheduler": "service", "ssm": "service",
	"secrets": "service", "lambda": "service", "eventsource": "service",
	"gateway": "gateway", "consumer": "consumer",
	"cognitotrigger": "runner", "devquiescence": "coordinator",
	"awsprotocol": "shared", "devcapture": "shared", "devactivity": "shared",
	"localexec": "shared", "sqsevent": "shared", "testperf": "shared",
}

func allowedInternalImport(owner, dependency string) bool {
	class, known := ownerClasses[owner]
	dependencyClass, dependencyKnown := ownerClasses[dependency]
	if !known || !dependencyKnown {
		return false
	}
	if owner == dependency || class == "composition" {
		return true
	}
	switch class {
	case "dispatcher":
		return dependency == "awsprotocol"
	case "gateway":
		return dependencyClass == "shared" || dependency == "devquiescence"
	case "consumer":
		// QueueBroker currently consumes messaging's detached queue/message types.
		return dependencyClass == "shared" || dependency == "messaging"
	default:
		return dependencyClass == "shared"
	}
}

func TestProductionOwnershipBoundaries(t *testing.T) {
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	module, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil || !strings.HasPrefix(string(module), "module github.com/lyeith/eventbus\n") {
		t.Fatalf("expected EventBus repository root %s: %v", root, err)
	}
	// Parse every platform/build-tag source. Tests may compose real services.
	err = filepath.WalkDir(filepath.Join(root, "internal"), func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		relative, err := filepath.Rel(filepath.Join(root, "internal"), path)
		if err != nil {
			return err
		}
		owner := strings.Split(filepath.ToSlash(relative), "/")[0]
		if _, known := ownerClasses[owner]; !known {
			t.Errorf("%s: classify new production owner %q", relative, owner)
			return nil
		}
		checkImports(t, path, func(dependency string) bool { return allowedInternalImport(owner, dependency) })
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	checkImports(t, filepath.Join(root, "main.go"), func(dependency string) bool { return dependency == "app" })
	err = filepath.WalkDir(filepath.Join(root, "cmd", "gateway"), func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() && strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go") {
			checkImports(t, path, func(dependency string) bool {
				return dependency == "gateway" || dependency == "devquiescence" || ownerClasses[dependency] == "shared"
			})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func checkImports(t *testing.T, path string, allowed func(string) bool) {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
	if err != nil {
		t.Errorf("%s: %v", path, err)
		return
	}
	for _, spec := range file.Imports {
		importPath, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			t.Errorf("%s: %v", path, err)
			continue
		}
		if !strings.HasPrefix(importPath, internalPrefix) {
			continue
		}
		dependency := strings.Split(strings.TrimPrefix(importPath, internalPrefix), "/")[0]
		if !allowed(dependency) {
			t.Errorf("%s: internal import %q crosses the documented ownership boundary", path, importPath)
		}
	}
}

func TestOwnershipRules(t *testing.T) {
	for _, test := range []struct {
		owner, dependency string
		allowed           bool
	}{
		{"app", "cognito", true}, {"server", "awsprotocol", true},
		{"server", "cognito", false}, {"cognito", "localexec", true},
		{"cognito", "messaging", false}, {"lambda", "app", false},
		{"messaging", "consumer", false}, {"consumer", "messaging", true},
		{"consumer", "lambda", false}, {"gateway", "devquiescence", true},
		{"gateway", "cognito", false}, {"cognitotrigger", "cognito", false},
		{"devcapture", "lambda", false}, {"devquiescence", "devactivity", true},
		{"newservice", "awsprotocol", false}, {"app", "newservice", false},
		{"messaging", "messaging", true},
	} {
		t.Run(test.owner+"/"+test.dependency, func(t *testing.T) {
			if got := allowedInternalImport(test.owner, test.dependency); got != test.allowed {
				t.Fatalf("allowed=%v, want %v", got, test.allowed)
			}
		})
	}
}
