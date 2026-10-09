// Local registration/reload adapter. This is not UpdateFunctionCode or another
// AWS management API; hosts explicitly own when application source is reloaded.
package lambda

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type functionGeneration struct{ retired bool }

func resolveExecutable(name string, function Function, root string) (executableFunction, error) {
	directory := root
	if function.WorkDir != "" {
		directory = function.WorkDir
		if !filepath.IsAbs(directory) {
			directory = filepath.Join(root, directory)
		}
	}
	info, err := os.Stat(directory)
	if err != nil || !info.IsDir() {
		return executableFunction{}, fmt.Errorf("Lambda function %q: work directory must exist", name)
	}
	entry := executableFunction{name: name, runtime: function.Runtime, timeout: function.Timeout, workDir: directory, environment: maps.Clone(function.Environment), command: append([]string(nil), function.Command...), generation: &functionGeneration{}}
	if len(entry.command) == 0 {
		if entry.runtime == "python" {
			entry.command = []string{"python3"}
		} else {
			entry.command = []string{"node"}
		}
	}
	executable := entry.command[0]
	if strings.ContainsAny(executable, "/\\") && !filepath.IsAbs(executable) {
		executable = filepath.Join(directory, executable)
	}
	executable, err = exec.LookPath(executable)
	if err != nil {
		return executableFunction{}, fmt.Errorf("Lambda function %q: executable: %w", name, err)
	}
	entry.command[0], err = filepath.Abs(executable)
	if err != nil {
		return executableFunction{}, err
	}
	if entry.runtime == "python" || entry.runtime == "node" {
		entry.module, entry.exported, _ = handlerReference(function.Handler, entry.runtime)
		if !filepath.IsAbs(entry.module) {
			entry.module = filepath.Join(directory, entry.module)
		}
		if entry.runtime == "node" {
			if _, err := os.Stat(entry.module); os.IsNotExist(err) {
				var matches []string
				for _, extension := range []string{".js", ".mjs", ".cjs"} {
					candidate := entry.module + extension
					if info, err := os.Stat(candidate); err == nil && info.Mode().IsRegular() {
						matches = append(matches, candidate)
					}
				}
				if len(matches) > 1 {
					return executableFunction{}, fmt.Errorf("Lambda function %q: handler module is ambiguous; include its file extension", name)
				}
				if len(matches) == 1 {
					entry.module = matches[0]
				}
			}
		}
		info, err := os.Stat(entry.module)
		if err != nil || !info.Mode().IsRegular() {
			return executableFunction{}, fmt.Errorf("Lambda function %q: handler must be a regular file", name)
		}
		file, err := os.Open(entry.module)
		if err != nil {
			return executableFunction{}, fmt.Errorf("Lambda function %q: handler cannot be read", name)
		}
		_ = file.Close()
	}
	return entry, nil
}

// RegisterFunction atomically replaces one immutable local target snapshot.
// Existing requests retain their snapshot; already accepted Events execute the
// old snapshot fresh. Active old warm calls finish, then retire. ctx bounds only
// waiting for those workers, never reverses a published registration.
func (service *Service) RegisterFunction(ctx context.Context, name string, function Function) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	config := &Config{Functions: map[string]Function{name: function}}
	if err := config.Validate(); err != nil {
		return err
	}
	entry, err := resolveExecutable(name, config.Functions[name], service.root)
	if err != nil {
		return err
	}
	service.mu.Lock()
	if service.closed {
		service.mu.Unlock()
		return errClosed
	}
	previous, exists := service.functions[name]
	service.functions[name] = entry
	var retired []*warmWorker
	if exists && service.warm != nil {
		retired = service.warm.retireGeneration(previous.generation)
	}
	service.mu.Unlock()
	return service.joinRetiredWorkers(ctx, retired)
}

// ReloadFunction resolves the existing local target again and retires its
// imported state. It performs no source polling, hashing, or implicit reload.
func (service *Service) ReloadFunction(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	previous, _, err := service.resolveTarget(name, "")
	if err != nil {
		return err
	}
	function := Function{Runtime: previous.runtime, Command: append([]string(nil), previous.command...), Environment: maps.Clone(previous.environment), Timeout: previous.timeout, WorkDir: previous.workDir}
	if previous.module != "" {
		function.Handler = previous.module + "#" + previous.exported
	}
	entry, err := resolveExecutable(previous.name, function, service.root)
	if err != nil {
		return err
	}
	service.mu.Lock()
	if service.closed {
		service.mu.Unlock()
		return errClosed
	}
	if service.functions[previous.name].generation != previous.generation {
		service.mu.Unlock()
		return errors.New("Lambda registration changed during source reload; retry reload")
	}
	service.functions[previous.name] = entry
	var retired []*warmWorker
	if service.warm != nil {
		retired = service.warm.retireGeneration(previous.generation)
	}
	service.mu.Unlock()
	return service.joinRetiredWorkers(ctx, retired)
}

func (service *Service) joinRetiredWorkers(ctx context.Context, workers []*warmWorker) error {
	var result error
	for _, worker := range workers {
		select {
		case <-worker.retired:
			result = errors.Join(result, worker.closeErr)
		case <-ctx.Done():
			return errors.Join(result, ctx.Err())
		}
	}
	return result
}
