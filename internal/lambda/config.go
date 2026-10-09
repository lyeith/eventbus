// Package lambda runs explicitly registered application functions through the
// AWS Lambda Invoke and Runtime API protocols. Apps own their handler code.
package lambda

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"
)

// Config is the resolved local recipe used by this service. Native protocol
// fields retain AWS semantics; file loading belongs to dev_config.go.
type Config struct {
	Functions      map[string]Function   `yaml:"functions"`
	DevAsync       *DevAsyncConfig       `yaml:"dev_async,omitempty"`
	DevActivity    DevActivity           `yaml:"-"`
	DevDiagnostics *DevDiagnosticsConfig `yaml:"dev_diagnostics,omitempty"`
}

// Function declares one local function. Command is an argv vector, never a
// shell command. Python and Node commands default to python3 and node.
type Function struct {
	Runtime     string            `yaml:"runtime"`
	Command     []string          `yaml:"command"`
	Handler     string            `yaml:"handler"`
	Environment map[string]string `yaml:"environment"`
	Timeout     time.Duration     `yaml:"timeout"`
	WorkDir     string            `yaml:"work_dir"`
}

var functionARNReference = regexp.MustCompile(`^arn:aws(?:-[a-z0-9-]+)?:lambda:[a-z]{2}(?:-[a-z0-9]+)+-[0-9]+:[0-9]{12}:function:(.+)$`)
var functionName = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}(?::[A-Za-z0-9_-]{1,128}|:\$LATEST)?$`)
var environmentName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
var exportedName = regexp.MustCompile(`^[A-Za-z_$][A-Za-z0-9_$]*$`)

func (config *Config) Validate() error {
	if config == nil || len(config.Functions) == 0 {
		return errors.New("Lambda configuration requires functions")
	}
	if err := validateDevAsync(config.DevAsync); err != nil {
		return err
	}
	if err := validateDevDiagnostics(config.DevDiagnostics); err != nil {
		return err
	}
	for name, function := range config.Functions {
		if !functionName.MatchString(name) {
			return fmt.Errorf("invalid Lambda function name %q", name)
		}
		switch function.Runtime {
		case "provided", "command":
			if len(function.Command) == 0 || function.Handler != "" {
				return fmt.Errorf("Lambda function %q: %s requires command and no handler", name, function.Runtime)
			}
		case "python", "node":
			if _, _, err := handlerReference(function.Handler, function.Runtime); err != nil {
				return fmt.Errorf("Lambda function %q: %w", name, err)
			}
		default:
			return fmt.Errorf("Lambda function %q: runtime must be provided, python, node or command", name)
		}
		for _, argument := range function.Command {
			if argument == "" || strings.ContainsRune(argument, 0) {
				return fmt.Errorf("Lambda function %q: command arguments must be nonempty and contain no NUL", name)
			}
		}
		if strings.ContainsRune(function.WorkDir, 0) {
			return fmt.Errorf("Lambda function %q: invalid work_dir", name)
		}
		if function.Timeout == 0 {
			function.Timeout = 10 * time.Second
		}
		if function.Timeout < time.Millisecond || function.Timeout > 900*time.Second {
			return fmt.Errorf("Lambda function %q: timeout must be 1ms..900s", name)
		}
		for key, value := range function.Environment {
			if !environmentName.MatchString(key) || strings.ContainsRune(value, 0) {
				return fmt.Errorf("Lambda function %q: invalid environment entry", name)
			}
		}
		config.Functions[name] = function
	}
	return nil
}

// References support AWS's file.export form and an explicit file#export form.
// For Python module.handler, the module's .py extension is inferred.
func handlerReference(reference, runtime string) (module, exported string, err error) {
	if strings.ContainsRune(reference, 0) {
		return "", "", errors.New("handler must be a file.export or file#export reference")
	}
	module, exported, found := strings.Cut(reference, "#")
	if !found {
		position := strings.LastIndex(reference, ".")
		if position <= 0 {
			return "", "", errors.New("handler must be a file.export or file#export reference")
		}
		module, exported = reference[:position], reference[position+1:]
	}
	if strings.TrimSpace(module) == "" || !exportedName.MatchString(exported) {
		return "", "", errors.New("handler must be a file.export or file#export reference")
	}
	if runtime == "python" && !strings.HasSuffix(module, ".py") {
		module = strings.ReplaceAll(module, ".", string(os.PathSeparator)) + ".py"
	}
	return module, exported, nil
}
