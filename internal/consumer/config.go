package consumer

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

const defaultMaxReceiveCount = 5

// validIdentifier matches valid Python identifiers (no code injection)
var validIdentifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// validatePythonHandler checks that the handler string is a valid dotted Python path.
// e.g., "platform_lib.audit.handlers.sns_writer.handler" — each part must be a valid Python identifier.
func validatePythonHandler(handler string) error {
	parts := strings.Split(handler, ".")
	if len(parts) < 2 {
		return fmt.Errorf("invalid handler format: %q (expected module.function)", handler)
	}
	for _, part := range parts {
		if !validIdentifier.MatchString(part) {
			return fmt.Errorf("invalid handler component: %q (must be valid Python identifier)", part)
		}
	}
	return nil
}

// validateGoHandler checks that the handler is a non-empty path (binary to execute).
func validateGoHandler(handler string) error {
	if handler == "" {
		return fmt.Errorf("go handler path cannot be empty")
	}
	if strings.ContainsAny(handler, ";|&`$") {
		return fmt.Errorf("invalid go handler path: %q (contains shell metacharacters)", handler)
	}
	return nil
}

type ConsumerConfig struct {
	Consumers []ConsumerEntry `yaml:"consumers"`
}

type ConsumerEntry struct {
	Name            string            `yaml:"name"`
	Type            string            `yaml:"type"` // "python" (default) or "go"
	Queue           string            `yaml:"queue"`
	DeadLetterQueue string            `yaml:"dead_letter_queue"`
	MaxReceiveCount int               `yaml:"max_receive_count"`
	Handler         string            `yaml:"handler"` // Python: dotted module path; Go: path to binary
	BatchSize       int               `yaml:"batch_size"`
	TimeoutSeconds  int               `yaml:"timeout_seconds"`
	Env             map[string]string `yaml:"env"`
}

func LoadConsumerConfig(path string) (*ConsumerConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read consumer config: %w", err)
	}

	var cfg ConsumerConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse consumer config: %w", err)
	}

	for i, c := range cfg.Consumers {
		if c.Name == "" {
			return nil, fmt.Errorf("consumer %d: name is required", i)
		}
		if c.Queue == "" {
			return nil, fmt.Errorf("consumer %q: queue is required", c.Name)
		}
		if c.Handler == "" {
			return nil, fmt.Errorf("consumer %q: handler is required", c.Name)
		}
		// Default type to "python" for backward compatibility
		if cfg.Consumers[i].Type == "" {
			cfg.Consumers[i].Type = "python"
		}
		switch cfg.Consumers[i].Type {
		case "python":
			if err := validatePythonHandler(c.Handler); err != nil {
				return nil, fmt.Errorf("consumer %q: %w", c.Name, err)
			}
		case "go":
			if err := validateGoHandler(c.Handler); err != nil {
				return nil, fmt.Errorf("consumer %q: %w", c.Name, err)
			}
		default:
			return nil, fmt.Errorf("consumer %q: unknown type %q (expected \"python\" or \"go\")", c.Name, c.Type)
		}
		if cfg.Consumers[i].BatchSize <= 0 {
			cfg.Consumers[i].BatchSize = 1
		}
		if cfg.Consumers[i].TimeoutSeconds <= 0 {
			cfg.Consumers[i].TimeoutSeconds = 60
		}
		if cfg.Consumers[i].DeadLetterQueue == "" {
			cfg.Consumers[i].DeadLetterQueue = c.Queue + "-dlq"
		}
		if cfg.Consumers[i].DeadLetterQueue == c.Queue {
			return nil, fmt.Errorf("consumer %q: dead-letter queue must differ from source queue", c.Name)
		}
		if cfg.Consumers[i].MaxReceiveCount <= 0 {
			cfg.Consumers[i].MaxReceiveCount = defaultMaxReceiveCount
		}
	}

	return &cfg, nil
}
