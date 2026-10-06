package consumer

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConsumerConfig(t *testing.T) {
	yaml := `consumers:
  - name: audit-writer
    queue: local-audit-events-queue
    handler: platform_lib.audit.handlers.sns_writer.handler
    batch_size: 10
    timeout_seconds: 60
    env:
      DYNAMODB_TABLE: audit-events
      DYNAMODB_ENDPOINT_URL: http://localhost:8000

  - name: import-processor
    type: go
    queue: local-data-hub-import-processing
    handler: build/import-processor
    batch_size: 1
    timeout_seconds: 120
    env:
      DYNAMODB_TABLE: local-data-hub
`
	path := filepath.Join(t.TempDir(), "consumers.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0644))

	cfg, err := LoadConsumerConfig(path)
	require.NoError(t, err)
	require.Len(t, cfg.Consumers, 2)

	assert.Equal(t, "audit-writer", cfg.Consumers[0].Name)
	assert.Equal(t, "local-audit-events-queue", cfg.Consumers[0].Queue)
	assert.Equal(t, "platform_lib.audit.handlers.sns_writer.handler", cfg.Consumers[0].Handler)
	assert.Equal(t, 10, cfg.Consumers[0].BatchSize)
	assert.Equal(t, 60, cfg.Consumers[0].TimeoutSeconds)
	assert.Equal(t, "local-audit-events-queue-dlq", cfg.Consumers[0].DeadLetterQueue)
	assert.Equal(t, defaultMaxReceiveCount, cfg.Consumers[0].MaxReceiveCount)
	assert.Equal(t, "audit-events", cfg.Consumers[0].Env["DYNAMODB_TABLE"])

	assert.Equal(t, "import-processor", cfg.Consumers[1].Name)
	assert.Equal(t, "go", cfg.Consumers[1].Type)
	assert.Equal(t, "build/import-processor", cfg.Consumers[1].Handler)
	assert.Equal(t, 1, cfg.Consumers[1].BatchSize)
	assert.Equal(t, 120, cfg.Consumers[1].TimeoutSeconds)
}

func TestLoadConsumerConfigDefaults(t *testing.T) {
	yaml := `consumers:
  - name: minimal
    queue: test-queue
    handler: my_module.handler
`
	path := filepath.Join(t.TempDir(), "consumers.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0644))

	cfg, err := LoadConsumerConfig(path)
	require.NoError(t, err)
	require.Len(t, cfg.Consumers, 1)

	assert.Equal(t, "python", cfg.Consumers[0].Type, "type defaults to python")
	assert.Equal(t, 1, cfg.Consumers[0].BatchSize)
	assert.Equal(t, 60, cfg.Consumers[0].TimeoutSeconds)
	assert.Equal(t, "test-queue-dlq", cfg.Consumers[0].DeadLetterQueue)
	assert.Equal(t, defaultMaxReceiveCount, cfg.Consumers[0].MaxReceiveCount)
}

func TestLoadConsumerConfigRejectsSourceAsDeadLetterQueue(t *testing.T) {
	yaml := `consumers:
  - name: invalid
    queue: same-queue
    dead_letter_queue: same-queue
    handler: my_module.handler
`
	path := filepath.Join(t.TempDir(), "consumers.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0644))

	_, err := LoadConsumerConfig(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must differ")
}

func TestLoadConsumerConfigGoType(t *testing.T) {
	yaml := `consumers:
  - name: audit-writer-go
    type: go
    queue: local-audit-events-queue
    handler: build/audit-sns-writer
    batch_size: 10
    timeout_seconds: 60
    env:
      DYNAMODB_TABLE: audit-events
      DYNAMODB_ENDPOINT_URL: http://localhost:8000
`
	path := filepath.Join(t.TempDir(), "consumers.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0644))

	cfg, err := LoadConsumerConfig(path)
	require.NoError(t, err)
	require.Len(t, cfg.Consumers, 1)

	assert.Equal(t, "go", cfg.Consumers[0].Type)
	assert.Equal(t, "build/audit-sns-writer", cfg.Consumers[0].Handler)
	assert.Equal(t, 10, cfg.Consumers[0].BatchSize)
}

func TestLoadConsumerConfigMixedTypes(t *testing.T) {
	yaml := `consumers:
  - name: python-consumer
    queue: queue-1
    handler: my_module.handler

  - name: go-consumer
    type: go
    queue: queue-2
    handler: build/my-handler
`
	path := filepath.Join(t.TempDir(), "consumers.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0644))

	cfg, err := LoadConsumerConfig(path)
	require.NoError(t, err)
	require.Len(t, cfg.Consumers, 2)

	assert.Equal(t, "python", cfg.Consumers[0].Type)
	assert.Equal(t, "go", cfg.Consumers[1].Type)
}

func TestLoadConsumerConfigUnknownType(t *testing.T) {
	yaml := `consumers:
  - name: bad
    type: rust
    queue: test-queue
    handler: some_handler
`
	path := filepath.Join(t.TempDir(), "consumers.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0644))

	_, err := LoadConsumerConfig(path)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown type")
}

func TestLoadConsumerConfigGoHandlerValidation(t *testing.T) {
	yaml := `consumers:
  - name: bad-go
    type: go
    queue: test-queue
    handler: "path;injection"
`
	path := filepath.Join(t.TempDir(), "consumers.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0644))

	_, err := LoadConsumerConfig(path)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "shell metacharacters")
}

func TestLoadConsumerConfigEmpty(t *testing.T) {
	yaml := `consumers: []`
	path := filepath.Join(t.TempDir(), "consumers.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0644))

	cfg, err := LoadConsumerConfig(path)
	require.NoError(t, err)
	assert.Empty(t, cfg.Consumers)
}

func TestLoadConsumerConfigNotFound(t *testing.T) {
	_, err := LoadConsumerConfig("/nonexistent/consumers.yaml")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "read consumer config")
}

func TestLoadConsumerConfigMissingName(t *testing.T) {
	yaml := `consumers:
  - queue: test-queue
    handler: my_module.handler
`
	path := filepath.Join(t.TempDir(), "consumers.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0644))

	_, err := LoadConsumerConfig(path)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "name is required")
}

func TestLoadConsumerConfigMissingHandler(t *testing.T) {
	yaml := `consumers:
  - name: test
    queue: test-queue
`
	path := filepath.Join(t.TempDir(), "consumers.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0644))

	_, err := LoadConsumerConfig(path)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "handler is required")
}

func TestLoadConsumerConfigInvalidHandler(t *testing.T) {
	yaml := `consumers:
  - name: test
    queue: test-queue
    handler: "os; os.system('evil')#.handler"
`
	path := filepath.Join(t.TempDir(), "consumers.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0644))

	_, err := LoadConsumerConfig(path)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid handler component")
}

func TestValidatePythonHandler(t *testing.T) {
	tests := []struct {
		handler string
		valid   bool
	}{
		{"platform_lib.audit.handlers.sns_writer.handler", true},
		{"my_module.handler", true},
		{"a.b.c.d.e", true},
		{"no_dot", false},                          // must have at least 2 parts
		{"os; import sys#.handler", false},         // injection attempt
		{"__import__('os').system.handler", false}, // another injection
		{".leading_dot.handler", false},            // empty first part
		{"module..handler", false},                 // empty middle part
	}

	for _, tc := range tests {
		t.Run(tc.handler, func(t *testing.T) {
			err := validatePythonHandler(tc.handler)
			if tc.valid {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestValidateGoHandler(t *testing.T) {
	tests := []struct {
		handler string
		valid   bool
	}{
		{"build/audit-sns-writer", true},
		{"/usr/local/bin/handler", true},
		{"./relative/path/handler", true},
		{"", false},
		{"path;rm -rf /", false},
		{"handler|cat /etc/passwd", false},
		{"handler&background", false},
		{"handler`inject`", false},
		{"handler$VAR", false},
	}

	for _, tc := range tests {
		t.Run(tc.handler, func(t *testing.T) {
			err := validateGoHandler(tc.handler)
			if tc.valid {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestLoadConsumerConfigInvalidYAML(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.yaml")
	require.NoError(t, os.WriteFile(path, []byte("{{invalid"), 0644))

	_, err := LoadConsumerConfig(path)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "parse consumer config")
}
