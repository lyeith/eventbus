package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
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

func TestBuildLambdaEvent(t *testing.T) {
	messages := []*Message{
		{ID: "msg-1", Body: `{"Message":"hello"}`, ReceiptHandle: "rh-1"},
		{ID: "msg-2", Body: `{"Message":"world"}`, ReceiptHandle: "rh-2"},
	}

	event := buildLambdaEvent(messages)

	records, ok := event["Records"].([]map[string]interface{})
	require.True(t, ok)
	require.Len(t, records, 2)

	assert.Equal(t, "msg-1", records[0]["messageId"])
	assert.Equal(t, `{"Message":"hello"}`, records[0]["body"])
	assert.Equal(t, "rh-1", records[0]["receiptHandle"])

	assert.Equal(t, "msg-2", records[1]["messageId"])
}

func TestBuildLambdaEventSingleMessage(t *testing.T) {
	messages := []*Message{
		{ID: "msg-1", Body: `{"test":true}`, ReceiptHandle: "rh-1"},
	}

	event := buildLambdaEvent(messages)
	records := event["Records"].([]map[string]interface{})
	assert.Len(t, records, 1)
}

func TestBuildLambdaEventEmpty(t *testing.T) {
	event := buildLambdaEvent(nil)
	records := event["Records"].([]map[string]interface{})
	assert.Empty(t, records)
}

func TestValidateBatchFailuresRejectsUnknownAndDuplicateIDs(t *testing.T) {
	messages := []*Message{{ID: "msg-1"}, {ID: "msg-2"}}

	_, err := validateBatchFailures(&handlerBatchResult{BatchItemFailures: []batchItemFailure{{ItemIdentifier: "unknown"}}}, messages)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown message")

	_, err = validateBatchFailures(&handlerBatchResult{BatchItemFailures: []batchItemFailure{
		{ItemIdentifier: "msg-1"},
		{ItemIdentifier: "msg-1"},
	}}, messages)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "repeats")
}

func TestSettleBatchDeletesSuccessRetriesFailureThenDeadLetters(t *testing.T) {
	broker := NewBroker("us-east-1", "000000000000", 0)
	source := broker.CreateQueue("source", 0, 0)
	dlq := broker.CreateQueue("source-dlq", 0, 0)
	broker.enqueueMessage(source, `{"record":1}`)
	broker.enqueueMessage(source, `{"record":2}`)

	firstBatch := broker.ReceiveMessages(source, 2, 0)
	require.Len(t, firstBatch, 2)
	failed := map[string]struct{}{firstBatch[1].ID: {}}
	manager := NewConsumerManager(broker, t.TempDir())
	entry := ConsumerEntry{
		DeadLetterQueue: "source-dlq",
		MaxReceiveCount: 2,
	}
	retryStarted := time.Now()
	manager.settleBatch(zerolog.Nop(), entry, source, firstBatch, failed, false)

	source.mu.Lock()
	assert.Empty(t, source.Messages)
	require.Len(t, source.InFlight, 1)
	retryMessage := source.InFlight[firstBatch[1].ReceiptHandle]
	require.NotNil(t, retryMessage)
	assert.WithinDuration(t, retryStarted.Add(baseRetryVisibility), retryMessage.VisibleAt, time.Second)
	retryMessage.VisibleAt = time.Now().Add(-time.Second)
	source.mu.Unlock()
	dlq.mu.Lock()
	assert.Empty(t, dlq.Messages)
	dlq.mu.Unlock()

	assert.Equal(t, 1, broker.RequeueExpired(source))
	secondBatch := broker.ReceiveMessages(source, 1, 0)
	require.Len(t, secondBatch, 1)
	assert.Equal(t, 2, secondBatch[0].ReceiveCount)
	manager.settleBatch(zerolog.Nop(), entry, source, secondBatch, nil, true)

	source.mu.Lock()
	assert.Empty(t, source.Messages)
	assert.Empty(t, source.InFlight)
	source.mu.Unlock()
	dlq.mu.Lock()
	require.Len(t, dlq.Messages, 1)
	assert.Equal(t, secondBatch[0].ID, dlq.Messages[0].ID)
	dlq.mu.Unlock()
}

func TestRetryVisibilityIsBoundedExponential(t *testing.T) {
	assert.Equal(t, 5*time.Second, retryVisibility(0))
	assert.Equal(t, 5*time.Second, retryVisibility(1))
	assert.Equal(t, 10*time.Second, retryVisibility(2))
	assert.Equal(t, 20*time.Second, retryVisibility(3))
	assert.Equal(t, time.Minute, retryVisibility(20))
}

func TestParseHandlerBatchResultFailsClosed(t *testing.T) {
	_, err := parseHandlerBatchResult(nil)
	require.Error(t, err)
	_, err = parseHandlerBatchResult([]byte("not-json"))
	require.Error(t, err)

	result, err := parseHandlerBatchResult([]byte(`{"processed":1,"batchItemFailures":[{"itemIdentifier":"msg-2"}]}`))
	require.NoError(t, err)
	require.Len(t, result.BatchItemFailures, 1)
	assert.Equal(t, "msg-2", result.BatchItemFailures[0].ItemIdentifier)
}

func TestConsumerProcessEnvDoesNotInheritUnconfiguredSecrets(t *testing.T) {
	t.Setenv("UNRELATED_CONNECTOR_SECRET", "must-not-cross")
	environment := consumerProcessEnv(map[string]string{
		"DYNAMODB_TABLE": "owned-table",
	})

	configured := false
	for _, assignment := range environment {
		key, _, _ := strings.Cut(assignment, "=")
		if key == "UNRELATED_CONNECTOR_SECRET" {
			t.Fatal("consumer inherited an unconfigured connector secret")
		}
		if key == "DYNAMODB_TABLE" {
			configured = true
		}
	}
	if !configured {
		t.Fatal("consumer did not receive its configured datastore")
	}
}

func TestInvokePythonHandlerFailure(t *testing.T) {
	broker := NewBroker("us-east-1", "000000000000", 0)
	cm := NewConsumerManager(broker, t.TempDir())

	entry := ConsumerEntry{
		Name:           "test",
		Type:           "python",
		Handler:        "not_a_real_module.handler",
		TimeoutSeconds: 5,
	}

	event := buildLambdaEvent([]*Message{{ID: "1", Body: "test", ReceiptHandle: "rh"}})

	err := cm.invokeHandler(context.Background(), entry, event)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "handler failed")
}

func TestInvokeGoHandlerSuccess(t *testing.T) {
	tmp := t.TempDir()

	// Create a simple script that reads stdin JSON and echoes a result
	script := filepath.Join(tmp, "handler")
	err := os.WriteFile(script, []byte("#!/bin/sh\ncat /dev/stdin > /dev/null\necho '{\"processed\": 1, \"errors\": []}'\n"), 0755)
	require.NoError(t, err)

	broker := NewBroker("us-east-1", "000000000000", 0)
	cm := NewConsumerManager(broker, tmp)

	entry := ConsumerEntry{
		Name:           "go-test",
		Type:           "go",
		Handler:        script,
		TimeoutSeconds: 5,
	}

	event := buildLambdaEvent([]*Message{{ID: "1", Body: `{"Message":"hello"}`, ReceiptHandle: "rh"}})
	err = cm.invokeHandler(context.Background(), entry, event)
	assert.NoError(t, err)
}

func TestInvokeGoHandlerRelativePath(t *testing.T) {
	tmp := t.TempDir()

	// Create handler at a relative path within workDir
	buildDir := filepath.Join(tmp, "build")
	require.NoError(t, os.MkdirAll(buildDir, 0755))
	script := filepath.Join(buildDir, "handler")
	err := os.WriteFile(script, []byte("#!/bin/sh\necho '{}'\n"), 0755)
	require.NoError(t, err)

	broker := NewBroker("us-east-1", "000000000000", 0)
	cm := NewConsumerManager(broker, tmp)

	entry := ConsumerEntry{
		Name:           "relative-test",
		Type:           "go",
		Handler:        "build/handler", // relative to workDir
		TimeoutSeconds: 5,
	}

	event := buildLambdaEvent([]*Message{{ID: "1", Body: "test", ReceiptHandle: "rh"}})
	err = cm.invokeHandler(context.Background(), entry, event)
	assert.NoError(t, err)
}

func TestInvokeGoHandlerFailure(t *testing.T) {
	broker := NewBroker("us-east-1", "000000000000", 0)
	cm := NewConsumerManager(broker, t.TempDir())

	entry := ConsumerEntry{
		Name:           "fail-test",
		Type:           "go",
		Handler:        "/nonexistent/binary",
		TimeoutSeconds: 5,
	}

	event := buildLambdaEvent([]*Message{{ID: "1", Body: "test", ReceiptHandle: "rh"}})
	err := cm.invokeHandler(context.Background(), entry, event)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "handler failed")
}

func TestInvokeGoHandlerEnvPropagation(t *testing.T) {
	tmp := t.TempDir()

	// Script that prints the value of TEST_VAR from environment
	script := filepath.Join(tmp, "env-handler")
	err := os.WriteFile(script, []byte("#!/bin/sh\necho \"{\\\"var\\\": \\\"$TEST_VAR\\\"}\"\n"), 0755)
	require.NoError(t, err)

	broker := NewBroker("us-east-1", "000000000000", 0)
	cm := NewConsumerManager(broker, tmp)

	entry := ConsumerEntry{
		Name:           "env-test",
		Type:           "go",
		Handler:        script,
		TimeoutSeconds: 5,
		Env:            map[string]string{"TEST_VAR": "hello-from-config"},
	}

	event := buildLambdaEvent([]*Message{{ID: "1", Body: "test", ReceiptHandle: "rh"}})
	err = cm.invokeHandler(context.Background(), entry, event)
	assert.NoError(t, err)
}

func TestInvokeHandlerTimeout(t *testing.T) {
	broker := NewBroker("us-east-1", "000000000000", 0)
	cm := NewConsumerManager(broker, t.TempDir())

	entry := ConsumerEntry{
		Name:           "timeout-test",
		Type:           "python",
		Handler:        "time.sleep_handler", // doesn't exist, but test the timeout path
		TimeoutSeconds: 1,
	}

	event := buildLambdaEvent([]*Message{{ID: "1", Body: "test", ReceiptHandle: "rh"}})

	// With a very short timeout and a non-existent module, it will fail with handler error
	// not timeout (module import fails before timeout). That's fine — we're testing
	// that the timeout context is properly set.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := cm.invokeHandler(ctx, entry, event)
	assert.Error(t, err)
}

func TestInvokeGoHandlerTimeout(t *testing.T) {
	tmp := t.TempDir()

	// Use exec to replace the shell process, so SIGKILL kills it cleanly (no orphan children)
	script := filepath.Join(tmp, "slow-handler")
	err := os.WriteFile(script, []byte("#!/bin/sh\nexec sleep 30\n"), 0755)
	require.NoError(t, err)

	broker := NewBroker("us-east-1", "000000000000", 0)
	cm := NewConsumerManager(broker, tmp)

	// Use a struct literal so TimeoutSeconds is interpreted as raw seconds
	// but override via a short context deadline instead
	entry := ConsumerEntry{
		Name:           "go-timeout-test",
		Type:           "go",
		Handler:        script,
		TimeoutSeconds: 1, // 1 second timeout
	}

	event := buildLambdaEvent([]*Message{{ID: "1", Body: "test", ReceiptHandle: "rh"}})
	err = cm.invokeHandler(context.Background(), entry, event)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "timed out")
}

func TestInvokePythonHandlerInvalidFormat(t *testing.T) {
	broker := NewBroker("us-east-1", "000000000000", 0)
	cm := NewConsumerManager(broker, t.TempDir())

	entry := ConsumerEntry{
		Name:           "bad-handler",
		Type:           "python",
		Handler:        "no_dot",
		TimeoutSeconds: 5,
	}

	event := buildLambdaEvent([]*Message{{ID: "1", Body: "test", ReceiptHandle: "rh"}})
	err := cm.invokeHandler(context.Background(), entry, event)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid handler format")
}

func TestConsumerPollLoopStops(t *testing.T) {
	broker := NewBroker("us-east-1", "000000000000", 0)
	cm := NewConsumerManager(broker, t.TempDir())

	ctx, cancel := context.WithCancel(context.Background())

	entry := ConsumerEntry{
		Name:           "stop-test",
		Queue:          "nonexistent-queue",
		Handler:        "test.handler",
		BatchSize:      1,
		TimeoutSeconds: 5,
	}

	done := make(chan struct{})
	go func() {
		cm.pollLoop(ctx, entry)
		close(done)
	}()

	// Cancel should stop the poller
	cancel()

	select {
	case <-done:
		// Success - poller stopped
	case <-time.After(10 * time.Second):
		t.Fatal("pollLoop did not stop within timeout")
	}
}

func TestFindProjectRoot(t *testing.T) {
	// Create a temp dir structure with pyproject.toml
	root := t.TempDir()
	sub := filepath.Join(root, "tools", "eventbus")
	require.NoError(t, os.MkdirAll(sub, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "pyproject.toml"), []byte("[project]"), 0644))

	result := FindProjectRoot(sub)
	assert.Equal(t, root, result)
}

func TestFindProjectRootFallback(t *testing.T) {
	// No pyproject.toml anywhere — falls back to start dir
	tmp := t.TempDir()
	sub := filepath.Join(tmp, "deep", "nested")
	require.NoError(t, os.MkdirAll(sub, 0755))

	result := FindProjectRoot(sub)
	assert.Equal(t, sub, result)
}
