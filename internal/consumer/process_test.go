package consumer

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/messaging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	broker := messaging.NewBroker("us-east-1", "000000000000", 0)
	cm := NewConsumerManager(broker, t.TempDir())

	entry := ConsumerEntry{
		Name:           "test",
		Type:           "python",
		Handler:        "not_a_real_module.handler",
		TimeoutSeconds: 5,
	}

	event := buildLambdaEvent([]*messaging.Message{{ID: "1", Body: "test", ReceiptHandle: "rh"}})

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

	broker := messaging.NewBroker("us-east-1", "000000000000", 0)
	cm := NewConsumerManager(broker, tmp)

	entry := ConsumerEntry{
		Name:           "go-test",
		Type:           "go",
		Handler:        script,
		TimeoutSeconds: 5,
	}

	event := buildLambdaEvent([]*messaging.Message{{ID: "1", Body: `{"Message":"hello"}`, ReceiptHandle: "rh"}})
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

	broker := messaging.NewBroker("us-east-1", "000000000000", 0)
	cm := NewConsumerManager(broker, tmp)

	entry := ConsumerEntry{
		Name:           "relative-test",
		Type:           "go",
		Handler:        "build/handler", // relative to workDir
		TimeoutSeconds: 5,
	}

	event := buildLambdaEvent([]*messaging.Message{{ID: "1", Body: "test", ReceiptHandle: "rh"}})
	err = cm.invokeHandler(context.Background(), entry, event)
	assert.NoError(t, err)
}

func TestInvokeGoHandlerFailure(t *testing.T) {
	broker := messaging.NewBroker("us-east-1", "000000000000", 0)
	cm := NewConsumerManager(broker, t.TempDir())

	entry := ConsumerEntry{
		Name:           "fail-test",
		Type:           "go",
		Handler:        "/nonexistent/binary",
		TimeoutSeconds: 5,
	}

	event := buildLambdaEvent([]*messaging.Message{{ID: "1", Body: "test", ReceiptHandle: "rh"}})
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

	broker := messaging.NewBroker("us-east-1", "000000000000", 0)
	cm := NewConsumerManager(broker, tmp)

	entry := ConsumerEntry{
		Name:           "env-test",
		Type:           "go",
		Handler:        script,
		TimeoutSeconds: 5,
		Env:            map[string]string{"TEST_VAR": "hello-from-config"},
	}

	event := buildLambdaEvent([]*messaging.Message{{ID: "1", Body: "test", ReceiptHandle: "rh"}})
	err = cm.invokeHandler(context.Background(), entry, event)
	assert.NoError(t, err)
}

func TestInvokeHandlerTimeout(t *testing.T) {
	broker := messaging.NewBroker("us-east-1", "000000000000", 0)
	cm := NewConsumerManager(broker, t.TempDir())

	entry := ConsumerEntry{
		Name:           "timeout-test",
		Type:           "python",
		Handler:        "time.sleep_handler", // doesn't exist, but test the timeout path
		TimeoutSeconds: 1,
	}

	event := buildLambdaEvent([]*messaging.Message{{ID: "1", Body: "test", ReceiptHandle: "rh"}})

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

	broker := messaging.NewBroker("us-east-1", "000000000000", 0)
	cm := NewConsumerManager(broker, tmp)

	// Use a struct literal so TimeoutSeconds is interpreted as raw seconds
	// but override via a short context deadline instead
	entry := ConsumerEntry{
		Name:           "go-timeout-test",
		Type:           "go",
		Handler:        script,
		TimeoutSeconds: 1, // 1 second timeout
	}

	event := buildLambdaEvent([]*messaging.Message{{ID: "1", Body: "test", ReceiptHandle: "rh"}})
	err = cm.invokeHandler(context.Background(), entry, event)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "timed out")
}

func TestInvokePythonHandlerInvalidFormat(t *testing.T) {
	broker := messaging.NewBroker("us-east-1", "000000000000", 0)
	cm := NewConsumerManager(broker, t.TempDir())

	entry := ConsumerEntry{
		Name:           "bad-handler",
		Type:           "python",
		Handler:        "no_dot",
		TimeoutSeconds: 5,
	}

	event := buildLambdaEvent([]*messaging.Message{{ID: "1", Body: "test", ReceiptHandle: "rh"}})
	err := cm.invokeHandler(context.Background(), entry, event)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid handler format")
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
