package consumer

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/localexec"
	"github.com/lyeith/eventbus/internal/messaging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseHandlerBatchResultFailsClosed(t *testing.T) {
	parse := func(data []byte) (*handlerBatchResult, error) {
		stdout := localexec.NewBoundedOutput(maxOutputBytes)
		_, err := stdout.Write(data)
		require.NoError(t, err)
		return parseHandlerBatchResult(stdout, localexec.NewBoundedOutput(maxOutputBytes))
	}
	_, err := parse(nil)
	require.Error(t, err)
	_, err = parse([]byte("not-json"))
	require.Error(t, err)

	result, err := parse([]byte(`{"processed":1,"batchItemFailures":[{"itemIdentifier":"msg-2"}]}`))
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
		Env:            fixtureToolEnvironment(),
	}

	event := buildLambdaEvent([]*messaging.Message{{ID: "1", Body: "test", ReceiptHandle: "rh"}})

	err := cm.invokeHandler(context.Background(), entry, event)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "handler failed")
}

func TestInvokePythonHandlerBatchResult(t *testing.T) {
	directory := t.TempDir()
	inputPath := filepath.Join(directory, "received-event.json")
	t.Setenv("UNRELATED_CONNECTOR_SECRET", "must-not-cross")
	source := `import json
import os
import pathlib
import sys

def handler(event, context):
    assert context is None
    assert os.environ["TEST_VAR"] == "hello-from-config"
    assert "UNRELATED_CONNECTOR_SECRET" not in os.environ
    assert pathlib.Path("fixture.txt").read_text() == "owned fixture"
    pathlib.Path(os.environ["EVENT_FILE"]).write_text(json.dumps(event))
    print("handler diagnostic", file=sys.stderr)
    return {"batchItemFailures": [{"itemIdentifier": event["Records"][1]["messageId"]}]}
`
	require.NoError(t, os.WriteFile(filepath.Join(directory, "batch.py"), []byte(source), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(directory, "fixture.txt"), []byte("owned fixture"), 0600))
	environment := fixtureToolEnvironment()
	environment["TEST_VAR"] = "hello-from-config"
	environment["EVENT_FILE"] = inputPath
	entry := ConsumerEntry{
		Name: "python-batch", Handler: "batch.handler", TimeoutSeconds: 5, Env: environment,
	}
	event := buildLambdaEvent([]*messaging.Message{
		{ID: "msg-1", Body: `{"Message":"hello"}`, ReceiptHandle: "receipt-1"},
		{ID: "msg-2", Body: "retry me", ReceiptHandle: "receipt-2"},
	})
	broker := messaging.NewBroker("us-east-1", "000000000000", 0)
	manager := NewConsumerManager(broker, directory)
	result, err := manager.invokeHandlerResult(t.Context(), entry, event)
	require.NoError(t, err)
	require.Len(t, result.BatchItemFailures, 1)
	assert.Equal(t, "msg-2", result.BatchItemFailures[0].ItemIdentifier)
	received, err := os.ReadFile(inputPath)
	require.NoError(t, err)
	expected, err := json.Marshal(event)
	require.NoError(t, err)
	assert.JSONEq(t, string(expected), string(received))
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
	directory := t.TempDir()
	marker := filepath.Join(directory, "started")
	source := `import os
import time

def handler(event, context):
    with open(os.environ["STARTED_FILE"], "w") as marker:
        marker.write("started")
    time.sleep(30)
    return {}
`
	require.NoError(t, os.WriteFile(filepath.Join(directory, "slow.py"), []byte(source), 0600))
	broker := messaging.NewBroker("us-east-1", "000000000000", 0)
	cm := NewConsumerManager(broker, directory)
	environment := fixtureToolEnvironment()
	environment["STARTED_FILE"] = marker
	entry := ConsumerEntry{
		Name: "timeout-test", Type: "python", Handler: "slow.handler",
		Env: environment, TimeoutSeconds: 3,
	}
	event := buildLambdaEvent([]*messaging.Message{{ID: "1", Body: "test", ReceiptHandle: "rh"}})
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	err := cm.invokeHandler(ctx, entry, event)
	require.ErrorContains(t, err, "timed out")
	_, err = os.Stat(marker)
	require.NoError(t, err, "the real Python handler must start before its timeout")
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

func TestParseHandlerBatchResultRejectsOverflowedOutput(t *testing.T) {
	for _, stream := range []string{"stdout", "stderr"} {
		t.Run(stream, func(t *testing.T) {
			// The retained stdout prefix is valid JSON exactly at the cap. Extra
			// bytes arrive in a later write and cannot be accepted as that prefix.
			stdout, stderr := localexec.NewBoundedOutput(8), localexec.NewBoundedOutput(8)
			_, err := stdout.Write([]byte("{}      "))
			require.NoError(t, err)
			_, err = parseHandlerBatchResult(stdout, stderr)
			require.NoError(t, err)
			overflowed := stdout
			if stream == "stderr" {
				overflowed = stderr
				_, err = stderr.Write([]byte("12345678"))
				require.NoError(t, err)
			}
			_, err = overflowed.Write([]byte("discarded"))
			require.NoError(t, err)
			result, err := parseHandlerBatchResult(stdout, stderr)
			require.ErrorContains(t, err, "output exceeded")
			require.Nil(t, result)
		})
	}
}

// The test host may wrap uv in a resource-owner launcher. Supply its existing
// ownership explicitly as fixture configuration; production consumers must not
// inherit arbitrary host control variables or connector credentials.
func fixtureToolEnvironment() map[string]string {
	environment := map[string]string{}
	if os.Getenv("SSD_DEV_RUN_ID") == "" {
		return environment
	}
	for _, name := range []string{
		"SSD_DEV_RUN_ID", "SSD_DEV_RECEIPT", "SSD_DEV_SCOPE", "INVOCATION_ID",
		"TMPDIR", "GOTMPDIR", "GOCACHE", "UV_CACHE_DIR",
		"DBUS_SESSION_BUS_ADDRESS", "XDG_RUNTIME_DIR",
	} {
		if value, ok := os.LookupEnv(name); ok {
			environment[name] = value
		}
	}
	return environment
}
