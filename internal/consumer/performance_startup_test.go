//go:build performance && (linux || darwin)

package consumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/messaging"
	"github.com/lyeith/eventbus/internal/server"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// Keep this fixed fixture unchanged across before/after runs. It measures five
// cold, joined batches, not an application's imports, datastore or timings.
const performanceConsumerHandler = `import time
module_started_unix_ns = time.time_ns()
sdk_started = time.perf_counter_ns()
import boto3
import botocore
from botocore.config import Config
sdk_import_ns = time.perf_counter_ns() - sdk_started
import json
import os
import pathlib
import sqlite3
import sys

def handler(event, context):
    started_unix_ns = time.time_ns()
    assert context is None
    assert os.path.realpath(sys.executable) == os.path.realpath(os.environ['PERF_EXPECTED_PYTHON'])
    assert os.path.realpath(sys.prefix) == os.path.realpath(os.environ['PERF_EXPECTED_PREFIX'])
    assert boto3.__version__ == '1.40.61'
    assert botocore.__version__ == '1.40.76'
    records = event['Records']
    assert len(records) == 5
    client_started = time.perf_counter_ns()
    client = boto3.client('sqs', endpoint_url=os.environ['PERF_ENDPOINT'],
                         region_name='us-east-1',
                         aws_access_key_id='performance-fixture',
                         aws_secret_access_key='performance-fixture',
                         config=Config(retries={'total_max_attempts': 1},
                                       connect_timeout=2, read_timeout=2))
    client_prepare_ns = time.perf_counter_ns() - client_started
    work_started = time.perf_counter_ns()
    try:
        with sqlite3.connect(os.environ['PERF_DATABASE']) as database:
            database.execute('CREATE TABLE IF NOT EXISTS effects (message_id TEXT PRIMARY KEY, body TEXT NOT NULL)')
            for record in records:
                response = client.send_message(QueueUrl=os.environ['PERF_CHECKPOINT_QUEUE'],
                                               MessageBody=record['body'])
                assert response['ResponseMetadata']['HTTPStatusCode'] == 200
                database.execute('INSERT INTO effects VALUES (?, ?)',
                                 (record['messageId'], record['body']))
            database.commit()
            count = database.execute('SELECT COUNT(*) FROM effects').fetchone()[0]
    finally:
        client.close()
    work_ns = time.perf_counter_ns() - work_started
    completed_unix_ns = time.time_ns()
    pathlib.Path(os.environ['PERF_TRACE']).write_text(json.dumps({
        'pid': os.getpid(), 'python_executable': sys.executable, 'python_prefix': sys.prefix,
        'python_version': sys.version.split()[0], 'boto3_version': boto3.__version__,
        'botocore_version': botocore.__version__,
        'module_started_unix_ns': module_started_unix_ns, 'sdk_import_ns': sdk_import_ns,
        'handler_started_unix_ns': started_unix_ns,
        'handler_completed_unix_ns': completed_unix_ns,
        'client_prepare_ns': client_prepare_ns, 'work_ns': work_ns,
        'processed': len(records), 'effect_count': count,
    }))
    return {'batchItemFailures': []}
`

type performanceConsumerTrace struct {
	PID                    int    `json:"pid"`
	Python                 string `json:"python_executable"`
	PythonPrefix           string `json:"python_prefix"`
	PythonVersion          string `json:"python_version"`
	Boto3Version           string `json:"boto3_version"`
	BotocoreVersion        string `json:"botocore_version"`
	ModuleStartedUnixNS    int64  `json:"module_started_unix_ns"`
	SDKImportNS            int64  `json:"sdk_import_ns"`
	HandlerStartedUnixNS   int64  `json:"handler_started_unix_ns"`
	HandlerCompletedUnixNS int64  `json:"handler_completed_unix_ns"`
	ClientPrepareNS        int64  `json:"client_prepare_ns"`
	WorkNS                 int64  `json:"work_ns"`
	Processed              int    `json:"processed"`
	EffectCount            int    `json:"effect_count"`
}

func TestPerformanceConsumerPythonStartup(t *testing.T) {
	python := os.Getenv("EVENTBUS_SMOKE_PYTHON")
	require.True(t, filepath.IsAbs(python), "select the existing frozen environment's absolute Python executable")
	info, err := os.Stat(python)
	require.NoError(t, err)
	require.False(t, info.IsDir())
	canonicalPython, err := filepath.EvalSymlinks(python)
	require.NoError(t, err)
	prefix := filepath.Dir(filepath.Dir(python))
	canonicalPrefix, err := filepath.EvalSymlinks(prefix)
	require.NoError(t, err)
	_, err = exec.LookPath("uv")
	require.NoError(t, err, "consumer launches through its actual uv command")
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)
	project := filepath.Clean(filepath.Join(filepath.Dir(filename), "..", ".."))
	directory := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(directory, "perf_consumer.py"), []byte(performanceConsumerHandler), 0600))
	serving := httptest.NewUnstartedServer(nil)
	_, portText, err := net.SplitHostPort(serving.Listener.Addr().String())
	require.NoError(t, err)
	port, err := strconv.Atoi(portText)
	require.NoError(t, err)
	broker := messaging.NewBroker("us-east-1", "000000000000", port)
	source := broker.CreateQueue("performance-source", time.Minute, 0)
	checkpoint := broker.CreateQueue("performance-checkpoint", time.Minute, 0)
	serving.Config.Handler = server.New(server.Services{Messaging: messaging.NewHandler(broker)})
	serving.Start()
	t.Cleanup(serving.Close)
	environment := fixtureToolEnvironment()
	for key, value := range map[string]string{
		"UV_PYTHON": python, "UV_PROJECT_ENVIRONMENT": prefix,
		"UV_NO_SYNC": "true", "UV_OFFLINE": "true", "UV_FROZEN": "true",
		"PYTHONPATH": directory, "PYTHONDONTWRITEBYTECODE": "1",
		"PERF_EXPECTED_PYTHON": python, "PERF_EXPECTED_PREFIX": prefix, "PERF_ENDPOINT": serving.URL,
		"PERF_CHECKPOINT_QUEUE": checkpoint.URL, "PERF_DATABASE": filepath.Join(directory, "effects.sqlite"),
		"PERF_TRACE":                filepath.Join(directory, "trace.json"),
		"AWS_EC2_METADATA_DISABLED": "true", "NO_PROXY": "*",
	} {
		environment[key] = value
	}
	manager := NewConsumerManager(broker, project)
	t.Cleanup(func() { require.NoError(t, manager.Wait(context.Background())) })
	entry := ConsumerEntry{Name: "performance-python", Type: "python", Handler: "perf_consumer.handler", TimeoutSeconds: 15,
		Env: environment, BatchSize: 5, MaxReceiveCount: 3, DeadLetterQueue: "performance-source-dlq"}
	database, err := sql.Open("sqlite", environment["PERF_DATABASE"])
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })
	for sample := 1; sample <= 5; sample++ {
		for index := 0; index < 5; index++ {
			_, err := broker.SendQueueMessage(source, messaging.QueueMessageInput{Body: fmt.Sprintf(`{"sample":%d,"index":%d}`, sample, index)})
			require.NoError(t, err)
		}
		messages, err := broker.ReceiveMessagesContext(t.Context(), source, 5, 0)
		require.NoError(t, err)
		require.Len(t, messages, 5)
		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		started := time.Now()
		result, err := manager.invokeHandlerResult(ctx, entry, buildLambdaEvent(messages, source))
		joined := time.Now()
		cancel()
		require.NoError(t, err)
		failures, err := validateBatchFailures(result, messages)
		require.NoError(t, err)
		require.Empty(t, failures)
		manager.settleBatch(zerolog.Nop(), entry, source, messages, failures, false)
		waiting, inflight := broker.QueueDepth(source)
		require.Zero(t, waiting)
		require.Zero(t, inflight)
		checkpoints, err := broker.ReceiveMessagesContext(t.Context(), checkpoint, 5, 0)
		require.NoError(t, err)
		require.Len(t, checkpoints, 5)
		for _, message := range checkpoints {
			require.True(t, broker.DeleteMessage(checkpoint, message.ReceiptHandle))
		}
		var trace performanceConsumerTrace
		data, err := os.ReadFile(environment["PERF_TRACE"])
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(data, &trace))
		actualPython, err := filepath.EvalSymlinks(trace.Python)
		require.NoError(t, err)
		require.Equal(t, canonicalPython, actualPython, "Python aliases must resolve to the selected interpreter")
		actualPrefix, err := filepath.EvalSymlinks(trace.PythonPrefix)
		require.NoError(t, err)
		require.Equal(t, canonicalPrefix, actualPrefix, "the handler must use the selected virtual environment")
		require.Equal(t, "1.40.61", trace.Boto3Version)
		require.Equal(t, "1.40.76", trace.BotocoreVersion)
		require.Equal(t, 5, trace.Processed)
		require.Equal(t, sample*5, trace.EffectCount)
		require.False(t, consumerChildRunning(trace.PID), "measurement ends only after the actual Python child joins")
		var effects int
		require.NoError(t, database.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM effects").Scan(&effects))
		require.Equal(t, sample*5, effects)
		require.GreaterOrEqual(t, trace.ModuleStartedUnixNS, started.UnixNano())
		require.GreaterOrEqual(t, joined.UnixNano(), trace.HandlerCompletedUnixNS)
		row := map[string]any{"schema_version": "eventbus.performance.consumer.v1", "sample": sample, "records": 5,
			"python_executable": trace.Python, "python_prefix": trace.PythonPrefix,
			"python_version": trace.PythonVersion, "boto3_version": trace.Boto3Version,
			"total_joined_ns": joined.Sub(started).Nanoseconds(), "launch_to_module_ns": trace.ModuleStartedUnixNS - started.UnixNano(),
			"sdk_import_ns": trace.SDKImportNS, "client_prepare_ns": trace.ClientPrepareNS, "handler_work_ns": trace.WorkNS,
			"after_handler_to_join_ns": joined.UnixNano() - trace.HandlerCompletedUnixNS, "ownership_joined": true}
		encoded, err := json.Marshal(row)
		require.NoError(t, err)
		t.Logf("PERF %s", encoded)
	}
}
