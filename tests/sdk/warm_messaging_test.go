//go:build sdksmoke

package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/devquiescence"
	"github.com/lyeith/eventbus/internal/eventsource"
	"github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/messaging"
	"github.com/lyeith/eventbus/internal/server"
	"github.com/stretchr/testify/require"
)

// Native producer/consumer paths share one opt-in managed worker. Completion
// means the invocation's native response and log boundary were observed; the
// process remains independently owned until the retained-worker drain joins it.
func TestWarmNativeMessagingPythonSDKSmoke(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires owned-process signal checks")
	}
	python := sdkPython(t)
	directory := t.TempDir()
	output := filepath.Join(directory, "effects.jsonl")
	deliveryPath := filepath.Join(directory, "deliveries.jsonl")
	var functions *lambda.Service
	var mappings *eventsource.Service
	owner := devquiescence.NewWithOptions(devquiescence.Options{Checks: []func() error{
		func() error {
			if functions != nil {
				return functions.DevEvidence()
			}
			return nil
		},
		func() error {
			if mappings != nil {
				return mappings.DevEvidence()
			}
			return nil
		},
	}, DrainHooks: []devquiescence.DrainHook{{
		Start:  func() error { return functions.DevBeginWarmDrain() },
		Resume: func() error { return functions.DevResumeWarm() },
	}}})
	var err error
	functions, err = lambda.NewService(&lambda.Config{
		Functions: map[string]lambda.Function{"warm-messaging:live": {
			Runtime: "python", Command: []string{python, "-E", "-s"},
			Handler: fixturePath("python", "warm_messaging_smoke.py") + "#handler", Timeout: 5 * time.Second,
			Environment: map[string]string{"WARM_MESSAGING_OUTPUT": output},
		}},
		DevWarm: &lambda.DevWarmConfig{MaxWorkers: 1}, DevActivity: owner,
		DevAsync: &lambda.DevAsyncConfig{Workers: 1, Capacity: 8, RetryDelays: []time.Duration{0, 0}, LogPath: filepath.Join(directory, "async.jsonl")},
	}, directory)
	require.NoError(t, err)
	serving := httptest.NewUnstartedServer(nil)
	broker := messaging.NewBroker("us-east-1", "000000000000", serving.Listener.Addr().(*net.TCPAddr).Port)
	capture, err := messaging.OpenSNSCapture(filepath.Join(directory, "sns.jsonl"))
	require.NoError(t, err)
	broker.SetSNSCapture(capture)
	broker.SetLambdaDelivery(sdkSNSLambdaDelivery{functions: functions})
	require.NoError(t, broker.SetDevActivity(owner))
	mappings, err = eventsource.New(eventsource.Options{Region: "us-east-1", AccountID: "000000000000",
		Dev: eventsource.DevOptions{Source: owner, Activity: owner, DeliveryCapture: &eventsource.DevDeliveryCaptureConfig{LogPath: deliveryPath}}},
		nativeEvidenceQueueSource{broker: broker}, nativeEvidenceInvoker{sdkMappingInvoker{functions: functions}})
	require.NoError(t, err)
	serving.Config.Handler = server.New(server.Services{Messaging: messaging.NewHandler(broker), Lambda: functions,
		EventSources: eventsource.NewHandler(mappings)})
	serving.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		mappingErr := mappings.Close(ctx)
		drainErr := functions.DrainAsync(ctx)
		serving.Close()
		closeErr := functions.Close(ctx)
		captureErr := capture.Close()
		require.NoError(t, errors.Join(mappingErr, drainErr, closeErr, captureErr))
	})
	env := append(sdkEnvironment(t.TempDir(), "", "", ""),
		"WARM_MESSAGING_ENDPOINT="+serving.URL, "WARM_MESSAGING_OUTPUT="+output,
		"WARM_MESSAGING_FUNCTION_ARN=arn:aws:lambda:us-east-1:000000000000:function:warm-messaging:live")
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	logs, err := runSDKProcess(ctx, python, fixturePath("python", "warm_messaging_smoke.py"), env)
	t.Logf("real warm Python SNS/SQS SDK proof:\n%s", logs)
	require.NoError(t, err)
	require.NoError(t, mappings.Close(ctx))
	require.NoError(t, functions.DrainAsync(ctx))
	data, err := os.ReadFile(output)
	require.NoError(t, err)
	sqsRequests := make(map[string]string)
	var initialized struct {
		Phase string `json:"phase"`
		PID   int    `json:"pid"`
	}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var row struct {
			Phase     string `json:"phase"`
			PID       int    `json:"pid"`
			Source    string `json:"source"`
			RequestID string `json:"request_id"`
			MessageID string `json:"native_message_id"`
		}
		require.NoError(t, json.Unmarshal([]byte(line), &row))
		if row.Phase == "init" {
			initialized.Phase, initialized.PID = row.Phase, row.PID
		}
		if row.Source == "sqs" {
			sqsRequests[row.MessageID] = row.RequestID
		}
	}
	require.Len(t, sqsRequests, 2)
	deliveryData, err := os.ReadFile(deliveryPath)
	require.NoError(t, err)
	terminal := 0
	for _, line := range strings.Split(strings.TrimSpace(string(deliveryData)), "\n") {
		var record struct {
			State           string `json:"state"`
			RequestID       string `json:"request_id"`
			CompletionScope string `json:"completion_scope"`
			Joined          bool   `json:"joined"`
			Messages        []struct {
				MessageID            string `json:"message_id"`
				Settlement           string `json:"settlement"`
				AcknowledgeAttempted bool   `json:"acknowledge_attempted"`
			} `json:"messages"`
		}
		require.NoError(t, json.Unmarshal([]byte(line), &record))
		if record.State == "admitted" {
			continue
		}
		terminal++
		require.Equal(t, "succeeded", record.State)
		require.Equal(t, "invocation", record.CompletionScope)
		require.True(t, record.Joined, "native invocation/log boundary joined; retained worker lease remains")
		require.Len(t, record.Messages, 1)
		message := record.Messages[0]
		require.Equal(t, sqsRequests[message.MessageID], record.RequestID)
		require.Equal(t, "mapping_settled", message.Settlement)
		require.True(t, message.AcknowledgeAttempted)
	}
	require.Equal(t, 2, terminal)
	require.Positive(t, initialized.PID)
	process, err := os.FindProcess(initialized.PID)
	require.NoError(t, err)
	defer process.Release()
	require.NoError(t, process.Signal(syscall.Signal(0)), "idle warm worker must still be owned")
	before := owner.Snapshot()
	require.Equal(t, 1, before.WorkCount, "invocations finished but worker lease remains")
	require.Len(t, before.Activities, 1)
	require.Equal(t, "lambda_warm_worker", before.Activities[0].Kind)
	held, err := owner.Quiesce(ctx)
	require.NoError(t, err)
	require.True(t, held.FixtureSafe)
	require.Equal(t, 0, held.WorkCount)
	require.Error(t, process.Signal(syscall.Signal(0)), "held cleanup cannot retain the worker process")
	_, err = owner.Resume(held.Generation)
	require.NoError(t, err)
}
