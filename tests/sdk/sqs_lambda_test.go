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
	"strings"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/eventsource"
	"github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/messaging"
	"github.com/lyeith/eventbus/internal/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The SDK fixture uses the same typed native queue/runtime contracts as app.
// Queue visibility/redrive and actual Lambda execution retain their core owners.
type sdkMappingSource struct{ broker *messaging.Broker }
type sdkMappingQueue struct {
	broker *messaging.Broker
	queue  *messaging.Queue
	info   eventsource.QueueInfo
}

func (source sdkMappingSource) ResolveQueue(ctx context.Context, arn string) (eventsource.Queue, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	queue := source.broker.GetQueueByARN(arn)
	info, err := source.broker.QueueInfo(queue)
	if err != nil {
		return nil, err
	}
	return sdkMappingQueue{broker: source.broker, queue: queue, info: eventsource.QueueInfo{ARN: info.ARN, VisibilityTimeout: info.VisibilityTimeout}}, nil
}
func (queue sdkMappingQueue) Info() eventsource.QueueInfo { return queue.info }
func (queue sdkMappingQueue) Receive(ctx context.Context) (*eventsource.Record, error) {
	messages, err := queue.broker.ReceiveMessagesContext(ctx, queue.queue, 1, time.Second)
	if err != nil || len(messages) == 0 {
		return nil, err
	}
	event := messaging.BuildSQSLambdaEvent(messages, queue.info.ARN)
	return &event.Records[0], nil
}
func (queue sdkMappingQueue) Delete(ctx context.Context, receipt string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return queue.broker.DeleteMessage(queue.queue, receipt), nil
}

type sdkMappingInvoker struct{ functions *lambda.Service }

func (invoker sdkMappingInvoker) ValidateTarget(ctx context.Context, arn string) (time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	info, err := invoker.functions.DescribeTarget(arn, "")
	return info.Timeout, err
}
func (invoker sdkMappingInvoker) InvokeTarget(ctx context.Context, arn string, payload []byte) error {
	result, err := invoker.functions.Execute(ctx, lambda.InvokeInput{FunctionName: arn, Payload: payload})
	if err == nil && result.FunctionError {
		return errors.New("Lambda function failed")
	}
	return err
}

type sqsMappingFixture struct {
	broker    *messaging.Broker
	mappings  *eventsource.Service
	functions *lambda.Service
	serving   *httptest.Server
	output    string
	gate      string
}

func newSQSMappingFixture(t *testing.T, python, owner string) *sqsMappingFixture {
	t.Helper()
	directory := t.TempDir()
	fixture := &sqsMappingFixture{output: filepath.Join(directory, "effects.jsonl"), gate: filepath.Join(directory, "gate")}
	function := lambda.Function{Runtime: "python", Command: []string{python, "-E", "-s"}, Handler: fixturePath("python", "sqs_lambda_smoke.py") + "#handler", Timeout: time.Second,
		Environment: map[string]string{"SQS_LAMBDA_OUTPUT": fixture.output, "SQS_LAMBDA_GATE": fixture.gate, "SQS_LAMBDA_OWNER": owner, "SQS_LAMBDA_VARIANT": "base"}}
	alias, short := function, function
	alias.Environment = map[string]string{"SQS_LAMBDA_OUTPUT": fixture.output, "SQS_LAMBDA_GATE": fixture.gate, "SQS_LAMBDA_OWNER": owner, "SQS_LAMBDA_VARIANT": "alias"}
	short.Timeout = 250 * time.Millisecond
	functions, err := lambda.NewService(&lambda.Config{Functions: map[string]lambda.Function{"sqs-handler": function, "sqs-handler:live": alias, "sqs-short": short}}, directory)
	require.NoError(t, err)
	fixture.functions = functions
	fixture.serving = httptest.NewUnstartedServer(nil)
	fixture.broker = messaging.NewBroker("us-east-1", "000000000000", fixture.serving.Listener.Addr().(*net.TCPAddr).Port)
	fixture.mappings, err = eventsource.New(eventsource.Options{Region: "us-east-1", AccountID: "000000000000"}, sdkMappingSource{fixture.broker}, sdkMappingInvoker{functions})
	require.NoError(t, err)
	fixture.serving.Config.Handler = server.New(server.Services{Messaging: messaging.NewHandler(fixture.broker), Lambda: functions, EventSources: eventsource.NewHandler(fixture.mappings)})
	fixture.serving.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		// Cancel producers before the runtime/HTTP owner, including the real
		// blocked teardown invocation intentionally left by the Python proof.
		mappingErr := fixture.mappings.Close(ctx)
		fixture.serving.Close()
		functionErr := functions.Close(ctx)
		if mappingErr != nil || functionErr != nil {
			t.Errorf("SQS mapping owner cleanup: mappings=%v Lambda=%v", mappingErr, functionErr)
		}
	})
	return fixture
}

func TestSQSLambdaPythonSDKSmoke(t *testing.T) {
	python := sdkPython(t)
	first, second := newSQSMappingFixture(t, python, "A"), newSQSMappingFixture(t, python, "B")
	pendingARN := filepath.Join(t.TempDir(), "pending-arn")
	env := append(sdkEnvironment(t.TempDir(), "", "", ""),
		"SQS_LAMBDA_ENDPOINT_A="+first.serving.URL, "SQS_LAMBDA_OUTPUT_A="+first.output, "SQS_LAMBDA_GATE_A="+first.gate,
		"SQS_LAMBDA_ENDPOINT_B="+second.serving.URL, "SQS_LAMBDA_OUTPUT_B="+second.output, "SQS_LAMBDA_GATE_B="+second.gate,
		"SQS_LAMBDA_PENDING_ARN="+pendingARN)
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	output, err := runSDKProcess(ctx, python, fixturePath("python", "sqs_lambda_smoke.py"), env)
	t.Logf("real SQS/Lambda boto3 + registered Python completion proof:\n%s", output)
	require.NoError(t, err)
	arn, err := os.ReadFile(pendingARN)
	require.NoError(t, err)
	queue := first.broker.GetQueueByARN(string(arn))
	require.NotNil(t, queue)
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer closeCancel()
	require.NoError(t, first.mappings.Close(closeCtx))
	// The canceled handler remains unacknowledged; its native lease/retry still
	// belongs to SQS. Releasing the gate after Close cannot revive the child.
	require.NoError(t, os.WriteFile(first.gate, []byte("after-close"), 0600))
	time.Sleep(50 * time.Millisecond)
	captured, err := os.ReadFile(first.output)
	require.NoError(t, err)
	for _, line := range strings.Split(strings.TrimSpace(string(captured)), "\n") {
		var row struct {
			Stage string               `json:"stage"`
			Event eventsource.SQSEvent `json:"event"`
		}
		require.NoError(t, json.Unmarshal([]byte(line), &row))
		if strings.Contains(row.Event.Records[0].Body, "teardown-pending") {
			assert.Equal(t, "started", row.Stage, "owner Close must prevent later handler completion")
		}
	}
	messages, err := first.broker.ReceiveMessagesContext(closeCtx, queue, 1, 2*time.Second)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	assert.Equal(t, 2, messages[0].ReceiveCount)
	assert.Contains(t, messages[0].Body, "teardown-pending")
}
