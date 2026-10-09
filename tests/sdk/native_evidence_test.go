//go:build sdksmoke

package sdk

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/devquiescence"
	"github.com/lyeith/eventbus/internal/eventsource"
	"github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/messaging"
	"github.com/lyeith/eventbus/internal/server"
	"github.com/stretchr/testify/require"
)

// These adapters project actual producer/runtime identities and queue-owner
// receipt states. They add no event, execution, retry or settlement policy.
type nativeEvidenceInvoker struct{ sdkMappingInvoker }

func (invoker nativeEvidenceInvoker) InvokeObservedTarget(ctx context.Context, arn string, payload []byte, onAdmission func(eventsource.InvocationMetadata) error) (eventsource.InvocationOutcome, error) {
	outcome, err := invoker.functions.ExecuteObserved(ctx, lambda.InvokeInput{FunctionName: arn, Payload: payload}, func(metadata lambda.InvocationMetadata) error {
		return onAdmission(eventsource.InvocationMetadata{RequestID: metadata.RequestID, FunctionARN: metadata.FunctionARN})
	})
	projected := eventsource.InvocationOutcome{Metadata: eventsource.InvocationMetadata{RequestID: outcome.Metadata.RequestID, FunctionARN: outcome.Metadata.FunctionARN},
		State: eventsource.InvocationState(outcome.State), CompletionScope: string(outcome.CompletionScope), OwnershipErr: outcome.OwnershipErr}
	if err == nil && outcome.Output.FunctionError {
		err = errors.New("Lambda function failed")
	}
	return projected, err
}

type nativeEvidenceQueueSource struct{ broker *messaging.Broker }
type nativeEvidenceQueue struct{ sdkMappingQueue }

func (source nativeEvidenceQueueSource) ResolveQueue(ctx context.Context, arn string) (eventsource.Queue, error) {
	queue, err := (sdkMappingSource{source.broker}).ResolveQueue(ctx, arn)
	if err != nil {
		return nil, err
	}
	return &nativeEvidenceQueue{queue.(sdkMappingQueue)}, nil
}
func (queue *nativeEvidenceQueue) AcknowledgeReceipt(ctx context.Context, receipt string) (eventsource.ReceiptOutcome, error) {
	outcome, err := queue.broker.EvaluateSQSLambdaReceiptContext(ctx, queue.queue, receipt, true)
	return eventsource.ReceiptOutcome(outcome), err
}
func (queue *nativeEvidenceQueue) InspectReceipt(ctx context.Context, receipt string) (eventsource.ReceiptOutcome, error) {
	outcome, err := queue.broker.EvaluateSQSLambdaReceiptContext(ctx, queue.queue, receipt, false)
	return eventsource.ReceiptOutcome(outcome), err
}
func (queue *nativeEvidenceQueue) RegisterRetained() (func(), error) {
	return queue.broker.RegisterSQSLambdaCustody(queue.queue)
}
func (queue *nativeEvidenceQueue) PendingRetained() (bool, <-chan struct{}, error) {
	return queue.broker.SQSLambdaCustodyState(queue.queue)
}
func (queue *nativeEvidenceQueue) ReceiveRetained(ctx context.Context, max int) ([]eventsource.Record, error) {
	event, err := queue.broker.ReceiveOwnedSQSLambdaEventContext(ctx, queue.queue, max, time.Second, eventsource.MaxBatchPayloadBytes)
	return event.Records, err
}

func TestNativeEvidencePythonSDKSmoke(t *testing.T) {
	python := sdkPython(t)
	directory := t.TempDir()
	root := filepath.Join(directory, "evidence")
	require.NoError(t, os.Mkdir(root, 0700))
	deliveryPath, diagnosticPath := filepath.Join(root, "deliveries.jsonl"), filepath.Join(root, "private-diagnostics.jsonl")
	asyncPath, snsPath := filepath.Join(root, "async.jsonl"), filepath.Join(root, "sns.jsonl")
	source, callback, control := httptest.NewUnstartedServer(nil), httptest.NewUnstartedServer(nil), httptest.NewUnstartedServer(nil)
	sourceURL, callbackURL := "http://"+source.Listener.Addr().String(), "http://"+callback.Listener.Addr().String()
	capture, err := messaging.OpenSNSCapture(snsPath)
	require.NoError(t, err)
	var functions *lambda.Service
	var mappings *eventsource.Service
	owner := devquiescence.NewWithOptions(devquiescence.Options{Checks: []func() error{
		capture.Err,
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
	}})
	require.NoError(t, owner.SetCallbackOrigin(callbackURL))
	t.Cleanup(func() {
		// Release fixture work even after assertions fail, while callbacks and
		// capture owners remain live. Terminal cancellation still joins children.
		if observations, err := os.Open(filepath.Join(root, "observations.jsonl")); err == nil {
			scanner := bufio.NewScanner(observations)
			for scanner.Scan() {
				var row struct {
					RequestID string `json:"request_id"`
				}
				if json.Unmarshal(scanner.Bytes(), &row) == nil && row.RequestID != "" {
					_ = os.WriteFile(filepath.Join(root, "release-"+row.RequestID), nil, 0600)
				}
			}
			_ = observations.Close()
		}
		barrierCtx, cancelBarrier := context.WithTimeout(context.Background(), 20*time.Second)
		owner.Shutdown()
		_, barrierErr := owner.Quiesce(barrierCtx)
		cancelBarrier()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		failures := []error{barrierErr}
		if mappings != nil {
			failures = append(failures, mappings.Close(ctx))
		}
		if functions != nil {
			failures = append(failures, functions.DrainAsync(ctx))
		}
		source.Close()
		callback.Close()
		control.Close()
		if functions != nil {
			failures = append(failures, functions.Close(ctx))
		}
		failures = append(failures, capture.Close())
		if err := errors.Join(failures...); err != nil {
			t.Errorf("native evidence SDK owned cleanup: %v", err)
		}
	})
	environment := map[string]string{"NATIVE_EVIDENCE_ROOT": root, "NATIVE_EVIDENCE_DATABASE": filepath.Join(directory, "business.db"),
		"NATIVE_EVIDENCE_CALLBACK": callbackURL, "NATIVE_EVIDENCE_PRIVATE_ERROR": "private-env-token-kept-only-in-diagnostics"}
	function := func(handler string, timeout time.Duration) lambda.Function {
		return lambda.Function{Runtime: "python", Command: []string{python, "-E", "-s"},
			Handler: fixturePath("python", "native_evidence_smoke.py") + "#" + handler, Timeout: timeout, Environment: environment}
	}
	functions, err = lambda.NewService(&lambda.Config{Functions: map[string]lambda.Function{
		"native-producer:live": function("producer", 6*time.Second), "native-queue:live": function("queue_handler", 6*time.Second),
		"native-queue:short": function("queue_handler", 1500*time.Millisecond), "native-sns:live": function("sns_handler", 6*time.Second),
		"native-sns:short": function("sns_handler", 1500*time.Millisecond)}, DevActivity: owner,
		DevAsync:       &lambda.DevAsyncConfig{Workers: 2, Capacity: 16, RetryDelays: []time.Duration{200 * time.Millisecond, 200 * time.Millisecond}, LogPath: asyncPath},
		DevDiagnostics: &lambda.DevDiagnosticsConfig{LogPath: diagnosticPath}}, directory)
	require.NoError(t, err)
	broker := messaging.NewBroker("us-east-1", "000000000000", source.Listener.Addr().(*net.TCPAddr).Port)
	require.NoError(t, broker.SetDevActivity(owner))
	broker.SetSNSCapture(capture)
	broker.SetLambdaDelivery(sdkSNSLambdaDelivery{functions})
	mappings, err = eventsource.New(eventsource.Options{Region: "us-east-1", AccountID: "000000000000",
		Dev: eventsource.DevOptions{Source: owner, Activity: owner, DeliveryCapture: &eventsource.DevDeliveryCaptureConfig{LogPath: deliveryPath}}},
		nativeEvidenceQueueSource{broker}, nativeEvidenceInvoker{sdkMappingInvoker{functions}})
	require.NoError(t, err)
	aws := server.New(server.Services{Messaging: messaging.NewHandler(broker), Lambda: functions, EventSources: eventsource.NewHandler(mappings)})
	source.Config.Handler = owner.Wrap(devquiescence.Source, aws, nil)
	callback.Config.Handler = owner.Wrap(devquiescence.Callback, aws, nil)
	control.Config.Handler = devquiescence.NewHandler(owner)
	source.Start()
	callback.Start()
	control.Start()
	env := append(sdkEnvironment(t.TempDir(), "", "", ""), "NATIVE_EVIDENCE_ROOT="+root, "NATIVE_EVIDENCE_DATABASE="+environment["NATIVE_EVIDENCE_DATABASE"],
		"NATIVE_EVIDENCE_SOURCE="+sourceURL, "NATIVE_EVIDENCE_CALLBACK="+callbackURL, "NATIVE_EVIDENCE_CONTROL="+control.URL,
		"NATIVE_EVIDENCE_DELIVERY="+deliveryPath, "NATIVE_EVIDENCE_DIAGNOSTICS="+diagnosticPath,
		"NATIVE_EVIDENCE_ASYNC="+asyncPath, "NATIVE_EVIDENCE_SNS="+snsPath, "NATIVE_EVIDENCE_PRIVATE_ERROR="+environment["NATIVE_EVIDENCE_PRIVATE_ERROR"])
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	output, err := runSDKProcess(ctx, python, fixturePath("python", "native_evidence_smoke.py"), env)
	t.Logf("actual HTTP producer, native handlers, correlated joined delivery and private diagnostics:\n%s", output)
	require.NoError(t, err)
}
