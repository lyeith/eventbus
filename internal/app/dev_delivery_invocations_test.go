package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/eventsource"
	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/messaging"
	"github.com/stretchr/testify/require"
)

type devObservedDeliveryBackend struct {
	invocationBackend
	observed func(context.Context, lambdaservice.InvokeInput, func(lambdaservice.InvocationMetadata) error) (lambdaservice.InvocationOutcome, error)
}

func (backend devObservedDeliveryBackend) DescribeTarget(string, string) (lambdaservice.TargetInfo, error) {
	return lambdaservice.TargetInfo{Timeout: time.Second}, nil
}
func (backend devObservedDeliveryBackend) ExecuteObserved(ctx context.Context, input lambdaservice.InvokeInput, admitted func(lambdaservice.InvocationMetadata) error) (lambdaservice.InvocationOutcome, error) {
	return backend.observed(ctx, input, admitted)
}

func TestDevDeliveryAdapterPreservesActualIdentityAndPrivateUncertainty(t *testing.T) {
	const target = "arn:aws:lambda:us-east-1:000000000000:function:actual:live"
	payload := []byte(`{"private":"must-not-enter-evidence"}`)
	metadata := lambdaservice.InvocationMetadata{RequestID: "actual-runtime-request", FunctionARN: target, FunctionName: "actual:live", Attempt: 1}
	uncertainty := errors.New("private cleanup uncertainty")
	started, observed := false, false
	backend := devObservedDeliveryBackend{observed: func(ctx context.Context, input lambdaservice.InvokeInput, admitted func(lambdaservice.InvocationMetadata) error) (lambdaservice.InvocationOutcome, error) {
		require.Equal(t, target, input.FunctionName)
		require.Equal(t, payload, input.Payload)
		require.Empty(t, input.Qualifier)
		require.Empty(t, input.ClientContext)
		require.Empty(t, input.TraceID)
		require.NoError(t, admitted(metadata))
		started = true
		return lambdaservice.InvocationOutcome{Metadata: metadata, State: lambdaservice.InvocationSucceeded, OwnershipErr: uncertainty, Output: lambdaservice.InvokeOutput{Payload: []byte(`{"private":"function-result"}`)}}, nil
	}}
	adapter := eventSourceLambdaInvoker{runtime: backend}
	outcome, err := adapter.InvokeObservedTarget(t.Context(), target, payload, func(actual eventsource.InvocationMetadata) error {
		require.False(t, started, "identity must be observed before child launch")
		require.Equal(t, metadata.RequestID, actual.RequestID)
		require.Equal(t, target, actual.FunctionARN)
		observed = true
		return nil
	})
	require.NoError(t, err, "private uncertainty must preserve the native result")
	require.True(t, observed)
	require.True(t, started)
	require.Equal(t, metadata.RequestID, outcome.Metadata.RequestID)
	require.Equal(t, target, outcome.Metadata.FunctionARN)
	require.Equal(t, "succeeded", string(outcome.State))
	require.ErrorIs(t, outcome.OwnershipErr, uncertainty)
}

func TestDevDeliveryAdapterRefusesBeforeLaunchAndSharesResultRedaction(t *testing.T) {
	for _, hookFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "native function timeout", true: "capture refusal"}[hookFailure], func(t *testing.T) {
			captureErr := errors.New("private admission capture failure")
			started := false
			backend := devObservedDeliveryBackend{observed: func(ctx context.Context, input lambdaservice.InvokeInput, admitted func(lambdaservice.InvocationMetadata) error) (lambdaservice.InvocationOutcome, error) {
				metadata := lambdaservice.InvocationMetadata{RequestID: "original-id", FunctionARN: input.FunctionName, Attempt: 1}
				if err := admitted(metadata); err != nil {
					return lambdaservice.InvocationOutcome{Metadata: metadata, State: lambdaservice.InvocationNotStarted, OwnershipErr: err}, err
				}
				started = true
				return lambdaservice.InvocationOutcome{Metadata: metadata, State: lambdaservice.InvocationTimedOut, Output: lambdaservice.InvokeOutput{FunctionError: true, Payload: []byte(`{"errorType":"Sandbox.Timedout","errorMessage":"private-handler-error"}`)}}, nil
			}}
			adapter := eventSourceLambdaInvoker{runtime: backend}
			outcome, err := adapter.InvokeObservedTarget(t.Context(), "arn:aws:lambda:us-east-1:000000000000:function:actual", []byte(`{}`), func(eventsource.InvocationMetadata) error {
				if hookFailure {
					return captureErr
				}
				return nil
			})
			require.Error(t, err)
			require.NotContains(t, err.Error(), "private")
			require.Equal(t, !hookFailure, started)
			if hookFailure {
				require.Equal(t, "not_started", string(outcome.State))
				require.ErrorIs(t, outcome.OwnershipErr, captureErr)
			} else {
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.Equal(t, "timed_out", string(outcome.State))
			}
		})
	}
}

func TestDevReceiptAdapterDelegatesInspectionAndSettlementToOriginalQueue(t *testing.T) {
	broker := messaging.NewBroker("us-east-1", "000000000000", 0)
	queue := broker.CreateQueue("evidence-ports", time.Minute, time.Hour)
	for range 2 {
		_, err := broker.SendQueueMessage(queue, messaging.QueueMessageInput{Body: "native-body"})
		require.NoError(t, err)
	}
	bound, err := (sqsMappingSource{broker: broker}).ResolveQueue(t.Context(), queue.ARN)
	require.NoError(t, err)
	adapter := bound.(*sqsMappingQueue)
	records, err := adapter.Receive(t.Context(), 2)
	require.NoError(t, err)
	require.Len(t, records, 2)
	before, err := adapter.InspectReceipt(t.Context(), records[0].ReceiptHandle)
	require.NoError(t, err)
	require.Equal(t, "unacknowledged", string(before))
	waiting, inFlight := broker.QueueDepth(queue)
	require.Zero(t, waiting)
	require.Equal(t, 2, inFlight, "evidence inspection must not settle another receipt")
	mapping, err := adapter.AcknowledgeReceipt(t.Context(), records[0].ReceiptHandle)
	require.NoError(t, err)
	require.Equal(t, "mapping_settled", string(mapping))
	require.True(t, broker.DeleteMessage(queue, records[1].ReceiptHandle))
	native, err := adapter.InspectReceipt(t.Context(), records[1].ReceiptHandle)
	require.NoError(t, err)
	require.Equal(t, "native_settled", string(native))
	joined, err := adapter.AcknowledgeReceipt(t.Context(), records[1].ReceiptHandle)
	require.NoError(t, err)
	require.Equal(t, native, joined)
	waiting, inFlight = broker.QueueDepth(queue)
	require.Zero(t, waiting)
	require.Zero(t, inFlight)
}
