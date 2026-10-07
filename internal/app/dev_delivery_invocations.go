package app

import (
	"context"

	"github.com/lyeith/eventbus/internal/eventsource"
	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
)

// These optional ports carry private evidence between service owners. Lambda
// supplies its actual identity before launch and its result after cleanup;
// messaging classifies only the original bound queue receipt.
type devLambdaObservedExecution interface {
	ExecuteObserved(context.Context, lambdaservice.InvokeInput, func(lambdaservice.InvocationMetadata) error) (lambdaservice.InvocationOutcome, error)
}

var _ eventsource.ObservedFunctionInvoker = eventSourceLambdaInvoker{}
var _ eventsource.ReceiptEvidenceQueue = (*sqsMappingQueue)(nil)

func (invoker eventSourceLambdaInvoker) InvokeObservedTarget(ctx context.Context, arn string, payload []byte, onAdmission func(eventsource.InvocationMetadata) error) (eventsource.InvocationOutcome, error) {
	if err := ctx.Err(); err != nil {
		return eventsource.InvocationOutcome{State: eventsource.InvocationNotStarted}, err
	}
	runtime, ok := invoker.runtime.(devLambdaObservedExecution)
	if !ok {
		return eventsource.InvocationOutcome{State: eventsource.InvocationNotStarted}, errLambdaInvocation
	}
	var observe func(lambdaservice.InvocationMetadata) error
	if onAdmission != nil {
		observe = func(metadata lambdaservice.InvocationMetadata) error {
			return onAdmission(eventsource.InvocationMetadata{RequestID: metadata.RequestID, FunctionARN: metadata.FunctionARN})
		}
	}
	outcome, err := runtime.ExecuteObserved(ctx, lambdaservice.InvokeInput{FunctionName: arn, Payload: payload}, observe)
	return eventsource.InvocationOutcome{
		Metadata:     eventsource.InvocationMetadata{RequestID: outcome.Metadata.RequestID, FunctionARN: outcome.Metadata.FunctionARN},
		State:        eventsource.InvocationState(outcome.State),
		OwnershipErr: outcome.OwnershipErr,
	}, redactLambdaExecution(outcome.Output, err)
}

func (queue *sqsMappingQueue) AcknowledgeReceipt(ctx context.Context, receipt string) (eventsource.ReceiptOutcome, error) {
	outcome, err := queue.broker.EvaluateSQSLambdaReceiptContext(ctx, queue.queue, receipt, true)
	return eventsource.ReceiptOutcome(outcome), err
}

func (queue *sqsMappingQueue) InspectReceipt(ctx context.Context, receipt string) (eventsource.ReceiptOutcome, error) {
	outcome, err := queue.broker.EvaluateSQSLambdaReceiptContext(ctx, queue.queue, receipt, false)
	return eventsource.ReceiptOutcome(outcome), err
}
