package app

import (
	"context"
	"time"

	"github.com/lyeith/eventbus/internal/eventsource"
	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/messaging"
)

// These adapters compose service-owned ports. They carry no delivery, lease or
// retry policy and never construct a second Lambda runtime.
type snsLambdaInvoker struct{ runtime lambdaAdmission }

var _ messaging.LambdaDelivery = snsLambdaInvoker{}

func (invoker snsLambdaInvoker) ValidateLambdaTarget(ctx context.Context, arn string) error {
	return validateLambdaTarget(ctx, invoker.runtime, arn)
}
func (invoker snsLambdaInvoker) AdmitSNSLambda(ctx context.Context, arn string, payload []byte) (string, error) {
	admission, err := admitLambdaTarget(ctx, invoker.runtime, arn, payload)
	return admission.RequestID, err
}

type lambdaExecutionMetadata interface {
	lambdaExecution
	DescribeTarget(string, string) (lambdaservice.TargetInfo, error)
}

type eventSourceLambdaInvoker struct{ runtime lambdaExecutionMetadata }

var _ eventsource.FunctionInvoker = eventSourceLambdaInvoker{}

func (invoker eventSourceLambdaInvoker) ValidateTarget(ctx context.Context, arn string) (time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if invoker.runtime == nil {
		return 0, errLambdaTargetUnavailable
	}
	info, err := invoker.runtime.DescribeTarget(arn, "")
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	if err != nil {
		return 0, errLambdaTargetUnavailable
	}
	return info.Timeout, nil
}
func (invoker eventSourceLambdaInvoker) InvokeTarget(ctx context.Context, arn string, payload []byte) error {
	return executeLambdaTarget(ctx, invoker.runtime, arn, payload)
}

type sqsMappingSource struct{ broker *messaging.Broker }

var _ eventsource.QueueSource = sqsMappingSource{}

func (source sqsMappingSource) ResolveQueue(ctx context.Context, arn string) (eventsource.Queue, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if source.broker == nil {
		return nil, messaging.ErrQueueUnavailable
	}
	queue := source.broker.GetQueueByARN(arn)
	info, err := source.broker.QueueInfo(queue)
	if err != nil {
		return nil, err
	}
	return &sqsMappingQueue{broker: source.broker, queue: queue, info: eventsource.QueueInfo{ARN: info.ARN, VisibilityTimeout: info.VisibilityTimeout}}, nil
}

type sqsMappingQueue struct {
	broker *messaging.Broker
	queue  *messaging.Queue
	info   eventsource.QueueInfo
}

var _ eventsource.Queue = (*sqsMappingQueue)(nil)

func (queue *sqsMappingQueue) Info() eventsource.QueueInfo { return queue.info }
func (queue *sqsMappingQueue) Receive(ctx context.Context, max int) ([]eventsource.Record, error) {
	event, err := queue.broker.ReceiveSQSLambdaEventContext(ctx, queue.queue, max, 20*time.Second, eventsource.MaxBatchPayloadBytes)
	return event.Records, err
}
func (queue *sqsMappingQueue) Delete(ctx context.Context, receipt string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if _, err := queue.broker.QueueInfo(queue.queue); err != nil {
		return false, err
	}
	return queue.broker.DeleteMessage(queue.queue, receipt), nil
}
