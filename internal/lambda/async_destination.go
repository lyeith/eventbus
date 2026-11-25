package lambda

import (
	"context"
	"encoding/json"
	"time"
)

// AsyncDestinationSink routes a completed native record to an owned resource.
// Delivery must honor cancellation and join its own work before returning.
// It may call Admit on this service without re-entering an async lock.
type AsyncDestinationSink interface {
	ValidateAsyncDestination(arn string, onFailure bool) error
	DeliverAsyncDestination(ctx context.Context, arn string, record AsyncInvocationRecord) error
}

// AsyncInvocationRecord is the AWS event delivered to a destination; it is
// separate from redacted harness evidence. Only the sink receives the payloads.
type AsyncInvocationRecord struct {
	Version         string                `json:"version"`
	Timestamp       time.Time             `json:"timestamp"`
	RequestContext  AsyncRequestContext   `json:"requestContext"`
	RequestPayload  json.RawMessage       `json:"requestPayload"`
	ResponseContext *AsyncResponseContext `json:"responseContext"`
	ResponsePayload json.RawMessage       `json:"responsePayload"`
}

type AsyncRequestContext struct {
	RequestID              string `json:"requestId"`
	FunctionARN            string `json:"functionArn"`
	Condition              string `json:"condition"`
	ApproximateInvokeCount int    `json:"approximateInvokeCount"`
}

type AsyncResponseContext struct {
	StatusCode      int    `json:"statusCode"`
	ExecutedVersion string `json:"executedVersion,omitempty"`
	FunctionError   string `json:"functionError,omitempty"`
}

func (service *Service) asyncDestinationRecord(task *asyncTask, terminal AsyncRecord, result invocationResult, condition string, status int) AsyncInvocationRecord {
	record := AsyncInvocationRecord{
		Version: "1.0", Timestamp: terminal.CompletedAt,
		RequestContext: AsyncRequestContext{RequestID: terminal.RequestID, FunctionARN: service.asyncFunctionARN(task.input.name),
			Condition: condition, ApproximateInvokeCount: terminal.Attempts},
		RequestPayload: append(json.RawMessage(nil), task.input.payload...),
	}
	if status != 0 {
		record.ResponseContext = &AsyncResponseContext{StatusCode: status}
		if status == 200 {
			record.ResponseContext.ExecutedVersion = executedVersion(task.input.name)
		}
		if result.functionError {
			record.ResponseContext.FunctionError = "Unhandled"
		}
		if json.Valid(result.payload) && !result.devCompletionLoss {
			record.ResponsePayload = append(json.RawMessage(nil), result.payload...)
		}
	}
	return record
}

// The caller holds asyncTransitionMu. The selected task belongs to this
// worker and is absent from asyncQueue. Release the lock for downstream
// admission, while preserving this worker's lifecycle and the event's slot.
// A failure to deliver does not retry the handler or publish its data.
func (service *Service) finishAsyncDestination(task *asyncTask, state, errorType, condition string, result invocationResult, status int) {
	destination := task.policy.destinations.OnFailure.Destination
	if state == "succeeded" {
		destination = task.policy.destinations.OnSuccess.Destination
	}
	if destination == "" || service.asyncDestinations == nil || task.ownershipErr != nil {
		service.finishAsyncCrash(task, state, errorType, result.devWorkerCrash)
		return
	}
	service.mu.Lock()
	record := asyncTerminalRecord(task.record, state, errorType, result.devWorkerCrash)
	task.finishing = true
	service.mu.Unlock()
	envelope := service.asyncDestinationRecord(task, record, result, condition, status)
	record.DestinationStatus = service.routeAsyncDestination(destination, envelope)
	service.settleAsyncRecord(task, record)
}

func (service *Service) routeAsyncDestination(destination string, record AsyncInvocationRecord) string {
	service.asyncTransitionMu.Unlock()
	defer service.asyncTransitionMu.Lock()
	ctx, cancel := context.WithTimeout(service.asyncContext, 30*time.Second)
	defer cancel()
	if ctx.Err() != nil || service.asyncDestinations.DeliverAsyncDestination(ctx, destination, record) != nil {
		return "failed"
	}
	return "delivered"
}
