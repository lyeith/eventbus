package app

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/scheduler"
	"github.com/lyeith/eventbus/internal/secrets"
)

// App composition adapts the Lambda owner's typed operations to the narrower
// consumer-owned ports. Service event/retry policy stays with each consumer.
type lambdaTargetValidator interface{ ValidateTarget(string, string) error }
type lambdaExecution interface {
	lambdaTargetValidator
	Execute(context.Context, lambdaservice.InvokeInput) (lambdaservice.InvokeOutput, error)
}
type lambdaAdmission interface {
	lambdaTargetValidator
	Admit(context.Context, lambdaservice.InvokeInput) (lambdaservice.Admission, error)
}

var errLambdaTargetUnavailable = errors.New("Lambda target is unavailable")
var errRotationInvocation = errors.New("Rotation function invocation failed")
var errRotationExecution = errors.New("Rotation function execution failed")

func validateLambdaTarget(ctx context.Context, runtime lambdaTargetValidator, arn string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if runtime == nil {
		return errLambdaTargetUnavailable
	}
	validationErr := runtime.ValidateTarget(arn, "")
	if err := ctx.Err(); err != nil {
		return err
	}
	if validationErr != nil {
		return errLambdaTargetUnavailable
	}
	return nil
}

type rotationLambdaInvoker struct{ runtime lambdaExecution }

var _ secrets.RotationInvoker = rotationLambdaInvoker{}

func (invoker rotationLambdaInvoker) ValidateRotationTarget(ctx context.Context, arn string) error {
	return validateLambdaTarget(ctx, invoker.runtime, arn)
}
func (invoker rotationLambdaInvoker) InvokeRotation(ctx context.Context, arn string, event secrets.RotationEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if invoker.runtime == nil {
		return errRotationInvocation
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return errRotationInvocation
	}
	result, err := invoker.runtime.Execute(ctx, lambdaservice.InvokeInput{FunctionName: arn, Payload: payload})
	if err != nil {
		// Canonical context errors carry no function data. All other errors and
		// handler payloads are redacted at this service boundary.
		if errors.Is(err, context.Canceled) {
			return context.Canceled
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return context.DeadlineExceeded
		}
		return errRotationInvocation
	}
	if result.FunctionError {
		var failure struct {
			ErrorType string `json:"errorType"`
		}
		if json.Unmarshal(result.Payload, &failure) == nil && (failure.ErrorType == "Sandbox.Timedout" || failure.ErrorType == "TimeoutError") {
			return context.DeadlineExceeded
		}
		return errRotationExecution
	}
	return nil
}

type schedulerLambdaInvoker struct{ runtime lambdaAdmission }

var _ scheduler.TargetInvoker = schedulerLambdaInvoker{}

func (invoker schedulerLambdaInvoker) ValidateTarget(ctx context.Context, arn string) error {
	return validateLambdaTarget(ctx, invoker.runtime, arn)
}

type lambdaAdmissionFailure struct {
	retryable    bool
	contextError error
}

func (failure *lambdaAdmissionFailure) Error() string   { return "Lambda target admission failed" }
func (failure *lambdaAdmissionFailure) Retryable() bool { return failure.retryable }
func (failure *lambdaAdmissionFailure) Unwrap() error   { return failure.contextError }

func (invoker schedulerLambdaInvoker) AdmitTarget(ctx context.Context, arn string, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return &lambdaAdmissionFailure{contextError: err}
	}
	if invoker.runtime == nil {
		return &lambdaAdmissionFailure{}
	}
	_, err := invoker.runtime.Admit(ctx, lambdaservice.InvokeInput{FunctionName: arn, Payload: payload})
	if err == nil {
		return nil
	}
	failure := &lambdaAdmissionFailure{}
	if errors.Is(err, context.Canceled) {
		failure.contextError = context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		failure.contextError = context.DeadlineExceeded
	}
	var native *lambdaservice.InvokeError
	if errors.As(err, &native) {
		failure.retryable = native.Status == http.StatusTooManyRequests || native.Status >= 500 && native.Status <= 599
	}
	return failure
}
