package lambda

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// InvokeInput is the local execution/admission seam. Payload remains raw JSON;
// callers own service-specific events and policies. ClientContext is decoded
// JSON here (the HTTP adapter owns base64) and applies only to Execute.
type InvokeInput struct {
	FunctionName, Qualifier string
	Payload                 []byte
	ClientContext, TraceID  string
}
type InvokeOutput struct {
	Payload                    []byte
	FunctionError              bool
	RequestID, ExecutedVersion string
}
type Admission struct{ RequestID string }

// InvokeError denotes failure before function execution/async acceptance.
// Function failures belong to InvokeOutput or asynchronous execution evidence.
type InvokeError struct {
	Code, Message string
	Status        int
}

func (err *InvokeError) Error() string { return err.Code + ": " + err.Message }
func invocationError(status int, code, message string) error {
	return &InvokeError{Code: code, Message: message, Status: status}
}

func (service *Service) resolveTarget(name, qualifier string) (executableFunction, string, error) {
	resolved, err := resolveName(name, qualifier)
	if err != nil {
		return executableFunction{}, "", invocationError(http.StatusBadRequest, "InvalidParameterValueException", err.Error())
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	entry, found := service.functions[resolved]
	if !found && strings.HasSuffix(resolved, ":$LATEST") {
		entry, found = service.functions[strings.TrimSuffix(resolved, ":$LATEST")]
	}
	if !found {
		return executableFunction{}, "", invocationError(http.StatusNotFound, "ResourceNotFoundException", "Function not found: "+resolved)
	}
	return entry, resolved, nil
}

// TargetInfo describes immutable registered execution settings, without exposing
// commands, environment or mutable runtime state to service coordinators.
type TargetInfo struct {
	FunctionName string
	Timeout      time.Duration
}

func (service *Service) DescribeTarget(name, qualifier string) (TargetInfo, error) {
	entry, resolved, err := service.resolveTarget(name, qualifier)
	if err != nil {
		return TargetInfo{}, err
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.closed {
		return TargetInfo{}, invocationError(http.StatusServiceUnavailable, "ServiceException", "Lambda service is closing")
	}
	return TargetInfo{FunctionName: resolved, Timeout: entry.timeout}, nil
}

func (service *Service) ValidateTarget(name, qualifier string) error {
	_, err := service.DescribeTarget(name, qualifier)
	return err
}

func validateExecution(input InvokeInput, limit int) error {
	if len(input.Payload) > limit {
		return invocationError(http.StatusRequestEntityTooLarge, "RequestTooLargeException", fmt.Sprintf("Request exceeds the %d byte limit", limit))
	}
	if !json.Valid(input.Payload) {
		return invocationError(http.StatusBadRequest, "InvalidRequestContentException", "Invocation payload must be JSON")
	}
	if input.ClientContext != "" && !json.Valid([]byte(input.ClientContext)) {
		return invocationError(http.StatusBadRequest, "InvalidParameterValueException", "ClientContext must be decoded JSON")
	}
	return nil
}

func prepareInvocation(entry executableFunction, name string, input InvokeInput, requestID string) invocation {
	value := invocation{payload: input.Payload, requestID: requestID, name: name, clientContext: input.ClientContext, traceID: input.TraceID}
	if strings.HasPrefix(input.FunctionName, "arn:") {
		value.functionARN = input.FunctionName
		if input.Qualifier != "" {
			value.functionARN += ":" + input.Qualifier
		}
	} else if account, _, found := strings.Cut(input.FunctionName, ":function:"); found {
		region := entry.environment["AWS_REGION"]
		if region == "" {
			region = "us-east-1"
		}
		value.functionARN = "arn:aws:lambda:" + region + ":" + account + ":function:" + name
	}
	return value
}

func executedVersion(name string) string {
	if _, qualifier, found := strings.Cut(name, ":"); found {
		if _, err := strconv.ParseUint(qualifier, 10, 64); err == nil {
			return qualifier
		}
	}
	return "$LATEST"
}

// Execute joins the handler response and output boundary plus any required
// process retirement. Successful warm environments remain owned by the service
// until reload, drain or Close. Coordinators share this single execution owner.
func (service *Service) Execute(ctx context.Context, input InvokeInput) (InvokeOutput, error) {
	outcome, err := service.execute(ctx, input, nil)
	if err != nil {
		return InvokeOutput{}, err
	}
	return outcome.Output, nil
}

func (service *Service) execute(ctx context.Context, input InvokeInput, onAdmission func(InvocationMetadata) error) (InvocationOutcome, error) {
	outcome := InvocationOutcome{State: InvocationNotStarted}
	if err := ctx.Err(); err != nil {
		return outcome, err
	}
	entry, name, err := service.resolveTarget(input.FunctionName, input.Qualifier)
	if err != nil {
		return outcome, err
	}
	if err := validateExecution(input, maxPayload); err != nil {
		return outcome, err
	}
	input.Payload = append([]byte(nil), input.Payload...)
	invocation := prepareInvocation(entry, name, input, uuid.NewString())
	invocation.onAdmission = onAdmission
	result, err := service.invoke(ctx, entry, invocation)
	if result.admitted {
		outcome.Metadata = invocationMetadata(entry, invocation)
		outcome.Output = InvokeOutput{Payload: result.payload, FunctionError: result.functionError, RequestID: invocation.requestID, ExecutedVersion: executedVersion(name)}
		outcome.State = result.state
		outcome.CompletionScope = result.completionScope
		outcome.OwnershipErr = result.ownershipErr
	}
	if err != nil {
		if errors.Is(err, errClosed) {
			err = invocationError(http.StatusServiceUnavailable, "ServiceException", "Lambda service is closing")
		}
		return outcome, err
	}
	if err := ctx.Err(); err != nil {
		return outcome, err
	}
	return outcome, nil
}
