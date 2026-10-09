package lambda

import (
	"encoding/json"
	"errors"
	"fmt"
)

// ExecutionPolicy is an application-composed private execution contract, never
// a local recipe or native AWS resource setting. Nil retains Lambda's native
// result/log behavior. Hosts keep private services off the HTTP router.
type ExecutionPolicy struct {
	MaxResponseBytes int
	MaxLogBytes      int
	PrivateErrors    bool
	// ValidateResponse runs on serialized success before a worker may be reused.
	// It must be immutable, concurrency-safe, and contain no transport/lifecycle I/O.
	ValidateResponse func([]byte) error
}

func validateExecutionPolicy(policy *ExecutionPolicy) error {
	if policy == nil {
		return nil
	}
	if policy.MaxResponseBytes < 0 || policy.MaxResponseBytes > maxPayload {
		return errors.New("private execution response bound must be 1..6291456, or omitted")
	}
	if policy.MaxLogBytes < 0 || policy.MaxLogBytes > maxLogs {
		return errors.New("private execution log bound must be 1..65536, or omitted")
	}
	return nil
}
func responseLimit(input invocation) int {
	if input.responseLimit > 0 {
		return input.responseLimit
	}
	return maxPayload
}
func responseTooLarge(input invocation) invocationResult {
	return failure("Function.ResponseSizeTooLarge", fmt.Sprintf("Response exceeds the %d byte limit", responseLimit(input)))
}
func (service *Service) applyExecutionPolicy(result invocationResult) invocationResult {
	policy := service.executionPolicy
	if policy == nil {
		return result
	}
	logLimit := maxLogs
	if policy.MaxLogBytes > 0 {
		logLimit = policy.MaxLogBytes
	}
	if result.diagnostics.tailBytes > int64(logLimit) {
		result = preserveLaunchEvidence(result, failure("EventBus.InvalidResponse", "Private execution output limit exceeded"))
	} else if !result.functionError && policy.ValidateResponse != nil && policy.ValidateResponse(result.payload) != nil {
		result = preserveLaunchEvidence(result, failure("EventBus.InvalidResponse", "Private execution returned an invalid response"))
	}
	if policy.PrivateErrors && result.functionError {
		var detail struct {
			Type string `json:"errorType"`
		}
		_ = json.Unmarshal(result.payload, &detail)
		kind := "EventBus.HandlerFailure"
		switch detail.Type {
		case "EventBus.InvalidResponse", "Function.ResponseSizeTooLarge", "Runtime.InvalidResponse":
			kind = "EventBus.InvalidResponse"
		case "Sandbox.Timedout":
			kind = "Sandbox.Timedout"
		}
		result = preserveLaunchEvidence(result, failure(kind, "Private execution failed"))
	}
	return result
}
