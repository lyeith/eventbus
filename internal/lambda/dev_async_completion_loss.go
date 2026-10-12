// Explicit development fault adapter. Native handlers, effects and retry policy
// remain owned by the ordinary asynchronous execution path.
package lambda

import (
	"context"
	"errors"
	"net/http"

	"github.com/google/uuid"
)

const devCompletionLostError = "Dev.CompletionLost"

type DevAsyncCompletionLoss struct {
	RequestID    string `json:"request_id"`
	FunctionName string `json:"function_name"`
	Attempt      int    `json:"attempt"`
	Armed        bool   `json:"armed"`
}

type DevAsyncControlError struct {
	Status int
	Code   string
}

func (err *DevAsyncControlError) Error() string { return err.Code }

func devAsyncControlError(status int, code string) error {
	return &DevAsyncControlError{Status: status, Code: code}
}

// DevAdmitAsyncCompletionLoss atomically arms the first handler attempt during
// ordinary native Event admission. Fast, unchanged handlers cannot outrun this
// one-shot; capture, capacity, validation and rollback use the same owner.
func (service *Service) DevAdmitAsyncCompletionLoss(ctx context.Context, input InvokeInput) (Admission, error) {
	if service == nil || !service.asyncCompletionLossEnabled {
		return Admission{}, devAsyncControlError(http.StatusNotFound, "completion_loss_disabled")
	}
	return service.admit(ctx, input, true)
}

// DevArmAsyncCompletionLoss targets only an already-owned Event's current or
// next handler attempt. Repeating the same live arm is idempotent. The decision
// is linearized against the actual joined completion, never against a timer.
func (service *Service) DevArmAsyncCompletionLoss(ctx context.Context, function, requestID string, attempt int) (DevAsyncCompletionLoss, error) {
	return service.devChangeAsyncCompletionLoss(ctx, function, requestID, attempt, true)
}

// DevClearAsyncCompletionLoss disarms the selected attempt. It cannot undo a
// completion already lost, arm another attempt, or affect another event.
func (service *Service) DevClearAsyncCompletionLoss(ctx context.Context, function, requestID string, attempt int) (DevAsyncCompletionLoss, error) {
	return service.devChangeAsyncCompletionLoss(ctx, function, requestID, attempt, false)
}

func (service *Service) devChangeAsyncCompletionLoss(ctx context.Context, function, requestID string, attempt int, arm bool) (DevAsyncCompletionLoss, error) {
	if service == nil || !service.asyncCompletionLossEnabled {
		return DevAsyncCompletionLoss{}, devAsyncControlError(http.StatusNotFound, "completion_loss_disabled")
	}
	if err := ctx.Err(); err != nil {
		return DevAsyncCompletionLoss{}, err
	}
	if function == "" || attempt < 1 || attempt > 3 {
		return DevAsyncCompletionLoss{}, devAsyncControlError(http.StatusBadRequest, "invalid_completion_loss_target")
	}
	if _, err := uuid.Parse(requestID); err != nil {
		return DevAsyncCompletionLoss{}, devAsyncControlError(http.StatusBadRequest, "invalid_async_request_id")
	}
	_, name, err := service.resolveTarget(function, "")
	if err != nil {
		return DevAsyncCompletionLoss{}, devAsyncControlError(http.StatusNotFound, "function_not_found")
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return DevAsyncCompletionLoss{}, err
	}
	task := service.asyncTasks[requestID]
	if task == nil || task.record.FunctionName != name {
		return DevAsyncCompletionLoss{}, devAsyncControlError(http.StatusNotFound, "async_event_not_found")
	}
	result := DevAsyncCompletionLoss{RequestID: requestID, FunctionName: name, Attempt: attempt}
	if !arm {
		if task.completionLossAttempt == attempt {
			task.completionLossAttempt = 0
		} else if task.completionLossAttempt != 0 {
			return DevAsyncCompletionLoss{}, devAsyncControlError(http.StatusConflict, "different_attempt_armed")
		}
		return result, nil
	}
	if service.closed || service.asyncClosed {
		return DevAsyncCompletionLoss{}, devAsyncControlError(http.StatusConflict, "async_service_closing")
	}
	expected := task.record.Attempts + 1
	if task.record.State == "running" {
		expected = task.record.Attempts
	}
	if attempt != expected || task.completionDecidedAttempt >= attempt {
		return DevAsyncCompletionLoss{}, devAsyncControlError(http.StatusConflict, "attempt_not_armable")
	}
	task.completionLossAttempt = attempt
	result.Armed = true
	return result, nil
}

// The native execution owner calls this only after a real attempt joins, before
// ordinary private diagnostics. It preserves the actual runtime result and
// consumes the one-shot even when the target handler fails. Nonlaunch throttles
// have no completion to lose and leave the upcoming handler attempt armed.
func (service *Service) devCompleteAsyncAttempt(input invocation, succeeded bool) bool {
	if !service.asyncCompletionLossEnabled {
		return false
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	task := service.asyncTasks[input.requestID]
	if task == nil || task.record.State != "running" || task.record.Attempts != input.attempt {
		return false
	}
	task.completionDecidedAttempt = input.attempt
	armed := task.completionLossAttempt == input.attempt
	task.completionLossAttempt = 0
	return armed && succeeded && !service.asyncAborted
}

func devAsyncHTTPError(err error) (int, string) {
	var control *DevAsyncControlError
	if errors.As(err, &control) {
		return control.Status, control.Code
	}
	var invokeErr *InvokeError
	if errors.As(err, &invokeErr) {
		return invokeErr.Status, invokeErr.Code
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return http.StatusServiceUnavailable, "async_control_interrupted"
	}
	return http.StatusInternalServerError, "async_control_failed"
}
