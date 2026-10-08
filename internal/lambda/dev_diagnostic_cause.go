// Private execution cause belongs to diagnostics, separate from legacy native
// timeout response projection. Snapshot it once after runner ownership joins.
package lambda

import (
	"context"
	"errors"
	"time"
)

var errFunctionBudget = errors.New("Lambda configured function budget expired")
var errServiceCancellation = errors.New("Lambda service stopped execution")

type diagnosticCompletion struct {
	at                  time.Time
	contextError, cause string
	synthesized         bool
}

func snapshotDiagnosticCompletion(ctx context.Context, synthesized bool) diagnosticCompletion {
	result := diagnosticCompletion{at: time.Now()}
	contextErr := ctx.Err()
	if contextErr == nil {
		return result
	}
	result.synthesized = synthesized
	result.contextError = "canceled"
	if errors.Is(contextErr, context.DeadlineExceeded) {
		result.contextError = "deadline_exceeded"
	}
	switch {
	case errors.Is(context.Cause(ctx), errFunctionBudget):
		result.cause = "function_timeout"
	case errors.Is(context.Cause(ctx), errServiceCancellation):
		result.cause = "service_canceled"
	case errors.Is(contextErr, context.DeadlineExceeded):
		result.cause = "caller_deadline"
	default:
		result.cause = "caller_canceled"
	}
	return result
}
