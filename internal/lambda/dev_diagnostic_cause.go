// Private execution cause belongs to diagnostics, separate from legacy native
// timeout response projection. Snapshot it once after runner ownership joins.
package lambda

import (
	"context"
	"errors"
	"time"
)

type diagnosticCompletion struct {
	at                            time.Time
	contextError, cause           string
	contextErr, cancellationCause error
	synthesized                   bool
}

func snapshotDiagnosticCompletion(ctx context.Context, synthesized bool) diagnosticCompletion {
	result := diagnosticCompletion{at: time.Now(), contextErr: ctx.Err()}
	if result.contextErr != nil {
		// Once Err is non-nil, the first cancellation cause is immutable.
		// A later cancellation must not attach a cause to a healthy snapshot.
		result.cancellationCause = context.Cause(ctx)
		result.synthesized = synthesized
	}
	result.contextError, result.cause = projectDiagnosticCause(result.contextErr, result.cancellationCause)
	return result
}

// Top-level and per-launch diagnostics project the same frozen native pair.
// This helper never reads a live context or owns native cancellation policy.
func projectDiagnosticCause(contextErr, cancellationCause error) (contextError, cause string) {
	if contextErr == nil {
		return "", ""
	}
	contextError = "canceled"
	if errors.Is(contextErr, context.DeadlineExceeded) {
		contextError = "deadline_exceeded"
	}
	switch {
	case errors.Is(cancellationCause, errFunctionBudget):
		return "deadline_exceeded", "function_timeout"
	case errors.Is(cancellationCause, errInitializationBudget):
		return "deadline_exceeded", "initialization_timeout"
	case errors.Is(cancellationCause, errPhaseProtocol):
		return contextError, "runtime_protocol_error"
	case errors.Is(cancellationCause, errServiceCancellation):
		return contextError, "service_canceled"
	case errors.Is(contextErr, context.DeadlineExceeded), errors.Is(cancellationCause, context.DeadlineExceeded):
		return "deadline_exceeded", "caller_deadline"
	default:
		return contextError, "caller_canceled"
	}
}
