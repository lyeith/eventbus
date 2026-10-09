// Development observation adapter for the original native execution owner.
package lambda

import "context"

// CompletionScope identifies what actually joined at response completion.
// Invocation scope retains a separately owned warm process; process scope has
// joined every launched process for this attempt (or did not launch one).
type CompletionScope string

const (
	CompletionProcess    CompletionScope = "process"
	CompletionInvocation CompletionScope = "invocation"
)

type InvocationState string

const (
	InvocationSucceeded  InvocationState = "succeeded"
	InvocationFailed     InvocationState = "failed"
	InvocationTimedOut   InvocationState = "timed_out"
	InvocationCanceled   InvocationState = "canceled"
	InvocationNotStarted InvocationState = "not_started"
)

// InvocationMetadata is the actual identity passed to the configured runner.
// It exposes no event, handler output, environment or execution command.
type InvocationMetadata struct {
	RequestID, FunctionARN, FunctionName string
	Attempt                              int
}

// InvocationOutcome returns after its response/output boundary and required
// retirement joins. Warm worker lifetimes remain separately owned. State is
// runner execution status, never a business-success assertion. OwnershipErr is
// private join/capture uncertainty, independent of native handler errors.
type InvocationOutcome struct {
	Metadata        InvocationMetadata
	Output          InvokeOutput
	State           InvocationState
	CompletionScope CompletionScope
	OwnershipErr    error
}

// ExecuteObserved uses the same validation, identity and runner as Execute.
// onAdmission runs outside mu inside an admitted lifetime, before child launch.
// A callback error prevents launch. Once admitted, identity and joined results
// remain available even when the ordinary Execute contract returns ctx.Err().
func (service *Service) ExecuteObserved(ctx context.Context, input InvokeInput, onAdmission func(InvocationMetadata) error) (InvocationOutcome, error) {
	return service.execute(ctx, input, onAdmission)
}

func invocationMetadata(entry executableFunction, input invocation) InvocationMetadata {
	return InvocationMetadata{RequestID: input.requestID, FunctionARN: functionARN(entry, input), FunctionName: input.name, Attempt: max(1, input.attempt)}
}
