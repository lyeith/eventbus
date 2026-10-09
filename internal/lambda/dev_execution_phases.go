// Private phase evidence projects frozen core facts; this adapter does not own
// deadlines, readiness, retry eligibility, native errors or process cleanup.
package lambda

type diagnosticPhase struct {
	InitAttempt        int             `json:"init_attempt"`
	Mode               string          `json:"mode"`
	CompletionScope    CompletionScope `json:"completion_scope,omitempty"`
	InitMS             float64         `json:"init_ms"`
	InvokeMS           float64         `json:"invoke_ms"`
	InitState          string          `json:"init_state,omitempty"`
	InvokeState        string          `json:"invoke_state,omitempty"`
	ProcessError       string          `json:"process_error,omitempty"`
	OwnershipConfirmed bool            `json:"ownership_confirmed"`
	OwnershipError     string          `json:"ownership_error,omitempty"`
	ContextError       string          `json:"context_error,omitempty"`
	TerminationCause   string          `json:"termination_cause,omitempty"`
	DetailTruncated    bool            `json:"detail_truncated,omitempty"`
}

func diagnosticPhases(phases []runtimePhaseRecord) []diagnosticPhase {
	if len(phases) == 0 {
		return nil
	}
	records := make([]diagnosticPhase, len(phases))
	for index, phase := range phases {
		record := diagnosticPhase{InitAttempt: phase.InitAttempt, Mode: phase.Mode,
			CompletionScope: phase.CompletionScope, InitMS: phase.InitMS, InvokeMS: phase.InvokeMS, InitState: phase.InitState, InvokeState: phase.InvokeState,
			OwnershipConfirmed: phase.OwnershipErr == nil}
		record.ContextError, record.TerminationCause = projectDiagnosticCause(phase.ContextErr, phase.CancellationCause)
		record.ProcessError, record.DetailTruncated = boundedDiagnosticDetail(phase.ProcessError)
		if phase.OwnershipErr != nil {
			var truncated bool
			record.OwnershipError, truncated = boundedDiagnosticDetail(phase.OwnershipErr.Error())
			record.DetailTruncated = record.DetailTruncated || truncated
		}
		records[index] = record
	}
	return records
}
