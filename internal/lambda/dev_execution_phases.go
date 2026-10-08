// Private phase evidence projects frozen core facts; this adapter does not own
// deadlines, readiness, retry eligibility, native errors or process cleanup.
package lambda

type diagnosticPhase struct {
	InitAttempt int     `json:"init_attempt"`
	Mode        string  `json:"mode"`
	InitMS      float64 `json:"init_ms"`
	InvokeMS    float64 `json:"invoke_ms"`
	InitState   string  `json:"init_state,omitempty"`
	InvokeState string  `json:"invoke_state,omitempty"`
}

func diagnosticPhases(phases []runtimePhaseRecord) []diagnosticPhase {
	if len(phases) == 0 {
		return nil
	}
	records := make([]diagnosticPhase, len(phases))
	for index, phase := range phases {
		records[index] = diagnosticPhase{InitAttempt: phase.InitAttempt, Mode: phase.Mode,
			InitMS: phase.InitMS, InvokeMS: phase.InvokeMS, InitState: phase.InitState, InvokeState: phase.InvokeState}
	}
	return records
}
