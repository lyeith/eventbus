// Explicit disposable-development shutdown control. Native rule state and the
// existing callback/retry owner remain separate from this permanent policy.
package events

// DevDiscardPendingAdmissions permanently stops recurring intake and unaccepted
// target retry waits. It is only for final disposable shutdown, never a
// resumable retained-owner barrier. The existing Close method still joins every
// real callback and reports ownership errors; a successful Lambda admission
// retains its accepted outcome even if cancellation raced its return.
func (s *Service) DevDiscardPendingAdmissions() {
	if s == nil {
		return
	}
	s.cancel()
	s.deliveryCancel()
}
