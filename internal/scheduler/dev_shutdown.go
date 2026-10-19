// Explicit disposable-development shutdown control. The existing schedule and
// callback owner retains all joins and native outcome publication.
package scheduler

// DevDiscardPendingAdmissions permanently stops future schedules and pending
// target admission retries. It is only for final disposable shutdown, never a
// resumable retained-owner barrier. Close remains the sole join/error owner;
// already accepted Lambda events remain Lambda-owned and accepted.
func (s *Service) DevDiscardPendingAdmissions() {
	if s == nil {
		return
	}
	s.cancel()
}
