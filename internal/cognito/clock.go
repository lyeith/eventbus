package cognito

import "time"

// now is shared by persisted timestamps, token signing and expiry admission.
// Clock replacement is an application-owned fixture control, never an AWS field.
func (s *CognitoStore) now() time.Time {
	s.clockMu.RLock()
	clock := s.clock
	s.clockMu.RUnlock()
	if clock == nil {
		return time.Now()
	}
	return clock()
}
