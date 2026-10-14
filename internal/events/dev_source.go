package events

import (
	"context"
	"errors"
)

// Dormant recurring clocks own no activity. Refused source admission parks on
// the atomically captured change signal; fencing never cancels accepted retries.
func (s *Service) beginSource(ctx context.Context, ruleChanged <-chan struct{}, name string) (func(error), bool) {
	if s.dev.Source == nil {
		return nil, ctx.Err() == nil
	}
	for ctx.Err() == nil {
		complete, changed, err := s.dev.Source.BeginSource("events.dispatch", name)
		if err == nil && complete != nil {
			return complete, true
		}
		if err == nil {
			s.recordEvidence(errors.New("EventBridge source admission returned no completion"))
			select {
			case <-ruleChanged:
			case <-ctx.Done():
			}
			return nil, false
		}
		select {
		case <-changed:
		case <-ruleChanged:
			return nil, false
		case <-ctx.Done():
			return nil, false
		}
	}
	return nil, false
}
