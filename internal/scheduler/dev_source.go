// Optional source admission is a development fence, not a native Schedule.State
// change. Accepted retries keep their original source lifetime until joined.
package scheduler

import "context"

func (s *Service) beginSource(ctx context.Context, name string) (func(error), bool) {
	if s.dev.Source == nil {
		return nil, ctx.Err() == nil
	}
	for {
		if ctx.Err() != nil {
			return nil, false
		}
		complete, changed, err := s.dev.Source.BeginSource("scheduler.dispatch", name)
		if err == nil && complete != nil {
			return complete, true
		}
		// Refusal and its notification were captured atomically by the source
		// owner. Resume between this call and the select cannot lose the wakeup.
		// Permanent refusal may return nil changed; native Close/Delete still
		// cancel the worker and join it without recreating any schedule state.
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, false
		}
	}
}
