// Optional harness boundaries; native schedule expressions and payloads remain core.
package events

import (
	"github.com/lyeith/eventbus/internal/devactivity"
	"time"
)

// Clock permits deterministic component tests without changing AWS rate units.
// Production construction uses the system clock and real timers.
type Clock interface {
	Now() time.Time
	NewTimer(time.Duration) Timer
}
type Timer interface {
	C() <-chan time.Time
	Stop() bool
}
type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }
func (systemClock) NewTimer(duration time.Duration) Timer {
	return systemTimer{time.NewTimer(duration)}
}

type systemTimer struct{ *time.Timer }

func (timer systemTimer) C() <-chan time.Time { return timer.Timer.C }

type Outcome struct {
	RuleARN  string `json:"rule_arn"`
	EventID  string `json:"event_id"`
	TargetID string `json:"target_id"`
	Status   string `json:"status"`
	Attempts int    `json:"attempts"`
	Code     string `json:"code,omitempty"`
}
type DevOptions struct {
	Source     devactivity.Source
	Clock      Clock
	MaxRules   int
	MaxPending int
	Observe    func(Outcome)
}

// DevEvidence returns only sticky ownership uncertainty, never business failure.
func (s *Service) DevEvidence() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.evidenceErr
}
