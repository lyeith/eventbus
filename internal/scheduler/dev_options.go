// Development harness source ownership, timing, group and capacity controls.
// Native schedule validation and retry policy remain in the service core.
package scheduler

import (
	"time"

	"github.com/lyeith/eventbus/internal/devactivity"
)

type DevOptions struct {
	// Source fences first due dispatch; accepted retries retain one lifetime.
	Source       devactivity.Source
	Groups       []string
	ExactSeconds bool
	RetryDelay   time.Duration
	MaxSchedules int
	Clock        func() time.Time
	Observe      func(Outcome)
}
