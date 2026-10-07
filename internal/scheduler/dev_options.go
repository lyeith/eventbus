// Development harness timing, group provisioning and capacity controls.
// Native schedule validation and retry policy remain in the service core.
package scheduler

import "time"

type DevOptions struct {
	Groups       []string
	ExactSeconds bool
	RetryDelay   time.Duration
	MaxSchedules int
	Clock        func() time.Time
	Observe      func(Outcome)
}
