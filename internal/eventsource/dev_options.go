package eventsource

import "time"

// DevOptions bounds local resources and timing; these fields are never native
// CreateEventSourceMapping request options. Native ceilings bound the configured
// invocation count; the local cap bounds process resources independently.
type DevOptions struct {
	MaxMappings          int
	MaxWorkersPerMapping int
	EmptyPollDelay       time.Duration
	Clock                func() time.Time
}
