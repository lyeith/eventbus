package eventsource

import "time"

// DevOptions bounds local resources and timing; these fields are never native
// CreateEventSourceMapping request options. Every mapping has one serial poller.
type DevOptions struct {
	MaxMappings    int
	EmptyPollDelay time.Duration
	Clock          func() time.Time
}
