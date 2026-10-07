package eventsource

import (
	"time"

	"github.com/lyeith/eventbus/internal/devactivity"
)

// DevOptions bounds local resources and timing; these fields are never native
// CreateEventSourceMapping request options. Native ceilings bound the configured
// invocation count; the local cap bounds process resources independently.
type DevOptions struct {
	MaxMappings          int
	MaxWorkersPerMapping int
	EmptyPollDelay       time.Duration
	Clock                func() time.Time
	// Both ports are immutable startup dependencies for the optional retained
	// owner. They are absent in ordinary mode and define no native mapping state.
	Source          devactivity.Source
	Activity        devactivity.Activity
	DeliveryCapture *DevDeliveryCaptureConfig
}
