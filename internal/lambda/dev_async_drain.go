// Development drain evidence projects the native async owner's first abort.
// It is metadata only, not persistence, delivery confirmation or recovery state.
package lambda

import (
	"sort"
	"time"
)

const devAsyncDrainLimit = 16

// DevAsyncDrainInvocation contains only already-published native task metadata.
// Queued-capture reservations have no published identity and are counted
// separately, never projected as an invented queued event.
type DevAsyncDrainInvocation struct {
	RequestID     string    `json:"request_id"`
	FunctionName  string    `json:"function_name"`
	State         string    `json:"state"`
	Attempts      int       `json:"attempts"`
	NextAttemptAt time.Time `json:"next_attempt_at,omitzero"`
}

// DevAsyncDrainSummary freezes why the first permanent async abort waited.
// Outstanding includes admission reservations. Omitted counts published tasks
// beyond the bounded list: Outstanding=PendingAdmissions+Shown+Omitted.
// States and retry times describe publication at CapturedAt, before abort
// rewrites records, and never claim that a queued event was delivered.
type DevAsyncDrainSummary struct {
	CapturedAt        time.Time                 `json:"captured_at"`
	Outstanding       int                       `json:"outstanding"`
	PendingAdmissions int                       `json:"pending_admissions"`
	Shown             int                       `json:"shown"`
	Omitted           int                       `json:"omitted"`
	Invocations       []DevAsyncDrainInvocation `json:"invocations"`
}

// DevAsyncDrainSummary returns a detached immutable first-abort snapshot.
// A zero CapturedAt means no async abort has occurred. Ordinary graceful
// drains and resumable application quiescence do not create a snapshot.
func (service *Service) DevAsyncDrainSummary() DevAsyncDrainSummary {
	if service == nil {
		return DevAsyncDrainSummary{}
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.devAsyncDrainSummary == nil {
		return DevAsyncDrainSummary{}
	}
	summary := *service.devAsyncDrainSummary
	summary.Invocations = make([]DevAsyncDrainInvocation, len(summary.Invocations))
	copy(summary.Invocations, service.devAsyncDrainSummary.Invocations)
	return summary
}

// The native abort owner holds Service.mu. Snapshot before flags/cancellation
// and without waiting for capture or taking the async transition mutex.
func (service *Service) captureDevAsyncDrainLocked() {
	if service.devAsyncDrainSummary != nil {
		return
	}
	entries := make([]DevAsyncDrainInvocation, 0, len(service.asyncTasks))
	for _, task := range service.asyncTasks {
		record := task.record
		entries = append(entries, DevAsyncDrainInvocation{
			RequestID: record.RequestID, FunctionName: record.FunctionName,
			State: record.State, Attempts: record.Attempts, NextAttemptAt: record.NextAttemptAt,
		})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].RequestID < entries[j].RequestID })
	shown := min(len(entries), devAsyncDrainLimit)
	summary := &DevAsyncDrainSummary{
		CapturedAt: time.Now().UTC(), Outstanding: service.asyncOutstanding,
		PendingAdmissions: service.asyncOutstanding - len(entries),
		Shown:             shown, Omitted: len(entries) - shown,
		Invocations: make([]DevAsyncDrainInvocation, shown),
	}
	copy(summary.Invocations, entries[:shown])
	service.devAsyncDrainSummary = summary
}
