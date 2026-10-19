// Explicit disposable-development shutdown policy. Native async work remains
// owned by the existing queue, attempt, capture and terminal-settlement paths.
package lambda

const devAsyncShutdownDiscarded = "Dev.ShutdownDiscarded"

type devAsyncShutdownState struct {
	discardPending bool
	discarded      int
}

// DevDiscardPendingAsync permanently fences Event admission and tells the
// existing owner to cancel pending work, including future retries. A dequeue
// claim that won under Service.mu may still launch and must join normally.
// This method performs no capture or transition-lock wait; accepted capture
// reservations remain owned and settle after their capture completes.
func (service *Service) DevDiscardPendingAsync() {
	if service == nil {
		return
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.devAsyncShutdown.discardPending {
		return
	}
	service.devAsyncShutdown.discardPending = true
	service.asyncClosed = true
	service.wakeAsyncLocked()
}

// DevDiscardedAsyncCount counts published intentional pending cancellations,
// independent of the bounded AsyncSnapshot history. Read it after DrainAsync
// or Close joins; evidence/cleanup errors still determine shutdown health.
func (service *Service) DevDiscardedAsyncCount() int {
	if service == nil {
		return 0
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	return service.devAsyncShutdown.discarded
}

// The existing terminal owner holds mu. Only the still-owned task can count;
// a failed capture still leaves a real cancellation and a sticky evidence error.
func (service *Service) recordDevAsyncDiscardLocked(task *asyncTask, record AsyncRecord) {
	if record.State == "canceled" && record.ErrorType == devAsyncShutdownDiscarded && service.asyncTasks[record.RequestID] == task {
		service.devAsyncShutdown.discarded++
	}
}
