// Package devactivity defines optional development ownership seams. It owns no
// service, fence state or policy; normal service construction leaves ports nil.
package devactivity

// Activity joins an accepted descendant/task lifetime. The completion error
// denotes uncertain ownership/evidence, never an ordinary business failure.
// Implementations must not call back into the observed service.
type Activity interface {
	BeginActivity(kind, requestID string) (complete func(error), err error)
}

// Source atomically admits autonomous roots only in an open source epoch.
// Success returns a completion and an intake-fence signal. Refusal returns the
// current change signal; callers park on it or their own cancellation and retry.
// Fencing intake must not cancel already accepted business execution or retries.
type Source interface {
	BeginSource(kind, requestID string) (complete func(error), changed <-chan struct{}, err error)
}
