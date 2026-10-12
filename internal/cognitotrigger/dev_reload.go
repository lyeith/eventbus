// Development source reload for a configured private trigger. This is not an
// AWS Cognito management operation and never changes challenge/session state.
package cognitotrigger

import (
	"context"
	"errors"
	"fmt"
)

type DevReloadErrorKind string

const (
	DevReloadNotConfigured   DevReloadErrorKind = "not_configured"
	DevReloadUnsupported     DevReloadErrorKind = "unsupported"
	DevReloadClosed          DevReloadErrorKind = "closed"
	DevReloadWaitInterrupted DevReloadErrorKind = "wait_interrupted"
	DevReloadFailed          DevReloadErrorKind = "failed"
)

// DevReloadError exposes a static category. Only caller cancellation/deadline
// may be unwrapped; runtime paths, handler configuration and diagnostics remain
// private to their existing owners.
type DevReloadError struct {
	Kind  DevReloadErrorKind
	Cause error
}

func (err *DevReloadError) Error() string {
	return "Cognito trigger reload: " + string(err.Kind)
}
func (err *DevReloadError) Unwrap() error { return err.Cause }

// DevTriggerReloader is the optional execution capability and HTTP adapter port.
type DevTriggerReloader interface {
	DevReloadTrigger(context.Context, string, string) error
}

// DevReloadTrigger reloads the selected configured source without changing its
// recipe. The runtime publishes a replacement before joining the old generation.
// A canceled wait does not roll back publication or cancel other invocations.
func (runner *Runner) DevReloadTrigger(ctx context.Context, poolID, triggerName string) error {
	if err := ctx.Err(); err != nil {
		return &DevReloadError{Kind: DevReloadWaitInterrupted, Cause: err}
	}
	if !runner.SupportsTrigger(poolID, triggerName) {
		return &DevReloadError{Kind: DevReloadNotConfigured}
	}
	execution, supported := runner.execution.(DevTriggerReloader)
	if !supported {
		return &DevReloadError{Kind: DevReloadUnsupported}
	}
	wait, cancel := context.WithCancel(ctx)
	defer cancel()
	runner.mu.Lock()
	if runner.closed {
		runner.mu.Unlock()
		return &DevReloadError{Kind: DevReloadClosed}
	}
	runner.next++
	id := runner.next
	var release func(error)
	if runner.devActivity != nil {
		var err error
		release, err = runner.devActivity.BeginActivity("cognito_trigger_reload", fmt.Sprintf("trigger-reload-%d", id))
		if err != nil || release == nil {
			runner.mu.Unlock()
			return &DevReloadError{Kind: DevReloadClosed}
		}
	}
	runner.active[id] = cancel
	runner.inflight.Add(1)
	runner.mu.Unlock()
	returned := false
	defer func() {
		// Even panic/Goexit must release admission and retain interruption as
		// dirty ownership. Normal waiter cancellation is clean: the runtime
		// still owns every published/retiring generation.
		var ownershipErr error
		if !returned {
			ownershipErr = errors.New("Cognito trigger reload ownership did not complete")
		}
		runner.mu.Lock()
		delete(runner.active, id)
		runner.ownershipErr = errors.Join(runner.ownershipErr, ownershipErr)
		if release != nil {
			release(ownershipErr)
		}
		runner.inflight.Done()
		runner.mu.Unlock()
	}()
	err := execution.DevReloadTrigger(wait, poolID, triggerName)
	returned = true
	if wait.Err() != nil {
		return &DevReloadError{Kind: DevReloadWaitInterrupted, Cause: wait.Err()}
	}
	if err != nil {
		var failure *DevReloadError
		if errors.As(err, &failure) {
			// Never reflect an adapter's arbitrary Kind/Cause through this dev API.
			switch failure.Kind {
			case DevReloadNotConfigured, DevReloadUnsupported, DevReloadClosed, DevReloadWaitInterrupted, DevReloadFailed:
				return &DevReloadError{Kind: failure.Kind}
			}
		}
		if errors.Is(err, context.Canceled) {
			return &DevReloadError{Kind: DevReloadWaitInterrupted, Cause: context.Canceled}
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return &DevReloadError{Kind: DevReloadWaitInterrupted, Cause: context.DeadlineExceeded}
		}
		return &DevReloadError{Kind: DevReloadFailed}
	}
	return nil
}
