// Final-process development policy. Native service owners retain all queue,
// retry, capture and join responsibilities; resumable quiescence never uses it.
package app

import (
	"errors"
	"fmt"

	"github.com/rs/zerolog/log"
)

type devShutdownPolicy string

const (
	devShutdownGraceful       devShutdownPolicy = "graceful"
	devShutdownDiscardPending devShutdownPolicy = "discard-pending"
)

func validateDevShutdownPolicy(policy devShutdownPolicy) error {
	switch policy {
	case devShutdownGraceful, devShutdownDiscardPending:
		return nil
	default:
		return fmt.Errorf("unknown shutdown policy %q (want graceful or discard-pending)", policy)
	}
}

// The application consumes explicit development ports instead of changing the
// native close contract. Activation must not wait for captures or active work.
type devAsyncShutdownOwner interface {
	DevDiscardPendingAsync()
	DevDiscardedAsyncCount() int
}
type devAdmissionShutdownOwner interface {
	DevDiscardPendingAdmissions()
}

func (listener *eventBusListener) beginShutdownPolicy() error {
	switch listener.shutdownPolicy {
	case "", devShutdownGraceful:
		return nil
	case devShutdownDiscardPending:
	default:
		return validateDevShutdownPolicy(listener.shutdownPolicy)
	}
	if listener.owned == nil {
		return nil
	}
	var failures []error
	if listener.owned.functions != nil {
		if owner, ok := listener.owned.functions.(devAsyncShutdownOwner); ok {
			owner.DevDiscardPendingAsync()
		} else {
			failures = append(failures, errors.New("Lambda owner does not support discard-pending shutdown"))
		}
	}
	// Stop unaccepted producer retries before a retained-owner join. Their
	// existing native finalizers release source leases, including callbacks
	// racing successful Lambda admission. Accepted Events remain Lambda-owned.
	for _, source := range []struct {
		name  string
		owner contextCloser
	}{
		{"Scheduler", listener.owned.scheduler},
		{"EventBridge Rules", listener.owned.events},
	} {
		if source.owner == nil {
			continue
		}
		if owner, ok := source.owner.(devAdmissionShutdownOwner); ok {
			owner.DevDiscardPendingAdmissions()
		} else {
			failures = append(failures, fmt.Errorf("%s owner does not support discard-pending shutdown", source.name))
		}
	}
	return errors.Join(failures...)
}

func (listener *eventBusListener) reportShutdownPolicy() {
	if listener.shutdownPolicy != devShutdownDiscardPending || listener.owned == nil {
		return
	}
	if owner, ok := listener.owned.functions.(devAsyncShutdownOwner); ok {
		log.Info().
			Str("shutdown_policy", string(listener.shutdownPolicy)).
			Int("canceled_events", owner.DevDiscardedAsyncCount()).
			Msg("Lambda async shutdown policy applied")
	}
}
