// Explicit development runtime tuning. This changes the existing pool budgets;
// it neither enables warm execution nor changes any native Lambda resource.
package lambda

import (
	"context"
	"errors"
	"sort"

	"github.com/google/uuid"
)

// DevWarmControlError carries only static development-control errors. Runtime
// details remain private and never enter the HTTP control response.
type DevWarmControlError struct {
	Code string
}

func (err *DevWarmControlError) Error() string { return err.Code }

func devWarmControlError(code string) error { return &DevWarmControlError{Code: code} }

func validateDevWarmUpdate(config DevWarmConfig) error {
	if config.MaxWorkers == 0 || config.MaxConcurrency == 0 {
		return devWarmControlError("invalid_warm_limits")
	}
	if err := validateDevWarm(&config); err != nil {
		return devWarmControlError("invalid_warm_limits")
	}
	return nil
}

// DevUpdateWarm atomically publishes explicit positive limits in the existing
// pool. Active calls retain their execution reservations and finish naturally;
// subsequent admission and idle retention use the new limits. Excess idle
// workers retire through the ordinary pool owner before a successful return.
// ctx bounds only that wait. Cancellation never rolls back published budgets,
// cancels active calls, or releases retiring workers before their actual join.
func (service *Service) DevUpdateWarm(ctx context.Context, config DevWarmConfig) (DevWarmConfig, error) {
	if err := ctx.Err(); err != nil {
		return DevWarmConfig{}, err
	}
	if err := validateDevWarmUpdate(config); err != nil {
		return DevWarmConfig{}, err
	}
	if service == nil || service.warm == nil {
		return DevWarmConfig{}, devWarmControlError("warm_disabled")
	}
	service.mu.Lock()
	if err := ctx.Err(); err != nil {
		service.mu.Unlock()
		return DevWarmConfig{}, err
	}
	if service.closed {
		service.mu.Unlock()
		return DevWarmConfig{}, devWarmControlError("warm_unavailable")
	}
	release, err := service.beginActivityLocked("lambda_warm_update", uuid.NewString())
	if err != nil {
		service.mu.Unlock()
		return DevWarmConfig{}, devWarmControlError("warm_unavailable")
	}
	service.inflight.Add(1)
	service.mu.Unlock()

	normal, ownershipErr := false, error(nil)
	defer func() {
		if !normal {
			ownershipErr = errors.Join(ownershipErr, errors.New("Lambda warm budget update was interrupted"))
		}
		service.mu.Lock()
		defer service.mu.Unlock()
		if ownershipErr != nil && service.invocationEvidenceErr == nil {
			service.invocationEvidenceErr = ownershipErr
		}
		service.inflight.Done()
		if release != nil {
			release(ownershipErr)
		}
	}()

	workers, err := service.warm.updateLimits(ctx, config)
	if err != nil {
		normal = true
		return DevWarmConfig{}, err
	}
	// Retirement remains independently owned if the caller stops waiting. A
	// joined boundary can precede pool-map removal, so the same retire owner
	// publishes removal before returning a successful update.
	for _, worker := range workers {
		select {
		case <-worker.retired:
			ownershipErr = errors.Join(ownershipErr, service.warm.retire(worker))
		case <-ctx.Done():
			normal = true
			return config, ctx.Err()
		}
	}
	normal = true
	if ownershipErr != nil {
		return config, devWarmControlError("warm_update_failed")
	}
	return config, nil
}

// updateLimits publishes both limits under the admission mutex. A retiring idle
// environment retains its old reservation until retirement joins, but already
// retiring workers are not counted as idle candidates for another eviction.
func (pool *warmWorkers) updateLimits(ctx context.Context, config DevWarmConfig) ([]*warmWorker, error) {
	pool.mu.Lock()
	if err := ctx.Err(); err != nil {
		pool.mu.Unlock()
		return nil, err
	}
	if pool.closed || pool.closeErr != nil {
		pool.mu.Unlock()
		return nil, devWarmControlError("warm_unavailable")
	}
	pool.idleLimit, pool.executionLimit = config.MaxWorkers, config.MaxConcurrency
	var idle, retiring []*warmWorker
	for worker, state := range pool.workers {
		switch state.phase {
		case warmIdle:
			idle = append(idle, worker)
		case warmRetiringIdle:
			retiring = append(retiring, worker)
		}
	}
	sort.Slice(idle, func(i, j int) bool {
		return pool.workers[idle[i]].idleOrder < pool.workers[idle[j]].idleOrder
	})
	if excess := len(idle) - config.MaxWorkers; excess > 0 {
		for _, worker := range idle[:excess] {
			pool.workers[worker] = warmWorkerState{phase: warmRetiringIdle}
			retiring = append(retiring, worker)
		}
	}
	pool.mu.Unlock()
	for _, worker := range retiring {
		go pool.retire(worker)
	}
	return retiring, nil
}
