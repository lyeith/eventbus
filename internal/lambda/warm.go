package lambda

import (
	"context"
	"errors"
	"net/http"
	"sync"
)

type warmWorkerPhase uint8

const (
	warmIdle warmWorkerPhase = iota
	warmExecuting
	warmRetiringIdle
	warmRetiringExecution
)

type warmWorkerState struct {
	phase     warmWorkerPhase
	idleOrder uint64
}

// warmWorkers is the sole pool/generation owner. Retained idle environments and
// execution reservations have separate budgets. Each retiring environment keeps
// its original budget until its process, listener and activity lease have joined.
// Never acquire Service.mu while holding this mutex.
type warmWorkers struct {
	mu                        sync.Mutex
	idleLimit, executionLimit int
	nextIdle                  uint64
	workers                   map[*warmWorker]warmWorkerState
	closed, draining          bool
	closeErr                  error
}

func newWarmWorkers(idleLimit, executionLimit int) *warmWorkers {
	if idleLimit == 0 {
		idleLimit = 2
	}
	if executionLimit == 0 {
		executionLimit = 64
	}
	return &warmWorkers{idleLimit: idleLimit, executionLimit: executionLimit, workers: make(map[*warmWorker]warmWorkerState)}
}

// countsLocked includes retirement so cancellation or reload cannot temporarily
// free capacity before the actual owner has joined.
func (pool *warmWorkers) countsLocked() (executing, retained int) {
	for _, state := range pool.workers {
		switch state.phase {
		case warmExecuting, warmRetiringExecution:
			executing++
		case warmIdle, warmRetiringIdle:
			retained++
		}
	}
	return
}

func (pool *warmWorkers) acquire(ctx context.Context, entry executableFunction, allowReuse bool) (*warmWorker, error) {
	for {
		pool.mu.Lock()
		if err := ctx.Err(); err != nil {
			pool.mu.Unlock()
			return nil, err
		}
		if pool.closeErr != nil {
			err := pool.closeErr
			pool.mu.Unlock()
			return nil, err
		}
		if pool.closed {
			pool.mu.Unlock()
			return nil, errClosed
		}
		executing, _ := pool.countsLocked()
		if executing >= pool.executionLimit {
			pool.mu.Unlock()
			return nil, invocationError(http.StatusTooManyRequests, "TooManyRequestsException", "ConcurrentInvocationLimitExceeded: Lambda execution concurrency is exhausted")
		}
		retain := allowReuse && !pool.draining && !entry.generation.retired
		var reused *warmWorker
		if retain {
			for worker, state := range pool.workers {
				if state.phase == warmIdle && worker.entry.generation == entry.generation {
					reused = worker
					pool.workers[worker] = warmWorkerState{phase: warmExecuting}
					break
				}
			}
		}
		if reused != nil {
			pool.mu.Unlock()
			select {
			case <-reused.processDone:
				pool.retire(reused)
				continue
			default:
				return reused, nil
			}
		}
		worker := &warmWorker{entry: entry, freshOnly: !retain, retired: make(chan struct{})}
		pool.workers[worker] = warmWorkerState{phase: warmExecuting}
		pool.mu.Unlock()
		return worker, nil
	}
}

func (pool *warmWorkers) release(worker *warmWorker, healthy bool) (CompletionScope, error) {
	pool.mu.Lock()
	retire := pool.mustRetireLocked(worker, healthy)
	_, retained := pool.countsLocked()
	var victim *warmWorker
	if !retire && retained >= pool.idleLimit {
		var oldest uint64
		for candidate, state := range pool.workers {
			if state.phase == warmIdle && (victim == nil || state.idleOrder < oldest) {
				victim, oldest = candidate, state.idleOrder
			}
		}
		if victim != nil {
			pool.workers[victim] = warmWorkerState{phase: warmRetiringIdle}
		}
	}
	if victim != nil {
		// Keep this execution reservation and the victim's idle reservation
		// until actual retirement joins. Other invocations can still use the
		// remaining bounded execution capacity.
		pool.mu.Unlock()
		victimErr := pool.retire(victim)
		pool.mu.Lock()
		retire = pool.mustRetireLocked(worker, healthy) || victimErr != nil
		if victimErr != nil {
			pool.mu.Unlock()
			return CompletionProcess, errors.Join(victimErr, pool.retire(worker))
		}
	}
	_, retained = pool.countsLocked()
	retire = retire || retained >= pool.idleLimit
	if !retire {
		pool.nextIdle++
		pool.workers[worker] = warmWorkerState{phase: warmIdle, idleOrder: pool.nextIdle}
	}
	pool.mu.Unlock()
	if retire {
		return CompletionProcess, pool.retire(worker)
	}
	return CompletionInvocation, nil
}

func (pool *warmWorkers) mustRetireLocked(worker *warmWorker, healthy bool) bool {
	if worker.freshOnly || !healthy || pool.closed || pool.draining || pool.closeErr != nil || worker.entry.generation.retired {
		return true
	}
	select {
	case <-worker.retired:
		return true
	default:
		return false
	}
}

func (pool *warmWorkers) retire(worker *warmWorker) error {
	pool.mu.Lock()
	if state, present := pool.workers[worker]; present {
		switch state.phase {
		case warmIdle:
			pool.workers[worker] = warmWorkerState{phase: warmRetiringIdle}
		case warmExecuting:
			pool.workers[worker] = warmWorkerState{phase: warmRetiringExecution}
		}
	}
	pool.mu.Unlock()
	err := worker.close()
	pool.mu.Lock()
	if _, present := pool.workers[worker]; present {
		delete(pool.workers, worker)
		pool.closeErr = errors.Join(pool.closeErr, err)
	}
	pool.mu.Unlock()
	return err
}

func (pool *warmWorkers) retireGeneration(generation *functionGeneration) []*warmWorker {
	pool.mu.Lock()
	generation.retired = true
	var workers, idle []*warmWorker
	for worker, state := range pool.workers {
		if worker.entry.generation == generation {
			workers = append(workers, worker)
			if state.phase == warmIdle {
				pool.workers[worker] = warmWorkerState{phase: warmRetiringIdle}
				idle = append(idle, worker)
			}
		}
	}
	pool.mu.Unlock()
	for _, worker := range idle {
		go pool.retire(worker)
	}
	return workers
}

func (pool *warmWorkers) beginDrain() {
	pool.mu.Lock()
	pool.draining = true
	var idle []*warmWorker
	for worker, state := range pool.workers {
		if state.phase == warmIdle {
			pool.workers[worker] = warmWorkerState{phase: warmRetiringIdle}
			idle = append(idle, worker)
		}
	}
	pool.mu.Unlock()
	for _, worker := range idle {
		go pool.retire(worker)
	}
}

func (service *Service) closeWarmWorkers() error {
	if service.warm == nil {
		return nil
	}
	pool := service.warm
	pool.mu.Lock()
	pool.closed = true
	var workers []*warmWorker
	for worker := range pool.workers {
		workers = append(workers, worker)
	}
	pool.mu.Unlock()
	for _, worker := range workers {
		_ = pool.retire(worker)
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()
	return pool.closeErr
}
