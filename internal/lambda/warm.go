package lambda

import (
	"context"
	"errors"
	"sync"
)

// warmWorkers is the sole pool/generation owner. Admission counts reservations,
// busy workers, idle workers and workers being retired against one global cap.
// Never acquire Service.mu while holding this mutex.
type warmWorkers struct {
	mu               sync.Mutex
	limit            int
	workers          map[*warmWorker]bool // true while reserved, executing or retiring
	wake             chan struct{}
	closed, draining bool
	closeErr         error
}

func newWarmWorkers(limit int) *warmWorkers {
	if limit == 0 {
		limit = 2
	}
	return &warmWorkers{limit: limit, workers: make(map[*warmWorker]bool), wake: make(chan struct{})}
}
func (pool *warmWorkers) changedLocked() { close(pool.wake); pool.wake = make(chan struct{}) }

func (pool *warmWorkers) acquire(ctx context.Context, entry executableFunction, allowReuse bool) (*warmWorker, error) {
acquire:
	for {
		pool.mu.Lock()
		if pool.closeErr != nil {
			err := pool.closeErr
			pool.mu.Unlock()
			return nil, err
		}
		if pool.closed {
			pool.mu.Unlock()
			return nil, errClosed
		}
		retain := allowReuse && !pool.draining && !entry.generation.retired
		var victim *warmWorker
		for worker, busy := range pool.workers {
			if !busy {
				if retain && worker.entry.generation == entry.generation {
					pool.workers[worker] = true
					pool.mu.Unlock()
					select {
					case <-worker.processDone:
						pool.retire(worker)
						continue acquire
					default:
						return worker, nil
					}
				}
				victim = worker
			}
		}
		if len(pool.workers) < pool.limit {
			worker := &warmWorker{entry: entry, freshOnly: !retain, retired: make(chan struct{})}
			pool.workers[worker] = true
			pool.mu.Unlock()
			return worker, nil
		}
		if victim != nil {
			pool.workers[victim] = true
			pool.mu.Unlock()
			pool.retire(victim)
			continue
		}
		wake := pool.wake
		pool.mu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (pool *warmWorkers) release(worker *warmWorker, healthy bool) (CompletionScope, error) {
	pool.mu.Lock()
	retire := worker.freshOnly || !healthy || pool.closed || pool.draining || worker.entry.generation.retired
	select {
	case <-worker.retired:
		retire = true
	default:
	}
	if !retire {
		pool.workers[worker] = false
		pool.changedLocked()
	}
	pool.mu.Unlock()
	if retire {
		return CompletionProcess, pool.retire(worker)
	}
	return CompletionInvocation, nil
}
func (pool *warmWorkers) retire(worker *warmWorker) error {
	err := worker.close()
	pool.mu.Lock()
	delete(pool.workers, worker)
	pool.closeErr = errors.Join(pool.closeErr, err)
	pool.changedLocked()
	pool.mu.Unlock()
	return err
}
func (pool *warmWorkers) retireGeneration(generation *functionGeneration) []*warmWorker {
	pool.mu.Lock()
	generation.retired = true
	var workers, idle []*warmWorker
	for worker, busy := range pool.workers {
		if worker.entry.generation == generation {
			workers = append(workers, worker)
			if !busy {
				pool.workers[worker] = true
				idle = append(idle, worker)
			}
		}
	}
	pool.changedLocked()
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
	for worker, busy := range pool.workers {
		if !busy {
			pool.workers[worker] = true
			idle = append(idle, worker)
		}
	}
	pool.changedLocked()
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
	pool.changedLocked()
	pool.mu.Unlock()
	for _, worker := range workers {
		_ = pool.retire(worker)
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()
	return pool.closeErr
}
