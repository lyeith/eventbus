package lambda

import (
	"context"
	"errors"
	"testing"
)

// Freeze the real close-before-pool-publication state without scheduling a
// background remover. close() has joined its owned lifetime and signaled
// retired, while the sole pool owner still holds the retirement reservation.
func warmJoinedBeforePoolPublication(t *testing.T, pool *warmWorkers, cleanupErr error) (*warmWorker, *warmWorker) {
	t.Helper()
	var workers []*warmWorker
	for index := 0; index < 2; index++ {
		worker, err := pool.acquire(context.Background(), executableFunction{generation: &functionGeneration{}}, true)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.release(worker, true); err != nil {
			t.Fatal(err)
		}
		workers = append(workers, worker)
	}
	retiring, peer := workers[0], workers[1]
	pool.mu.Lock()
	pool.workers[retiring] = warmWorkerState{phase: warmRetiringIdle}
	pool.mu.Unlock()
	retiring.closeErr = cleanupErr
	if err := retiring.close(); !errors.Is(err, cleanupErr) {
		t.Fatalf("joined close: %v, expected %v", err, cleanupErr)
	}
	<-retiring.retired
	warmAssertCounts(t, pool, 0, 2)
	return retiring, peer
}

func TestWarmReloadJoinPublishesRetirementRemoval(t *testing.T) {
	for _, cleanupErr := range []error{nil, errors.New("owned retirement uncertainty")} {
		name := "healthy"
		if cleanupErr != nil {
			name = "uncertain"
		}
		t.Run(name, func(t *testing.T) {
			pool := newWarmWorkers(2, 2)
			retiring, peer := warmJoinedBeforePoolPublication(t, pool, cleanupErr)
			service := &Service{warm: pool}
			err := service.joinRetiredWorkers(context.Background(), []*warmWorker{retiring})
			if !errors.Is(err, cleanupErr) {
				t.Fatalf("reload join: %v, expected %v", err, cleanupErr)
			}
			warmAssertCounts(t, pool, 0, 1)
			pool.mu.Lock()
			_, present := pool.workers[peer]
			retainedErr := pool.closeErr
			pool.mu.Unlock()
			if !present || !errors.Is(retainedErr, cleanupErr) {
				t.Fatalf("peer/error ownership changed: peer=%v err=%v", present, retainedErr)
			}
			if err := pool.retire(peer); err != nil {
				t.Fatal(err)
			}
			warmAssertCounts(t, pool, 0, 0)
		})
	}
}
