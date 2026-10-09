package lambda

import (
	"context"
	"testing"
)

func TestWarmPoolNeverReusesClosedSuccessfulWorker(t *testing.T) {
	pool := newWarmWorkers(1)
	entry := executableFunction{generation: &functionGeneration{}}
	worker, err := pool.acquire(context.Background(), entry, true)
	if err != nil {
		t.Fatal(err)
	}
	// A valid Runtime API response wins over a simultaneous exit; native success
	// does not authorize retaining an already retired execution environment.
	if err := worker.close(); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.release(worker, true); err != nil {
		t.Fatal(err)
	}
	next, err := pool.acquire(context.Background(), entry, true)
	if err != nil {
		t.Fatal(err)
	}
	if next == worker {
		t.Fatal("closed worker returned as healthy idle capacity")
	}
	pool.release(next, false)
}

func TestWarmPoolEvictsUnexpectedIdleProcessExit(t *testing.T) {
	service, _, _ := warmTestService(t, "python", 1)
	first := warmTestExecute(t, service, `{}`)
	service.warm.mu.Lock()
	var worker *warmWorker
	for candidate := range service.warm.workers {
		worker = candidate
	}
	service.warm.mu.Unlock()
	worker.cancel()
	<-worker.processDone
	second := warmTestExecute(t, service, `{}`)
	if first["pid"] == second["pid"] || second["count"] != float64(1) {
		t.Fatalf("unexpected dead idle worker reused %v", second)
	}
}
