package lambda

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestWarmConfigurationSeparatesIdleRetentionAndExecution(t *testing.T) {
	pool := newWarmWorkers(0, 0)
	if pool.idleLimit != 2 || pool.executionLimit != 64 {
		t.Fatalf("defaults: idle=%d execution=%d", pool.idleLimit, pool.executionLimit)
	}
	for _, config := range []DevWarmConfig{
		{MaxWorkers: -1}, {MaxWorkers: 33}, {MaxConcurrency: -1}, {MaxConcurrency: 1025},
	} {
		if err := validateDevWarm(&config); err == nil {
			t.Fatalf("accepted invalid configuration %+v", config)
		}
	}
	for _, config := range []DevWarmConfig{
		{}, {MaxWorkers: 1, MaxConcurrency: 1}, {MaxWorkers: 32, MaxConcurrency: 1024},
	} {
		if err := validateDevWarm(&config); err != nil {
			t.Fatalf("valid configuration %+v: %v", config, err)
		}
	}
}

func warmAssertThrottled(t *testing.T, err error) {
	t.Helper()
	var rejection *InvokeError
	if !errors.As(err, &rejection) || rejection.Status != http.StatusTooManyRequests || rejection.Code != "TooManyRequestsException" {
		t.Fatalf("expected native concurrency rejection, got %v", err)
	}
}

func warmAssertCounts(t *testing.T, pool *warmWorkers, executions, retention int) {
	t.Helper()
	pool.mu.Lock()
	defer pool.mu.Unlock()
	running, retained := pool.countsLocked()
	if running != executions || retained != retention || len(pool.workers) != executions+retention {
		t.Fatalf("environments=%d executions=%d retained=%d, want %d+%d", len(pool.workers), running, retained, executions, retention)
	}
}

func TestWarmRetiringExecutionRetainsCapacityThroughJoin(t *testing.T) {
	pool := newWarmWorkers(1, 1)
	entry := executableFunction{generation: &functionGeneration{}}
	worker, err := pool.acquire(context.Background(), entry, true)
	if err != nil {
		t.Fatal(err)
	}
	reached, proceed := make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	defer unblock.Do(func() { close(proceed) })
	worker.release = func(error) { close(reached); <-proceed }
	joined := make(chan error, 1)
	go func() {
		_, err := pool.release(worker, false)
		joined <- err
	}()
	select {
	case <-reached:
	case <-time.After(time.Second):
		t.Fatal("retirement did not reach owned lifetime join")
	}
	warmAssertCounts(t, pool, 1, 0)
	_, err = pool.acquire(context.Background(), entry, true)
	warmAssertThrottled(t, err)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := pool.acquire(canceled, entry, true); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled reservation: %v", err)
	}
	unblock.Do(func() { close(proceed) })
	if err := <-joined; err != nil {
		t.Fatal(err)
	}
	warmAssertCounts(t, pool, 0, 0)
	replacement, err := pool.acquire(context.Background(), entry, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.release(replacement, false); err != nil {
		t.Fatal(err)
	}
	// A repeated retirement never resurrects a deleted zero-state map entry.
	if err := pool.retire(worker); err != nil {
		t.Fatal(err)
	}
	warmAssertCounts(t, pool, 0, 0)
}

func TestWarmIdleRetirementRetainsSeparateBoundThroughDrain(t *testing.T) {
	pool := newWarmWorkers(1, 2)
	entry := executableFunction{generation: &functionGeneration{}}
	idle, err := pool.acquire(context.Background(), entry, true)
	if err != nil {
		t.Fatal(err)
	}
	if scope, err := pool.release(idle, true); err != nil || scope != CompletionInvocation {
		t.Fatalf("retention: %s %v", scope, err)
	}
	reached, proceed := make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	defer unblock.Do(func() { close(proceed) })
	idle.release = func(error) { close(reached); <-proceed }
	pool.beginDrain()
	select {
	case <-reached:
	case <-time.After(time.Second):
		t.Fatal("idle retirement did not reach owned lifetime join")
	}
	var fresh []*warmWorker
	for index := 0; index < 2; index++ {
		worker, err := pool.acquire(context.Background(), entry, true)
		if err != nil || !worker.freshOnly {
			t.Fatalf("drain reservation: %+v %v", worker, err)
		}
		fresh = append(fresh, worker)
	}
	warmAssertCounts(t, pool, 2, 1)
	_, err = pool.acquire(context.Background(), entry, true)
	warmAssertThrottled(t, err)
	for _, worker := range fresh {
		if scope, err := pool.release(worker, true); err != nil || scope != CompletionProcess {
			t.Fatalf("drain completion: %s %v", scope, err)
		}
	}
	warmAssertCounts(t, pool, 0, 1)
	unblock.Do(func() { close(proceed) })
	<-idle.retired
	// retire() removes the environment after close() signals its joined boundary.
	if err := pool.retire(idle); err != nil {
		t.Fatal(err)
	}
	warmAssertCounts(t, pool, 0, 0)
}

func TestWarmExecutionSaturationHasNoWaitingQueue(t *testing.T) {
	pool := newWarmWorkers(2, 4)
	entry := executableFunction{generation: &functionGeneration{}}
	start := make(chan struct{})
	type reservation struct {
		worker *warmWorker
		err    error
	}
	ready := make(chan reservation, 32)
	for index := 0; index < cap(ready); index++ {
		go func() {
			<-start
			worker, err := pool.acquire(context.Background(), entry, true)
			ready <- reservation{worker, err}
		}()
	}
	close(start)
	var workers []*warmWorker
	for index := 0; index < cap(ready); index++ {
		select {
		case result := <-ready:
			if result.err != nil {
				warmAssertThrottled(t, result.err)
			} else {
				workers = append(workers, result.worker)
			}
		case <-time.After(time.Second):
			t.Fatal("saturated acquire waited behind the admitted executions")
		}
	}
	if len(workers) != 4 {
		t.Fatalf("executions=%d, want 4", len(workers))
	}
	warmAssertCounts(t, pool, 4, 0)
	for _, worker := range workers {
		if _, err := pool.release(worker, true); err != nil {
			t.Fatal(err)
		}
	}
	warmAssertCounts(t, pool, 0, 2)
	for _, worker := range workers {
		if err := pool.retire(worker); err != nil {
			t.Fatal(err)
		}
	}
	warmAssertCounts(t, pool, 0, 0)
}

func TestWarmLRURetentionReplacesOldIdleOnlyAfterItsJoin(t *testing.T) {
	pool := newWarmWorkers(1, 1)
	oldEntry := executableFunction{generation: &functionGeneration{}}
	old, err := pool.acquire(context.Background(), oldEntry, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.release(old, true); err != nil {
		t.Fatal(err)
	}
	newEntry := executableFunction{generation: &functionGeneration{}}
	current, err := pool.acquire(context.Background(), newEntry, true)
	if err != nil {
		t.Fatal(err)
	}
	reached, proceed := make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	defer unblock.Do(func() { close(proceed) })
	old.release = func(error) { close(reached); <-proceed }
	type result struct {
		scope CompletionScope
		err   error
	}
	completed := make(chan result, 1)
	go func() {
		scope, err := pool.release(current, true)
		completed <- result{scope, err}
	}()
	select {
	case <-reached:
	case <-time.After(time.Second):
		t.Fatal("LRU eviction did not reach owned retirement")
	}
	warmAssertCounts(t, pool, 1, 1)
	_, err = pool.acquire(context.Background(), newEntry, true)
	warmAssertThrottled(t, err)
	select {
	case result := <-completed:
		t.Fatalf("retained new environment before eviction joined: %+v", result)
	default:
	}
	unblock.Do(func() { close(proceed) })
	resultValue := <-completed
	if resultValue.scope != CompletionInvocation || resultValue.err != nil {
		t.Fatalf("new retention: %+v", resultValue)
	}
	warmAssertCounts(t, pool, 0, 1)
	reused, err := pool.acquire(context.Background(), newEntry, true)
	if err != nil || reused != current {
		t.Fatalf("recent environment not reused: %p %v", reused, err)
	}
	if _, err := pool.release(reused, false); err != nil {
		t.Fatal(err)
	}
	warmAssertCounts(t, pool, 0, 0)
}
