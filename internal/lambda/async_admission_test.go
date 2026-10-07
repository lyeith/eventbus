package lambda

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// Observe the first Err result before the caller can cancel, without depending
// on sleeps or scheduler timing to place cancellation between admission checks.
type observedAdmissionContext struct {
	context.Context
	firstCheck chan struct{}
	once       sync.Once
}

func (ctx *observedAdmissionContext) Err() error {
	err := ctx.Context.Err()
	ctx.once.Do(func() { close(ctx.firstCheck) })
	return err
}

func TestEventCancellationBeforeAdmissionLockRefusesPublication(t *testing.T) {
	service := newAsyncTestService(t, map[string]Function{"event": providedFunction(t, "echo")}, t.TempDir(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	observed := &observedAdmissionContext{Context: ctx, firstCheck: make(chan struct{})}
	type outcome struct {
		admission Admission
		err       error
	}
	done := make(chan outcome, 1)
	service.mu.Lock()
	go func() {
		admission, err := service.Admit(observed, InvokeInput{FunctionName: "event", Payload: []byte(`{}`)})
		done <- outcome{admission, err}
	}()
	select {
	case <-observed.firstCheck:
		// The initial Err call read nil while publication remains locked out.
		cancel()
		service.mu.Unlock()
	case <-time.After(5 * time.Second):
		service.mu.Unlock()
		t.Fatal("admission did not reach its initial context check")
	}
	select {
	case result := <-done:
		if !errors.Is(result.err, context.Canceled) || result.admission.RequestID != "" {
			t.Fatalf("canceled admission published work: %#v %v", result.admission, result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled admission did not return")
	}
	if records := service.AsyncSnapshot(); len(records) != 0 {
		t.Fatalf("canceled admission left execution evidence: %#v", records)
	}
	// Refusal must not poison the owner or consume capacity.
	admission, err := service.Admit(context.Background(), InvokeInput{FunctionName: "event", Payload: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	waitAsyncState(t, service, admission.RequestID, "succeeded")
}
