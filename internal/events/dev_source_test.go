package events

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

type sourceFence struct {
	mu       sync.Mutex
	open     bool
	changed  chan struct{}
	entered  chan string
	released chan error
	count    int
}

func newSource() *sourceFence {
	return &sourceFence{changed: make(chan struct{}), entered: make(chan string, 20), released: make(chan error, 20)}
}
func (f *sourceFence) BeginSource(kind, id string) (func(error), <-chan struct{}, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entered <- kind + ":" + id
	if !f.open {
		return nil, f.changed, errors.New("intake fenced")
	}
	f.count++
	return func(err error) { f.mu.Lock(); f.count--; f.mu.Unlock(); f.released <- err }, f.changed, nil
}
func (f *sourceFence) set(open bool) {
	f.mu.Lock()
	f.open = open
	close(f.changed)
	f.changed = make(chan struct{})
	f.mu.Unlock()
}
func TestRecurringSourceFenceParksAndResumesWithoutChangingNativeRule(t *testing.T) {
	clock := newClock()
	source := newSource()
	entered, release := make(chan context.Context, 1), make(chan struct{})
	var releaseOnce sync.Once
	done := make(chan Outcome, 1)
	s := serviceFor(t, clock, invoker{admit: func(ctx context.Context, _ string, _ []byte) error { entered <- ctx; <-release; return ctx.Err() }}, DevOptions{Source: source, Observe: func(out Outcome) { done <- out }})
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	putRule(t, s, "source")
	putTarget(t, s, "source", Target{})
	await(t, clock.registered)
	clock.advance(time.Minute)
	assert.Equal(t, "events.dispatch:source", await(t, source.entered))
	rule, err := s.DescribeRule(RuleInput{Name: "source"})
	require.NoError(t, err)
	assert.Equal(t, "ENABLED", rule.State)
	source.mu.Lock()
	assert.Zero(t, source.count)
	source.mu.Unlock()
	clock.advance(5 * time.Minute)
	source.set(true)
	assert.Equal(t, "events.dispatch:source", await(t, source.entered))
	invocationCtx := await(t, entered)
	source.set(false)
	require.NoError(t, invocationCtx.Err(), "intake fence must not cancel accepted execution")
	source.mu.Lock()
	assert.Equal(t, 1, source.count)
	source.mu.Unlock()
	closed := make(chan error, 1)
	go func() { closed <- s.Close(context.Background()) }()
	select {
	case err := <-closed:
		t.Fatalf("source close crossed held callback: %v", err)
	default:
	}
	releaseOnce.Do(func() { close(release) })
	require.NoError(t, await(t, closed))
	require.NoError(t, await(t, source.released))
	assert.Equal(t, "accepted", await(t, done).Status)
	source.mu.Lock()
	assert.Zero(t, source.count)
	source.mu.Unlock()
	s.mu.Lock()
	assert.Zero(t, s.pending)
	s.mu.Unlock()
}
func TestCloseBudgetStopsUnacceptedRetryWaitAndReleasesOwnedSource(t *testing.T) {
	clock := newClock()
	source := newSource()
	source.set(true)
	first := make(chan struct{}, 1)
	s := serviceFor(t, clock, invoker{admit: func(context.Context, string, []byte) error { first <- struct{}{}; return admissionError(true) }}, DevOptions{Source: source})
	putRule(t, s, "retry")
	putTarget(t, s, "retry", Target{})
	await(t, clock.registered)
	clock.advance(time.Minute)
	await(t, first)
	for duration := await(t, clock.registered); duration >= time.Minute; duration = await(t, clock.registered) {
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, s.Close(ctx), context.Canceled)
	require.NoError(t, await(t, source.released), "known canceled retry is separate from close uncertainty")
	select {
	case <-first:
		t.Fatal("expired close admitted another retry")
	default:
	}
	require.ErrorIs(t, s.DevEvidence(), context.Canceled)
}
