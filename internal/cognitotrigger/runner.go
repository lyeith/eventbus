package cognitotrigger

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/lyeith/eventbus/internal/devactivity"
)

const (
	MaxEventBytes      = 1 << 20
	MaxResultBytes     = 1 << 20
	MaxDiagnosticBytes = 64 << 10
)

type ErrorKind string

const (
	Timeout         ErrorKind = "timeout"
	HandlerFailure  ErrorKind = "handler_failure"
	InvalidResponse ErrorKind = "invalid_response"
	NotConfigured   ErrorKind = "not_configured"
	Closed          ErrorKind = "closed"
)

// InvocationError never reflects event values, handler results, or diagnostics.
type InvocationError struct {
	Kind    ErrorKind
	Trigger string
	Cause   error
}

func (e *InvocationError) Error() string {
	return fmt.Sprintf("Cognito %s trigger: %s", e.Trigger, e.Kind)
}
func (e *InvocationError) Unwrap() error       { return e.Cause }
func (e *InvocationError) FailureKind() string { return string(e.Kind) }

// Execution is the consumer-owned runtime seam. App composes its adapter; the
// runtime owns process/worker lifetimes and returns only private-safe failures.
type Execution interface {
	Execute(context.Context, string, string, []byte) (ExecutionResult, error)
	Close(context.Context) error
}
type ExecutionResult struct {
	Payload      []byte
	Failure      ErrorKind
	OwnershipErr error
}
type retainedExecution interface {
	DevBeginWarmDrain() error
	DevResumeWarm() error
	DevEvidence() error
}

// Runner owns event/result validation, admission and Cognito's five-second
// deadline. Execution owns the actual joined child and retained-worker boundary.
type Runner struct {
	pools        map[string]map[string]time.Duration
	execution    Execution
	mu           sync.Mutex
	closed       bool
	next         uint64
	active       map[uint64]context.CancelFunc
	inflight     sync.WaitGroup
	done         chan struct{}
	devActivity  devactivity.Activity
	ownershipErr error
}

func New(config *Config, execution Execution) (*Runner, error) {
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	if execution == nil {
		return nil, errors.New("Cognito trigger execution must be configured")
	}
	runner := &Runner{pools: make(map[string]map[string]time.Duration), execution: execution, active: make(map[uint64]context.CancelFunc), done: make(chan struct{}), devActivity: config.DevActivity}
	for poolID, pool := range config.Pools {
		entries := make(map[string]time.Duration)
		for name, entry := range pool.entries() {
			seconds := entry.TimeoutSeconds
			if seconds == 0 {
				seconds = defaultTimeoutSeconds
			}
			entries[name] = time.Duration(seconds) * time.Second
		}
		runner.pools[poolID] = entries
	}
	return runner, nil
}
func (r *Runner) Supports(poolID string) bool {
	if r == nil {
		return false
	}
	_, ok := r.pools[poolID]
	return ok
}
func (r *Runner) SupportsTrigger(poolID, name string) bool {
	if r == nil {
		return false
	}
	_, ok := r.pools[poolID][name]
	return ok
}

// DecodeResponse is the strict serialized full-event contract. UseNumber keeps
// native response numbers intact; trailing JSON and a missing response refuse.
func DecodeResponse(payload []byte) (map[string]any, error) {
	if len(payload) > MaxResultBytes {
		return nil, errors.New("trigger response limit exceeded")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var result map[string]any
	if err := decoder.Decode(&result); err != nil || result == nil {
		return nil, errors.New("invalid trigger event response")
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, errors.New("invalid trigger event response")
	}
	if response, ok := result["response"].(map[string]any); !ok || response == nil {
		return nil, errors.New("invalid trigger event response")
	}
	return result, nil
}
func (r *Runner) Invoke(ctx context.Context, poolID, name string, event map[string]any) (map[string]any, error) {
	if !r.SupportsTrigger(poolID, name) {
		return nil, &InvocationError{Kind: NotConfigured, Trigger: name}
	}
	execution, cancel := context.WithTimeout(ctx, r.pools[poolID][name])
	defer cancel()
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, &InvocationError{Kind: Closed, Trigger: name}
	}
	r.next++
	id := r.next
	var release func(error)
	if r.devActivity != nil {
		var err error
		release, err = r.devActivity.BeginActivity("cognito_trigger", fmt.Sprintf("trigger-%d", id))
		if err != nil || release == nil {
			r.mu.Unlock()
			return nil, &InvocationError{Kind: Closed, Trigger: name}
		}
	}
	r.active[id] = cancel
	r.inflight.Add(1)
	r.mu.Unlock()
	var ownershipErr error
	executionEntered, executionReturned := false, false
	defer func() {
		recovered := recover()
		if executionEntered && !executionReturned {
			ownershipErr = errors.Join(ownershipErr, errors.New("Cognito trigger ownership did not complete"))
		}
		r.mu.Lock()
		delete(r.active, id)
		r.ownershipErr = errors.Join(r.ownershipErr, ownershipErr)
		if release != nil {
			release(ownershipErr)
		}
		r.inflight.Done()
		r.mu.Unlock()
		if recovered != nil {
			panic(recovered)
		}
	}()
	input, err := json.Marshal(event)
	if err != nil || len(input) > MaxEventBytes {
		return nil, &InvocationError{Kind: InvalidResponse, Trigger: name, Cause: err}
	}
	executionEntered = true
	output, err := r.execution.Execute(execution, poolID, name, input)
	executionReturned = true
	ownershipErr = output.OwnershipErr
	if execution.Err() != nil {
		return nil, &InvocationError{Kind: Timeout, Trigger: name, Cause: execution.Err()}
	}
	if err != nil || ownershipErr != nil {
		return nil, &InvocationError{Kind: HandlerFailure, Trigger: name, Cause: errors.Join(err, ownershipErr)}
	}
	if output.Failure != "" {
		kind := output.Failure
		if kind != Timeout && kind != InvalidResponse && kind != HandlerFailure && kind != Closed {
			kind = HandlerFailure
		}
		return nil, &InvocationError{Kind: kind, Trigger: name}
	}
	result, err := DecodeResponse(output.Payload)
	if err != nil {
		return nil, &InvocationError{Kind: InvalidResponse, Trigger: name}
	}
	return result, nil
}

// Close fences/cancels admission, then an uncanceled owner joins every invocation
// and the runtime. A waiting caller may time out and rejoin the same cleanup.
func (r *Runner) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	first := !r.closed
	if first {
		r.closed = true
		for _, cancel := range r.active {
			cancel()
		}
	}
	r.mu.Unlock()
	if first {
		go func() {
			r.inflight.Wait()
			err := r.execution.Close(context.Background())
			r.mu.Lock()
			r.ownershipErr = errors.Join(r.ownershipErr, err)
			r.mu.Unlock()
			close(r.done)
		}()
	}
	select {
	case <-r.done:
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.ownershipErr
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (r *Runner) DevBeginWarmDrain() error {
	if r == nil {
		return nil
	}
	if runtime, ok := r.execution.(retainedExecution); ok {
		return runtime.DevBeginWarmDrain()
	}
	return nil
}
func (r *Runner) DevResumeWarm() error {
	if r == nil {
		return nil
	}
	if runtime, ok := r.execution.(retainedExecution); ok {
		return runtime.DevResumeWarm()
	}
	return nil
}
func (r *Runner) DevEvidence() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	err := r.ownershipErr
	r.mu.Unlock()
	if runtime, ok := r.execution.(retainedExecution); ok {
		err = errors.Join(err, runtime.DevEvidence())
	}
	return err
}
