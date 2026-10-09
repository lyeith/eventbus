package cognitotrigger

import (
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type triggerActivityRecorder struct {
	mu                     sync.Mutex
	active, begun          int
	completed              []error
	kind, id               string
	reject, missingRelease bool
}

func (observer *triggerActivityRecorder) BeginActivity(kind, id string) (func(error), error) {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if observer.reject {
		return nil, errors.New("private observer admission failure")
	}
	if observer.missingRelease {
		return nil, nil
	}
	observer.active++
	observer.begun++
	observer.kind, observer.id = kind, id
	return func(err error) {
		observer.mu.Lock()
		defer observer.mu.Unlock()
		observer.active--
		observer.completed = append(observer.completed, err)
	}, nil
}
func (observer *triggerActivityRecorder) snapshot() (int, int, []error, string, string) {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	return observer.active, observer.begun, append([]error(nil), observer.completed...), observer.kind, observer.id
}
func fixtureConfig() *Config {
	return &Config{Pools: map[string]Pool{"owned-pool": {
		DefineAuthChallenge: &Entry{Handler: "application.mjs"}, CreateAuthChallenge: &Entry{Handler: "application.mjs"}, VerifyAuthChallengeResponse: &Entry{Handler: "application.mjs"},
	}}}
}

type fixtureExecution struct {
	execute func(context.Context, string, string, []byte) (ExecutionResult, error)
	close   func(context.Context) error
}

func (execution fixtureExecution) Execute(ctx context.Context, pool, name string, input []byte) (ExecutionResult, error) {
	if execution.execute != nil {
		return execution.execute(ctx, pool, name, input)
	}
	return ExecutionResult{Payload: input}, nil
}
func (execution fixtureExecution) Close(ctx context.Context) error {
	if execution.close != nil {
		return execution.close(ctx)
	}
	return nil
}
func requireTriggerActivityReleased(t *testing.T, observer *triggerActivityRecorder, expected error) {
	t.Helper()
	active, begun, completed, kind, id := observer.snapshot()
	require.Zero(t, active)
	require.Equal(t, 1, begun)
	require.Len(t, completed, 1)
	require.Equal(t, "cognito_trigger", kind)
	require.Equal(t, "trigger-1", id)
	if expected == nil {
		require.NoError(t, completed[0])
	} else {
		require.ErrorIs(t, completed[0], expected)
	}
}
func TestDevActivitySeparatesNativeResultFromOwnershipEvidence(t *testing.T) {
	for _, kind := range []ErrorKind{"", HandlerFailure, InvalidResponse, Timeout} {
		for _, uncertain := range []bool{false, true} {
			name := string(kind)
			if uncertain {
				name += "/uncertain"
			} else {
				name += "/clean"
			}
			t.Run(name, func(t *testing.T) {
				observer := &triggerActivityRecorder{}
				config := fixtureConfig()
				config.DevActivity = observer
				var ownershipErr error
				if uncertain {
					ownershipErr = errors.New("private cleanup uncertainty")
				}
				execution := fixtureExecution{execute: func(_ context.Context, pool, trigger string, input []byte) (ExecutionResult, error) {
					require.Equal(t, "owned-pool", pool)
					require.Equal(t, DefineAuthChallenge, trigger)
					return ExecutionResult{Payload: input, Failure: kind, OwnershipErr: ownershipErr}, nil
				}}
				runner, err := New(config, execution)
				require.NoError(t, err)
				_, err = runner.Invoke(t.Context(), "owned-pool", DefineAuthChallenge, map[string]any{"response": map[string]any{}})
				expected := kind
				if uncertain {
					expected = HandlerFailure
				}
				if expected == "" {
					require.NoError(t, err)
				} else {
					var invocation *InvocationError
					require.ErrorAs(t, err, &invocation)
					require.Equal(t, expected, invocation.Kind)
					require.NotContains(t, err.Error(), "private")
				}
				requireTriggerActivityReleased(t, observer, ownershipErr)
				for range 2 {
					err = runner.Close(t.Context())
					if ownershipErr == nil {
						require.NoError(t, err)
					} else {
						require.ErrorIs(t, err, ownershipErr)
					}
				}
			})
		}
	}
}
func TestDevActivityRefusalPrecedesExecution(t *testing.T) {
	for _, missing := range []bool{false, true} {
		observer := &triggerActivityRecorder{reject: !missing, missingRelease: missing}
		config := fixtureConfig()
		config.DevActivity = observer
		runner, err := New(config, fixtureExecution{execute: func(context.Context, string, string, []byte) (ExecutionResult, error) {
			t.Fatal("refused invocation executed")
			return ExecutionResult{}, nil
		}})
		require.NoError(t, err)
		_, err = runner.Invoke(t.Context(), "owned-pool", DefineAuthChallenge, map[string]any{"response": map[string]any{}})
		var invocation *InvocationError
		require.ErrorAs(t, err, &invocation)
		require.Equal(t, Closed, invocation.Kind)
		require.NoError(t, runner.Close(t.Context()))
	}
}
func TestCloseWaitsForExecutionAndRuntimeJoins(t *testing.T) {
	observer := &triggerActivityRecorder{}
	config := fixtureConfig()
	config.DevActivity = observer
	entered, retiring := make(chan struct{}), make(chan struct{})
	executeGate, closeGate := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-executeGate:
		default:
			close(executeGate)
		}
		select {
		case <-closeGate:
		default:
			close(closeGate)
		}
	})
	runner, err := New(config, fixtureExecution{
		execute: func(ctx context.Context, _, _ string, input []byte) (ExecutionResult, error) {
			close(entered)
			<-ctx.Done()
			<-executeGate
			return ExecutionResult{}, ctx.Err()
		},
		close: func(context.Context) error { close(retiring); <-closeGate; return nil },
	})
	require.NoError(t, err)
	invoked := make(chan error, 1)
	go func() {
		_, err := runner.Invoke(context.Background(), "owned-pool", DefineAuthChallenge, map[string]any{"response": map[string]any{}})
		invoked <- err
	}()
	<-entered
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, runner.Close(ctx), context.DeadlineExceeded)
	active, _, completed, _, _ := observer.snapshot()
	require.Equal(t, 1, active)
	require.Empty(t, completed)
	close(executeGate)
	var failure *InvocationError
	require.ErrorAs(t, <-invoked, &failure)
	require.Equal(t, Timeout, failure.Kind)
	require.ErrorIs(t, failure, context.Canceled)
	<-retiring
	select {
	case <-runner.done:
		t.Fatal("runtime cleanup was not joined")
	default:
	}
	close(closeGate)
	require.NoError(t, runner.Close(t.Context()))
	requireTriggerActivityReleased(t, observer, nil)
}
func TestInvocationRejectsInvalidInputBeforeExecution(t *testing.T) {
	for _, event := range []map[string]any{{"response": make(chan bool)}, {"large": strings.Repeat("x", MaxEventBytes+1)}} {
		runner, err := New(fixtureConfig(), fixtureExecution{execute: func(context.Context, string, string, []byte) (ExecutionResult, error) {
			t.Fatal("invalid input executed")
			return ExecutionResult{}, nil
		}})
		require.NoError(t, err)
		_, err = runner.Invoke(t.Context(), "owned-pool", DefineAuthChallenge, event)
		var invocation *InvocationError
		require.ErrorAs(t, err, &invocation)
		require.Equal(t, InvalidResponse, invocation.Kind)
		require.NoError(t, runner.Close(t.Context()))
	}
}
func TestDecodeResponseContract(t *testing.T) {
	for _, value := range []string{`null`, `[]`, `{}`, `{"response":null}`, `{"response":[]}`, `{"response":"value"}`, `{"response":{}} {}`} {
		_, err := DecodeResponse([]byte(value))
		require.Error(t, err)
	}
	result, err := DecodeResponse([]byte(`{"response":{"count":9007199254740993}}`))
	require.NoError(t, err)
	require.Equal(t, json.Number("9007199254740993"), result["response"].(map[string]any)["count"])
}
func TestImmutableConfigurationAndSupports(t *testing.T) {
	config := fixtureConfig()
	observer := &triggerActivityRecorder{}
	config.DevActivity = observer
	runner, err := New(config, fixtureExecution{})
	require.NoError(t, err)
	config.Pools["owned-pool"].DefineAuthChallenge.TimeoutSeconds = 1
	config.DevActivity = &triggerActivityRecorder{reject: true}
	require.Equal(t, 5*time.Second, runner.pools["owned-pool"][DefineAuthChallenge])
	require.True(t, runner.Supports("owned-pool"))
	require.False(t, runner.Supports("absent"))
	_, err = runner.Invoke(t.Context(), "owned-pool", DefineAuthChallenge, map[string]any{"response": map[string]any{}})
	require.NoError(t, err)
	requireTriggerActivityReleased(t, observer, nil)
	_, err = runner.Invoke(t.Context(), "absent", DefineAuthChallenge, map[string]any{})
	var failure *InvocationError
	require.ErrorAs(t, err, &failure)
	require.Equal(t, NotConfigured, failure.Kind)
	require.NoError(t, runner.Close(t.Context()))
}

func TestExecutionGoexitLeavesDirtyJoinedOwnership(t *testing.T) {
	observer := &triggerActivityRecorder{}
	config := fixtureConfig()
	config.DevActivity = observer
	runner, err := New(config, fixtureExecution{execute: func(context.Context, string, string, []byte) (ExecutionResult, error) {
		runtime.Goexit()
		return ExecutionResult{}, nil
	}})
	require.NoError(t, err)
	var returned atomic.Bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = runner.Invoke(context.Background(), "owned-pool", DefineAuthChallenge, map[string]any{"response": map[string]any{}})
		returned.Store(true)
	}()
	<-done
	require.False(t, returned.Load())
	active, begun, completed, _, _ := observer.snapshot()
	require.Zero(t, active)
	require.Equal(t, 1, begun)
	require.Len(t, completed, 1)
	require.ErrorContains(t, completed[0], "ownership did not complete")
	require.ErrorContains(t, runner.Close(t.Context()), "ownership did not complete")
}
