package cognitotrigger

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type reloadTestExecution struct {
	fixtureExecution
	reload func(context.Context, string, string) error
}

func (execution reloadTestExecution) DevReloadTrigger(ctx context.Context, pool, trigger string) error {
	if execution.reload != nil {
		return execution.reload(ctx, pool, trigger)
	}
	return nil
}

func requireReloadError(t *testing.T, err error, kind DevReloadErrorKind) {
	t.Helper()
	var failure *DevReloadError
	require.ErrorAs(t, err, &failure)
	require.Equal(t, kind, failure.Kind)
	require.NotContains(t, err.Error(), "PRIVATE")
}

func TestDevTriggerReloadRefusesBeforeExecution(t *testing.T) {
	var calls atomic.Int32
	reloadable := reloadTestExecution{reload: func(context.Context, string, string) error { calls.Add(1); return nil }}
	runner, err := New(fixtureConfig(), reloadable)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runner.Close(context.Background())) })
	var absent *Runner
	requireReloadError(t, absent.DevReloadTrigger(t.Context(), "owned-pool", DefineAuthChallenge), DevReloadNotConfigured)
	for _, target := range []struct{ pool, trigger string }{{"missing", DefineAuthChallenge}, {"owned-pool", "Unknown"}, {"", DefineAuthChallenge}, {"owned-pool", ""}} {
		requireReloadError(t, runner.DevReloadTrigger(t.Context(), target.pool, target.trigger), DevReloadNotConfigured)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	err = runner.DevReloadTrigger(canceled, "owned-pool", DefineAuthChallenge)
	requireReloadError(t, err, DevReloadWaitInterrupted)
	require.ErrorIs(t, err, context.Canceled)
	unsupported, err := New(fixtureConfig(), fixtureExecution{})
	require.NoError(t, err)
	requireReloadError(t, unsupported.DevReloadTrigger(t.Context(), "owned-pool", DefineAuthChallenge), DevReloadUnsupported)
	require.NoError(t, unsupported.Close(t.Context()))
	require.NoError(t, runner.Close(t.Context()))
	requireReloadError(t, runner.DevReloadTrigger(t.Context(), "owned-pool", DefineAuthChallenge), DevReloadClosed)
	require.Zero(t, calls.Load())
}

func TestDevTriggerReloadTracksCleanWaitAndRedactsPortErrors(t *testing.T) {
	for _, fault := range []error{
		nil,
		errors.New("PRIVATE_HANDLER_PATH"),
		errors.Join(errors.New("PRIVATE_HANDLER_PATH"), context.DeadlineExceeded),
		&DevReloadError{Kind: "PRIVATE_HANDLER_PATH", Cause: errors.New("PRIVATE_ENVIRONMENT")},
	} {
		observer := &triggerActivityRecorder{}
		config := fixtureConfig()
		config.DevActivity = observer
		runner, err := New(config, reloadTestExecution{reload: func(_ context.Context, pool, trigger string) error {
			require.Equal(t, "owned-pool", pool)
			require.Equal(t, CreateAuthChallenge, trigger)
			return fault
		}})
		require.NoError(t, err)
		err = runner.DevReloadTrigger(t.Context(), "owned-pool", CreateAuthChallenge)
		if fault == nil {
			require.NoError(t, err)
		} else if errors.Is(fault, context.DeadlineExceeded) {
			requireReloadError(t, err, DevReloadWaitInterrupted)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.NotContains(t, errors.Unwrap(err).Error(), "PRIVATE")
		} else {
			requireReloadError(t, err, DevReloadFailed)
			require.Nil(t, errors.Unwrap(err))
		}
		active, begun, completed, kind, id := observer.snapshot()
		require.Zero(t, active)
		require.Equal(t, 1, begun)
		require.Equal(t, "cognito_trigger_reload", kind)
		require.Equal(t, "trigger-reload-1", id)
		require.Equal(t, []error{nil}, completed, "ordinary source/wait failures are not abandoned ownership")
		require.NoError(t, runner.DevEvidence())
		require.NoError(t, runner.Close(t.Context()))
	}
}

func TestDevTriggerReloadActivityRefusalDoesNotExecute(t *testing.T) {
	for _, missing := range []bool{false, true} {
		config := fixtureConfig()
		config.DevActivity = &triggerActivityRecorder{reject: !missing, missingRelease: missing}
		runner, err := New(config, reloadTestExecution{reload: func(context.Context, string, string) error {
			t.Fatal("refused reload executed")
			return nil
		}})
		require.NoError(t, err)
		requireReloadError(t, runner.DevReloadTrigger(t.Context(), "owned-pool", DefineAuthChallenge), DevReloadClosed)
		require.NoError(t, runner.Close(t.Context()))
	}
}

func TestDevTriggerReloadCloseCancelsWaitThenJoinsPortAndRuntime(t *testing.T) {
	entered, retiring := make(chan struct{}), make(chan struct{})
	reloadGate, closeGate := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-reloadGate:
		default:
			close(reloadGate)
		}
		select {
		case <-closeGate:
		default:
			close(closeGate)
		}
	})
	observer := &triggerActivityRecorder{}
	config := fixtureConfig()
	config.DevActivity = observer
	runner, err := New(config, reloadTestExecution{
		fixtureExecution: fixtureExecution{close: func(context.Context) error { close(retiring); <-closeGate; return nil }},
		reload: func(ctx context.Context, _, _ string) error {
			close(entered)
			<-ctx.Done()
			<-reloadGate
			return errors.Join(errors.New("PRIVATE_WAIT_DETAIL"), ctx.Err())
		},
	})
	require.NoError(t, err)
	reloaded := make(chan error, 1)
	go func() { reloaded <- runner.DevReloadTrigger(context.Background(), "owned-pool", DefineAuthChallenge) }()
	<-entered
	budget, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, runner.Close(budget), context.DeadlineExceeded)
	active, _, completed, _, _ := observer.snapshot()
	require.Equal(t, 1, active)
	require.Empty(t, completed)
	close(reloadGate)
	err = <-reloaded
	requireReloadError(t, err, DevReloadWaitInterrupted)
	require.ErrorIs(t, err, context.Canceled)
	<-retiring
	select {
	case <-runner.done:
		t.Fatal("runtime Close returned before its actual join")
	default:
	}
	close(closeGate)
	require.NoError(t, runner.Close(t.Context()))
	active, _, completed, _, _ = observer.snapshot()
	require.Zero(t, active)
	require.Equal(t, []error{nil}, completed)
}

func TestDevTriggerReloadInterruptedPortRetainsDirtyOwnership(t *testing.T) {
	for _, mode := range []string{"panic", "goexit"} {
		t.Run(mode, func(t *testing.T) {
			runner, err := New(fixtureConfig(), reloadTestExecution{reload: func(context.Context, string, string) error {
				if mode == "panic" {
					panic("controlled private reload panic")
				}
				runtime.Goexit()
				return nil
			}})
			require.NoError(t, err)
			done := make(chan struct{})
			var returned atomic.Bool
			go func() {
				defer close(done)
				defer func() { _ = recover() }()
				_ = runner.DevReloadTrigger(context.Background(), "owned-pool", DefineAuthChallenge)
				returned.Store(true)
			}()
			<-done
			require.False(t, returned.Load())
			require.ErrorContains(t, runner.DevEvidence(), "reload ownership did not complete")
			require.ErrorContains(t, runner.Close(t.Context()), "reload ownership did not complete")
		})
	}
}
