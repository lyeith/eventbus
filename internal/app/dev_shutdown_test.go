package app

import (
	"context"
	"flag"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/devquiescence"
	"github.com/stretchr/testify/require"
)

func TestCLIShutdownPolicy(t *testing.T) {
	for _, tc := range []struct {
		name    string
		args    []string
		want    devShutdownPolicy
		invalid bool
	}{
		{name: "default", want: devShutdownGraceful},
		{name: "explicit graceful", args: []string{"--shutdown-policy", "graceful"}, want: devShutdownGraceful},
		{name: "disposable", args: []string{"--shutdown-policy", "discard-pending"}, want: devShutdownDiscardPending},
		{name: "unknown", args: []string{"--shutdown-policy", "drop"}, invalid: true},
		{name: "explicit empty", args: []string{"--shutdown-policy="}, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			flags := flag.NewFlagSet("eventbus", flag.ContinueOnError)
			flags.SetOutput(io.Discard)
			cfg, err := readConfig(flags, tc.args)
			if tc.invalid {
				require.ErrorContains(t, err, "unknown shutdown policy")
				require.Equal(t, config{}, cfg)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, cfg.shutdownPolicy)
		})
	}
}

type devShutdownFunctions struct {
	orderedLambdaLifecycle
	discard func()
}

func (owner devShutdownFunctions) DevDiscardPendingAsync()     { owner.discard() }
func (owner devShutdownFunctions) DevDiscardedAsyncCount() int { return 3 }

type devShutdownAdmissions struct {
	contextCloser
	discard func()
}

func (owner devShutdownAdmissions) DevDiscardPendingAdmissions() { owner.discard() }

func TestDiscardPendingPolicyPrecedesRetainedJoinAndResourceRelease(t *testing.T) {
	retained := devquiescence.New()
	var order []string
	completeEvent, err := retained.BeginActivity("lambda_async", "event")
	require.NoError(t, err)
	completeScheduler, _, err := retained.BeginSource("scheduler.dispatch", "schedule")
	require.NoError(t, err)
	completeRule, _, err := retained.BeginSource("events.dispatch", "rule")
	require.NoError(t, err)

	discard := func(name string, release func(error)) func() {
		return func() {
			require.Equal(t, devquiescence.Shutdown, retained.Snapshot().State,
				"permanent source fence must precede any irreversible discard")
			order = append(order, name+".discard")
			release(nil)
		}
	}
	source := func(name string, release func(error)) contextCloser {
		return devShutdownAdmissions{
			contextCloser: contextCloseFunc(func(context.Context) error {
				require.Zero(t, retained.Snapshot().WorkCount)
				order = append(order, name+".close")
				return nil
			}),
			discard: discard(name, release),
		}
	}
	owned := &eventBusLifecycle{
		functions: devShutdownFunctions{
			orderedLambdaLifecycle: orderedLambdaLifecycle{&order},
			discard:                discard("lambda", completeEvent),
		},
		scheduler: source("scheduler", completeScheduler),
		events:    source("events", completeRule),
		ses:       closeFunc(func() error { order = append(order, "capture.close"); return nil }),
	}
	listener := newEventBusListener(&http.Server{}, owned, 2*time.Second)
	listener.devRetained = &devRetainedHTTP{owner: retained, callbacks: &http.Server{}}
	listener.shutdownPolicy = devShutdownDiscardPending

	require.NoError(t, listener.Shutdown(t.Context()))
	require.NoError(t, listener.Shutdown(t.Context()), "final policy and joins run once")
	require.Equal(t, []string{
		"lambda.discard", "scheduler.discard", "events.discard",
		"scheduler.close", "events.close", "lambda.drain", "lambda.close", "capture.close",
	}, order)
	snapshot := retained.Snapshot()
	require.Zero(t, snapshot.WorkCount)
	require.False(t, snapshot.FixtureSafe, "final disposal never authorizes fixture cleanup or resume")
}

func TestGracefulShutdownDoesNotActivateDiscardPorts(t *testing.T) {
	for _, policy := range []devShutdownPolicy{"", devShutdownGraceful} {
		t.Run(string(policy), func(t *testing.T) {
			var order []string
			unexpected := func() { t.Fatal("graceful shutdown activated discard") }
			owned := &eventBusLifecycle{
				functions: devShutdownFunctions{orderedLambdaLifecycle{&order}, unexpected},
				events: devShutdownAdmissions{
					contextCloser: contextCloseFunc(func(context.Context) error { return nil }),
					discard:       unexpected,
				},
			}
			listener := newEventBusListener(&http.Server{}, owned, time.Second)
			listener.shutdownPolicy = policy
			require.NoError(t, listener.Shutdown(t.Context()))
			require.Equal(t, []string{"lambda.drain", "lambda.close"}, order)
		})
	}
}

func TestDiscardPendingUnsupportedOwnerWithholdsResourceRelease(t *testing.T) {
	var order []string
	owned := &eventBusLifecycle{
		functions: orderedLambdaLifecycle{&order},
		ses:       closeFunc(func() error { t.Fatal("resources released after policy failure"); return nil }),
	}
	listener := newEventBusListener(&http.Server{}, owned, time.Second)
	listener.shutdownPolicy = devShutdownDiscardPending
	require.ErrorContains(t, listener.Shutdown(t.Context()), "Lambda owner does not support")
	require.Equal(t, []string{"lambda.drain"}, order)
}
