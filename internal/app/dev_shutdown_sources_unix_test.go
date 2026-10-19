//go:build linux || darwin

package app

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/devquiescence"
	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/scheduler"
	"github.com/stretchr/testify/require"
)

// Observe the actual shared adapter's native capacity refusal, without
// replacing Lambda admission or the Scheduler retry/source owners.
type observedShutdownAdmission struct {
	lambdaAdmission
	rejected chan struct{}
}

func (owner observedShutdownAdmission) Admit(ctx context.Context, input lambdaservice.InvokeInput) (lambdaservice.Admission, error) {
	admission, err := owner.lambdaAdmission.Admit(ctx, input)
	var native *lambdaservice.InvokeError
	if errors.As(err, &native) && native.Status == http.StatusTooManyRequests {
		select {
		case owner.rejected <- struct{}{}:
		default:
		}
	}
	return admission, err
}

func TestDiscardPendingRetainedShutdownJoinsCapacityRejectedScheduler(t *testing.T) {
	directory := t.TempDir()
	executable, err := os.Executable()
	require.NoError(t, err)
	retained := devquiescence.New()
	functions, err := lambdaservice.NewService(&lambdaservice.Config{
		Functions: map[string]lambdaservice.Function{
			"retry:live": {
				Runtime: "provided",
				Command: []string{executable, "-test.run=^TestDisposableShutdownProvidedProcess$"},
				Timeout: 5 * time.Second,
				Environment: map[string]string{
					"EVENTBUS_DISPOSABLE_SHUTDOWN_HANDLER": "1",
					"DISPOSABLE_SHUTDOWN_ROOT":             directory,
				},
			},
		},
		DevAsync:    &lambdaservice.DevAsyncConfig{Workers: 1, Capacity: 1, LogWriter: io.Discard},
		DevActivity: retained,
	}, directory)
	require.NoError(t, err)
	t.Cleanup(func() {
		functions.DevDiscardPendingAsync()
		_ = functions.Close(context.Background())
	})
	admission, err := functions.Admit(t.Context(), lambdaservice.InvokeInput{
		FunctionName: "retry:live", Payload: []byte(`{"label":"retry","fail":true}`),
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		rows := functions.AsyncSnapshot()
		return len(rows) == 1 && rows[0].RequestID == admission.RequestID && rows[0].State == "retrying" && time.Until(rows[0].NextAttemptAt) > 30*time.Second
	}, 5*time.Second, 5*time.Millisecond, "real failed handler did not enter the native minute retry")

	rejected := make(chan struct{}, 1)
	observed := make(chan scheduler.Outcome, 1)
	schedules, err := scheduler.New(scheduler.Options{
		Region: "us-east-1", AccountID: "000000000000",
		Dev: scheduler.DevOptions{
			ExactSeconds: true, RetryDelay: time.Hour, Source: retained,
			Observe: func(outcome scheduler.Outcome) { observed <- outcome },
		},
	}, asyncLambdaTargetInvoker{runtime: observedShutdownAdmission{functions, rejected}})
	require.NoError(t, err)
	t.Cleanup(func() {
		schedules.DevDiscardPendingAdmissions()
		_ = schedules.Close(context.Background())
	})
	payload := `{"label":"must-not-start"}`
	_, err = schedules.Create(t.Context(), scheduler.CreateInput{
		Name:               "capacity-rejected",
		ScheduleExpression: "at(" + time.Now().UTC().Add(-time.Second).Format("2006-01-02T15:04:05") + ")",
		FlexibleTimeWindow: &scheduler.FlexibleTimeWindow{Mode: "OFF"},
		Target: &scheduler.Target{
			Arn:     "arn:aws:lambda:us-east-1:000000000000:function:retry:live",
			RoleArn: "arn:aws:iam::000000000000:role/owned-test",
			Input:   &payload,
		},
	})
	require.NoError(t, err)
	select {
	case <-rejected:
	case <-time.After(5 * time.Second):
		t.Fatal("Scheduler did not reach the actual native pending-capacity refusal")
	}
	require.Equal(t, 2, retained.Snapshot().WorkCount, "Lambda Event plus unaccepted Scheduler source lease")

	listener := newEventBusListener(&http.Server{}, &eventBusLifecycle{functions: functions, scheduler: schedules}, 2*time.Second)
	listener.devRetained = &devRetainedHTTP{owner: retained, callbacks: &http.Server{}}
	listener.shutdownPolicy = devShutdownDiscardPending
	require.NoError(t, listener.Shutdown(t.Context()), "neither native minute retry nor producer hour retry may hold the retained join")
	outcome := <-observed
	require.Equal(t, "canceled", outcome.Status)
	require.Equal(t, 1, outcome.Attempts)
	require.Equal(t, 1, functions.DevDiscardedAsyncCount(), "unaccepted producer delivery is separate from accepted Lambda Event")
	snapshot := retained.Snapshot()
	require.Zero(t, snapshot.WorkCount)
	require.Empty(t, snapshot.EvidenceFailure)
	require.False(t, snapshot.FixtureSafe)
	_, err = os.Stat(filepath.Join(directory, "started-must-not-start.json"))
	require.ErrorIs(t, err, os.ErrNotExist)
}
