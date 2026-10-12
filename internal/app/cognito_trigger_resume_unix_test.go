//go:build linux || darwin

package app

import (
	"context"
	"encoding/json"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/cognitotrigger"
	"github.com/lyeith/eventbus/internal/devquiescence"
	"github.com/stretchr/testify/require"
)

// The coordinator observes the real worker's lease release before the callback
// returns to worker.close(). This holds only the publication gap after actual
// process/listener cleanup, without replacing either runtime lifecycle owner.
type heldWarmRetirementActivity struct {
	owner              *devquiescence.Coordinator
	entered, release   chan struct{}
	holdOnce, openOnce sync.Once
}

func (activity *heldWarmRetirementActivity) BeginActivity(kind, requestID string) (func(error), error) {
	release, err := activity.owner.BeginActivity(kind, requestID)
	if err != nil || kind != "lambda_warm_worker" {
		return release, err
	}
	return func(err error) {
		release(err)
		activity.holdOnce.Do(func() {
			close(activity.entered)
			<-activity.release
		})
	}, nil
}

func (activity *heldWarmRetirementActivity) open() {
	activity.openOnce.Do(func() { close(activity.release) })
}

func TestCognitoWarmResumeJoinsRetirementPublicationAfterQuiescence(t *testing.T) {
	config, directory := triggerProbe(t, true)
	var runner *cognitotrigger.Runner
	owner := devquiescence.NewWithOptions(devquiescence.Options{
		Checks: []func() error{func() error { return runner.DevEvidence() }},
		DrainHooks: []devquiescence.DrainHook{{
			Start:  func() error { return runner.DevBeginWarmDrain() },
			Resume: func() error { return runner.DevResumeWarm() },
		}},
	})
	activity := &heldWarmRetirementActivity{owner: owner, entered: make(chan struct{}), release: make(chan struct{})}
	config.DevActivity = activity
	var err error
	runner, err = NewCognitoTriggers(config, directory)
	require.NoError(t, err)
	type resumedResult struct {
		snapshot devquiescence.Snapshot
		err      error
	}
	var resumed <-chan resumedResult
	resumeJoined := false
	t.Cleanup(func() {
		activity.open()
		if resumed != nil && !resumeJoined {
			select {
			case <-resumed:
			case <-time.After(5 * time.Second):
				t.Error("owned resume transition did not join")
			}
		}
		require.NoError(t, runner.Close(context.Background()))
	})
	initial, err := runner.Invoke(t.Context(), "owned-pool", cognitotrigger.DefineAuthChallenge, awsEvent("DefineAuthChallenge_Authentication"))
	require.NoError(t, err)
	firstPID := triggerProbePID(t, initial["response"].(map[string]any))
	require.NoError(t, syscall.Kill(firstPID, 0))
	held, err := owner.Quiesce(t.Context())
	require.NoError(t, err)
	require.True(t, held.FixtureSafe)
	require.Zero(t, held.WorkCount)
	require.ErrorIs(t, syscall.Kill(firstPID, 0), syscall.ESRCH, "safe quiescence observes actual process cleanup")
	select {
	case <-activity.entered:
	case <-time.After(time.Second):
		t.Fatal("worker did not reach the held retirement publication boundary")
	}
	completed := make(chan resumedResult, 1)
	resumed = completed
	go func() {
		snapshot, err := owner.Resume(held.Generation)
		completed <- resumedResult{snapshot, err}
	}()
	require.Eventually(t, func() bool {
		for _, activity := range owner.Snapshot().Activities {
			if activity.Kind == "dev.resume" {
				return true
			}
		}
		return false
	}, time.Second, time.Millisecond, "resume must wait in its existing transition instead of poisoning healthy evidence")
	select {
	case result := <-resumed:
		resumeJoined = true
		t.Fatalf("resume escaped before worker retirement published: %+v", result)
	default:
	}
	// Resume must not hold Lambda's pool mutex while joining the existing
	// retirement. Evidence uses that mutex and must remain independently readable.
	evidence := make(chan error, 1)
	go func() { evidence <- runner.DevEvidence() }()
	select {
	case err := <-evidence:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("resume held the pool mutex across an existing retirement join")
	}
	activity.open()
	select {
	case result := <-resumed:
		resumeJoined = true
		require.NoError(t, result.err)
		require.Equal(t, held.Generation+1, result.snapshot.Generation)
		require.Equal(t, devquiescence.Open, result.snapshot.State)
		require.Empty(t, result.snapshot.EvidenceFailure)
		require.Zero(t, result.snapshot.WorkCount)
	case <-time.After(5 * time.Second):
		t.Fatal("resume did not join retirement after publication was released")
	}
	next, err := runner.Invoke(t.Context(), "owned-pool", cognitotrigger.DefineAuthChallenge, awsEvent("DefineAuthChallenge_Authentication"))
	require.NoError(t, err)
	response := next["response"].(map[string]any)
	require.Equal(t, json.Number("1"), response["count"])
	secondPID := triggerProbePID(t, response)
	require.NotEqual(t, firstPID, secondPID)
	require.NoError(t, runner.Close(t.Context()))
	require.ErrorIs(t, syscall.Kill(secondPID, 0), syscall.ESRCH)
	require.NoError(t, runner.DevEvidence())
}
