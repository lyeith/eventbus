package cognitotrigger

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/devactivity"
	"github.com/lyeith/eventbus/internal/localexec"
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

func activityFixtureRunner(t *testing.T, source string, observer devactivity.Activity) (*Runner, *Config) {
	t.Helper()
	_, err := exec.LookPath("node")
	require.NoError(t, err, "Node is required for custom-trigger ownership contracts")
	directory := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(directory, "handler.mjs"), []byte(source), 0600))
	config := fixtureConfig("handler.mjs", nil)
	config.DevActivity = observer
	runner, err := New(config, directory)
	require.NoError(t, err)
	// Expected ownership failures are asserted by the test, while cleanup still
	// joins every runner if a prior assertion fails.
	t.Cleanup(func() { _ = runner.Close(context.Background()) })
	return runner, config
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
	for _, scenario := range []struct {
		name, source string
		kind         ErrorKind
		timeout      bool
	}{
		{"success", `export async function handler(event){return event;}`, "", false},
		{"handler-error", `export async function handler(){throw new Error("Runtime.InternalError");}`, HandlerFailure, false},
		{"invalid-response", `export async function handler(){return {};}`, InvalidResponse, false},
		{"overflow", `export async function handler(event){process.stderr.write("x".repeat(128<<10));return event;}`, InvalidResponse, false},
		{"timeout", `export async function handler(){await new Promise(()=>{});}`, Timeout, true},
	} {
		for _, uncertain := range []bool{false, true} {
			name := scenario.name + "/clean"
			if uncertain {
				name = scenario.name + "/uncertain"
			}
			t.Run(name, func(t *testing.T) {
				observer := &triggerActivityRecorder{}
				runner, _ := activityFixtureRunner(t, scenario.source, observer)
				var ownershipErr error
				if uncertain {
					ownershipErr = errors.New("private cleanup uncertainty")
				}
				runner.processCleanup = func(command *exec.Cmd) error {
					return errors.Join(localexec.Cleanup(command), ownershipErr)
				}
				ctx := t.Context()
				if scenario.timeout {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, 100*time.Millisecond)
					defer cancel()
				}
				result, err := runner.Invoke(ctx, "owned-pool", DefineAuthChallenge, awsEvent("DefineAuthChallenge_Authentication"))
				kind := scenario.kind
				if uncertain && !scenario.timeout && scenario.name != "overflow" {
					kind = HandlerFailure
				}
				if kind == "" {
					require.NoError(t, err)
					require.NotNil(t, result["response"])
				} else {
					var invocation *InvocationError
					require.ErrorAs(t, err, &invocation)
					require.Equal(t, kind, invocation.Kind)
					require.NotContains(t, err.Error(), "private")
					if scenario.timeout {
						require.ErrorIs(t, err, context.DeadlineExceeded)
					}
				}
				requireTriggerActivityReleased(t, observer, ownershipErr)
				for range 2 {
					err := runner.Close(t.Context())
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

func TestDevActivityWaitsForActualCleanupBeforeInvocationAndCloseReturn(t *testing.T) {
	observer := &triggerActivityRecorder{}
	runner, _ := activityFixtureRunner(t, `export async function handler(event){return event;}`, observer)
	entered := make(chan bool, 1)
	gate := make(chan struct{})
	var once sync.Once
	openGate := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(openGate)
	runner.processCleanup = func(command *exec.Cmd) error {
		err := localexec.Cleanup(command)
		entered <- command.ProcessState != nil && command.ProcessState.Exited()
		<-gate
		return err
	}
	invoked := make(chan error, 1)
	go func() {
		_, err := runner.Invoke(context.Background(), "owned-pool", DefineAuthChallenge, awsEvent("DefineAuthChallenge_Authentication"))
		invoked <- err
	}()
	select {
	case reaped := <-entered:
		require.True(t, reaped, "direct child must already be reaped before cleanup")
	case <-time.After(3 * time.Second):
		t.Fatal("trigger did not reach cleanup")
	}
	active, begun, completed, _, _ := observer.snapshot()
	require.Equal(t, 1, active)
	require.Equal(t, 1, begun)
	require.Empty(t, completed)
	select {
	case err := <-invoked:
		t.Fatalf("invoke returned before cleanup: %v", err)
	default:
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, runner.Close(ctx), context.DeadlineExceeded)
	active, _, completed, _, _ = observer.snapshot()
	require.Equal(t, 1, active)
	require.Empty(t, completed)
	openGate()
	var invocation *InvocationError
	require.ErrorAs(t, <-invoked, &invocation)
	require.Equal(t, Timeout, invocation.Kind)
	require.ErrorIs(t, invocation, context.Canceled)
	require.NoError(t, runner.Close(t.Context()))
	requireTriggerActivityReleased(t, observer, nil)
}

func TestDevActivityRefusalPrecedesRegistrationAndNodeLaunch(t *testing.T) {
	for _, missingRelease := range []bool{false, true} {
		t.Run(map[bool]string{false: "error", true: "missing-release"}[missingRelease], func(t *testing.T) {
			observer := &triggerActivityRecorder{reject: !missingRelease, missingRelease: missingRelease}
			runner, _ := activityFixtureRunner(t, `import fs from "node:fs"; fs.writeFileSync("launched", "yes"); export async function handler(event){return event;}`, observer)
			_, err := runner.Invoke(t.Context(), "owned-pool", DefineAuthChallenge, awsEvent("DefineAuthChallenge_Authentication"))
			var invocation *InvocationError
			require.ErrorAs(t, err, &invocation)
			require.Equal(t, Closed, invocation.Kind)
			require.NotContains(t, err.Error(), "private")
			_, err = os.Stat(filepath.Join(runner.workDir, "launched"))
			require.ErrorIs(t, err, os.ErrNotExist)
			runner.mu.Lock()
			active := len(runner.active)
			runner.mu.Unlock()
			require.Zero(t, active)
			require.NoError(t, runner.Close(t.Context()))
		})
	}
}

func TestDevActivityConfigurationIsCopiedBeforeStartup(t *testing.T) {
	original := &triggerActivityRecorder{}
	runner, config := activityFixtureRunner(t, `export async function handler(event){return event;}`, original)
	replacement := &triggerActivityRecorder{reject: true}
	config.DevActivity = replacement
	_, err := runner.Invoke(t.Context(), "owned-pool", DefineAuthChallenge, awsEvent("DefineAuthChallenge_Authentication"))
	require.NoError(t, err)
	requireTriggerActivityReleased(t, original, nil)
	_, begun, completed, _, _ := replacement.snapshot()
	require.Zero(t, begun)
	require.Empty(t, completed)
	require.NoError(t, runner.Close(t.Context()))
}

func TestDevActivityInheritedPipesKeepPrivateUncertaintySticky(t *testing.T) {
	observer := &triggerActivityRecorder{}
	runner, _ := activityFixtureRunner(t, `import {spawn} from "node:child_process";
export async function handler(event){
  const child=spawn(process.execPath,["-e","setInterval(()=>{},1000);setTimeout(()=>process.exit(0),5000)"],{stdio:"inherit"});
  child.unref();
  return event;
}`, observer)
	started := time.Now()
	_, err := runner.Invoke(t.Context(), "owned-pool", DefineAuthChallenge, awsEvent("DefineAuthChallenge_Authentication"))
	var invocation *InvocationError
	require.ErrorAs(t, err, &invocation)
	require.Equal(t, HandlerFailure, invocation.Kind)
	require.ErrorIs(t, err, exec.ErrWaitDelay)
	require.Less(t, time.Since(started), 3*time.Second)
	requireTriggerActivityReleased(t, observer, exec.ErrWaitDelay)
	require.ErrorIs(t, runner.Close(t.Context()), exec.ErrWaitDelay)
	require.ErrorIs(t, runner.Close(t.Context()), exec.ErrWaitDelay)
}
