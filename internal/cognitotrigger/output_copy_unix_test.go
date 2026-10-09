//go:build linux || darwin

package cognitotrigger

import (
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTriggerExitErrorCannotHideOutputOwnershipUncertainty(t *testing.T) {
	observer := &triggerActivityRecorder{}
	runner, _ := activityFixtureRunner(t, `import {spawn} from 'node:child_process';
export async function handler(event){
  const child=spawn(process.execPath,['-e','setInterval(()=>{},1000);setTimeout(()=>process.exit(0),5000)'],{stdio:'inherit'});
  child.unref();
  process.exit(7);
}`, observer)
	_, err := runner.Invoke(t.Context(), "owned-pool", DefineAuthChallenge, awsEvent("DefineAuthChallenge_Authentication"))
	var failure *InvocationError
	require.ErrorAs(t, err, &failure)
	require.Equal(t, HandlerFailure, failure.Kind, "private pipe evidence must not replace native trigger failure policy")
	var exited *exec.ExitError
	require.ErrorAs(t, failure, &exited)
	require.Equal(t, 7, exited.ExitCode())
	require.NotErrorIs(t, failure, exec.ErrWaitDelay, "Go's existing exit precedence remains the caller's error")
	requireTriggerActivityReleased(t, observer, exec.ErrWaitDelay)
	require.ErrorIs(t, runner.Close(t.Context()), exec.ErrWaitDelay)
	require.ErrorIs(t, runner.Close(t.Context()), exec.ErrWaitDelay)
}
