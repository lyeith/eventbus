//go:build linux || darwin

package app

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/cognitotrigger"
	"github.com/lyeith/eventbus/internal/devquiescence"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

const cognitoRuntimeProbe = `import fs from 'node:fs';
let count=0;
export async function handler(event,context){
  fs.writeFileSync(process.env.PID_FILE,String(process.pid));
  count++;
  if(event.test==='throw'){throw new Error('PRIVATE_CHALLENGE_THROW');}
  if(event.test==='cycle'){event.response.loop=event;return event;}
  if(event.test==='invalid'){return {};}
  if(event.test==='large'){event.response.data='x'.repeat(2<<20);return event;}
  if(event.test==='log-overflow'){process.stderr.write('x'.repeat(128<<10));}
  if(event.test==='pending'){await new Promise(()=>{});}
  if(event.test==='diagnostics'){fs.writeSync(1,'PRIVATE_CHALLENGE_STDOUT');process.stderr.write('PRIVATE_CHALLENGE_STDERR');}
  event.response={pid:process.pid,count,setting:process.env.SETTING,
    requestId:context.awsRequestId,functionName:context.functionName,
    waits:context.callbackWaitsForEmptyEventLoop,remaining:context.getRemainingTimeInMillis()};
  return event;
}`

func triggerProbePID(t *testing.T, response map[string]any) int {
	t.Helper()
	pid, err := strconv.Atoi(response["pid"].(json.Number).String())
	require.NoError(t, err)
	return pid
}
func triggerProbe(t *testing.T, warm bool) (*cognitotrigger.Config, string) {
	t.Helper()
	directory := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(directory, "probe.mjs"), []byte(cognitoRuntimeProbe), 0600))
	config := fixtureConfig("probe.mjs", map[string]string{"PID_FILE": filepath.Join(directory, "pid"), "SETTING": "first"})
	if warm {
		config.DevWarm = &cognitotrigger.DevWarmConfig{MaxWorkers: 3}
	}
	return config, directory
}
func TestCognitoManagedTriggersFreshDefaultAndWarmScope(t *testing.T) {
	for _, warm := range []bool{false, true} {
		t.Run(map[bool]string{false: "fresh", true: "warm"}[warm], func(t *testing.T) {
			config, directory := triggerProbe(t, warm)
			execution, err := newCognitoTriggerExecution(config, directory)
			require.NoError(t, err)
			require.Len(t, execution.runtimeTargets(), 1, "identical entries within one pool are one local function")
			runner, err := cognitotrigger.New(config, execution)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, runner.Close(context.Background())) })
			seenRequests := map[string]bool{}
			seenPIDs := map[int]bool{}
			for index, trigger := range []string{cognitotrigger.DefineAuthChallenge, cognitotrigger.CreateAuthChallenge, cognitotrigger.VerifyAuthChallengeResponse} {
				result, err := runner.Invoke(t.Context(), "owned-pool", trigger, awsEvent(trigger+"_Authentication"))
				require.NoError(t, err)
				response := result["response"].(map[string]any)
				pid := triggerProbePID(t, response)
				seenPIDs[pid] = true
				request := response["requestId"].(string)
				require.NotEmpty(t, request)
				require.False(t, seenRequests[request])
				seenRequests[request] = true
				require.Equal(t, "handler", response["functionName"])
				require.Equal(t, false, response["waits"])
				remaining, err := response["remaining"].(json.Number).Int64()
				require.NoError(t, err)
				require.Positive(t, remaining)
				require.LessOrEqual(t, remaining, int64(5000))
				if warm {
					require.Equal(t, json.Number(strconv.Itoa(index+1)), response["count"])
					require.NoError(t, syscall.Kill(pid, 0))
				}
				if !warm {
					require.Equal(t, json.Number("1"), response["count"])
					require.ErrorIs(t, syscall.Kill(pid, 0), syscall.ESRCH)
				}
			}
			if warm {
				require.Len(t, seenPIDs, 1)
			} else {
				require.Len(t, seenPIDs, 3)
			}
			require.NoError(t, runner.Close(t.Context()))
			for pid := range seenPIDs {
				require.ErrorIs(t, syscall.Kill(pid, 0), syscall.ESRCH)
			}
		})
	}
}

// Test-only view over immutable app-owned references, without runtime internals.
func (execution *cognitoTriggerExecution) runtimeTargets() map[string]bool {
	targets := map[string]bool{}
	for _, entries := range execution.targets {
		for _, target := range entries {
			targets[target] = true
		}
	}
	return targets
}
func TestCognitoManagedTriggersIsolatePoolAndEnvironment(t *testing.T) {
	config, directory := triggerProbe(t, true)
	config.Pools["owned-pool"].CreateAuthChallenge.Env = map[string]string{"PID_FILE": filepath.Join(directory, "pid"), "SETTING": "second"}
	other := fixtureConfig("probe.mjs", map[string]string{"PID_FILE": filepath.Join(directory, "pid"), "SETTING": "first"})
	config.Pools["other-pool"] = other.Pools["owned-pool"]
	execution, err := newCognitoTriggerExecution(config, directory)
	require.NoError(t, err)
	require.Len(t, execution.runtimeTargets(), 3)
	runner, err := cognitotrigger.New(config, execution)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runner.Close(context.Background())) })
	pids := map[int]bool{}
	for _, test := range []struct{ pool, trigger, setting string }{{"owned-pool", cognitotrigger.DefineAuthChallenge, "first"}, {"owned-pool", cognitotrigger.CreateAuthChallenge, "second"}, {"other-pool", cognitotrigger.DefineAuthChallenge, "first"}} {
		for index := 1; index <= 2; index++ {
			result, err := runner.Invoke(t.Context(), test.pool, test.trigger, awsEvent(test.trigger+"_Authentication"))
			require.NoError(t, err)
			response := result["response"].(map[string]any)
			require.Equal(t, test.setting, response["setting"])
			require.Equal(t, json.Number(strconv.Itoa(index)), response["count"])
			pids[triggerProbePID(t, response)] = true
		}
	}
	require.Len(t, pids, 3)
	require.NoError(t, runner.Close(t.Context()))
	for pid := range pids {
		require.ErrorIs(t, syscall.Kill(pid, 0), syscall.ESRCH)
	}
}
func TestCognitoWarmInvalidOutputsRetireBeforeReturn(t *testing.T) {
	for _, test := range []struct {
		name string
		kind cognitotrigger.ErrorKind
	}{{"throw", cognitotrigger.HandlerFailure}, {"cycle", cognitotrigger.InvalidResponse}, {"invalid", cognitotrigger.InvalidResponse}, {"large", cognitotrigger.InvalidResponse}, {"log-overflow", cognitotrigger.InvalidResponse}, {"pending", cognitotrigger.Timeout}} {
		t.Run(test.name, func(t *testing.T) {
			config, directory := triggerProbe(t, true)
			runner, err := NewCognitoTriggers(config, directory)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, runner.Close(context.Background())) })
			// Initialize successfully outside the failure budget. This tests a real
			// retained worker, rather than timing out before Node can enter a handler.
			warmup, err := runner.Invoke(t.Context(), "owned-pool", cognitotrigger.DefineAuthChallenge, awsEvent("DefineAuthChallenge_Authentication"))
			require.NoError(t, err)
			warmPID := triggerProbePID(t, warmup["response"].(map[string]any))
			require.NoError(t, os.Remove(filepath.Join(directory, "pid")))
			ctx := t.Context()
			if test.name == "pending" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 2*time.Second)
				defer cancel()
			}
			event := awsEvent("DefineAuthChallenge_Authentication")
			event["test"] = test.name
			_, err = runner.Invoke(ctx, "owned-pool", cognitotrigger.DefineAuthChallenge, event)
			var failure *cognitotrigger.InvocationError
			require.ErrorAs(t, err, &failure)
			require.Equal(t, test.kind, failure.Kind)
			require.NotContains(t, err.Error(), "PRIVATE")
			data, readErr := os.ReadFile(filepath.Join(directory, "pid"))
			require.NoError(t, readErr)
			pid, readErr := strconv.Atoi(string(data))
			require.NoError(t, readErr)
			require.Equal(t, warmPID, pid, "failure executes in the previously retained worker")
			if test.name == "pending" {
				require.ErrorIs(t, err, context.DeadlineExceeded)
			}
			require.ErrorIs(t, syscall.Kill(pid, 0), syscall.ESRCH, "failed private worker joins before returning")
			result, err := runner.Invoke(t.Context(), "owned-pool", cognitotrigger.DefineAuthChallenge, awsEvent("DefineAuthChallenge_Authentication"))
			require.NoError(t, err)
			response := result["response"].(map[string]any)
			require.Equal(t, json.Number("1"), response["count"])
			require.NotEqual(t, pid, triggerProbePID(t, response))
		})
	}
}
func TestCognitoPrivateDiagnosticsStayPrivateInFreshAndWarm(t *testing.T) {
	var output bytes.Buffer
	prior := log.Logger
	log.Logger = zerolog.New(&output).Level(zerolog.DebugLevel)
	t.Cleanup(func() { log.Logger = prior })
	for _, warm := range []bool{false, true} {
		config, directory := triggerProbe(t, warm)
		runner, err := NewCognitoTriggers(config, directory)
		require.NoError(t, err)
		event := awsEvent("DefineAuthChallenge_Authentication")
		event["test"] = "diagnostics"
		_, err = runner.Invoke(t.Context(), "owned-pool", cognitotrigger.DefineAuthChallenge, event)
		require.NoError(t, err)
		require.NoError(t, runner.Close(t.Context()))
	}
	require.NotContains(t, output.String(), "PRIVATE_CHALLENGE")
}
func TestCognitoWarmRetainedDrainAndResumeJoinPrivateWorkers(t *testing.T) {
	config, directory := triggerProbe(t, true)
	var runner *cognitotrigger.Runner
	owner := devquiescence.NewWithOptions(devquiescence.Options{Checks: []func() error{func() error { return runner.DevEvidence() }},
		DrainHooks: []devquiescence.DrainHook{{Start: func() error { return runner.DevBeginWarmDrain() }, Resume: func() error { return runner.DevResumeWarm() }}}})
	config.DevActivity = owner
	var err error
	runner, err = NewCognitoTriggers(config, directory)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runner.Close(context.Background())) })
	result, err := runner.Invoke(t.Context(), "owned-pool", cognitotrigger.DefineAuthChallenge, awsEvent("DefineAuthChallenge_Authentication"))
	require.NoError(t, err)
	firstPID := triggerProbePID(t, result["response"].(map[string]any))
	require.NoError(t, syscall.Kill(firstPID, 0))
	snapshot, err := owner.Quiesce(t.Context())
	require.NoError(t, err)
	require.True(t, snapshot.FixtureSafe)
	require.Zero(t, snapshot.WorkCount)
	require.ErrorIs(t, syscall.Kill(firstPID, 0), syscall.ESRCH)
	_, err = owner.Resume(snapshot.Generation)
	require.NoError(t, err)
	result, err = runner.Invoke(t.Context(), "owned-pool", cognitotrigger.DefineAuthChallenge, awsEvent("DefineAuthChallenge_Authentication"))
	require.NoError(t, err)
	response := result["response"].(map[string]any)
	require.Equal(t, json.Number("1"), response["count"])
	require.NotEqual(t, firstPID, triggerProbePID(t, response))
	require.NoError(t, runner.Close(t.Context()))
}
func TestCognitoManagedStartupRejectsMissingRuntimeAndModule(t *testing.T) {
	directory := t.TempDir()
	config := fixtureConfig("missing.mjs", nil)
	_, err := NewCognitoTriggers(config, directory)
	require.ErrorContains(t, err, "handler")
	config.Node = filepath.Join(directory, "absent-node")
	_, err = NewCognitoTriggers(config, directory)
	require.ErrorContains(t, err, "executable")
}

func TestCognitoPrivateUndefinedReturnContract(t *testing.T) {
	for _, warm := range []bool{false, true} {
		for _, async := range []bool{false, true} {
			name := map[bool]string{false: "fresh", true: "warm"}[warm] + "/" + map[bool]string{false: "sync", true: "async"}[async]
			t.Run(name, func(t *testing.T) {
				directory := t.TempDir()
				prefix := ""
				if async {
					prefix = "async "
				}
				source := "import assert from 'node:assert/strict';export " + prefix + "function handler(event,context,callback){assert.equal(callback,undefined);assert.equal(context.done,undefined);}"
				require.NoError(t, os.WriteFile(filepath.Join(directory, "undefined.mjs"), []byte(source), 0600))
				config := fixtureConfig("undefined.mjs", nil)
				if warm {
					config.DevWarm = &cognitotrigger.DevWarmConfig{MaxWorkers: 1}
				}
				runner, err := NewCognitoTriggers(config, directory)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, runner.Close(context.Background())) })
				_, err = runner.Invoke(t.Context(), "owned-pool", cognitotrigger.DefineAuthChallenge, awsEvent("DefineAuthChallenge_Authentication"))
				var failure *cognitotrigger.InvocationError
				require.ErrorAs(t, err, &failure)
				require.Equal(t, cognitotrigger.InvalidResponse, failure.Kind, "direct undefined return is a response error, never a callback timeout")
			})
		}
	}
}
