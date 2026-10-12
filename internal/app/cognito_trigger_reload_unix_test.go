//go:build linux || darwin

package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/cognitotrigger"
	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
	"github.com/stretchr/testify/require"
)

func privateReloadSource(version string) string {
	return fmt.Sprintf(`import fs from 'node:fs';
let count = 0;
export async function handler(event, context) {
  count++;
  const response = {version:%q, pid:process.pid, count,
    setting:process.env.SETTING, remaining:context.getRemainingTimeInMillis()};
  if (event.hold) {
    fs.writeFileSync(event.started, String(process.pid));
    while (!fs.existsSync(event.release)) await new Promise(resolve=>setTimeout(resolve,5));
  }
  event.response=response;
  return event;
}`, version)
}

func TestCognitoPrivateReloadSharesAliasesAndPreservesOtherGenerations(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "reload.mjs")
	writeSource := func(version string) {
		t.Helper()
		temporary := sourcePath + ".tmp"
		require.NoError(t, os.WriteFile(temporary, []byte(privateReloadSource(version)), 0600))
		require.NoError(t, os.Rename(temporary, sourcePath))
	}
	writeSource("before")
	config := fixtureConfig("reload.mjs", map[string]string{"SETTING": "original"})
	config.DevWarm = &cognitotrigger.DevWarmConfig{MaxWorkers: 4, MaxConcurrency: 4}
	config.Pools["other-pool"] = fixtureConfig("reload.mjs", map[string]string{"SETTING": "other"}).Pools["owned-pool"]
	execution, err := newCognitoTriggerExecution(config, directory)
	require.NoError(t, err)
	require.Len(t, execution.runtimeTargets(), 2, "aliases share one target per pool")
	runner, err := cognitotrigger.New(config, execution)
	require.NoError(t, err)
	public, err := lambdaservice.NewService(&lambdaservice.Config{
		Functions: map[string]lambdaservice.Function{"public": {Runtime: "node", Handler: "reload.mjs#handler",
			Environment: map[string]string{"SETTING": "public"}, Timeout: 5 * time.Second}},
		DevWarm:  &lambdaservice.DevWarmConfig{MaxWorkers: 1},
		DevAsync: &lambdaservice.DevAsyncConfig{LogWriter: io.Discard},
	}, directory)
	require.NoError(t, err)
	seenPIDs := make(map[int]bool)
	t.Cleanup(func() {
		require.NoError(t, runner.Close(context.Background()))
		require.NoError(t, public.Close(context.Background()))
		for pid := range seenPIDs {
			require.ErrorIs(t, syscall.Kill(pid, 0), syscall.ESRCH, "owned private/public fixture must join")
		}
	})
	responseOf := func(result map[string]any) map[string]any {
		t.Helper()
		response := result["response"].(map[string]any)
		seenPIDs[triggerProbePID(t, response)] = true
		return response
	}
	invoke := func(pool, trigger string) map[string]any {
		t.Helper()
		event := awsEvent(trigger + "_Authentication")
		event["userPoolId"] = pool
		result, err := runner.Invoke(t.Context(), pool, trigger, event)
		require.NoError(t, err)
		return responseOf(result)
	}
	publicResponse := func() map[string]any {
		t.Helper()
		out, err := public.Execute(t.Context(), lambdaservice.InvokeInput{FunctionName: "public", Payload: []byte(`{"response":{}}`)})
		require.NoError(t, err)
		require.False(t, out.FunctionError)
		var result map[string]any
		decoder := json.NewDecoder(bytes.NewReader(out.Payload))
		decoder.UseNumber()
		require.NoError(t, decoder.Decode(&result))
		return responseOf(result)
	}
	type heldResult struct {
		result map[string]any
		err    error
	}
	startHeld := func(index int) (int, string, <-chan heldResult) {
		t.Helper()
		started, release := filepath.Join(directory, fmt.Sprintf("held-%d", index)), filepath.Join(directory, fmt.Sprintf("release-%d", index))
		event := awsEvent("DefineAuthChallenge_Authentication")
		event["hold"], event["started"], event["release"] = true, started, release
		finished := make(chan heldResult, 1)
		go func() {
			result, err := runner.Invoke(context.Background(), "owned-pool", cognitotrigger.DefineAuthChallenge, event)
			finished <- heldResult{result, err}
		}()
		require.Eventually(t, func() bool {
			data, err := os.ReadFile(started)
			if err != nil {
				return false
			}
			_, err = strconv.Atoi(string(data))
			return err == nil
		}, 2*time.Second, 5*time.Millisecond, "actual old handler must hold before reload")
		data, err := os.ReadFile(started)
		require.NoError(t, err)
		pid, err := strconv.Atoi(string(data))
		require.NoError(t, err)
		seenPIDs[pid] = true
		return pid, release, finished
	}
	waitExited := func(pid int) {
		t.Helper()
		require.Eventually(t, func() bool { return errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) }, 2*time.Second, 5*time.Millisecond)
	}
	before := invoke("owned-pool", cognitotrigger.DefineAuthChallenge)
	heldPID, heldGate, oldFinished := startHeld(1)
	require.Equal(t, triggerProbePID(t, before), heldPID, "old active work reuses the original environment")
	idle := invoke("owned-pool", cognitotrigger.CreateAuthChallenge)
	idlePID := triggerProbePID(t, idle)
	require.NotEqual(t, heldPID, idlePID)
	other := invoke("other-pool", cognitotrigger.DefineAuthChallenge)
	otherPID := triggerProbePID(t, other)
	publicBefore := publicResponse()
	publicPID := triggerProbePID(t, publicBefore)

	// Mutating the original recipe cannot change the execution snapshot. Reload
	// only refreshes the source belonging to the existing immutable target.
	for _, entry := range []*cognitotrigger.Entry{
		config.Pools["owned-pool"].DefineAuthChallenge,
		config.Pools["owned-pool"].CreateAuthChallenge,
		config.Pools["owned-pool"].VerifyAuthChallengeResponse,
	} {
		entry.Env["SETTING"] = "mutated-recipe"
		entry.Handler = "missing-private-recipe.mjs#handler"
		entry.TimeoutSeconds = 1
	}
	writeSource("after")
	reloaded := make(chan error, 1)
	go func() {
		reloaded <- runner.DevReloadTrigger(context.Background(), "owned-pool", cognitotrigger.CreateAuthChallenge)
	}()
	// Reload publishes before retiring old idle workers. Their actual exit is
	// the publication observation, rather than an arbitrary sleep or a runtime
	// generation test hook. The other old worker remains naturally active.
	waitExited(idlePID)
	require.NoError(t, syscall.Kill(heldPID, 0))
	select {
	case err := <-reloaded:
		t.Fatalf("reload returned before active old generation joined: %v", err)
	default:
	}
	var replacementPID int
	for index, trigger := range []string{cognitotrigger.DefineAuthChallenge, cognitotrigger.CreateAuthChallenge, cognitotrigger.VerifyAuthChallengeResponse} {
		response := invoke("owned-pool", trigger)
		require.Equal(t, "after", response["version"])
		require.Equal(t, "original", response["setting"])
		require.Equal(t, json.Number(strconv.Itoa(index+1)), response["count"])
		remaining, err := response["remaining"].(json.Number).Int64()
		require.NoError(t, err)
		require.Greater(t, remaining, int64(1000), "recipe timeout mutation must not apply")
		pid := triggerProbePID(t, response)
		require.NotEqual(t, heldPID, pid)
		require.NotEqual(t, idlePID, pid)
		if index == 0 {
			replacementPID = pid
		} else {
			require.Equal(t, replacementPID, pid, "all coalesced aliases must use one replacement")
		}
	}
	otherAfter := invoke("other-pool", cognitotrigger.CreateAuthChallenge)
	require.Equal(t, otherPID, triggerProbePID(t, otherAfter))
	require.Equal(t, "before", otherAfter["version"])
	require.Equal(t, "other", otherAfter["setting"])
	require.Equal(t, json.Number("2"), otherAfter["count"])
	publicAfter := publicResponse()
	require.Equal(t, publicPID, triggerProbePID(t, publicAfter))
	require.Equal(t, "before", publicAfter["version"])
	require.Equal(t, "public", publicAfter["setting"])
	require.Equal(t, json.Number("2"), publicAfter["count"])
	require.NoError(t, os.WriteFile(heldGate, nil, 0600))
	select {
	case old := <-oldFinished:
		require.NoError(t, old.err, "reload must not cancel active trigger work")
		oldResponse := responseOf(old.result)
		require.Equal(t, heldPID, triggerProbePID(t, oldResponse))
		require.Equal(t, "before", oldResponse["version"])
		require.Equal(t, json.Number("2"), oldResponse["count"])
	case <-time.After(2 * time.Second):
		t.Fatal("active old trigger did not finish naturally")
	}
	select {
	case err := <-reloaded:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("reload did not join the naturally completed old generation")
	}
	require.ErrorIs(t, syscall.Kill(heldPID, 0), syscall.ESRCH)
	require.NoError(t, runner.DevEvidence())

	// An idle-only reload joins retirement before returning.
	require.NoError(t, runner.DevReloadTrigger(t.Context(), "owned-pool", cognitotrigger.VerifyAuthChallengeResponse))
	require.ErrorIs(t, syscall.Kill(replacementPID, 0), syscall.ESRCH)
	afterIdle := invoke("owned-pool", cognitotrigger.CreateAuthChallenge)
	require.Equal(t, "after", afterIdle["version"])
	require.Equal(t, json.Number("1"), afterIdle["count"])
	timedHeldPID, timedGate, timedFinished := startHeld(2)
	require.Equal(t, triggerProbePID(t, afterIdle), timedHeldPID)
	timedIdlePID := triggerProbePID(t, invoke("owned-pool", cognitotrigger.VerifyAuthChallengeResponse))
	writeSource("latest")
	wait, cancelWait := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelWait()
	request := httptest.NewRequest(http.MethodPost, "/__eventbus/dev/cognito/pools/owned-pool/triggers/DefineAuthChallenge/reload", nil).WithContext(wait)
	interrupted := httptest.NewRecorder()
	cognitotrigger.NewDevReloadHandler(runner).ServeHTTP(interrupted, request)
	require.Equal(t, 504, interrupted.Code)
	require.JSONEq(t, `{"error":"reload_wait_interrupted"}`, interrupted.Body.String())
	require.NotContains(t, interrupted.Body.String(), directory)
	require.NotContains(t, interrupted.Body.String(), "cognito_trigger_")
	waitExited(timedIdlePID)
	require.NoError(t, syscall.Kill(timedHeldPID, 0), "caller timeout must not cancel the old active worker")
	latest := invoke("owned-pool", cognitotrigger.CreateAuthChallenge)
	require.Equal(t, "latest", latest["version"], "wait interruption cannot roll back published source")
	require.Equal(t, "original", latest["setting"])
	require.Equal(t, json.Number("1"), latest["count"])
	require.NoError(t, os.WriteFile(timedGate, nil, 0600))
	select {
	case old := <-timedFinished:
		require.NoError(t, old.err)
		oldResponse := responseOf(old.result)
		require.Equal(t, "after", oldResponse["version"])
		require.Equal(t, timedHeldPID, triggerProbePID(t, oldResponse))
	case <-time.After(2 * time.Second):
		t.Fatal("interrupted reload left old active trigger work blocked")
	}
	require.ErrorIs(t, syscall.Kill(timedHeldPID, 0), syscall.ESRCH)
	require.NoError(t, runner.DevEvidence())

	// Source validation failure must retain the previous usable generation,
	// while the returned error contains no handler path or configuration.
	require.NoError(t, os.Remove(sourcePath))
	failed := httptest.NewRecorder()
	cognitotrigger.NewDevReloadHandler(runner).ServeHTTP(failed,
		httptest.NewRequest(http.MethodPost, "/__eventbus/dev/cognito/pools/owned-pool/triggers/VerifyAuthChallengeResponse/reload", nil))
	require.Equal(t, 503, failed.Code)
	require.JSONEq(t, `{"error":"reload_failed"}`, failed.Body.String())
	require.NotContains(t, failed.Body.String(), directory)
	require.NotContains(t, failed.Body.String(), "reload.mjs")
	stillUsable := invoke("owned-pool", cognitotrigger.VerifyAuthChallengeResponse)
	require.Equal(t, triggerProbePID(t, latest), triggerProbePID(t, stillUsable))
	require.Equal(t, "latest", stillUsable["version"])
	require.Equal(t, otherPID, triggerProbePID(t, invoke("other-pool", cognitotrigger.VerifyAuthChallengeResponse)))
	require.Equal(t, publicPID, triggerProbePID(t, publicResponse()))
}
