package cognitotrigger

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func fixtureConfig(handler string, env map[string]string) *Config {
	return &Config{Pools: map[string]Pool{"owned-pool": {
		DefineAuthChallenge:         &Entry{Handler: handler, Env: env},
		CreateAuthChallenge:         &Entry{Handler: handler, Env: env},
		VerifyAuthChallengeResponse: &Entry{Handler: handler, Env: env},
	}}}
}

func fixtureRunner(t *testing.T, filename, source string, env map[string]string) (*Runner, string) {
	t.Helper()
	_, err := exec.LookPath("node")
	require.NoError(t, err, "Node is required for custom-trigger process contracts")
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, filename), []byte(source), 0600))
	runner, err := New(fixtureConfig(filename, env), dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runner.Close(context.Background())) })
	return runner, dir
}

func awsEvent(source string) map[string]any {
	return map[string]any{
		"version": "1", "triggerSource": source, "region": "us-east-1",
		"userPoolId": "owned-pool", "userName": "stable-username",
		"callerContext": map[string]any{"awsSdkVersion": "aws-sdk-js-3", "clientId": "owned-client"},
		"request": map[string]any{
			"userAttributes": map[string]any{"sub": "stable-sub", "email": "person@example.test"},
			"session": []any{
				map[string]any{"challengeName": "SRP_A", "challengeResult": true},
				map[string]any{"challengeName": "PASSWORD_VERIFIER", "challengeResult": true},
			},
			"clientMetadata": map[string]any{"run": "owned-run"}, "userNotFound": false,
		},
		"response": map[string]any{},
	}
}

func TestInvokeRealNodeESMAndCJSHandlers(t *testing.T) {
	dir := t.TempDir()
	_, err := exec.LookPath("node")
	require.NoError(t, err)
	files := map[string]string{
		"define.mjs": `import assert from 'node:assert/strict';
export async function decide(event, context) {
  assert.equal(event.triggerSource, 'DefineAuthChallenge_Authentication');
  assert.equal(event.userPoolId, 'owned-pool');
  assert.equal(event.userName, 'stable-username');
  assert.equal(event.callerContext.clientId, 'owned-client');
  assert.equal(event.request.userAttributes.sub, 'stable-sub');
  assert.deepEqual(event.request.session.map(x => x.challengeName), ['SRP_A','PASSWORD_VERIFIER']);
  assert.equal(event.request.session[1].challengeResult, true);
  assert.equal(event.request.clientMetadata.run, 'owned-run');
  assert.equal(event.request.userNotFound, false);
  assert.ok(context.awsRequestId);
  assert.ok(context.getRemainingTimeInMillis() > 0);
  console.log('define diagnostics');
  event.response = {challengeName:'CUSTOM_CHALLENGE',issueTokens:false,failAuthentication:false};
  return event;
}`,
		"create.cjs": `const assert = require('node:assert/strict');
exports.handler = async (event) => {
  assert.equal(event.request.challengeName, 'CUSTOM_CHALLENGE');
  assert.equal(process.env.APP_SETTING, 'declared');
  process.stdout.write('create diagnostic line\\n');
  event.response = {
    publicChallengeParameters:{prompt:'Repeat the fixture answer'},
    privateChallengeParameters:{answer:'fixture-answer'},
    challengeMetadata:'generic-fixture'
  };
  return event;
};`,
		"verify.mjs": `export const handler = async event => {
  event.response.answerCorrect = event.request.challengeAnswer === event.request.privateChallengeParameters.answer;
  return event;
};`,
	}
	for name, source := range files {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(source), 0600))
	}
	config := &Config{Pools: map[string]Pool{"owned-pool": {
		DefineAuthChallenge:         &Entry{Handler: "define.mjs#decide"},
		CreateAuthChallenge:         &Entry{Handler: "create.cjs", Env: map[string]string{"APP_SETTING": "declared"}},
		VerifyAuthChallengeResponse: &Entry{Handler: "verify.mjs"},
	}}}
	runner, err := New(config, dir)
	require.NoError(t, err)
	defer func() { require.NoError(t, runner.Close(t.Context())) }()
	require.True(t, runner.Supports("owned-pool"))
	require.False(t, runner.Supports("unconfigured"))
	result, err := runner.Invoke(t.Context(), "owned-pool", DefineAuthChallenge, awsEvent("DefineAuthChallenge_Authentication"))
	require.NoError(t, err)
	require.Equal(t, "CUSTOM_CHALLENGE", result["response"].(map[string]any)["challengeName"])
	create := awsEvent("CreateAuthChallenge_Authentication")
	create["request"].(map[string]any)["challengeName"] = "CUSTOM_CHALLENGE"
	result, err = runner.Invoke(t.Context(), "owned-pool", CreateAuthChallenge, create)
	require.NoError(t, err)
	response := result["response"].(map[string]any)
	require.Equal(t, "fixture-answer", response["privateChallengeParameters"].(map[string]any)["answer"])
	require.NotContains(t, response["publicChallengeParameters"], "answer")
	verify := awsEvent("VerifyAuthChallengeResponse_Authentication")
	verify["request"].(map[string]any)["privateChallengeParameters"] = response["privateChallengeParameters"]
	verify["request"].(map[string]any)["challengeAnswer"] = "fixture-answer"
	result, err = runner.Invoke(t.Context(), "owned-pool", VerifyAuthChallengeResponse, verify)
	require.NoError(t, err)
	require.Equal(t, true, result["response"].(map[string]any)["answerCorrect"])
	verify["request"].(map[string]any)["challengeAnswer"] = "wrong"
	result, err = runner.Invoke(t.Context(), "owned-pool", VerifyAuthChallengeResponse, verify)
	require.NoError(t, err)
	require.Equal(t, false, result["response"].(map[string]any)["answerCorrect"])
}

func TestInvokeFailsClosedForHandlerAndProtocolErrors(t *testing.T) {
	for _, scenario := range []struct {
		name, source string
		kind         ErrorKind
	}{
		{"throw", `export async function handler(){ throw new Error('PRIVATE_VALUE_DO_NOT_REFLECT'); }`, HandlerFailure},
		{"missing-export", `export async function other(event){return event;}`, HandlerFailure},
		{"undefined", `export async function handler(){}`, InvalidResponse},
		{"null", `export async function handler(){return null;}`, InvalidResponse},
		{"response-scalar", `export async function handler(){return {response:'yes'};}`, InvalidResponse},
		{"circular", `export async function handler(event){event.response.loop=event;return event;}`, InvalidResponse},
		{"extra-results", `import fs from 'node:fs'; export async function handler(event){fs.writeSync(1,'{}\\n');return event;}`, InvalidResponse},
		{"large-result", `export async function handler(event){event.response.large='x'.repeat(2<<20);return event;}`, InvalidResponse},
		{"large-diagnostics", `export async function handler(event){process.stderr.write('x'.repeat(128<<10));return event;}`, InvalidResponse},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			runner, _ := fixtureRunner(t, "handler.mjs", scenario.source, nil)
			_, err := runner.Invoke(t.Context(), "owned-pool", DefineAuthChallenge, awsEvent("DefineAuthChallenge_Authentication"))
			var invocation *InvocationError
			require.ErrorAs(t, err, &invocation)
			require.Equal(t, scenario.kind, invocation.Kind)
			require.NotContains(t, err.Error(), "PRIVATE_VALUE")
		})
	}
}

func TestInvokeTimeoutAndCloseOwnInflightExecution(t *testing.T) {
	runner, _ := fixtureRunner(t, "handler.mjs", `export async function handler(){await new Promise(()=>{});}`, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := runner.Invoke(ctx, "owned-pool", DefineAuthChallenge, awsEvent("DefineAuthChallenge_Authentication"))
	var invocation *InvocationError
	require.ErrorAs(t, err, &invocation)
	require.Equal(t, Timeout, invocation.Kind)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(started), 2*time.Second)
	inflight := make(chan error, 1)
	go func() {
		_, err := runner.Invoke(context.Background(), "owned-pool", DefineAuthChallenge, awsEvent("DefineAuthChallenge_Authentication"))
		inflight <- err
	}()
	require.Eventually(t, func() bool { runner.mu.Lock(); defer runner.mu.Unlock(); return len(runner.active) == 1 }, time.Second, time.Millisecond)
	require.NoError(t, runner.Close(t.Context()))
	require.Error(t, <-inflight)
	_, err = runner.Invoke(t.Context(), "owned-pool", DefineAuthChallenge, awsEvent("DefineAuthChallenge_Authentication"))
	require.ErrorAs(t, err, &invocation)
	require.Equal(t, Closed, invocation.Kind)
	require.NoError(t, runner.Close(t.Context()))
}

func TestInvokeConfigurationIsImmutableAndMissingPoolCannotFallback(t *testing.T) {
	runner, dir := fixtureRunner(t, "handler.mjs", `export async function handler(event){event.response.setting=process.env.APP_SETTING;return event;}`, map[string]string{"APP_SETTING": "original"})
	config := fixtureConfig("handler.mjs", map[string]string{"APP_SETTING": "original"})
	second, err := New(config, dir)
	require.NoError(t, err)
	defer second.Close(t.Context())
	config.Pools["owned-pool"].DefineAuthChallenge.Env["APP_SETTING"] = "changed"
	result, err := second.Invoke(t.Context(), "owned-pool", DefineAuthChallenge, awsEvent("DefineAuthChallenge_Authentication"))
	require.NoError(t, err)
	require.Equal(t, "original", result["response"].(map[string]any)["setting"])
	_, err = runner.Invoke(t.Context(), "missing", DefineAuthChallenge, awsEvent("DefineAuthChallenge_Authentication"))
	var invocation *InvocationError
	require.ErrorAs(t, err, &invocation)
	require.Equal(t, NotConfigured, invocation.Kind)
	_, err = runner.Invoke(t.Context(), "owned-pool", "UnknownTrigger", awsEvent("DefineAuthChallenge_Authentication"))
	require.ErrorAs(t, err, &invocation)
	require.Equal(t, NotConfigured, invocation.Kind)
}

func TestInvokeDoesNotInheritAmbientCredentialsOrNodeOptions(t *testing.T) {
	t.Setenv("AWS_PROFILE", "ambient-profile")
	t.Setenv("AWS_ACCESS_KEY_ID", "ambient-key")
	t.Setenv("HTTPS_PROXY", "ambient-proxy")
	t.Setenv("NODE_OPTIONS", "--require absent-ambient-loader")
	runner, _ := fixtureRunner(t, "handler.mjs", `export async function handler(event){event.response.environment={
awsProfile:process.env.AWS_PROFILE??null,proxy:process.env.HTTPS_PROXY??null,nodeOptions:process.env.NODE_OPTIONS??null,
key:process.env.AWS_ACCESS_KEY_ID,setting:process.env.APP_SETTING};return event;}`, map[string]string{"AWS_ACCESS_KEY_ID": "declared-key", "APP_SETTING": "declared-setting"})
	result, err := runner.Invoke(t.Context(), "owned-pool", DefineAuthChallenge, awsEvent("DefineAuthChallenge_Authentication"))
	require.NoError(t, err)
	environment := result["response"].(map[string]any)["environment"].(map[string]any)
	require.Nil(t, environment["awsProfile"])
	require.Nil(t, environment["proxy"])
	require.Nil(t, environment["nodeOptions"])
	require.Equal(t, "declared-key", environment["key"])
	require.Equal(t, "declared-setting", environment["setting"])
}

func TestBoundedOutputConsumesBytesWithoutAcceptingTruncation(t *testing.T) {
	copyOutput := &boundedOutput{limit: 3}
	nCopied, errCopied := io.Copy(copyOutput, io.LimitReader(strings.NewReader("12345"), 5))
	require.NoError(t, errCopied)
	require.EqualValues(t, 5, nCopied)
	require.Equal(t, "123", copyOutput.String())
	require.True(t, copyOutput.overflow)
	output := &boundedOutput{limit: 3}
	n, err := output.Write([]byte("12345"))
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, "123", output.String())
	require.True(t, output.overflow)
	n, err = output.Write([]byte("more"))
	require.NoError(t, err)
	require.Equal(t, 4, n)
	require.Equal(t, "123", output.String())
}

func TestCloseCanceledCallerCanRejoinCleanup(t *testing.T) {
	runner, _ := fixtureRunner(t, "handler.mjs", `export async function handler(event){return event;}`, nil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := runner.Close(ctx)
	require.True(t, err == nil || errors.Is(err, context.Canceled))
	require.NoError(t, runner.Close(t.Context()))
}

func TestInvokeResultPreservesJSONNumbers(t *testing.T) {
	runner, _ := fixtureRunner(t, "handler.mjs", `export async function handler(event){event.response.count=5;return event;}`, nil)
	result, err := runner.Invoke(t.Context(), "owned-pool", DefineAuthChallenge, awsEvent("DefineAuthChallenge_Authentication"))
	require.NoError(t, err)
	require.Equal(t, json.Number("5"), result["response"].(map[string]any)["count"])
}
