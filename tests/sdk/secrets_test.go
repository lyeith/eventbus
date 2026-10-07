//go:build sdksmoke

package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/devcapture"
	"github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/secrets"
	"github.com/lyeith/eventbus/internal/server"
	"github.com/stretchr/testify/require"
)

// This adapter is composition only. Production's equivalent belongs to app;
// Secrets owns its port and workflow, Lambda owns every runtime/child.
type sdkRotationInvoker struct{ functions *lambda.Service }

func (invoker sdkRotationInvoker) ValidateRotationTarget(_ context.Context, name string) error {
	return invoker.functions.ValidateTarget(name, "")
}
func (invoker sdkRotationInvoker) InvokeRotation(ctx context.Context, name string, event secrets.RotationEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	result, err := invoker.functions.Execute(ctx, lambda.InvokeInput{FunctionName: name, Payload: payload})
	if err != nil {
		return err
	}
	if result.FunctionError {
		return errors.New("rotation handler failed")
	}
	return nil
}

func TestSecretsPythonSDKAndLambdaRotationSmoke(t *testing.T) {
	python := sdkPython(t)
	directory := t.TempDir()
	serving := httptest.NewUnstartedServer(nil)
	endpoint := "http://" + serving.Listener.Addr().String()
	target, events, failure := filepath.Join(directory, "target.json"), filepath.Join(directory, "events.jsonl"), filepath.Join(directory, "failure")
	outcomePath := filepath.Join(directory, "outcomes.jsonl")
	capture, err := devcapture.Open(outcomePath, "Secrets smoke")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, capture.Close()) })
	arn := "arn:aws:lambda:us-east-1:000000000000:function:rotate-python"
	functions, err := lambda.NewService(&lambda.Config{Functions: map[string]lambda.Function{
		"rotate-python": {Runtime: "python", Command: []string{python, "-E", "-s"}, Handler: fixturePath("python", "rotation_handler.py") + "#handler", Timeout: 5 * time.Second, Environment: map[string]string{
			"AWS_REGION": "us-east-1", "AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test", "AWS_EC2_METADATA_DISABLED": "true", "NO_PROXY": "*",
			"SECRETS_MANAGER_ENDPOINT": endpoint, "ROTATION_TARGET": target, "ROTATION_EVENTS": events, "ROTATION_FAILURE": failure,
		}},
	}}, directory)
	require.NoError(t, err)
	var outcomeMu sync.Mutex
	outcomes := []secrets.RotationOutcome{}
	store := secrets.NewSecretsStore("us-east-1", "000000000000")
	rotation := secrets.NewRotationService(store, sdkRotationInvoker{functions}, secrets.RotationOptions{MaxAttempts: 1, AttemptTimeout: 6 * time.Second, Observer: func(outcome secrets.RotationOutcome) {
		outcomeMu.Lock()
		outcomes = append(outcomes, outcome)
		if err := capture.Append(outcome); err != nil {
			t.Errorf("record rotation outcome: %v", err)
		}
		outcomeMu.Unlock()
	}})
	serving.Config.Handler = server.New(server.Services{Secrets: secrets.NewHandler(store, rotation), Lambda: functions})
	serving.Start()
	t.Cleanup(func() {
		// Accepted rotation callbacks must reach AWS HTTP until their owner joins.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, rotation.Drain(ctx))
		require.NoError(t, functions.DrainAsync(ctx))
		serving.Close()
		require.NoError(t, rotation.Close(ctx))
		require.NoError(t, functions.Close(ctx))
	})
	env := append(sdkEnvironment(directory, "", "", ""), "SECRETS_MANAGER_ENDPOINT="+endpoint, "ROTATION_TARGET="+target, "ROTATION_EVENTS="+events, "ROTATION_FAILURE="+failure, "ROTATION_FUNCTION_ARN="+arn, "ROTATION_OUTCOMES="+outcomePath)
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	output, err := runSDKProcess(ctx, python, fixturePath("python", "smoke_secrets.py"), env)
	t.Logf("real boto3 + registered Python Lambda rotation:\n%s", output)
	require.NoError(t, err)
	closeCtx, closeCancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer closeCancel()
	require.NoError(t, rotation.Close(closeCtx))
	outcomeMu.Lock()
	defer outcomeMu.Unlock()
	require.Len(t, outcomes, 4)
	require.Equal(t, "succeeded", outcomes[0].Status)
	require.Equal(t, "succeeded", outcomes[1].Status)
	require.Equal(t, "testSecret", outcomes[1].Step)
	require.Equal(t, "handler_failure", outcomes[2].Status)
	require.Equal(t, "succeeded", outcomes[3].Status)
	data, err := json.Marshal(outcomes)
	require.NoError(t, err)
	require.NotContains(t, string(data), "private-handler-error")
	info, err := os.Stat(events)
	require.NoError(t, err)
	require.Greater(t, info.Size(), int64(0))
}
