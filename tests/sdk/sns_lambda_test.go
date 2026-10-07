//go:build sdksmoke

package sdk

import (
	"context"
	"net"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/messaging"
	"github.com/lyeith/eventbus/internal/server"
	"github.com/stretchr/testify/require"
)

// Test composition connects the same producer/runtime seams as app; it owns no
// alternate execution, filter, queue or retry policy.
type sdkSNSLambdaDelivery struct{ functions *lambda.Service }

func (delivery sdkSNSLambdaDelivery) ValidateLambdaTarget(ctx context.Context, arn string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return delivery.functions.ValidateTarget(arn, "")
}
func (delivery sdkSNSLambdaDelivery) AdmitSNSLambda(ctx context.Context, arn string, payload []byte) (string, error) {
	admission, err := delivery.functions.Admit(ctx, lambda.InvokeInput{FunctionName: arn, Payload: payload})
	return admission.RequestID, err
}

func TestSNSLambdaPythonSDKSmoke(t *testing.T) {
	python := sdkPython(t)
	env := sdkEnvironment(t.TempDir(), "", "", "")
	for _, owner := range []string{"A", "B"} {
		directory := t.TempDir()
		output, gate := filepath.Join(directory, "effects.jsonl"), filepath.Join(directory, "gate")
		asyncPath, snsPath := filepath.Join(directory, "lambda.jsonl"), filepath.Join(directory, "sns.jsonl")
		function := lambda.Function{Runtime: "python", Command: []string{python, "-E", "-s"}, Handler: fixturePath("python", "sns_lambda_smoke.py") + "#handler", Timeout: 5 * time.Second,
			Environment: map[string]string{"SNS_LAMBDA_OUTPUT": output, "SNS_LAMBDA_GATE": gate, "SNS_LAMBDA_OWNER": owner, "SNS_LAMBDA_VARIANT": "base"}}
		alias := function
		alias.Environment = map[string]string{"SNS_LAMBDA_OUTPUT": output, "SNS_LAMBDA_GATE": gate, "SNS_LAMBDA_OWNER": owner, "SNS_LAMBDA_VARIANT": "alias"}
		short := alias
		short.Timeout = time.Second
		functions, err := lambda.NewService(&lambda.Config{Functions: map[string]lambda.Function{"sns-handler": function, "sns-handler:live": alias, "sns-handler:short": short},
			DevAsync: &lambda.DevAsyncConfig{Workers: 1, Capacity: 1, RetryDelays: []time.Duration{0, 0}, LogPath: asyncPath}}, directory)
		require.NoError(t, err)
		capture, err := messaging.OpenSNSCapture(snsPath)
		require.NoError(t, err)
		serving := httptest.NewUnstartedServer(nil)
		broker := messaging.NewBroker("us-east-1", "000000000000", serving.Listener.Addr().(*net.TCPAddr).Port)
		broker.SetSNSCapture(capture)
		broker.SetLambdaDelivery(sdkSNSLambdaDelivery{functions})
		serving.Config.Handler = server.New(server.Services{Messaging: messaging.NewHandler(broker), Lambda: functions})
		serving.Start()
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			drainErr := functions.DrainAsync(ctx)
			serving.Close()
			closeErr := functions.Close(ctx)
			captureErr := capture.Close()
			if drainErr != nil || closeErr != nil || captureErr != nil {
				t.Errorf("SNS/Lambda owned cleanup: drain=%v close=%v capture=%v", drainErr, closeErr, captureErr)
			}
		})
		env = append(env, "SNS_LAMBDA_ENDPOINT_"+owner+"="+serving.URL, "SNS_LAMBDA_OUTPUT_"+owner+"="+output, "SNS_LAMBDA_GATE_"+owner+"="+gate,
			"SNS_LAMBDA_ASYNC_"+owner+"="+asyncPath, "SNS_LAMBDA_CAPTURE_"+owner+"="+snsPath)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Second)
	defer cancel()
	output, err := runSDKProcess(ctx, python, fixturePath("python", "sns_lambda_smoke.py"), env)
	t.Logf("real SNS boto3 + registered Python Lambda proof:\n%s", output)
	require.NoError(t, err)
}
