//go:build sdksmoke

package sdk

import (
	"context"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/messaging"
	"github.com/lyeith/eventbus/internal/server"
	"github.com/stretchr/testify/require"
)

// This acceptance test uses the native SNS admission adapter and managed Python
// runtime. The handler commits a real side effect before waiting on its own
// request gate; only the optional development completion boundary is faulted.
func TestAsyncCompletionLossPythonSDKSmoke(t *testing.T) {
	python := sdkPython(t)
	for _, mode := range []string{"fresh", "warm"} {
		t.Run(mode, func(t *testing.T) {
			env := append(sdkEnvironment(t.TempDir(), "", "", ""), "ASYNC_COMPLETION_MODE="+mode)
			var services []*lambda.Service
			for _, owner := range []string{"A", "B", "disabled"} {
				root := t.TempDir()
				asyncPath := filepath.Join(root, "async.jsonl")
				snsPath := filepath.Join(root, "sns.jsonl")
				diagnosticPath := filepath.Join(root, "private-diagnostics.jsonl")
				function := func(name string) lambda.Function {
					return lambda.Function{
						Runtime: "python", Command: []string{python, "-E", "-s"},
						Handler: fixturePath("python", "async_completion_loss_smoke.py") + "#handler",
						Timeout: 12 * time.Second,
						Environment: map[string]string{
							"ASYNC_COMPLETION_ROOT":     root,
							"ASYNC_COMPLETION_OWNER":    owner,
							"ASYNC_COMPLETION_FUNCTION": name,
						},
					}
				}
				config := &lambda.Config{
					Functions: map[string]lambda.Function{
						"processor:live": function("processor:live"),
						"other:live":     function("other:live"),
						"ungated:live":   function("ungated:live"),
					},
					DevAsync: &lambda.DevAsyncConfig{
						Workers: 4, Capacity: 16, RetryDelays: []time.Duration{0, 0},
						LogPath: asyncPath, CompletionLoss: owner != "disabled",
					},
					DevDiagnostics: &lambda.DevDiagnosticsConfig{LogPath: diagnosticPath},
				}
				ungated := config.Functions["ungated:live"]
				ungated.Environment["ASYNC_COMPLETION_UNGATED"] = "true"
				config.Functions["ungated:live"] = ungated
				if mode == "warm" {
					config.DevWarm = &lambda.DevWarmConfig{MaxWorkers: 4}
				}
				functions, err := lambda.NewService(config, root)
				require.NoError(t, err)
				services = append(services, functions)
				var capture *messaging.SNSCapture
				var serving *httptest.Server
				t.Cleanup(func() {
					// Release current and future fixture gates after any assertion
					// failure; Go still owns joined runtime/server closure.
					_ = os.WriteFile(filepath.Join(root, "cleanup"), nil, 0600)
					ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
					defer cancel()
					drainErr := functions.DrainAsync(ctx)
					if serving != nil {
						serving.Close()
					}
					closeErr := functions.Close(ctx)
					var captureErr error
					if capture != nil {
						captureErr = capture.Close()
					}
					if drainErr != nil || closeErr != nil || captureErr != nil {
						t.Errorf("completion-loss SDK owned cleanup: drain=%v close=%v capture=%v", drainErr, closeErr, captureErr)
					}
				})
				capture, err = messaging.OpenSNSCapture(snsPath)
				require.NoError(t, err)
				serving = httptest.NewUnstartedServer(nil)
				broker := messaging.NewBroker("us-east-1", "000000000000", serving.Listener.Addr().(*net.TCPAddr).Port)
				broker.SetSNSCapture(capture)
				broker.SetLambdaDelivery(sdkSNSLambdaDelivery{functions})
				serving.Config.Handler = server.New(server.Services{
					Messaging: messaging.NewHandler(broker),
					Lambda:    functions, LambdaDev: lambda.NewDevAsyncHandler(functions),
				})
				serving.Start()
				env = append(env, "ASYNC_COMPLETION_ENDPOINT_"+owner+"="+serving.URL,
					"ASYNC_COMPLETION_ROOT_"+owner+"="+root)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 80*time.Second)
			defer cancel()
			output, err := runSDKProcess(ctx, python, fixturePath("python", "async_completion_loss_smoke.py"), env)
			t.Logf("real SNS boto3 completion-loss proof (%s):\n%s", mode, output)
			require.NoError(t, err)
			for _, service := range services {
				require.NoError(t, service.DevEvidence(), "injected completion loss is deliberate retry evidence, not capture or lifetime uncertainty")
			}
		})
	}
}
