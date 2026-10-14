//go:build sdksmoke

package sdk

import (
	"context"
	"errors"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/cognito"
	"github.com/lyeith/eventbus/internal/devquiescence"
	"github.com/lyeith/eventbus/internal/eventsource"
	"github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/messaging"
	"github.com/lyeith/eventbus/internal/server"
	"github.com/stretchr/testify/require"
)

// The fixture composes existing native owners. Only application work gates use
// files; crash selection uses real SNS/mapping admission IDs, never process IDs.
func TestWorkerCrashNativeProducerPythonSDKSmoke(t *testing.T) {
	python := sdkPython(t)
	for _, mode := range []string{"fresh", "warm"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			var functions *lambda.Service
			var mappings *eventsource.Service
			owner := devquiescence.NewWithOptions(devquiescence.Options{
				Checks: []func() error{
					func() error {
						if functions != nil {
							return functions.DevEvidence()
						}
						return nil
					},
					func() error {
						if mappings != nil {
							return mappings.DevEvidence()
						}
						return nil
					},
				},
				DrainHooks: []devquiescence.DrainHook{{
					Start: func() error {
						if functions != nil {
							return functions.DevBeginWarmDrain()
						}
						return nil
					},
					Resume: func() error {
						if functions != nil {
							return functions.DevResumeWarm()
						}
						return nil
					},
				}},
			})
			serving := httptest.NewUnstartedServer(nil)
			endpoint := "http://" + serving.Listener.Addr().String()
			store, err := cognito.OpenCognitoStore(filepath.Join(root, "cognito.db"))
			require.NoError(t, err)
			capture, err := messaging.OpenSNSCapture(filepath.Join(root, "sns.jsonl"))
			require.NoError(t, err)
			t.Cleanup(func() {
				_ = os.WriteFile(filepath.Join(root, "cleanup"), nil, 0600)
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				var failures []error
				if mappings != nil {
					failures = append(failures, mappings.Close(ctx))
				}
				if functions != nil {
					failures = append(failures, functions.DrainAsync(ctx))
				}
				serving.Close()
				if functions != nil {
					failures = append(failures, functions.Close(ctx))
				}
				failures = append(failures, capture.Close(), store.Close())
				require.NoError(t, errors.Join(failures...))
			})
			function := lambda.Function{
				Runtime: "python", Command: []string{python, "-E", "-s"},
				Handler: fixturePath("python", "worker_crash_smoke.py") + "#handler", Timeout: 4 * time.Second,
				Environment: map[string]string{"WORKER_CRASH_ROOT": root},
			}
			config := &lambda.Config{
				Functions:      map[string]lambda.Function{"crash-sns:live": function, "crash-sqs:live": function},
				DevAsync:       &lambda.DevAsyncConfig{Workers: 2, Capacity: 16, RetryDelays: []time.Duration{200 * time.Millisecond, 200 * time.Millisecond}, LogPath: filepath.Join(root, "async.jsonl")},
				DevDiagnostics: &lambda.DevDiagnosticsConfig{LogPath: filepath.Join(root, "private-diagnostics.jsonl")},
				DevWorkerCrash: &lambda.DevWorkerCrashConfig{Enabled: true}, DevActivity: owner,
			}
			if mode == "warm" {
				config.DevWarm = &lambda.DevWarmConfig{MaxWorkers: 4, MaxConcurrency: 4}
			}
			functions, err = lambda.NewService(config, root)
			require.NoError(t, err)
			broker := messaging.NewBroker("us-east-1", "000000000000", serving.Listener.Addr().(*net.TCPAddr).Port)
			broker.SetSNSCapture(capture)
			broker.SetLambdaDelivery(sdkSNSLambdaDelivery{functions})
			require.NoError(t, broker.SetDevActivity(owner))
			mappings, err = eventsource.New(eventsource.Options{Region: "us-east-1", AccountID: "000000000000",
				Dev: eventsource.DevOptions{Source: owner, Activity: owner, DeliveryCapture: &eventsource.DevDeliveryCaptureConfig{LogPath: filepath.Join(root, "deliveries.jsonl")}}},
				nativeEvidenceQueueSource{broker}, nativeEvidenceInvoker{sdkMappingInvoker{functions}})
			require.NoError(t, err)
			aws := server.New(server.Services{Messaging: messaging.NewHandler(broker), Lambda: functions,
				LambdaWorkerDev: lambda.NewDevWorkerCrashHandler(functions), EventSources: eventsource.NewHandler(mappings),
				Cognito: cognito.NewHandler(store, cognito.Options{Region: "us-east-1", AccountID: "000000000000", IssuerBase: endpoint})})
			serving.Config.Handler = owner.Wrap(devquiescence.Source, aws, nil)
			serving.Start()
			environment := append(sdkEnvironment(t.TempDir(), "", "", ""),
				"WORKER_CRASH_ENDPOINT="+endpoint, "WORKER_CRASH_ROOT="+root, "WORKER_CRASH_MODE="+mode)
			ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
			defer cancel()
			logs, err := runSDKProcess(ctx, python, fixturePath("python", "worker_crash_smoke.py"), environment)
			t.Logf("real SNS/SQS native producers and one-shot worker death (%s):\n%s", mode, logs)
			require.NoError(t, err)
			require.NoError(t, functions.DevEvidence())
			require.NoError(t, mappings.DevEvidence())
			held, err := owner.Quiesce(ctx)
			require.NoError(t, err)
			require.True(t, held.FixtureSafe, "intentional worker death preserves proven lifetime ownership")
			require.Zero(t, held.WorkCount, "native calls and retained workers all joined")
		})
	}
}
