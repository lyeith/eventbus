package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// Exercise production startup: only the selected public recipe enables this
// port. Cognito's separately constructed private functions never enter it.
func TestRunWorkerCrashControlRequiresPublicOptIn(t *testing.T) {
	for _, test := range []struct {
		name            string
		public, enabled bool
		status          int
	}{
		{"private-only", false, false, 404},
		{"public-disabled", true, false, 404},
		{"public-enabled", true, true, 200},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := runTestConfig(t)
			cfg.port = retainedTestPort(t)
			cfg.cognitoLog = filepath.Join(cfg.workDir, "cognito.jsonl")
			cfg.snsLog = filepath.Join(cfg.workDir, "sns.jsonl")
			require.NoError(t, os.WriteFile(filepath.Join(cfg.workDir, "auth.mjs"), []byte("export async function handler(event){return event;}"), 0600))
			require.NoError(t, os.WriteFile(filepath.Join(cfg.workDir, "triggers.yaml"), []byte("pools:\n  owned-pool:\n    DefineAuthChallenge: {handler: auth.mjs}\n    CreateAuthChallenge: {handler: auth.mjs}\n    VerifyAuthChallengeResponse: {handler: auth.mjs}\n"), 0600))
			cfg.cognitoTriggers = "triggers.yaml"
			if test.public {
				executable, err := os.Executable()
				require.NoError(t, err)
				recipe, err := yaml.Marshal(lambdaservice.Config{
					Functions:      map[string]lambdaservice.Function{"public:live": {Runtime: "command", Command: []string{executable}, Timeout: time.Second}},
					DevWorkerCrash: &lambdaservice.DevWorkerCrashConfig{Enabled: test.enabled},
					DevAsync:       &lambdaservice.DevAsyncConfig{LogPath: filepath.Join(cfg.workDir, "async.jsonl")},
				})
				require.NoError(t, err)
				cfg.lambdaFunctions = filepath.Join(cfg.workDir, "functions.yaml")
				require.NoError(t, os.WriteFile(cfg.lambdaFunctions, recipe, 0600))
			}
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { done <- run(ctx, cfg) }()
			t.Cleanup(func() {
				cancel()
				select {
				case err := <-done:
					require.NoError(t, err)
				case <-time.After(15 * time.Second):
					t.Error("worker-crash app startup did not join")
				}
			})
			client := &http.Client{Timeout: 3 * time.Second}
			defer client.CloseIdleConnections()
			endpoint := fmt.Sprintf("http://127.0.0.1:%d", cfg.port)
			require.Eventually(t, func() bool {
				response, err := client.Get(endpoint + "/health")
				if err != nil {
					return false
				}
				_ = response.Body.Close()
				return response.StatusCode == 200
			}, 5*time.Second, 10*time.Millisecond)
			response, err := client.Get(endpoint + "/__eventbus/dev/lambda/invocations")
			require.NoError(t, err)
			data, err := io.ReadAll(response.Body)
			_ = response.Body.Close()
			require.NoError(t, err)
			require.Equal(t, test.status, response.StatusCode, string(data))
			require.NotContains(t, string(data), "cognito_trigger_")
			if test.enabled {
				var snapshot map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(data, &snapshot))
				require.Contains(t, snapshot, "invocations")
				require.Contains(t, snapshot, "events")
			}
			body := strings.NewReader(`{"function_name":"cognito_trigger_1","attempt":1}`)
			request, err := http.NewRequest(http.MethodPut, endpoint+"/__eventbus/dev/lambda/invocations/00000000-0000-4000-8000-000000000000/worker-crash", body)
			require.NoError(t, err)
			response, err = client.Do(request)
			require.NoError(t, err)
			_ = response.Body.Close()
			require.Equal(t, 404, response.StatusCode, "public control cannot select a private Cognito registration")
			if test.enabled {
				// Worker-crash opt-in must not enable the accepted, separate completion fault.
				response, err = client.Get(endpoint + "/__eventbus/dev/lambda/async/events")
				require.NoError(t, err)
				_ = response.Body.Close()
				require.Equal(t, 404, response.StatusCode)
			}
		})
	}
}
