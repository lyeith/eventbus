//go:build sdksmoke

package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lyeith/eventbus/internal/app"
	"github.com/lyeith/eventbus/internal/cognito"
	"github.com/lyeith/eventbus/internal/cognitotrigger"
	"github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/server"
	"github.com/lyeith/eventbus/internal/ses"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestCognitoTriggerReloadJavascriptSDKSmoke(t *testing.T) {
	node := sdkNode(t)
	directory := t.TempDir()
	module := filepath.Join(directory, "reload_shared.mjs")
	source, err := os.ReadFile(fixturePath("javascript", "triggers", "reload_shared.mjs"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(module, source, 0600))
	observations, emailPath := filepath.Join(directory, "observations.jsonl"), filepath.Join(directory, "emails.jsonl")
	poolA := "us-east-1_reloadA" + strings.ReplaceAll(uuid.NewString(), "-", "")
	poolB := "us-east-1_reloadB" + strings.ReplaceAll(uuid.NewString(), "-", "")
	endpoint := startCognitoTriggerReloadFixture(t, node, directory, module, observations, emailPath, poolA, poolB)
	environment := append(sdkEnvironment(directory, endpoint, poolA, ""),
		"SMOKE_STATE_PATH="+filepath.Join(directory, "state.json"),
		"SES_CAPTURE_PATH="+emailPath, "RELOAD_OTHER_POOL="+poolB,
		"RELOAD_OBSERVATIONS="+observations, "RELOAD_SHARED_SOURCE="+module,
		"RELOAD_COGNITO_PACKAGE="+fixturePath("javascript", "node_modules", "@aws-sdk", "client-cognito-identity-provider", "dist-cjs", "index.js"),
		"RELOAD_SRP_HELPER="+fixturePath("javascript", "srp.mjs"),
		"RELOAD_COMMON_HELPER="+fixturePath("javascript", "common.mjs"),
	)
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	output, err := runJavascriptSDKProcess(ctx, node, fixturePath("javascript", "cognito_trigger_reload.mjs"), nil, environment)
	t.Logf("real Cognito SRP/custom-auth + SES shared private source reload:\n%s", output)
	require.NoError(t, err)
}

// The default lane embeds the production composition. Release verification can
// run the same maintained SDK scenario through an explicitly selected binary.
func startCognitoTriggerReloadFixture(t *testing.T, node, directory, module, observations, emailPath, poolA, poolB string) string {
	t.Helper()
	serving := httptest.NewUnstartedServer(nil)
	endpoint := "http://" + serving.Listener.Addr().String()
	entry := &cognitotrigger.Entry{
		Handler: module + "#handler", TimeoutSeconds: 5,
		Env: map[string]string{
			"SES_ENDPOINT_URL": endpoint, "RELOAD_OBSERVATIONS": observations,
			"RELOAD_SETTING":     "unchanged",
			"RELOAD_SES_PACKAGE": fixturePath("javascript", "node_modules", "@aws-sdk", "client-sesv2", "dist-cjs", "index.js"),
		},
	}
	pool := cognitotrigger.Pool{DefineAuthChallenge: entry, CreateAuthChallenge: entry, VerifyAuthChallengeResponse: entry}
	triggersConfig := &cognitotrigger.Config{
		Node: node, DevWarm: &cognitotrigger.DevWarmConfig{MaxWorkers: 2},
		Pools: map[string]cognitotrigger.Pool{poolA: pool, poolB: pool},
	}
	publicConfig := &lambda.Config{
		Functions: map[string]lambda.Function{"public-shared": {
			Runtime: "node", Command: []string{node}, Handler: module + "#handler", Timeout: 5 * time.Second,
			Environment: map[string]string{"RELOAD_OBSERVATIONS": observations, "RELOAD_SETTING": "unchanged"},
		}},
		DevWarm:  &lambda.DevWarmConfig{MaxWorkers: 1},
		DevAsync: &lambda.DevAsyncConfig{LogPath: filepath.Join(directory, "async.jsonl")},
	}
	if binary := os.Getenv("EVENTBUS_SMOKE_BINARY"); binary != "" {
		port := serving.Listener.Addr().(*net.TCPAddr).Port
		require.NoError(t, serving.Listener.Close())
		startCognitoTriggerReloadBinary(t, binary, directory, endpoint, port, emailPath, poolA, poolB, triggersConfig, publicConfig)
		return endpoint
	}
	store, err := cognito.OpenCognitoStore(filepath.Join(directory, "cognito.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	for _, pool := range []string{poolA, poolB} {
		require.NoError(t, store.UpsertPool(t.Context(), pool, "us-east-1"))
	}
	capture, err := ses.OpenSESCapture(emailPath)
	require.NoError(t, err)
	emails := ses.NewSESManager(ses.SESFixtures{}, capture)
	t.Cleanup(func() { require.NoError(t, emails.Close()) })
	triggers, err := app.NewCognitoTriggers(triggersConfig, directory)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, triggers.DevEvidence())
		require.NoError(t, triggers.Close(ctx))
	})
	public, err := lambda.NewService(publicConfig, directory)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, public.DevEvidence())
		require.NoError(t, public.Close(ctx))
	})
	serving.Config.Handler = server.New(server.Services{
		Cognito:     cognito.NewHandler(store, cognito.Options{IssuerBase: endpoint, Triggers: triggers}),
		CognitoDev:  cognitotrigger.NewDevReloadHandler(triggers),
		CognitoURLs: &server.CognitoURLs{Issuer: endpoint, JWKS: endpoint},
		SES:         ses.NewHandler(emails), Lambda: public,
	})
	serving.Start()
	t.Cleanup(serving.Close)
	return endpoint
}

func startCognitoTriggerReloadBinary(t *testing.T, binary, directory, endpoint string, port int, emailPath, poolA, poolB string, triggers *cognitotrigger.Config, public *lambda.Config) {
	t.Helper()
	require.True(t, filepath.IsAbs(binary), "EVENTBUS_SMOKE_BINARY must select an absolute owned executable")
	info, err := os.Stat(binary)
	require.NoError(t, err)
	require.False(t, info.IsDir())
	writeRecipe := func(name string, value any) string {
		path := filepath.Join(directory, name)
		data, err := yaml.Marshal(value)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, data, 0600))
		return path
	}
	seedPath := writeRecipe("pools.yaml", cognito.CognitoSeedFile{Pools: []cognito.CognitoSeedPool{
		{ID: poolA, Region: "us-east-1"}, {ID: poolB, Region: "us-east-1"},
	}})
	triggersPath := writeRecipe("triggers.yaml", triggers)
	functionsPath := writeRecipe("functions.yaml", public)
	logPath := filepath.Join(directory, "cli.log")
	log, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	require.NoError(t, err)
	command := exec.Command(binary,
		"--port", strconv.Itoa(port), "--work-dir", directory, "--issuer-base", endpoint,
		"--cognito-db", filepath.Join(directory, "cognito.db"), "--cognito-pools", seedPath,
		"--cognito-triggers", triggersPath, "--lambda-functions", functionsPath,
		"--ses-log", emailPath, "--sns-log", filepath.Join(directory, "sns.jsonl"),
		"--cognito-log", filepath.Join(directory, "notifications.jsonl"),
		"--s3-endpoint", "http://127.0.0.1:1",
	)
	command.Dir = directory
	command.Env = sdkEnvironment(directory, endpoint, poolA, "")
	command.Stdout, command.Stderr = log, log
	if err := command.Start(); err != nil {
		_ = log.Close()
		t.Fatalf("start selected packaged EventBus: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait(); close(done) }()
	t.Cleanup(func() {
		_ = command.Process.Signal(syscall.SIGTERM)
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("packaged EventBus joined shutdown: %v", err)
			}
		case <-time.After(15 * time.Second):
			_ = command.Process.Kill()
			<-done
			t.Error("packaged EventBus did not join after SIGTERM")
		}
		assertCognitoTriggerReloadWorkersExited(t, filepath.Join(directory, "observations.jsonl"))
		if err := log.Close(); err != nil {
			t.Errorf("close packaged EventBus log: %v", err)
		}
		if t.Failed() {
			data, err := os.ReadFile(logPath)
			require.NoError(t, err)
			t.Logf("owned packaged EventBus output:\n%s", data)
		}
	})
	client := &http.Client{Timeout: time.Second}
	t.Cleanup(client.CloseIdleConnections)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			data, _ := os.ReadFile(logPath)
			t.Fatalf("packaged EventBus exited before readiness: %v\n%s", err, data)
		default:
		}
		response, err := client.Get(endpoint + "/health")
		if err == nil {
			var health struct {
				Issuer string `json:"cognito_issuer"`
			}
			decodeErr := json.NewDecoder(response.Body).Decode(&health)
			_ = response.Body.Close()
			if response.StatusCode == 200 && decodeErr == nil && health.Issuer == endpoint {
				// The random seeded pool identifies this owned process; a port
				// reuse cannot make an unrelated service pass readiness.
				jwks, err := client.Get(endpoint + "/" + poolA + "/.well-known/jwks.json")
				if err == nil {
					var keys struct {
						Keys []json.RawMessage `json:"keys"`
					}
					decodeErr := json.NewDecoder(jwks.Body).Decode(&keys)
					_ = jwks.Body.Close()
					if jwks.StatusCode == 200 && decodeErr == nil && len(keys.Keys) > 0 {
						t.Logf("packaged EventBus SDK lane: %s", binary)
						return
					}
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	data, _ := os.ReadFile(logPath)
	t.Fatalf("packaged EventBus did not become ready:\n%s", data)
}

// Parent exit alone does not prove native warm-worker cleanup. These identities
// come from the real private/public handler observations in the maintained proof.
func assertCognitoTriggerReloadWorkersExited(t *testing.T, observations string) {
	t.Helper()
	data, err := os.ReadFile(observations)
	if err != nil {
		if os.IsNotExist(err) && t.Failed() {
			t.Log("partial packaged run produced no worker observations; worker exit coverage is unavailable")
			return
		}
		t.Errorf("read packaged worker observations: %v", err)
		return
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	pids := make(map[int]struct{})
	for {
		var observed struct {
			PID int `json:"pid"`
		}
		if err := decoder.Decode(&observed); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Errorf("decode packaged worker observation: %v", err)
			break
		}
		if observed.PID <= 0 {
			t.Error("packaged handler observation has no actual worker PID")
			continue
		}
		pids[observed.PID] = struct{}{}
	}
	if len(pids) == 0 {
		if t.Failed() {
			t.Log("partial packaged run has no observed workers to verify")
		} else {
			t.Error("packaged SDK proof produced no worker observations")
		}
		return
	}
	for pid := range pids {
		if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
			t.Errorf("observed native worker %d still exists after packaged EventBus joined shutdown: %v", pid, err)
		}
	}
}
