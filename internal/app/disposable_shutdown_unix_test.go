//go:build linux || darwin

package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/lyeith/eventbus/internal/cognito"
	"github.com/lyeith/eventbus/internal/devquiescence"
	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

type disposableShutdownObservation struct {
	Label     string `json:"label"`
	RequestID string `json:"request_id"`
	PID       int    `json:"pid"`
}

func disposableShutdownSNS(endpoint string) *sns.Client {
	return sns.New(sns.Options{Region: "us-east-1", BaseEndpoint: aws.String(endpoint),
		Credentials: credentials.NewStaticCredentialsProvider("owned-test", "owned-test", ""), Retryer: aws.NopRetryer{},
		HTTPClient: &http.Client{Timeout: 2 * time.Second}})
}

// This ordinary application handler speaks the real native Runtime API. Active
// invocations make actual SDK/JWKS callbacks after shutdown has started.
func TestDisposableShutdownProvidedProcess(t *testing.T) {
	if os.Getenv("EVENTBUS_DISPOSABLE_SHUTDOWN_HANDLER") != "1" {
		return
	}
	if err := disposableShutdownInvocation(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	os.Exit(0)
}

func disposableShutdownInvocation() error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 2 * time.Second}
	defer client.CloseIdleConnections()
	endpoint := "http://" + os.Getenv("AWS_LAMBDA_RUNTIME_API") + "/2018-06-01/runtime/invocation/"
	response, err := client.Get(endpoint + "next")
	if err != nil {
		return err
	}
	var event struct {
		Label string `json:"label"`
		Block bool   `json:"block"`
		Fail  bool   `json:"fail"`
	}
	err = json.NewDecoder(response.Body).Decode(&event)
	_ = response.Body.Close()
	id := response.Header.Get("Lambda-Runtime-Aws-Request-Id")
	if err != nil || response.StatusCode != http.StatusOK || id == "" {
		return errors.New("invalid native Runtime API invocation")
	}
	root := os.Getenv("DISPOSABLE_SHUTDOWN_ROOT")
	write := func(name string, value any) error {
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}
		if err = os.WriteFile(filepath.Join(root, name+".tmp"), data, 0600); err != nil {
			return err
		}
		return os.Rename(filepath.Join(root, name+".tmp"), filepath.Join(root, name))
	}
	if err = write("started-"+event.Label+".json", disposableShutdownObservation{event.Label, id, os.Getpid()}); err != nil {
		return err
	}
	waitGate := func(name string) error {
		for {
			if _, err := os.Stat(filepath.Join(root, "cleanup")); err == nil {
				return nil
			}
			if _, err := os.Stat(filepath.Join(root, name)); err == nil {
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(5 * time.Millisecond):
			}
		}
	}
	if event.Block {
		if err = waitGate("probe-" + event.Label); err != nil {
			return err
		}
		if _, cleanup := os.Stat(filepath.Join(root, "cleanup")); errors.Is(cleanup, os.ErrNotExist) {
			topics, err := disposableShutdownSNS(os.Getenv("DISPOSABLE_SHUTDOWN_CALLBACK")).ListTopics(ctx, &sns.ListTopicsInput{})
			if err != nil {
				return fmt.Errorf("native SDK callback during shutdown: %w", err)
			}
			found := false
			for _, topic := range topics.Topics {
				if aws.ToString(topic.TopicArn) == os.Getenv("DISPOSABLE_SHUTDOWN_TOPIC") {
					found = true
				}
			}
			if !found {
				return errors.New("SDK callback lost native broker resource during shutdown")
			}
			keys, err := client.Get(os.Getenv("DISPOSABLE_SHUTDOWN_CALLBACK") + "/" + os.Getenv("DISPOSABLE_SHUTDOWN_POOL") + "/.well-known/jwks.json")
			if err != nil {
				return err
			}
			var jwks struct {
				Keys []json.RawMessage `json:"keys"`
			}
			err = json.NewDecoder(keys.Body).Decode(&jwks)
			_ = keys.Body.Close()
			if err != nil || keys.StatusCode != http.StatusOK || len(jwks.Keys) == 0 {
				return errors.New("Cognito callback lost its signing store during shutdown")
			}
			if err = write("callback-"+event.Label+".json", map[string]any{"request_id": id, "sdk_topic_present": true, "jwks_present": true}); err != nil {
				return err
			}
		}
		if err = waitGate("release-" + event.Label); err != nil {
			return err
		}
	}
	operation, payload := "response", `{"handled":true}`
	if event.Fail {
		operation, payload = "error", `{"errorType":"Application.ExpectedFailure","errorMessage":"owned handler failed normally"}`
	}
	ack, err := client.Post(endpoint+id+"/"+operation, "application/json", strings.NewReader(payload))
	if err != nil {
		return err
	}
	_ = ack.Body.Close()
	if ack.StatusCode != http.StatusAccepted {
		return fmt.Errorf("runtime %s returned %d", operation, ack.StatusCode)
	}
	return nil
}

func TestCLIDiscardPendingShutdownJoinsActiveNativeCallbacks(t *testing.T) {
	for _, retained := range []bool{false, true} {
		name := "ordinary"
		if retained {
			name = "retained"
		}
		t.Run(name, func(t *testing.T) {
			directory := t.TempDir()
			port := cliStartupPort(t)
			endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)
			callback, callbackPort := endpoint, 0
			if retained {
				for callbackPort == 0 || callbackPort == port {
					callbackPort = cliStartupPort(t)
				}
				callback = fmt.Sprintf("http://127.0.0.1:%d", callbackPort)
			}
			pool := "us-east-1_disposable" + strconv.Itoa(port)
			topicName := "disposable-" + strconv.Itoa(port)
			topicARN := "arn:aws:sns:us-east-1:000000000000:" + topicName
			executable, err := os.Executable()
			require.NoError(t, err)
			functions := map[string]lambdaservice.Function{}
			for _, label := range []string{"retry", "success", "failure", "queued"} {
				functions[label+":live"] = lambdaservice.Function{Runtime: "provided", Command: []string{executable, "-test.run=^TestDisposableShutdownProvidedProcess$"}, Timeout: 20 * time.Second,
					Environment: map[string]string{"EVENTBUS_DISPOSABLE_SHUTDOWN_HANDLER": "1", "DISPOSABLE_SHUTDOWN_ROOT": directory,
						"DISPOSABLE_SHUTDOWN_CALLBACK": callback, "DISPOSABLE_SHUTDOWN_POOL": pool, "DISPOSABLE_SHUTDOWN_TOPIC": topicARN}}
			}
			asyncPath, diagnosticPath := filepath.Join(directory, "async.jsonl"), filepath.Join(directory, "diagnostics.jsonl")
			recipe, err := yaml.Marshal(lambdaservice.Config{Functions: functions, DevAsync: &lambdaservice.DevAsyncConfig{Workers: 2, Capacity: 4, LogPath: asyncPath}, DevDiagnostics: &lambdaservice.DevDiagnosticsConfig{LogPath: diagnosticPath}})
			require.NoError(t, err)
			functionsPath := filepath.Join(directory, "functions.yaml")
			require.NoError(t, os.WriteFile(functionsPath, recipe, 0600))
			seed, err := yaml.Marshal(cognito.CognitoSeedFile{Pools: []cognito.CognitoSeedPool{{ID: pool, Region: "us-east-1"}}})
			require.NoError(t, err)
			seedPath := filepath.Join(directory, "pools.yaml")
			require.NoError(t, os.WriteFile(seedPath, seed, 0600))
			args, paths := cliStartupArguments(directory, port)
			args = append(args, "--issuer-base", endpoint, "--lambda-functions", functionsPath, "--cognito-pools", seedPath, "--shutdown-policy=discard-pending")
			if retained {
				args = append(args, "--retained-owner-callback-port", strconv.Itoa(callbackPort))
			}
			command := exec.Command(executable, append([]string{"-test.run=^TestCLIStartupProcess$", "--"}, args...)...)
			command.Dir = directory
			command.Env = append(os.Environ(), "EVENTBUS_CLI_STARTUP_CHILD=1")
			command.WaitDelay = time.Second
			outputPath := filepath.Join(directory, "cli.log")
			output, err := os.OpenFile(outputPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			require.NoError(t, err)
			command.Stdout, command.Stderr = output, output
			require.NoError(t, command.Start())
			done := make(chan struct{})
			var waitErr error
			go func() { waitErr = command.Wait(); close(done) }()
			client := &http.Client{Timeout: time.Second}
			t.Cleanup(func() {
				_ = os.WriteFile(filepath.Join(directory, "cleanup"), nil, 0600)
				_ = command.Process.Signal(syscall.SIGTERM)
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					_ = command.Process.Kill()
					<-done
				}
				client.CloseIdleConnections()
				_ = output.Close()
				if t.Failed() {
					data, _ := os.ReadFile(outputPath)
					t.Logf("owned CLI output:\n%s", data)
				}
			})
			getJSON := func(path string, target any) error {
				response, err := client.Get(endpoint + path)
				if err != nil {
					return err
				}
				defer response.Body.Close()
				if response.StatusCode != http.StatusOK {
					return fmt.Errorf("GET %s: %d", path, response.StatusCode)
				}
				return json.NewDecoder(response.Body).Decode(target)
			}
			require.Eventually(t, func() bool {
				var health struct {
					Issuer string `json:"cognito_issuer"`
				}
				if getJSON("/health", &health) != nil || health.Issuer != endpoint {
					return false
				}
				var keys struct {
					Keys []json.RawMessage `json:"keys"`
				}
				return getJSON("/"+pool+"/.well-known/jwks.json", &keys) == nil && len(keys.Keys) > 0
			}, 5*time.Second, 10*time.Millisecond, "owned CLI did not reach readiness")
			created, err := disposableShutdownSNS(endpoint).CreateTopic(t.Context(), &sns.CreateTopicInput{Name: aws.String(topicName)})
			require.NoError(t, err)
			require.Equal(t, topicARN, aws.ToString(created.TopicArn))
			readRows := func(path string) [][]byte {
				data, err := os.ReadFile(path)
				if errors.Is(err, os.ErrNotExist) {
					return nil
				}
				require.NoError(t, err)
				lines := bytes.Split(data, []byte{'\n'})
				return lines[:len(lines)-1]
			}
			asyncRows := func() []lambdaservice.AsyncRecord {
				var records []lambdaservice.AsyncRecord
				for _, line := range readRows(asyncPath) {
					var row lambdaservice.AsyncRecord
					require.NoError(t, json.Unmarshal(line, &row))
					records = append(records, row)
				}
				return records
			}
			latest := func(label string) (lambdaservice.AsyncRecord, bool) {
				var found lambdaservice.AsyncRecord
				ok := false
				for _, record := range asyncRows() {
					if record.FunctionName == label+":live" {
						found, ok = record, true
					}
				}
				return found, ok
			}
			invoke := func(label string, block, fail bool) {
				payload, _ := json.Marshal(map[string]any{"label": label, "block": block, "fail": fail})
				request, err := http.NewRequest(http.MethodPost, endpoint+"/2015-03-31/functions/"+label+":live/invocations", bytes.NewReader(payload))
				require.NoError(t, err)
				request.Header.Set("Content-Type", "application/json")
				request.Header.Set("X-Amz-Invocation-Type", "Event")
				response, err := client.Do(request)
				require.NoError(t, err)
				defer response.Body.Close()
				data, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				require.Equal(t, http.StatusAccepted, response.StatusCode, string(data))
				require.Empty(t, data)
			}
			observation := func(label string) disposableShutdownObservation {
				path := filepath.Join(directory, "started-"+label+".json")
				require.Eventually(t, func() bool { _, err := os.Stat(path); return err == nil }, 5*time.Second, 5*time.Millisecond, "native %s handler did not start", label)
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				var row disposableShutdownObservation
				require.NoError(t, json.Unmarshal(data, &row))
				require.Equal(t, label, row.Label)
				require.NotEmpty(t, row.RequestID)
				require.Positive(t, row.PID)
				return row
			}
			invoke("retry", false, true)
			retryObservation := observation("retry")
			require.Eventually(t, func() bool { row, ok := latest("retry"); return ok && row.State == "retrying" && row.Attempts == 1 }, 5*time.Second, 5*time.Millisecond)
			retry, _ := latest("retry")
			require.Equal(t, retryObservation.RequestID, retry.RequestID)
			require.Greater(t, time.Until(retry.NextAttemptAt), 30*time.Second, "native minute retry must exceed CLI shutdown budget")
			invoke("success", true, false)
			successObservation := observation("success")
			invoke("failure", true, true)
			failureObservation := observation("failure")
			invoke("queued", false, false)
			queued, ok := latest("queued")
			require.True(t, ok)
			require.Equal(t, "queued", queued.State)
			require.Zero(t, queued.Attempts)
			_, err = os.Stat(filepath.Join(directory, "started-queued.json"))
			require.ErrorIs(t, err, os.ErrNotExist)
			shutdownStart := time.Now()
			require.NoError(t, command.Process.Signal(syscall.SIGTERM))
			if retained {
				require.Eventually(t, func() bool {
					var state devquiescence.Snapshot
					return getJSON(devquiescence.ControlPath, &state) == nil && state.State == devquiescence.Shutdown
				}, time.Second, 5*time.Millisecond, "retained source fence was not published")
			} else {
				// All four outstanding slots are already owned. A pre-fence
				// probe therefore gets native 429 without creating an event;
				// native 503 confirms the permanent admission fence is visible.
				require.Eventually(t, func() bool {
					request, err := http.NewRequest(http.MethodPost, endpoint+"/2015-03-31/functions/queued:live/invocations", strings.NewReader(`{"label":"probe"}`))
					require.NoError(t, err)
					request.Header.Set("X-Amz-Invocation-Type", "Event")
					response, err := client.Do(request)
					require.NoError(t, err)
					_ = response.Body.Close()
					if response.StatusCode == http.StatusServiceUnavailable {
						require.Equal(t, "ServiceException", response.Header.Get("X-Amzn-ErrorType"))
						return true
					}
					require.Equal(t, http.StatusTooManyRequests, response.StatusCode, "fence probe must never admit extra work")
					return false
				}, time.Second, 5*time.Millisecond, "disposable policy fence was not published")
			}
			select {
			case <-done:
				t.Fatalf("CLI exited before active handlers finished: %v", waitErr)
			default:
			}
			for _, label := range []string{"success", "failure"} {
				require.NoError(t, os.WriteFile(filepath.Join(directory, "probe-"+label), nil, 0600))
				require.Eventually(t, func() bool { _, err := os.Stat(filepath.Join(directory, "callback-"+label+".json")); return err == nil }, 3*time.Second, 5*time.Millisecond, "active %s handler lost SDK/JWKS peers during shutdown", label)
			}
			_, err = os.Stat(filepath.Join(directory, "started-queued.json"))
			require.ErrorIs(t, err, os.ErrNotExist, "policy must not launch queued accepted event")
			for _, label := range []string{"success", "failure"} {
				require.NoError(t, os.WriteFile(filepath.Join(directory, "release-"+label), nil, 0600))
			}
			select {
			case <-done:
				require.NoError(t, waitErr)
			case <-time.After(8 * time.Second):
				t.Fatal("disposable shutdown waited for an unusable native retry")
			}
			require.Less(t, time.Since(shutdownStart), 10*time.Second)
			for label, expected := range map[string]struct {
				state    string
				attempts int
				id       string
			}{"retry": {"canceled", 1, retryObservation.RequestID}, "queued": {"canceled", 0, queued.RequestID}, "success": {"succeeded", 1, successObservation.RequestID}, "failure": {"canceled", 1, failureObservation.RequestID}} {
				terminal, ok := latest(label)
				require.True(t, ok)
				require.Equal(t, expected.id, terminal.RequestID)
				require.Equal(t, expected.state, terminal.State)
				require.Equal(t, expected.attempts, terminal.Attempts)
				if expected.state == "canceled" {
					require.Equal(t, "Dev.ShutdownDiscarded", terminal.ErrorType)
				} else {
					require.Empty(t, terminal.ErrorType)
				}
			}
			// The real CLI uses ConsoleWriter; remove only its ANSI styling.
			ansi := regexp.MustCompile("\x1b\\[[0-9;]*m")
			foundSummary := false
			for _, line := range readRows(outputPath) {
				text := ansi.ReplaceAllString(string(line), "")
				if strings.Contains(text, "Lambda async shutdown policy applied") {
					require.Contains(t, strings.Fields(text), "shutdown_policy=discard-pending")
					require.Contains(t, strings.Fields(text), "canceled_events=3")
					foundSummary = true
				}
			}
			require.True(t, foundSummary, "CLI must explicitly report discarded native events")
			diagnostics := map[string]struct {
				RequestID          string `json:"request_id"`
				State              string `json:"state"`
				OwnershipConfirmed bool   `json:"ownership_confirmed"`
				FunctionError      bool   `json:"function_error"`
			}{}
			for _, line := range readRows(diagnosticPath) {
				var row struct {
					RequestID          string `json:"request_id"`
					State              string `json:"state"`
					OwnershipConfirmed bool   `json:"ownership_confirmed"`
					FunctionError      bool   `json:"function_error"`
				}
				require.NoError(t, json.Unmarshal(line, &row))
				diagnostics[row.RequestID] = row
			}
			require.Len(t, diagnostics, 3, "unclaimed queued event has no handler diagnostic")
			for _, id := range []string{retryObservation.RequestID, failureObservation.RequestID} {
				row := diagnostics[id]
				require.Equal(t, "failed", row.State)
				require.True(t, row.FunctionError)
				require.True(t, row.OwnershipConfirmed)
			}
			require.Equal(t, "succeeded", diagnostics[successObservation.RequestID].State)
			require.True(t, diagnostics[successObservation.RequestID].OwnershipConfirmed)
			for _, row := range []disposableShutdownObservation{retryObservation, successObservation, failureObservation} {
				require.ErrorIs(t, syscall.Kill(row.PID, 0), syscall.ESRCH, "real native child %s remained after CLI joined", row.Label)
			}
			for _, number := range []int{port, callbackPort} {
				if number == 0 {
					continue
				}
				listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", number))
				require.NoError(t, err, "owned listener survived CLI shutdown")
				require.NoError(t, listener.Close())
			}
			store, err := cognito.OpenCognitoStore(paths[0])
			require.NoError(t, err)
			require.NoError(t, store.Close(), "clean discard must release store ownership")
		})
	}
}
