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
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/lyeith/eventbus/internal/cognito"
	"github.com/lyeith/eventbus/internal/devquiescence"
	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

const (
	gracefulEventSecret = "graceful-event-private-value"
	gracefulEnvSecret   = "graceful-environment-private-value"
	gracefulLogSecret   = "graceful-handler-private-log"
	gracefulErrorSecret = "graceful-handler-private-error"
)

type gracefulShutdownEvent struct {
	Label    string `json:"label"`
	Failures int    `json:"failures"`
	Block    bool   `json:"block"`
	Secret   string `json:"secret"`
}

type gracefulShutdownObservation struct {
	RequestID string                `json:"request_id"`
	Attempt   int                   `json:"attempt"`
	PID       int                   `json:"pid"`
	StartedAt time.Time             `json:"started_at"`
	Event     gracefulShutdownEvent `json:"event"`
}

// This application fixture obtains real native invocations. Its external
// attempt files observe retries; only Lambda owns their admission and timing.
func TestGracefulShutdownProvidedProcess(t *testing.T) {
	if os.Getenv("EVENTBUS_GRACEFUL_SHUTDOWN_HANDLER") != "1" {
		return
	}
	if err := gracefulShutdownInvocation(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	os.Exit(0)
}

func gracefulShutdownInvocation() error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 2 * time.Second}
	defer client.CloseIdleConnections()
	api := "http://" + os.Getenv("AWS_LAMBDA_RUNTIME_API") + "/2018-06-01/runtime/invocation/"
	response, err := client.Get(api + "next")
	if err != nil {
		return err
	}
	var event gracefulShutdownEvent
	err = json.NewDecoder(response.Body).Decode(&event)
	_ = response.Body.Close()
	id := response.Header.Get("Lambda-Runtime-Aws-Request-Id")
	if err != nil || response.StatusCode != http.StatusOK || id == "" || (event.Label != "retry" && event.Label != "active") {
		return errors.New("invalid native Runtime API invocation")
	}
	root := os.Getenv("GRACEFUL_SHUTDOWN_ROOT")
	attempt := 0
	for candidate := 1; candidate <= 3; candidate++ {
		file, openErr := os.OpenFile(filepath.Join(root, fmt.Sprintf("claimed-%s-%d", event.Label, candidate)), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if errors.Is(openErr, os.ErrExist) {
			continue
		}
		if openErr != nil {
			return openErr
		}
		if err = file.Close(); err != nil {
			return err
		}
		attempt = candidate
		break
	}
	if attempt == 0 {
		return errors.New("unexpected additional native attempt")
	}
	write := func(name string, value any) error {
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}
		path := filepath.Join(root, name)
		if err = os.WriteFile(path+".tmp", data, 0600); err != nil {
			return err
		}
		return os.Rename(path+".tmp", path)
	}
	observation := gracefulShutdownObservation{id, attempt, os.Getpid(), time.Now().UTC(), event}
	if err = write(fmt.Sprintf("observed-%s-%d.json", event.Label, attempt), observation); err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, gracefulLogSecret)
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
	}
	if attempt > event.Failures {
		if _, cleanup := os.Stat(filepath.Join(root, "cleanup")); errors.Is(cleanup, os.ErrNotExist) {
			callback := os.Getenv("GRACEFUL_SHUTDOWN_CALLBACK")
			topics, err := disposableShutdownSNS(callback).ListTopics(ctx, &sns.ListTopicsInput{})
			if err != nil {
				return fmt.Errorf("native SDK callback while draining: %w", err)
			}
			found := false
			for _, topic := range topics.Topics {
				found = found || aws.ToString(topic.TopicArn) == os.Getenv("GRACEFUL_SHUTDOWN_TOPIC")
			}
			if !found {
				return errors.New("SDK callback lost its native topic")
			}
			keys, err := client.Get(callback + "/" + os.Getenv("GRACEFUL_SHUTDOWN_POOL") + "/.well-known/jwks.json")
			if err != nil {
				return err
			}
			var jwks struct {
				Keys []json.RawMessage `json:"keys"`
			}
			err = json.NewDecoder(keys.Body).Decode(&jwks)
			_ = keys.Body.Close()
			if err != nil || keys.StatusCode != http.StatusOK || len(jwks.Keys) == 0 {
				return errors.New("Cognito callback lost its signing store")
			}
			if err = write(fmt.Sprintf("callback-%s-%d.json", event.Label, attempt), map[string]any{
				"request_id": id, "sdk_topic_present": true, "jwks_present": true,
			}); err != nil {
				return err
			}
		}
	}
	if event.Block {
		if err = waitGate("release-" + event.Label); err != nil {
			return err
		}
	}
	operation, payload := "response", `{"handled":true}`
	if attempt <= event.Failures {
		operation = "error"
		payload = `{"errorType":"Application.ExpectedFailure","errorMessage":"` + gracefulErrorSecret + `"}`
	}
	ack, err := client.Post(api+id+"/"+operation, "application/json", strings.NewReader(payload))
	if err != nil {
		return err
	}
	_ = ack.Body.Close()
	if ack.StatusCode != http.StatusAccepted {
		return fmt.Errorf("runtime %s returned %d", operation, ack.StatusCode)
	}
	return nil
}

// Each CLI owns its listeners, state, callback resources and actual children.
// The same test can run a release artifact without replacing its handlers.
type gracefulShutdownCLI struct {
	t                   *testing.T
	directory, endpoint string
	asyncPath, logPath  string
	diagnosticPath      string
	databasePath        string
	ports               []int
	command             *exec.Cmd
	done                chan struct{}
	waitErr             error
	client              *http.Client
}

func startGracefulShutdownCLI(t *testing.T, grace, secondRetry time.Duration, retained bool) *gracefulShutdownCLI {
	t.Helper()
	f := &gracefulShutdownCLI{t: t, directory: t.TempDir(), done: make(chan struct{}), client: &http.Client{Timeout: 3 * time.Second}}
	port, callbackPort := cliStartupPort(t), 0
	f.ports = []int{port}
	f.endpoint = fmt.Sprintf("http://127.0.0.1:%d", port)
	callback := f.endpoint
	if retained {
		for callbackPort == 0 || callbackPort == port {
			callbackPort = cliStartupPort(t)
		}
		f.ports = append(f.ports, callbackPort)
		callback = fmt.Sprintf("http://127.0.0.1:%d", callbackPort)
	}
	pool, topic := "us-east-1_graceful"+strconv.Itoa(port), "graceful-"+strconv.Itoa(port)
	topicARN := "arn:aws:sns:us-east-1:000000000000:" + topic
	executable, err := os.Executable()
	require.NoError(t, err)
	functions := map[string]lambdaservice.Function{}
	for _, label := range []string{"retry", "active"} {
		functions[label+":live"] = lambdaservice.Function{
			Runtime: "provided", Command: []string{executable, "-test.run=^TestGracefulShutdownProvidedProcess$"}, Timeout: 20 * time.Second,
			Environment: map[string]string{"EVENTBUS_GRACEFUL_SHUTDOWN_HANDLER": "1", "GRACEFUL_SHUTDOWN_ROOT": f.directory,
				"GRACEFUL_SHUTDOWN_CALLBACK": callback, "GRACEFUL_SHUTDOWN_POOL": pool, "GRACEFUL_SHUTDOWN_TOPIC": topicARN,
				"PRIVATE_ENV_VALUE": gracefulEnvSecret},
		}
	}
	f.asyncPath, f.diagnosticPath = filepath.Join(f.directory, "async.jsonl"), filepath.Join(f.directory, "diagnostics.jsonl")
	recipe, err := yaml.Marshal(lambdaservice.Config{Functions: functions,
		DevAsync:       &lambdaservice.DevAsyncConfig{Workers: 2, Capacity: 2, RetryDelays: []time.Duration{0, secondRetry}, LogPath: f.asyncPath},
		DevDiagnostics: &lambdaservice.DevDiagnosticsConfig{LogPath: f.diagnosticPath}})
	require.NoError(t, err)
	functionsPath := filepath.Join(f.directory, "functions.yaml")
	require.NoError(t, os.WriteFile(functionsPath, recipe, 0600))
	seed, err := yaml.Marshal(cognito.CognitoSeedFile{Pools: []cognito.CognitoSeedPool{{ID: pool, Region: "us-east-1"}}})
	require.NoError(t, err)
	seedPath := filepath.Join(f.directory, "pools.yaml")
	require.NoError(t, os.WriteFile(seedPath, seed, 0600))
	args, paths := cliStartupArguments(f.directory, port)
	f.databasePath = paths[0]
	args = append(args, "--issuer-base", f.endpoint, "--lambda-functions", functionsPath, "--cognito-pools", seedPath,
		"--shutdown-timeout", grace.String())
	if retained {
		args = append(args, "--retained-owner-callback-port", strconv.Itoa(callbackPort))
	}
	f.command = exec.Command(executable, append([]string{"-test.run=^TestCLIStartupProcess$", "--"}, args...)...)
	if binary := os.Getenv("EVENTBUS_SMOKE_BINARY"); binary != "" {
		require.True(t, filepath.IsAbs(binary), "EVENTBUS_SMOKE_BINARY must be an absolute artifact path")
		info, err := os.Stat(binary)
		require.NoError(t, err)
		require.False(t, info.IsDir())
		f.command = exec.Command(binary, args...)
	}
	f.command.Dir, f.command.Env = f.directory, append(os.Environ(), "EVENTBUS_CLI_STARTUP_CHILD=1")
	f.command.WaitDelay = time.Second
	f.logPath = filepath.Join(f.directory, "cli.log")
	output, err := os.OpenFile(f.logPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	require.NoError(t, err)
	f.command.Stdout, f.command.Stderr = output, output
	require.NoError(t, f.command.Start())
	go func() { f.waitErr = f.command.Wait(); close(f.done) }()
	t.Cleanup(func() {
		_ = os.WriteFile(filepath.Join(f.directory, "cleanup"), nil, 0600)
		_ = f.command.Process.Signal(syscall.SIGTERM)
		select {
		case <-f.done:
		case <-time.After(5 * time.Second):
			_ = f.command.Process.Kill()
			<-f.done
		}
		f.client.CloseIdleConnections()
		_ = output.Close()
		if t.Failed() {
			data, _ := os.ReadFile(f.logPath)
			t.Logf("owned CLI output:\n%s", data)
		}
	})
	require.Eventually(t, func() bool {
		var health struct {
			Issuer string `json:"cognito_issuer"`
		}
		return f.get("/health", &health) == nil && health.Issuer == f.endpoint
	}, 5*time.Second, 10*time.Millisecond, "owned CLI did not become ready")
	created, err := disposableShutdownSNS(f.endpoint).CreateTopic(t.Context(), &sns.CreateTopicInput{Name: aws.String(topic)})
	require.NoError(t, err)
	require.Equal(t, topicARN, aws.ToString(created.TopicArn))
	return f
}

func (f *gracefulShutdownCLI) get(path string, target any) error {
	response, err := f.client.Get(f.endpoint + path)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: %d", path, response.StatusCode)
	}
	return json.NewDecoder(response.Body).Decode(target)
}

func (f *gracefulShutdownCLI) invoke(label string, failures int, block bool) {
	f.t.Helper()
	payload, err := json.Marshal(gracefulShutdownEvent{label, failures, block, gracefulEventSecret})
	require.NoError(f.t, err)
	request, err := http.NewRequest(http.MethodPost, f.endpoint+"/2015-03-31/functions/"+label+":live/invocations", bytes.NewReader(payload))
	require.NoError(f.t, err)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Amz-Invocation-Type", "Event")
	response, err := f.client.Do(request)
	require.NoError(f.t, err)
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	require.NoError(f.t, err)
	require.Equal(f.t, http.StatusAccepted, response.StatusCode, string(data))
	require.Empty(f.t, data)
}

func (f *gracefulShutdownCLI) rows(path string) [][]byte {
	f.t.Helper()
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	require.NoError(f.t, err)
	lines := bytes.Split(data, []byte{'\n'})
	return lines[:len(lines)-1] // Ignore only an unfinished append.
}

func (f *gracefulShutdownCLI) latest(label string) lambdaservice.AsyncRecord {
	f.t.Helper()
	var latest lambdaservice.AsyncRecord
	for _, line := range f.rows(f.asyncPath) {
		var record lambdaservice.AsyncRecord
		require.NoError(f.t, json.Unmarshal(line, &record))
		if record.FunctionName == label+":live" {
			latest = record
		}
	}
	return latest
}

func (f *gracefulShutdownCLI) awaitRetry() lambdaservice.AsyncRecord {
	f.t.Helper()
	require.Eventually(f.t, func() bool {
		record := f.latest("retry")
		return record.State == "retrying" && record.Attempts == 2
	}, 5*time.Second, 5*time.Millisecond, "two real failures did not publish the final native retry")
	return f.latest("retry")
}

func (f *gracefulShutdownCLI) observation(label string, attempt int) gracefulShutdownObservation {
	f.t.Helper()
	path := filepath.Join(f.directory, fmt.Sprintf("observed-%s-%d.json", label, attempt))
	require.Eventually(f.t, func() bool { _, err := os.Stat(path); return err == nil }, 5*time.Second, 5*time.Millisecond)
	data, err := os.ReadFile(path)
	require.NoError(f.t, err)
	var observation gracefulShutdownObservation
	require.NoError(f.t, json.Unmarshal(data, &observation))
	require.NotEmpty(f.t, observation.RequestID)
	require.Positive(f.t, observation.PID)
	require.Equal(f.t, attempt, observation.Attempt)
	return observation
}

func (f *gracefulShutdownCLI) gate(name string) {
	f.t.Helper()
	require.NoError(f.t, os.WriteFile(filepath.Join(f.directory, name), nil, 0600))
}

func (f *gracefulShutdownCLI) callback(label string, attempt int, id string) {
	f.t.Helper()
	path := filepath.Join(f.directory, fmt.Sprintf("callback-%s-%d.json", label, attempt))
	require.Eventually(f.t, func() bool { _, err := os.Stat(path); return err == nil }, 3*time.Second, 5*time.Millisecond, "native handler lost SDK/JWKS peers while draining")
	data, err := os.ReadFile(path)
	require.NoError(f.t, err)
	var observed struct {
		RequestID string `json:"request_id"`
		SDK       bool   `json:"sdk_topic_present"`
		JWKS      bool   `json:"jwks_present"`
	}
	require.NoError(f.t, json.Unmarshal(data, &observed))
	require.Equal(f.t, id, observed.RequestID)
	require.True(f.t, observed.SDK)
	require.True(f.t, observed.JWKS)
}

func (f *gracefulShutdownCLI) quiesce(milliseconds, status int) devquiescence.Snapshot {
	f.t.Helper()
	body := fmt.Sprintf(`{"timeout_ms":%d}`, milliseconds)
	response, err := f.client.Post(f.endpoint+devquiescence.ControlPath+"/quiesce", "application/json", strings.NewReader(body))
	require.NoError(f.t, err)
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	require.NoError(f.t, err)
	require.Equal(f.t, status, response.StatusCode, string(data))
	var state devquiescence.Snapshot
	require.NoError(f.t, json.Unmarshal(data, &state))
	return state
}

func (f *gracefulShutdownCLI) stop() time.Time {
	f.t.Helper()
	started := time.Now()
	require.NoError(f.t, f.command.Process.Signal(syscall.SIGTERM))
	return started
}

func (f *gracefulShutdownCLI) wait(timeout time.Duration, success bool) {
	f.t.Helper()
	select {
	case <-f.done:
		if success {
			require.NoError(f.t, f.waitErr)
		} else {
			var exit *exec.ExitError
			require.ErrorAs(f.t, f.waitErr, &exit)
			require.NotZero(f.t, exit.ExitCode(), "expired grace cannot claim a clean drain")
		}
	case <-time.After(timeout):
		f.t.Fatal("CLI did not join within its expected final grace")
	}
}

func (f *gracefulShutdownCLI) assertJoined() {
	f.t.Helper()
	paths, err := filepath.Glob(filepath.Join(f.directory, "observed-*.json"))
	require.NoError(f.t, err)
	for _, path := range paths {
		data, err := os.ReadFile(path)
		require.NoError(f.t, err)
		var observation gracefulShutdownObservation
		require.NoError(f.t, json.Unmarshal(data, &observation))
		require.ErrorIs(f.t, syscall.Kill(observation.PID, 0), syscall.ESRCH, "native handler process survived CLI join")
	}
	for _, port := range f.ports {
		listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		require.NoError(f.t, err, "owned listener survived final shutdown")
		require.NoError(f.t, listener.Close())
	}
	store, err := cognito.OpenCognitoStore(f.databasePath)
	require.NoError(f.t, err)
	require.NoError(f.t, store.Close())
}

func TestCLIConfiguredGraceJoinsLateNativeRetryAndCallbacks(t *testing.T) {
	f := startGracefulShutdownCLI(t, 90*time.Second, 35*time.Second, false)
	f.invoke("retry", 2, false)
	retry := f.awaitRetry()
	require.Greater(t, time.Until(retry.NextAttemptAt), 30*time.Second, "final native retry must outlast the previous final-stop budget")
	f.invoke("active", 0, true)
	active := f.observation("active", 1)
	stoppedAt := f.stop()
	f.gate("probe-active")
	f.callback("active", 1, active.RequestID)
	f.gate("release-active")
	f.wait(50*time.Second, true)
	terminal := f.latest("retry")
	require.Equal(t, retry.RequestID, terminal.RequestID)
	require.Equal(t, "succeeded", terminal.State)
	require.Equal(t, 3, terminal.Attempts)
	require.Empty(t, terminal.ErrorType)
	for attempt := 1; attempt <= 3; attempt++ {
		observation := f.observation("retry", attempt)
		require.Equal(t, retry.RequestID, observation.RequestID)
		require.Equal(t, gracefulShutdownEvent{"retry", 2, false, gracefulEventSecret}, observation.Event)
		if attempt == 3 {
			require.True(t, observation.StartedAt.After(stoppedAt.Add(30*time.Second)), "the final native attempt must actually launch after the old budget")
		}
	}
	f.callback("retry", 3, retry.RequestID)
	require.Equal(t, "succeeded", f.latest("active").State)
	require.Equal(t, 1, f.latest("active").Attempts)
	log, err := os.ReadFile(f.logPath)
	require.NoError(t, err)
	require.NotContains(t, string(log), "Lambda async drain timed out")
	require.NotContains(t, string(log), "Dev.ShutdownDiscarded")
	f.assertJoined()
	t.Log("native attempt 3 succeeded after the old 30s budget; original request/payload, overlapping handler, SDK/JWKS callbacks and child/listener/store joins preserved")
}

func TestCLIConfiguredGraceTimeoutReportsPendingNativeRetry(t *testing.T) {
	f := startGracefulShutdownCLI(t, 250*time.Millisecond, 5*time.Second, true)
	f.invoke("retry", 2, false)
	retry := f.awaitRetry()
	require.Greater(t, time.Until(retry.NextAttemptAt), time.Second)
	f.stop()
	f.wait(5*time.Second, false)
	terminal := f.latest("retry")
	require.Equal(t, retry.RequestID, terminal.RequestID)
	require.Equal(t, "canceled", terminal.State)
	require.Equal(t, "ServiceShutdown", terminal.ErrorType)
	require.Equal(t, 2, terminal.Attempts)
	_, err := os.Stat(filepath.Join(f.directory, "observed-retry-3.json"))
	require.ErrorIs(t, err, os.ErrNotExist, "expired final grace cannot claim a third delivery")
	ansi := regexp.MustCompile("\x1b\\[[0-9;]*m")
	found := false
	for _, line := range f.rows(f.logPath) {
		text := ansi.ReplaceAllString(string(line), "")
		if !strings.Contains(text, "Lambda async drain timed out") {
			continue
		}
		require.False(t, found, "timeout summary must be emitted once")
		found = true
		for _, field := range []string{"shutdown_timeout_ms=250", "outstanding=1", "pending_admissions=0", "shown=1", "omitted=0"} {
			require.Contains(t, strings.Fields(text), field)
		}
		require.Contains(t, text, "captured_at=")
		index := strings.Index(text, "invocations=")
		require.NotEqual(t, -1, index)
		var invocations []struct {
			RequestID     string    `json:"request_id"`
			FunctionName  string    `json:"function_name"`
			State         string    `json:"state"`
			Attempts      int       `json:"attempts"`
			NextAttemptAt time.Time `json:"next_attempt_at"`
		}
		require.NoError(t, json.NewDecoder(strings.NewReader(text[index+len("invocations="):])).Decode(&invocations))
		require.Len(t, invocations, 1)
		require.Equal(t, retry.RequestID, invocations[0].RequestID)
		require.Equal(t, "retry:live", invocations[0].FunctionName)
		require.Equal(t, "retrying", invocations[0].State)
		require.Equal(t, 2, invocations[0].Attempts)
		require.True(t, retry.NextAttemptAt.Equal(invocations[0].NextAttemptAt))
	}
	require.True(t, found, "expired final grace must describe the actual pending native retry")
	log, err := os.ReadFile(f.logPath)
	require.NoError(t, err)
	for _, secret := range []string{gracefulEventSecret, gracefulEnvSecret, gracefulLogSecret, gracefulErrorSecret} {
		require.NotContains(t, string(log), secret, "CLI summary must not duplicate private payloads, environment, logs or function errors")
	}
	f.assertJoined()
	t.Log("expired final grace exited nonzero, retained exact pending request/next-retry metadata, canceled with ServiceShutdown and joined resources without a third attempt")
}

func TestCLIQuiesceTimeoutRecoversNativeRetryBeforeFinalStop(t *testing.T) {
	f := startGracefulShutdownCLI(t, 3*time.Second, 750*time.Millisecond, true)
	f.invoke("retry", 2, false)
	retry := f.awaitRetry()
	state := f.quiesce(20, http.StatusRequestTimeout)
	require.Equal(t, devquiescence.Draining, state.State)
	require.Positive(t, state.WorkCount)
	require.Empty(t, state.EvidenceFailure)
	require.False(t, state.FixtureSafe)
	select {
	case <-f.done:
		t.Fatalf("resumable quiesce timeout unexpectedly stopped the CLI: %v", f.waitErr)
	default:
	}
	state = f.quiesce(2000, http.StatusOK)
	require.Equal(t, devquiescence.Held, state.State)
	require.True(t, state.FixtureSafe)
	require.Zero(t, state.WorkCount)
	require.Empty(t, state.EvidenceFailure)
	terminal := f.latest("retry")
	require.Equal(t, retry.RequestID, terminal.RequestID)
	require.Equal(t, "succeeded", terminal.State)
	require.Equal(t, 3, terminal.Attempts)
	f.callback("retry", 3, retry.RequestID)
	f.stop()
	f.wait(5*time.Second, true)
	log, err := os.ReadFile(f.logPath)
	require.NoError(t, err)
	require.NotContains(t, string(log), "Lambda async drain timed out")
	require.NotContains(t, string(log), "ServiceShutdown")
	f.assertJoined()
	t.Log("quiesce deadline remained resumable; the original native retry completed with live SDK/JWKS peers, held proof recovered and final stop joined cleanly")
}
