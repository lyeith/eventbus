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
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	smtypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/lyeith/eventbus/internal/cognito"
	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/secrets"
	"github.com/lyeith/eventbus/internal/server"
	"github.com/stretchr/testify/require"
)

// The actual test executable runs as a provided Lambda runtime. Its SDK requests
// return through the EventBus listener independently of the parent goroutine.
func TestBackgroundDrainProvidedProcess(t *testing.T) {
	if os.Getenv("EVENTBUS_BACKGROUND_DRAIN_PROCESS") != "1" {
		return
	}
	if err := backgroundProvidedInvocation(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	os.Exit(0)
}

type backgroundDrainEvent struct {
	Kind               string `json:"kind,omitempty"`
	SecretID           string `json:"SecretId"`
	ClientRequestToken string `json:"ClientRequestToken"`
	Step               string `json:"Step,omitempty"`
}

func backgroundProvidedInvocation() error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 10 * time.Second}
	endpoint := "http://" + os.Getenv("AWS_LAMBDA_RUNTIME_API") + "/2018-06-01/runtime/invocation/"
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"next", nil)
	if err != nil {
		return err
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	requestID := response.Header.Get("Lambda-Runtime-Aws-Request-Id")
	var event backgroundDrainEvent
	decodeErr := json.NewDecoder(response.Body).Decode(&event)
	_ = response.Body.Close()
	if decodeErr != nil || response.StatusCode != http.StatusOK || requestID == "" {
		return errors.New("provided runtime did not receive an invocation")
	}
	var callbackErr error
	if event.Kind != "probe" {
		if event.Kind == "event" || event.Step == "createSecret" {
			gate, err := http.NewRequestWithContext(ctx, http.MethodGet, os.Getenv("BACKGROUND_GATE_URL"), nil)
			if err != nil {
				return err
			}
			gate.Header.Set("X-Background-Pid", strconv.Itoa(os.Getpid()))
			ack, err := client.Do(gate)
			if err != nil {
				return err
			}
			_ = ack.Body.Close()
			if ack.StatusCode != http.StatusOK {
				return errors.New("background gate failed")
			}
		}
		callbackErr = backgroundSDKCallback(ctx, backgroundSecretsSDK(os.Getenv("BACKGROUND_AWS_ENDPOINT"), client), event)
	}
	suffix, payload := "/response", []byte(`{"completed":true}`)
	if callbackErr != nil {
		suffix, payload = "/error", []byte(`{"errorType":"BackgroundSDKError","errorMessage":"SDK callback failed"}`)
	}
	reply, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+requestID+suffix, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	reply.Header.Set("Content-Type", "application/json")
	ack, err := client.Do(reply)
	if err != nil {
		return err
	}
	_ = ack.Body.Close()
	if ack.StatusCode != http.StatusAccepted {
		return fmt.Errorf("runtime response status %d", ack.StatusCode)
	}
	return nil
}

func backgroundSecretsSDK(endpoint string, client *http.Client) *secretsmanager.Client {
	return secretsmanager.NewFromConfig(aws.Config{
		Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""), RetryMaxAttempts: 1, HTTPClient: client,
	}, func(options *secretsmanager.Options) { options.BaseEndpoint = aws.String(endpoint) })
}

func backgroundSDKCallback(ctx context.Context, client *secretsmanager.Client, event backgroundDrainEvent) error {
	if event.Kind == "event" {
		if _, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: aws.String(event.SecretID)}); err != nil {
			return err
		}
		_, err := client.PutSecretValue(ctx, &secretsmanager.PutSecretValueInput{
			SecretId: aws.String(event.SecretID), ClientRequestToken: aws.String(event.ClientRequestToken), SecretString: aws.String("updated-event"),
		})
		return err
	}
	metadata, err := client.DescribeSecret(ctx, &secretsmanager.DescribeSecretInput{SecretId: aws.String(event.SecretID)})
	if err != nil {
		return err
	}
	switch event.Step {
	case "createSecret":
		_, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
			SecretId: aws.String(event.SecretID), VersionId: aws.String(event.ClientRequestToken), VersionStage: aws.String("AWSPENDING"),
		})
		var missing *smtypes.ResourceNotFoundException
		if !errors.As(err, &missing) {
			return errors.New("rotation did not reserve an empty pending version")
		}
		_, err = client.PutSecretValue(ctx, &secretsmanager.PutSecretValueInput{
			SecretId: aws.String(event.SecretID), ClientRequestToken: aws.String(event.ClientRequestToken),
			SecretString: aws.String("updated-rotation"), VersionStages: []string{"AWSPENDING"},
		})
		return err
	case "setSecret", "testSecret":
		value, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
			SecretId: aws.String(event.SecretID), VersionId: aws.String(event.ClientRequestToken), VersionStage: aws.String("AWSPENDING"),
		})
		if err == nil && aws.ToString(value.SecretString) != "updated-rotation" {
			return errors.New("rotation pending value changed")
		}
		return err
	case "finishSecret":
		current := ""
		for id, stages := range metadata.VersionIdsToStages {
			for _, stage := range stages {
				if stage == "AWSCURRENT" {
					current = id
				}
			}
		}
		_, err := client.UpdateSecretVersionStage(ctx, &secretsmanager.UpdateSecretVersionStageInput{
			SecretId: aws.String(event.SecretID), VersionStage: aws.String("AWSCURRENT"),
			MoveToVersionId: aws.String(event.ClientRequestToken), RemoveFromVersionId: aws.String(current),
		})
		return err
	default:
		return errors.New("unexpected rotation step")
	}
}

// Observers delegate the full drain to the real owners. Their signals release
// the handler only after the app has entered the relevant background join.
type backgroundLambdaOwner struct {
	*lambdaservice.Service
	entered chan struct{}
	phase   *atomic.Bool
	once    sync.Once
}

func (owner *backgroundLambdaOwner) DrainAsync(ctx context.Context) error {
	owner.once.Do(func() { owner.phase.Store(true); close(owner.entered) })
	return owner.Service.DrainAsync(ctx)
}

type backgroundRotationOwner struct {
	*secrets.RotationService
	entered chan struct{}
	phase   *atomic.Bool
	once    sync.Once
}

func (owner *backgroundRotationOwner) Drain(ctx context.Context) error {
	owner.once.Do(func() { owner.phase.Store(true); close(owner.entered) })
	return owner.RotationService.Drain(ctx)
}

type backgroundSDKObservation struct {
	operation     string
	during        bool
	before, after error
	closed        bool
}
type backgroundDrainFixture struct {
	serving       *httptest.Server
	manager       *eventBusListener
	store         *cognito.CognitoStore
	secrets       *secrets.SecretsStore
	functions     *backgroundLambdaOwner
	rotation      *backgroundRotationOwner
	client        *secretsmanager.Client
	gateEntered   chan struct{}
	processIDs    chan int
	releaseGate   func()
	outcomes      chan secrets.RotationOutcome
	observations  chan backgroundSDKObservation
	captureClosed *atomic.Bool
}

const backgroundRotationARN = "arn:aws:lambda:us-east-1:000000000000:function:background"

func newBackgroundDrainFixture(t *testing.T, timeout time.Duration) *backgroundDrainFixture {
	t.Helper()
	directory := t.TempDir()
	store, err := cognito.OpenCognitoStore(filepath.Join(directory, "cognito.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	serving := httptest.NewUnstartedServer(nil)
	t.Cleanup(serving.Close)
	endpoint := "http://" + serving.Listener.Addr().String()
	executable, err := os.Executable()
	require.NoError(t, err)
	function := lambdaservice.Function{
		Runtime: "provided", Command: []string{executable, "-test.run=^TestBackgroundDrainProvidedProcess$"}, Timeout: 10 * time.Second,
		Environment: map[string]string{
			"EVENTBUS_BACKGROUND_DRAIN_PROCESS": "1", "BACKGROUND_AWS_ENDPOINT": endpoint, "BACKGROUND_GATE_URL": endpoint + "/__background/gate",
		},
	}
	functions, err := lambdaservice.NewService(&lambdaservice.Config{
		Functions: map[string]lambdaservice.Function{"background": function, "probe": function},
		DevAsync:  &lambdaservice.DevAsyncConfig{Workers: 1, Capacity: 4, RetryDelays: []time.Duration{0, 0}, LogPath: filepath.Join(directory, "async.jsonl")},
	}, directory)
	require.NoError(t, err)
	secretStore := secrets.NewSecretsStore("us-east-1", "000000000000")
	outcomes := make(chan secrets.RotationOutcome, 8)
	rotation := secrets.NewRotationService(secretStore, rotationLambdaInvoker{runtime: functions}, secrets.RotationOptions{
		MaxAttempts: 1, AttemptTimeout: 10 * time.Second, Observer: func(outcome secrets.RotationOutcome) { outcomes <- outcome },
	})
	var phase, captureClosed atomic.Bool
	functionOwner := &backgroundLambdaOwner{Service: functions, entered: make(chan struct{}), phase: &phase}
	rotationOwner := &backgroundRotationOwner{RotationService: rotation, entered: make(chan struct{}), phase: &phase}
	owned := &eventBusLifecycle{
		store: store, rotation: rotationOwner, functions: functionOwner,
		ses: closeFunc(func() error { captureClosed.Store(true); return nil }),
	}
	manager := newEventBusListener(serving.Config, owned, timeout)
	gateEntered, release := make(chan struct{}, 1), make(chan struct{})
	processIDs := make(chan int, 8)
	var releaseOnce sync.Once
	releaseGate := func() { releaseOnce.Do(func() { close(release) }) }
	observations := make(chan backgroundSDKObservation, 64)
	awsHandler := server.New(server.Services{Secrets: secrets.NewHandler(secretStore, rotation), Lambda: functions})
	serving.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/__background/gate" {
			pid, _ := strconv.Atoi(r.Header.Get("X-Background-Pid"))
			processIDs <- pid
			select {
			case gateEntered <- struct{}{}:
			default:
			}
			select {
			case <-release:
				w.WriteHeader(http.StatusOK)
			case <-r.Context().Done():
			}
			return
		}
		target := r.Header.Get("X-Amz-Target")
		if !strings.HasPrefix(target, "secretsmanager.") {
			awsHandler.ServeHTTP(w, r)
			return
		}
		observation := backgroundSDKObservation{
			operation: strings.TrimPrefix(target, "secretsmanager."), during: phase.Load(), before: readCognitoStore(context.Background(), store),
		}
		awsHandler.ServeHTTP(w, r)
		observation.after = readCognitoStore(context.Background(), store)
		observation.closed = captureClosed.Load()
		observations <- observation
	})
	serving.Start()
	t.Cleanup(func() {
		releaseGate()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// Failed drains retain their result; direct owner cleanup still joins the
		// canceled children before this fixture releases its resources.
		if err := manager.Shutdown(ctx); err != nil {
			t.Logf("fixture listener cleanup: %v", err)
		}
		serving.Close()
		if err := rotation.Close(ctx); err != nil {
			t.Logf("fixture rotation cleanup: %v", err)
		}
		if err := functions.Close(ctx); err != nil {
			t.Logf("fixture Lambda cleanup: %v", err)
		}
	})
	return &backgroundDrainFixture{
		serving: serving, manager: manager, store: store, secrets: secretStore, functions: functionOwner, rotation: rotationOwner,
		client: backgroundSecretsSDK(endpoint, &http.Client{Timeout: 2 * time.Second}), gateEntered: gateEntered, processIDs: processIDs, releaseGate: releaseGate,
		outcomes: outcomes, observations: observations, captureClosed: &captureClosed,
	}
}

func backgroundInvoke(ctx context.Context, client *http.Client, endpoint, function string, payload []byte) (int, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/2015-03-31/functions/"+function+"/invocations", bytes.NewReader(payload))
	if err != nil {
		return 0, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Amz-Invocation-Type", "Event")
	response, err := client.Do(request)
	if err != nil {
		return 0, err
	}
	_, readErr := io.Copy(io.Discard, response.Body)
	return response.StatusCode, errors.Join(readErr, response.Body.Close())
}
func backgroundAwait(t *testing.T, done <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func TestEventBusBackgroundDrainPreservesSDKCallbacks(t *testing.T) {
	for _, kind := range []string{"event", "rotation"} {
		t.Run(kind, func(t *testing.T) {
			fixture := newBackgroundDrainFixture(t, 8*time.Second)
			token := strings.Repeat("2", 32)
			created, err := fixture.client.CreateSecret(t.Context(), &secretsmanager.CreateSecretInput{
				Name: aws.String("background-" + kind), ClientRequestToken: aws.String(strings.Repeat("1", 32)), SecretString: aws.String("original"),
			})
			require.NoError(t, err)
			entered := fixture.functions.entered
			if kind == "event" {
				payload, err := json.Marshal(backgroundDrainEvent{Kind: "event", SecretID: aws.ToString(created.ARN), ClientRequestToken: token})
				require.NoError(t, err)
				status, err := backgroundInvoke(t.Context(), fixture.serving.Client(), fixture.serving.URL, "background", payload)
				require.NoError(t, err)
				require.Equal(t, http.StatusAccepted, status)
			} else {
				entered = fixture.rotation.entered
				_, err := fixture.client.RotateSecret(t.Context(), &secretsmanager.RotateSecretInput{
					SecretId: created.ARN, ClientRequestToken: aws.String(token), RotationLambdaARN: aws.String(backgroundRotationARN),
				})
				require.NoError(t, err)
			}
			backgroundAwait(t, fixture.gateEntered, "accepted handler to enter the listener")
			done := make(chan struct{})
			var shutdownErr error
			go func() { shutdownErr = fixture.manager.Shutdown(context.Background()); close(done) }()
			backgroundAwait(t, entered, "background quiesce")
			if kind == "event" {
				// A probe accepted just before the barrier is harmless and joins
				// normally; after the barrier the native Event wire must refuse.
				require.Eventually(t, func() bool {
					status, err := backgroundInvoke(t.Context(), fixture.serving.Client(), fixture.serving.URL, "probe", []byte(`{"kind":"probe"}`))
					return err == nil && status == http.StatusServiceUnavailable
				}, time.Second, 5*time.Millisecond, "new Event requests must be refused during quiesce")
			}
			current, err := fixture.client.GetSecretValue(t.Context(), &secretsmanager.GetSecretValueInput{SecretId: created.ARN})
			require.NoError(t, err, "the native AWS listener must remain reachable during quiesce")
			require.Equal(t, "original", aws.ToString(current.SecretString))
			require.NoError(t, readCognitoStore(t.Context(), fixture.store))
			require.False(t, fixture.captureClosed.Load())
			select {
			case <-done:
				t.Fatalf("shutdown returned before the accepted handler settled: %v", shutdownErr)
			default:
			}
			fixture.releaseGate()
			backgroundAwait(t, done, "accepted SDK callbacks and listener shutdown")
			require.NoError(t, shutdownErr)
			value, err := fixture.secrets.GetValue(aws.ToString(created.ARN), "", "")
			require.NoError(t, err)
			require.Equal(t, token, value.VersionID)
			require.Equal(t, "updated-"+kind, value.SecretString)
			if kind == "event" {
				var accepted []lambdaservice.AsyncRecord
				for _, record := range fixture.functions.AsyncSnapshot() {
					if record.FunctionName == "background" {
						accepted = append(accepted, record)
					}
				}
				require.Len(t, accepted, 1)
				require.Equal(t, "succeeded", accepted[0].State)
				require.Equal(t, 1, accepted[0].Attempts)
				require.False(t, accepted[0].CompletedAt.IsZero())
			} else {
				select {
				case outcome := <-fixture.outcomes:
					require.Equal(t, "succeeded", outcome.Status)
					require.Equal(t, "finishSecret", outcome.Step)
					require.Equal(t, token, outcome.VersionID)
				default:
					t.Fatal("joined rotation did not report its outcome")
				}
			}
			sawWriteDuringDrain := false
			for len(fixture.observations) > 0 {
				callback := <-fixture.observations
				require.NoError(t, callback.before, callback.operation)
				require.NoError(t, callback.after, callback.operation)
				require.False(t, callback.closed, "capture closed during "+callback.operation)
				if callback.during && (callback.operation == "PutSecretValue" || callback.operation == "UpdateSecretVersionStage") {
					sawWriteDuringDrain = true
				}
			}
			require.True(t, sawWriteDuringDrain, "the provided handler must make an SDK write after quiesce began")
			require.True(t, fixture.captureClosed.Load())
			require.Error(t, readCognitoStore(t.Context(), fixture.store), "SQLite closes only after accepted callbacks finish")
			_, err = fixture.functions.Execute(t.Context(), lambdaservice.InvokeInput{FunctionName: "background", Payload: []byte("{}")})
			require.Error(t, err, "final close must refuse new synchronous runtime use")
			require.NoError(t, fixture.manager.Shutdown(t.Context()), "successful shutdown is shared and repeatable")
		})
	}
}

func TestEventBusBackgroundDrainDeadlineRetainsResources(t *testing.T) {
	fixture := newBackgroundDrainFixture(t, 150*time.Millisecond)
	created, err := fixture.client.CreateSecret(t.Context(), &secretsmanager.CreateSecretInput{Name: aws.String("deadline"), SecretString: aws.String("original")})
	require.NoError(t, err)
	payload, err := json.Marshal(backgroundDrainEvent{Kind: "event", SecretID: aws.ToString(created.ARN), ClientRequestToken: strings.Repeat("3", 32)})
	require.NoError(t, err)
	status, err := backgroundInvoke(t.Context(), fixture.serving.Client(), fixture.serving.URL, "background", payload)
	require.NoError(t, err)
	require.Equal(t, http.StatusAccepted, status)
	backgroundAwait(t, fixture.gateEntered, "accepted handler before deadline")
	err = fixture.manager.Shutdown(context.Background())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NoError(t, readCognitoStore(t.Context(), fixture.store), "failed quiesce must retain SQLite")
	require.False(t, fixture.captureClosed.Load(), "failed quiesce must retain capture ownership")
	records := fixture.functions.AsyncSnapshot()
	require.Len(t, records, 1)
	require.Equal(t, "canceled", records[0].State, "the timed-out accepted handler must be joined")
	require.False(t, records[0].CompletedAt.IsZero())
	value, err := fixture.secrets.GetValue(aws.ToString(created.ARN), "", "")
	require.NoError(t, err)
	require.Equal(t, "original", value.SecretString, "canceled accepted work must not make a late SDK write")
	require.ErrorIs(t, fixture.manager.Shutdown(context.Background()), context.DeadlineExceeded, "later callers must observe the failed drain")
}

func TestEventBusMixedBackgroundDeadlineJoinsEveryOwner(t *testing.T) {
	fixture := newBackgroundDrainFixture(t, 150*time.Millisecond)
	created, err := fixture.client.CreateSecret(t.Context(), &secretsmanager.CreateSecretInput{Name: aws.String("mixed-event"), SecretString: aws.String("original-event")})
	require.NoError(t, err)
	payload, err := json.Marshal(backgroundDrainEvent{Kind: "event", SecretID: aws.ToString(created.ARN), ClientRequestToken: strings.Repeat("3", 32)})
	require.NoError(t, err)
	status, err := backgroundInvoke(t.Context(), fixture.serving.Client(), fixture.serving.URL, "background", payload)
	require.NoError(t, err)
	require.Equal(t, http.StatusAccepted, status)
	backgroundAwait(t, fixture.gateEntered, "accepted Lambda event")
	rotationSecret, err := fixture.client.CreateSecret(t.Context(), &secretsmanager.CreateSecretInput{Name: aws.String("mixed-rotation"), SecretString: aws.String("original-rotation")})
	require.NoError(t, err)
	_, err = fixture.client.RotateSecret(t.Context(), &secretsmanager.RotateSecretInput{
		SecretId: rotationSecret.ARN, ClientRequestToken: aws.String(strings.Repeat("4", 32)), RotationLambdaARN: aws.String(backgroundRotationARN),
	})
	require.NoError(t, err)
	backgroundAwait(t, fixture.gateEntered, "accepted rotation handler")
	err = fixture.manager.Shutdown(context.Background())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	// Rotation is the first deadline failure. Its result must not leave the
	// independent, already accepted Lambda Event running or accepting new work.
	records := fixture.functions.AsyncSnapshot()
	require.Len(t, records, 1)
	require.Equal(t, "canceled", records[0].State)
	require.False(t, records[0].CompletedAt.IsZero())
	select {
	case outcome := <-fixture.outcomes:
		require.Equal(t, "canceled", outcome.Status)
		require.Equal(t, "createSecret", outcome.Step)
	default:
		t.Fatal("the failed rotation drain did not join and report its handler")
	}
	_, err = fixture.functions.Admit(t.Context(), lambdaservice.InvokeInput{FunctionName: "probe", Payload: []byte("{}")})
	var refused *lambdaservice.InvokeError
	require.ErrorAs(t, err, &refused)
	require.Equal(t, http.StatusServiceUnavailable, refused.Status)
	require.NoError(t, readCognitoStore(t.Context(), fixture.store), "independent drain errors must retain SQLite")
	require.False(t, fixture.captureClosed.Load())
	require.Len(t, fixture.processIDs, 2, "both real runtime processes must have reached the gate")
	seen := map[int]bool{}
	for len(fixture.processIDs) > 0 {
		pid := <-fixture.processIDs
		require.Positive(t, pid)
		require.False(t, seen[pid])
		seen[pid] = true
		process, err := os.FindProcess(pid)
		require.NoError(t, err)
		err = process.Signal(syscall.Signal(0))
		_ = process.Release()
		require.True(t, errors.Is(err, os.ErrProcessDone) || errors.Is(err, syscall.ESRCH), "runtime process %d survived the returned drain: %v", pid, err)
	}
	for id, expected := range map[string]string{aws.ToString(created.ARN): "original-event", aws.ToString(rotationSecret.ARN): "original-rotation"} {
		value, err := fixture.secrets.GetValue(id, "", "")
		require.NoError(t, err)
		require.Equal(t, expected, value.SecretString)
	}
	require.ErrorIs(t, fixture.manager.Shutdown(context.Background()), context.DeadlineExceeded, "failed drains remain terminal")
}
