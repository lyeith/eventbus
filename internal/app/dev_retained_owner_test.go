package app

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/cognito"
	"github.com/lyeith/eventbus/internal/devquiescence"
	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/messaging"
	"github.com/lyeith/eventbus/internal/server"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestRetainedOwnerCLIProfileValidation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		args  []string
		valid bool
		port  int
	}{
		{"off", nil, true, 0},
		{"on", []string{"--retained-owner-callback-port", "4101"}, true, 4101},
		{"same listener", []string{"--retained-owner-callback-port", "4100"}, false, 0},
		{"invalid low", []string{"--retained-owner-callback-port", "-1"}, false, 0},
		{"invalid high", []string{"--retained-owner-callback-port", "65536"}, false, 0},
		{"autonomous legacy consumers", []string{"--retained-owner-callback-port", "4101", "--consumers", "consumers.yaml"}, false, 0},
		{"tracked trigger runner", []string{"--retained-owner-callback-port", "4101", "--cognito-triggers", "triggers.yaml"}, true, 4101},
	} {
		t.Run(tc.name, func(t *testing.T) {
			flags := flag.NewFlagSet("retained", flag.ContinueOnError)
			flags.SetOutput(io.Discard)
			cfg, err := readConfig(flags, tc.args)
			if !tc.valid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.port, cfg.retainedCallbackPort)
		})
	}
}

type retainedActionTrap struct{ calls int }

func (h *retainedActionTrap) ServeAction(w http.ResponseWriter, r *http.Request, action string) {
	h.calls++
	w.WriteHeader(204)
}
func (h *retainedActionTrap) ServeQuery(w http.ResponseWriter, r *http.Request, action string) {
	h.calls++
	w.WriteHeader(204)
}

func TestRetainedProfileKeepsTrackedServicesAndRefusesUntrackedSources(t *testing.T) {
	core := &retainedActionTrap{}
	restCalls := 0
	rest := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { restCalls++; w.WriteHeader(204) })
	router := server.New(retainedServices(server.Services{Messaging: core, Secrets: core, Firehose: core, EventSources: rest, Scheduler: rest}))
	for _, tc := range []struct{ path, target, body string }{
		{"/", "secretsmanager.RotateSecret", "{}"},
		{"/", "AmazonSQS.StartMessageMoveTask", "{}"},
		{"/", "", "Action=StartMessageMoveTask&Version=2012-11-05"},
	} {
		r := httptest.NewRequest("POST", tc.path, strings.NewReader(tc.body))
		if tc.target != "" {
			r.Header.Set("X-Amz-Target", tc.target)
		} else {
			r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		}
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		require.Equal(t, 503, w.Code)
		require.Contains(t, w.Body.String(), "retained-owner profile")
	}
	require.Zero(t, core.calls)
	for _, tc := range []struct{ path, target, body string }{
		{"/2015-03-31/event-source-mappings", "", "{}"},
		{"/schedules", "", "{}"},
		{"/", "Firehose_20150804.PutRecord", "{}"},
		{"/", "secretsmanager.GetSecretValue", "{}"},
		{"/", "", "Action=Subscribe&Version=2010-03-31&Protocol=firehose"},
	} {
		r := httptest.NewRequest("POST", tc.path, strings.NewReader(tc.body))
		if tc.target != "" {
			r.Header.Set("X-Amz-Target", tc.target)
		} else {
			r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		}
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		require.Equal(t, 204, w.Code, tc.path+" "+tc.target)
	}
	require.Equal(t, 2, restCalls)
	require.Equal(t, 3, core.calls)
}

func TestRetainedCleanupCannotRouteDeletionHeaderIntoNativeInvoke(t *testing.T) {
	invoked := false
	router := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { invoked = true; w.WriteHeader(204) })
	owner := devquiescence.New()
	_, err := owner.Quiesce(t.Context())
	require.NoError(t, err)
	cleanup := owner.Wrap(devquiescence.Callback, router, retainedCleanupHandler(router))
	for _, tc := range []struct {
		path, target, body string
		allowed            bool
	}{
		{"/2015-03-31/functions/function/invocations", "AmazonSQS.DeleteQueue", `{}`, false},
		{"/", "AmazonSQS.SendMessage", `{}`, false},
		{"/", "AmazonSQS.DeleteQueue", `{}`, true},
		{"/", "", "Action=Publish&Version=2010-03-31&Message=x", false},
		{"/", "", "Action=DeleteTopic&Version=2012-11-05", false},
		{"/", "", "Action=DeleteTopic&Version=2010-03-31", true},
		{"/queue/owned", "", "Action=DeleteMessageBatch&Version=2012-11-05", true},
		{"/", "", "Action=DeleteConfigurationSet&Version=2010-12-01", true},
		{"/", "", "Action=DeleteConfigurationSetEventDestination&Version=2010-12-01", true},
		{"/", "", "Action=DeleteConfigurationSet&Version=2010-03-31", false},
		{"/queue/owned", "", "Action=DeleteConfigurationSet&Version=2010-12-01", false},
		{"/", "", "Action=CreateConfigurationSet&Version=2010-12-01", false},
		{"/", "", "Action=UpdateConfigurationSetEventDestination&Version=2010-12-01", false},
		{"/", "", "Action=SendEmail&Version=2010-12-01", false},
		{"/__eventbus/dev/ses/outcomes", "", "Action=DeleteConfigurationSet&Version=2010-12-01", false},
		{"/__eventbus/dev/lambda/functions/processor/reload", "", "Action=DeleteConfigurationSet&Version=2010-12-01", false},
	} {
		invoked = false
		r := httptest.NewRequest("POST", tc.path, strings.NewReader(tc.body))
		if tc.target != "" {
			r.Header.Set("X-Amz-Target", tc.target)
		} else {
			r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		}
		w := httptest.NewRecorder()
		cleanup.ServeHTTP(w, r)
		require.Equal(t, tc.allowed, invoked, tc.path+" "+tc.target+" "+tc.body)
		if tc.allowed {
			require.Equal(t, 204, w.Code)
		} else {
			require.Equal(t, 503, w.Code)
		}
	}
}

func retainedTestPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())
	return port
}

type retainedResponse struct {
	body   []byte
	status int
	err    error
}

func retainedRequest(client *http.Client, method, endpoint, body, contentType string) retainedResponse {
	request, err := http.NewRequest(method, endpoint, strings.NewReader(body))
	if err != nil {
		return retainedResponse{err: err}
	}
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	response, err := client.Do(request)
	if err != nil {
		return retainedResponse{err: err}
	}
	defer response.Body.Close()
	result, err := io.ReadAll(response.Body)
	return retainedResponse{body: result, status: response.StatusCode, err: err}
}
func retainedHTTP(t *testing.T, client *http.Client, method, endpoint, body, contentType string, want int) []byte {
	t.Helper()
	response := retainedRequest(client, method, endpoint, body, contentType)
	require.NoError(t, response.err)
	require.Equal(t, want, response.status, string(response.body))
	return response.body
}
func retainedControl(t *testing.T, client *http.Client, endpoint, operation, body string, want int) devquiescence.Snapshot {
	t.Helper()
	method := "POST"
	if operation == "" {
		method = "GET"
	}
	raw := retainedHTTP(t, client, method, endpoint+devquiescence.ControlPath+operation, body, "application/json", want)
	var status devquiescence.Snapshot
	require.NoError(t, json.Unmarshal(raw, &status))
	return status
}
func retainedQuery(t *testing.T, client *http.Client, endpoint, action string, fields url.Values, want int) []byte {
	t.Helper()
	if fields == nil {
		fields = url.Values{}
	}
	fields.Set("Action", action)
	fields.Set("Version", "2010-03-31")
	return retainedHTTP(t, client, "POST", endpoint+"/", fields.Encode(), "application/x-www-form-urlencoded", want)
}

// This proof starts the production app, its two real listeners and actual Go
// provided-runtime children; fixture ports cannot substitute for the app wiring.
func TestProductionRetainedOwnerFenceCleanupResume(t *testing.T) {
	cfg := runTestConfig(t)
	cfg.port, cfg.retainedCallbackPort = retainedTestPort(t), retainedTestPort(t)
	require.NotEqual(t, cfg.port, cfg.retainedCallbackPort)
	source, callback := fmt.Sprintf("http://127.0.0.1:%d", cfg.port), fmt.Sprintf("http://127.0.0.1:%d", cfg.retainedCallbackPort)
	cfg.issuerBase = source
	observations := filepath.Join(cfg.workDir, "observations")
	require.NoError(t, os.Mkdir(observations, 0700))
	executable, err := os.Executable()
	require.NoError(t, err)
	recipe, err := yaml.Marshal(lambdaservice.Config{Functions: map[string]lambdaservice.Function{"retained:live": {
		Runtime: "provided", Command: []string{executable, "-test.run=^TestMessagingCompositionProvidedProcess$"}, Timeout: 10 * time.Second,
		Environment: map[string]string{"EVENTBUS_MESSAGING_COMPOSITION_PROCESS": "1", "MESSAGING_OBSERVATIONS": observations, "MESSAGING_HANDLER": "retained-alias"},
	}}, DevAsync: &lambdaservice.DevAsyncConfig{LogPath: filepath.Join(cfg.workDir, "lambda.jsonl")}})
	require.NoError(t, err)
	cfg.lambdaFunctions = filepath.Join(cfg.workDir, "functions.yaml")
	require.NoError(t, os.WriteFile(cfg.lambdaFunctions, recipe, 0600))
	cfg.snsLog, cfg.cognitoLog = filepath.Join(cfg.workDir, "sns.jsonl"), filepath.Join(cfg.workDir, "cognito.jsonl")
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg) }()
	t.Cleanup(func() {
		_ = os.WriteFile(filepath.Join(observations, "sns-release"), nil, 0600)
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(35 * time.Second):
			t.Error("retained application failed to join")
		}
	})
	client := &http.Client{Timeout: 15 * time.Second}
	defer client.CloseIdleConnections()
	for _, endpoint := range []string{source, callback} {
		require.Eventually(t, func() bool {
			response, err := client.Get(endpoint + "/health")
			if err != nil {
				return false
			}
			response.Body.Close()
			return response.StatusCode == 200
		}, 5*time.Second, 10*time.Millisecond)
	}
	topic := fmt.Sprintf("arn:aws:sns:%s:%s:retained-owned", cfg.region, cfg.accountID)
	sentinel := fmt.Sprintf("arn:aws:sns:%s:%s:sentinel", cfg.region, cfg.accountID)
	retainedQuery(t, client, source, "CreateTopic", url.Values{"Name": {"retained-owned"}}, 200)
	retainedQuery(t, client, source, "CreateTopic", url.Values{"Name": {"sentinel"}}, 200)
	target := fmt.Sprintf("arn:aws:lambda:%s:%s:function:retained:live", cfg.region, cfg.accountID)
	retainedQuery(t, client, source, "Subscribe", url.Values{"TopicArn": {topic}, "Protocol": {"lambda"}, "Endpoint": {target}}, 200)
	retainedQuery(t, client, source, "Publish", url.Values{"TopicArn": {topic}, "Message": {"first-suite"}}, 200)
	require.Eventually(t, func() bool { _, err := os.Stat(filepath.Join(observations, "sns-started")); return err == nil }, 5*time.Second, 10*time.Millisecond)
	synchronous := make(chan retainedResponse, 1)
	go func() {
		synchronous <- retainedRequest(client, "POST", callback+"/2015-03-31/functions/retained:live/invocations", `{"Records":[{"EventSource":"aws:sns"}]}`, "application/json")
	}()
	require.Eventually(t, func() bool {
		status := retainedControl(t, client, source, "", "", 200)
		kinds := map[string]bool{}
		for _, activity := range status.Activities {
			kinds[activity.Kind] = true
		}
		return kinds["lambda_async"] && kinds["lambda_invoke"]
	}, 5*time.Second, 10*time.Millisecond)
	quiesced := make(chan retainedResponse, 1)
	go func() {
		quiesced <- retainedRequest(client, "POST", source+devquiescence.ControlPath+"/quiesce", `{"timeout_ms":8000}`, "application/json")
	}()
	require.Eventually(t, func() bool { return retainedControl(t, client, source, "", "", 200).State == devquiescence.Draining }, time.Second, 10*time.Millisecond)
	retainedQuery(t, client, source, "Publish", url.Values{"TopicArn": {topic}, "Message": {"blocked"}}, 503)
	retainedQuery(t, client, callback, "GetTopicAttributes", url.Values{"TopicArn": {sentinel}}, 200)
	select {
	case result := <-quiesced:
		t.Fatalf("barrier returned before actual gated children joined: %+v", result)
	default:
	}
	require.NoError(t, os.WriteFile(filepath.Join(observations, "sns-release"), nil, 0600))
	syncResult := <-synchronous
	require.NoError(t, syncResult.err)
	require.Equal(t, 200, syncResult.status, string(syncResult.body))
	var held devquiescence.Snapshot
	select {
	case result := <-quiesced:
		require.NoError(t, result.err)
		require.Equal(t, 200, result.status, string(result.body))
		require.NoError(t, json.Unmarshal(result.body, &held))
	case <-time.After(10 * time.Second):
		t.Fatal("actual child completion did not satisfy retained barrier")
	}
	require.True(t, held.FixtureSafe)
	require.Equal(t, devquiescence.Held, held.State)
	require.Zero(t, held.WorkCount)
	got, err := messagingCompositionObservations(observations)
	require.NoError(t, err)
	require.Len(t, got, 2)
	for _, observation := range got {
		require.Equal(t, target, observation.FunctionARN)
		require.Equal(t, "retained-alias", observation.Handler)
	}
	retainedHTTP(t, client, "POST", callback+"/2015-03-31/functions/retained:live/invocations", `{"Records":[{"EventSource":"aws:sns"}]}`, "application/json", 503)
	retainedQuery(t, client, callback, "DeleteTopic", url.Values{"TopicArn": {topic}}, 200)
	resumed := retainedControl(t, client, source, "/resume", fmt.Sprintf(`{"generation":%d}`, held.Generation), 200)
	require.Equal(t, held.Generation+1, resumed.Generation)
	require.False(t, resumed.FixtureSafe)
	retainedQuery(t, client, source, "GetTopicAttributes", url.Values{"TopicArn": {sentinel}}, 200)
	retainedQuery(t, client, source, "CreateTopic", url.Values{"Name": {"retained-next"}}, 200)
	next := fmt.Sprintf("arn:aws:sns:%s:%s:retained-next", cfg.region, cfg.accountID)
	retainedQuery(t, client, source, "Subscribe", url.Values{"TopicArn": {next}, "Protocol": {"lambda"}, "Endpoint": {target}}, 200)
	retainedQuery(t, client, source, "Publish", url.Values{"TopicArn": {next}, "Message": {"second-suite"}}, 200)
	second := retainedControl(t, client, source, "/quiesce", `{"timeout_ms":8000}`, 200)
	require.True(t, second.FixtureSafe)
	got, err = messagingCompositionObservations(observations)
	require.NoError(t, err)
	require.Len(t, got, 3)
	retainedQuery(t, client, callback, "DeleteTopic", url.Values{"TopicArn": {next}}, 200)
}

// This fixture exposes only lifecycle sequencing; real child execution and
// observer transfer are covered by the production and SDK runtime proofs.
type retainedLifecycleFunctions struct {
	drain func(context.Context) error
	close func(context.Context) error
}

func (f retainedLifecycleFunctions) DrainAsync(ctx context.Context) error {
	if f.drain != nil {
		return f.drain(ctx)
	}
	return nil
}
func (f retainedLifecycleFunctions) Close(ctx context.Context) error {
	if f.close != nil {
		return f.close(ctx)
	}
	return nil
}

func TestRetainedListenerShutdownPreservesLateAcceptedCallbacks(t *testing.T) {
	owner := devquiescence.New()
	root, err := owner.BeginActivity("lambda_async", "accepted-root")
	require.NoError(t, err)
	childStarted, childRelease := make(chan struct{}), make(chan struct{})
	work := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		child, err := owner.BeginActivity("lambda_async", "late-descendant")
		if err != nil {
			http.Error(w, err.Error(), 503)
			return
		}
		close(childStarted)
		root(nil)
		<-childRelease
		child(nil)
		w.WriteHeader(200)
	})
	source := httptest.NewServer(owner.Wrap(devquiescence.Source, work, nil))
	peer := httptest.NewServer(owner.Wrap(devquiescence.Callback, work, nil))
	var released bool
	t.Cleanup(func() {
		if !released {
			close(childRelease)
		}
		root(nil)
		source.Close()
		peer.Close()
	})
	drained := make(chan struct{}, 1)
	functions := retainedLifecycleFunctions{drain: func(context.Context) error { drained <- struct{}{}; return nil }}
	manager := newEventBusListener(source.Config, &eventBusLifecycle{functions: functions}, 2*time.Second)
	manager.devRetained = &devRetainedHTTP{owner: owner, callbacks: peer.Config}
	stopped := make(chan error, 1)
	go func() { stopped <- manager.Shutdown(context.Background()) }()
	require.Eventually(t, func() bool { return owner.Snapshot().State == devquiescence.Shutdown }, time.Second, time.Millisecond)
	// Accepted roots must still admit new descendants after shutdown fences.
	callbackDone := make(chan retainedResponse, 1)
	go func() { callbackDone <- retainedRequest(peer.Client(), "POST", peer.URL, "{}", "application/json") }()
	select {
	case <-childStarted:
	case <-time.After(time.Second):
		t.Fatal("shutdown fenced an accepted callback descendant")
	}
	select {
	case <-drained:
		t.Fatal("permanent Event drain began before callback chain joined")
	default:
	}
	select {
	case <-stopped:
		t.Fatal("shutdown returned while accepted descendant remained active")
	default:
	}
	close(childRelease)
	released = true
	response := <-callbackDone
	require.NoError(t, response.err)
	require.Equal(t, 200, response.status)
	require.NoError(t, <-stopped)
	require.Len(t, drained, 1)
	require.Zero(t, owner.Snapshot().WorkCount)
	_, err = owner.Resume(1)
	require.ErrorIs(t, err, devquiescence.ErrShutdown)
}

func TestRetainedListenerFailedJoinAbortsNativeThenJoinsReceivedEnvelope(t *testing.T) {
	owner := devquiescence.New()
	received, release := make(chan struct{}), make(chan struct{})
	source := httptest.NewServer(owner.Wrap(devquiescence.Source, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(received)
		<-release
		w.WriteHeader(200)
	}), nil))
	peer := httptest.NewServer(owner.Wrap(devquiescence.Callback, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }), nil))
	released := false
	t.Cleanup(func() {
		if !released {
			close(release)
		}
		source.Close()
		peer.Close()
	})
	clientDone := make(chan retainedResponse, 1)
	go func() { clientDone <- retainedRequest(source.Client(), "POST", source.URL, "{}", "application/json") }()
	<-received
	store, err := cognito.OpenCognitoStore(filepath.Join(t.TempDir(), "retained.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	aborted := make(chan struct{}, 2)
	closeCalls := 0
	functions := retainedLifecycleFunctions{close: func(ctx context.Context) error {
		closeCalls++
		if closeCalls == 1 && ctx.Err() == nil {
			return fmt.Errorf("failed retained join did not request native abort")
		}
		if closeCalls == 2 && ctx.Err() != nil {
			return fmt.Errorf("actual native join inherited the expired budget")
		}
		// Both peers are still live until native process ownership has joined.
		response, err := peer.Client().Get(peer.URL + "/health")
		if err != nil {
			return err
		}
		response.Body.Close()
		if response.StatusCode != 200 {
			return fmt.Errorf("AWS peer closed before native join")
		}
		aborted <- struct{}{}
		return nil
	}}
	manager := newEventBusListener(source.Config, &eventBusLifecycle{store: store, functions: functions}, 30*time.Millisecond)
	manager.devRetained = &devRetainedHTTP{owner: owner, callbacks: peer.Config}
	stopped := make(chan error, 1)
	go func() { stopped <- manager.Shutdown(context.Background()) }()
	select {
	case <-aborted:
	case <-time.After(time.Second):
		t.Fatal("failed join did not abort/join native owner")
	}
	require.Equal(t, 1, owner.Snapshot().WorkCount, "socket closure cannot settle a received publication")
	select {
	case <-stopped:
		t.Fatal("shutdown returned before the received envelope actually finished")
	default:
	}
	require.NoError(t, readCognitoStore(context.Background(), store), "stores must remain available through late request join")
	close(release)
	released = true
	err = <-stopped
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, 2, closeCalls, "abort must be followed by actual joined cleanup")
	<-clientDone
	require.Zero(t, owner.Snapshot().WorkCount)
	require.False(t, owner.Snapshot().FixtureSafe)
	require.NoError(t, readCognitoStore(context.Background(), store), "failed final shutdown retains dirty store")
	require.ErrorIs(t, manager.Shutdown(context.Background()), context.DeadlineExceeded)
}

type retainedCaptureWriter struct{ fail bool }

func (w *retainedCaptureWriter) Write(data []byte) (int, error) {
	if w.fail {
		return 0, io.ErrClosedPipe
	}
	return len(data), nil
}

func TestHeldNativeUnsubscribeCaptureFailurePreventsResume(t *testing.T) {
	writer := &retainedCaptureWriter{}
	capture := messaging.NewSNSCapture(writer)
	broker := messaging.NewBroker("us-east-1", "000000000000", 4100)
	broker.SetSNSCapture(capture)
	topic := broker.CreateTopic("retained-unsubscribe")
	subscription, err := broker.Subscribe(topic.ARN, "email", "owned@example.test", nil)
	require.NoError(t, err)
	router := server.New(server.Services{Messaging: messaging.NewHandler(broker)})
	request := httptest.NewRequest("POST", "/", strings.NewReader(url.Values{"Action": {"ConfirmSubscription"}, "Version": {"2010-03-31"}, "TopicArn": {topic.ARN}, "Token": {subscription.Token}}.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	require.Equal(t, 200, response.Code, response.Body.String())
	owner := devquiescence.New(capture.Err)
	held, err := owner.Quiesce(t.Context())
	require.NoError(t, err)
	require.True(t, held.FixtureSafe)
	writer.fail = true
	request = httptest.NewRequest("POST", "/", strings.NewReader(url.Values{"Action": {"Unsubscribe"}, "Version": {"2010-03-31"}, "SubscriptionArn": {subscription.ARN}}.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response = httptest.NewRecorder()
	owner.Wrap(devquiescence.Callback, router, retainedCleanupHandler(router)).ServeHTTP(response, request)
	require.Equal(t, 500, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), "Unable to capture unsubscribe confirmation")
	require.ErrorIs(t, capture.Err(), io.ErrClosedPipe)
	require.False(t, owner.Snapshot().FixtureSafe)
	require.Zero(t, owner.Snapshot().CleanupEnvelopes)
	_, err = owner.Resume(held.Generation)
	require.ErrorIs(t, err, devquiescence.ErrEvidence)
	_, err = owner.Quiesce(t.Context())
	require.ErrorIs(t, err, devquiescence.ErrEvidence)
	require.ErrorIs(t, capture.Close(), io.ErrClosedPipe)
}
