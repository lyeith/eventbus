//go:build sdksmoke && integration

package sdk

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/app"
	"github.com/lyeith/eventbus/internal/cognito"
	"github.com/lyeith/eventbus/internal/cognitotrigger"
	"github.com/lyeith/eventbus/internal/devquiescence"
	"github.com/lyeith/eventbus/internal/eventsource"
	"github.com/lyeith/eventbus/internal/firehose"
	"github.com/lyeith/eventbus/internal/gateway"
	"github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/localexec"
	"github.com/lyeith/eventbus/internal/messaging"
	"github.com/lyeith/eventbus/internal/scheduler"
	"github.com/lyeith/eventbus/internal/server"
	"github.com/lyeith/eventbus/internal/ses"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// The SDK lane starts its own installed RustFS binary and disposable data. It
// never points at a developer's inbox or application object store.
func newRetainedStackRustFS(t *testing.T) string {
	t.Helper()
	binary := os.Getenv("EVENTBUS_SMOKE_RUSTFS")
	if binary == "" {
		binary = "/home/spite/.local/share/ssd-dev/tools/rustfs/rc.2/rustfs"
	}
	require.True(t, filepath.IsAbs(binary), "select an already installed absolute RustFS binary")
	_, err := os.Stat(binary)
	require.NoError(t, err)
	reservation, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := reservation.Addr().String()
	require.NoError(t, reservation.Close())
	directory := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(directory, "data"), 0700))
	ctx, cancel := context.WithCancel(context.Background())
	command := exec.CommandContext(ctx, binary, "server", filepath.Join(directory, "data"), "--address", address, "--console-address", "127.0.0.1:0")
	command.Env = []string{"HOME=" + directory, "RUSTFS_ACCESS_KEY=test", "RUSTFS_SECRET_KEY=testtest123", "RUSTFS_CONSOLE_ENABLE=false", "NO_PROXY=*"}
	for _, name := range []string{"PATH", "TMPDIR", "TMP", "TEMP", "LD_LIBRARY_PATH"} {
		if value := os.Getenv(name); value != "" {
			command.Env = append(command.Env, name+"="+value)
		}
	}
	diagnostics := localexec.NewBoundedOutput(1 << 20)
	command.Stdout, command.Stderr = diagnostics, diagnostics
	require.NoError(t, localexec.Configure(command))
	require.NoError(t, command.Start())
	done := make(chan error, 1)
	go func() {
		_ = command.Wait() // Owned cancellation normally terminates RustFS by signal.
		done <- localexec.Cleanup(command)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case cleanupErr := <-done:
			require.NoError(t, cleanupErr, "owned RustFS process group must be settled")
		case <-time.After(8 * time.Second):
			t.Errorf("owned RustFS did not join: %s", diagnostics.String())
		}
		require.False(t, retainedSDKAlive(command.Process.Pid), "owned RustFS child must be reaped")
	})
	endpoint := "http://" + address
	client := &http.Client{Timeout: time.Second}
	defer client.CloseIdleConnections()
	require.Eventually(t, func() bool {
		response, err := client.Get(endpoint + "/health/ready")
		if err != nil {
			return false
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		return response.StatusCode == http.StatusOK
	}, 15*time.Second, 25*time.Millisecond, "owned RustFS readiness: %s", diagnostics.String())
	return endpoint
}

// The queue adapter binds both native receive/ack and retained custody to one
// queue instance. It carries no receipt, batching, retry or execution policy.
type retainedStackQueueSource struct{ broker *messaging.Broker }
type retainedStackQueue struct{ sdkMappingQueue }

func (source retainedStackQueueSource) ResolveQueue(ctx context.Context, arn string) (eventsource.Queue, error) {
	bound, err := (sdkMappingSource{broker: source.broker}).ResolveQueue(ctx, arn)
	if err != nil {
		return nil, err
	}
	return &retainedStackQueue{sdkMappingQueue: bound.(sdkMappingQueue)}, nil
}
func (queue *retainedStackQueue) RegisterRetained() (func(), error) {
	return queue.broker.RegisterSQSLambdaCustody(queue.queue)
}
func (queue *retainedStackQueue) PendingRetained() (bool, <-chan struct{}, error) {
	return queue.broker.SQSLambdaCustodyState(queue.queue)
}
func (queue *retainedStackQueue) ReceiveRetained(ctx context.Context, max int) (eventsource.Batch, error) {
	return queue.broker.ReceiveOwnedSQSLambdaBatchContext(ctx, queue.queue, max, time.Second, eventsource.MaxBatchPayloadBytes)
}

type retainedStackRow struct {
	Stage     string   `json:"stage"`
	Chain     string   `json:"chain"`
	RequestID string   `json:"request_id"`
	PID       int      `json:"pid"`
	Attempt   int      `json:"attempt"`
	IDs       []string `json:"ids"`
	Receipts  []string `json:"receipts"`
	ARNs      []string `json:"source_arns"`
	Counts    []int    `json:"counts"`
}

type retainedStackFixture struct {
	root, python, node, pool, s3 string
	source, callback, control    *httptest.Server
	proxy, edge                  *httptest.Server
	owner                        *devquiescence.Coordinator
	functions                    *lambda.Service
	mappings                     *eventsource.Service
	schedules                    *scheduler.Service
	triggers                     *cognitotrigger.Runner
	broker                       *messaging.Broker
	delivery                     *firehose.FirehoseManager
	store                        *cognito.CognitoStore
	sns                          *messaging.SNSCapture
	emails                       *ses.SESManager
	emailCapture                 *ses.SESCapture
	gateway                      *gateway.Gateway
	available                    atomic.Bool
	cancelRequeue                context.CancelFunc
	requeueDone                  <-chan struct{}
	closed                       sync.Once
}

func newRetainedStackFixture(t *testing.T, python, node, s3Endpoint string) *retainedStackFixture {
	t.Helper()
	fixture := &retainedStackFixture{root: t.TempDir(), python: python, node: node, pool: "us-east-1_retainedstack", s3: s3Endpoint,
		source: httptest.NewUnstartedServer(nil), callback: httptest.NewUnstartedServer(nil), control: httptest.NewUnstartedServer(nil)}
	t.Cleanup(func() { fixture.close(t) })
	sourceURL := "http://" + fixture.source.Listener.Addr().String()
	callbackURL := "http://" + fixture.callback.Listener.Addr().String()
	controlURL := "http://" + fixture.control.Listener.Addr().String()
	var err error
	fixture.sns, err = messaging.OpenSNSCapture(filepath.Join(fixture.root, "sns.jsonl"))
	require.NoError(t, err)
	fixture.emailCapture, err = ses.OpenSESCapture(filepath.Join(fixture.root, "ses.jsonl"))
	require.NoError(t, err)
	fixture.emails = ses.NewSESManager(ses.SESFixtures{}, fixture.emailCapture)
	target, err := url.Parse(s3Endpoint)
	require.NoError(t, err)
	forward := httputil.NewSingleHostReverseProxy(target)
	fixture.proxy = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !fixture.available.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		forward.ServeHTTP(w, r)
	}))
	fixture.delivery = firehose.NewFirehoseManager("us-east-1", "000000000000", fixture.proxy.URL, "test", "testtest123")
	require.NoError(t, fixture.delivery.SetMetadataExtractor(firehose.NewGoJQMetadataExtractor()))
	fixture.owner = devquiescence.NewWithOptions(devquiescence.Options{
		Checks:     []func() error{fixture.sns.Err, fixture.emailCapture.Err, fixture.delivery.DevEvidence},
		DrainHooks: []devquiescence.DrainHook{{Start: fixture.delivery.DevBeginDrain, Resume: fixture.delivery.DevResume}},
	})
	require.NoError(t, fixture.owner.SetCallbackOrigin(callbackURL))
	require.NoError(t, fixture.delivery.SetDevActivity(fixture.owner))
	fixture.broker = messaging.NewBroker("us-east-1", "000000000000", fixture.source.Listener.Addr().(*net.TCPAddr).Port)
	require.NoError(t, fixture.broker.SetDevActivity(fixture.owner))
	fixture.broker.SetSNSCapture(fixture.sns)
	fixture.broker.SetFirehoseDelivery(fixture.delivery)

	lostStarted, triggerStarted := make(chan struct{}), make(chan struct{})
	var lostOnce, triggerOnce sync.Once
	controls := devquiescence.NewHandler(fixture.owner)
	fixture.control.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/__fixture/observed" && r.Method == http.MethodPost {
			var row retainedStackRow
			if json.NewDecoder(r.Body).Decode(&row) != nil {
				http.Error(w, "invalid observation", http.StatusBadRequest)
				return
			}
			if row.Stage == "persist-started" && row.Chain == "lost" && row.Attempt == 1 {
				lostOnce.Do(func() { close(lostStarted) })
			}
			if row.Stage == "trigger-started" {
				triggerOnce.Do(func() { close(triggerStarted) })
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		controls.ServeHTTP(w, r)
	})
	fixture.control.Start()
	environment := map[string]string{"STACK_ROOT": fixture.root, "STACK_SOURCE": sourceURL, "STACK_CALLBACK": callbackURL,
		"STACK_CONTROL": controlURL, "STACK_S3": s3Endpoint}
	functions := make(map[string]lambda.Function)
	for name, handler := range map[string]string{"batch": "batch_handler", "sns": "sns_handler", "persist": "persist_handler",
		"scheduled": "scheduled_handler", "cleanup": "cleanup_handler", "cleanup-nested": "cleanup_nested_handler", "gateway": "gateway_handler", "authorize": "authorizer_handler"} {
		timeout := 20 * time.Second
		if name == "batch" {
			timeout = 10 * time.Second
		}
		functions["stack-"+name+":live"] = lambda.Function{Runtime: "python", Command: []string{python, "-E", "-s"},
			Handler: fixturePath("python", "retained_stack_smoke.py") + "#" + handler, Timeout: timeout, Environment: environment}
	}
	fixture.functions, err = lambda.NewService(&lambda.Config{Functions: functions, DevActivity: fixture.owner,
		DevAsync: &lambda.DevAsyncConfig{Workers: 4, Capacity: 32, RetryDelays: []time.Duration{4 * time.Second, 4 * time.Second}, LogPath: filepath.Join(fixture.root, "async.jsonl")}}, fixture.root)
	require.NoError(t, err)
	fixture.broker.SetLambdaDelivery(sdkSNSLambdaDelivery{functions: fixture.functions})
	fixture.mappings, err = eventsource.New(eventsource.Options{Region: "us-east-1", AccountID: "000000000000",
		Dev: eventsource.DevOptions{Source: fixture.owner, Activity: fixture.owner}}, retainedStackQueueSource{fixture.broker}, sdkMappingInvoker{fixture.functions})
	require.NoError(t, err)
	fixture.schedules, err = scheduler.New(scheduler.Options{Region: "us-east-1", AccountID: "000000000000",
		Dev: scheduler.DevOptions{Source: fixture.owner, ExactSeconds: true}}, schedulerSDKTarget{fixture.functions})
	require.NoError(t, err)
	fixture.store, err = cognito.OpenCognitoStore(filepath.Join(fixture.root, "cognito.db"))
	require.NoError(t, err)
	require.NoError(t, fixture.store.UpsertPool(t.Context(), fixture.pool, "us-east-1"))
	entry := func(name string) *cognitotrigger.Entry {
		return &cognitotrigger.Entry{Handler: "triggers/" + name + "#handler", TimeoutSeconds: 5}
	}
	create := entry("retained_create.mjs")
	create.Env = map[string]string{"STACK_ROOT": fixture.root, "STACK_CONTROL": controlURL, "SES_ENDPOINT_URL": callbackURL}
	fixture.triggers, err = app.NewCognitoTriggers(&cognitotrigger.Config{Node: node, DevActivity: fixture.owner,
		Pools: map[string]cognitotrigger.Pool{fixture.pool: {DefineAuthChallenge: entry("define.mjs"), CreateAuthChallenge: create, VerifyAuthChallengeResponse: entry("verify.mjs")}}}, fixturePath("javascript"))
	require.NoError(t, err)
	aws := server.New(server.Services{Messaging: messaging.NewHandler(fixture.broker), Lambda: fixture.functions,
		EventSources: eventsource.NewHandler(fixture.mappings), Scheduler: scheduler.NewHandler(fixture.schedules), Firehose: firehose.NewHandler(fixture.delivery),
		Cognito:     cognito.NewHandler(fixture.store, cognito.Options{DevProfile: cognito.DevProfileLegacyFixtures, IssuerBase: sourceURL, Triggers: fixture.triggers}),
		CognitoURLs: &server.CognitoURLs{Issuer: sourceURL, JWKS: sourceURL}, SES: ses.NewHandler(fixture.emails)})
	// Exact declared cleanup matches native target and invocation mode before
	// entering the shared cleanup epoch; all execution still uses the native router.
	cleanup := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/2015-03-31/functions/stack-cleanup:live/invocations" ||
			(r.Header.Get("X-Amz-Invocation-Type") != "" && r.Header.Get("X-Amz-Invocation-Type") != "RequestResponse") || r.URL.RawQuery != "" {
			devquiescence.WriteAdmissionError(w, r, devquiescence.ErrFenced)
			return
		}
		complete, err := fixture.owner.BeginCleanup(fixture.owner.Snapshot().Generation, "cleanup.registered", "stack-cleanup")
		if err != nil {
			devquiescence.WriteAdmissionError(w, r, err)
			return
		}
		defer complete(nil)
		aws.ServeHTTP(w, r)
	})
	// Owned transport faults close the SDK connection after actual child
	// admission, while the same native handler retains an independent context.
	lostReply := func(w http.ResponseWriter, admitted <-chan struct{}, execute func()) {
		finished, observer := make(chan struct{}), make(chan struct{})
		go func() {
			defer close(observer)
			select {
			case <-admitted:
				connection, _, err := w.(http.Hijacker).Hijack()
				if err == nil {
					_ = connection.Close()
				}
			case <-finished:
			}
		}()
		execute()
		close(finished)
		<-observer
	}
	var sourceDropped, nestedDropped, authDropped atomic.Bool
	sourceWork := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Amz-Target") == "AWSCognitoIdentityProviderService.RespondToAuthChallenge" && authDropped.CompareAndSwap(false, true) {
			lostReply(w, triggerStarted, func() {
				recorder := httptest.NewRecorder()
				aws.ServeHTTP(recorder, r.WithContext(context.WithoutCancel(r.Context())))
				if recorder.Code == http.StatusOK {
					_ = os.WriteFile(filepath.Join(fixture.root, "cognito-response.json"), recorder.Body.Bytes(), 0600)
				}
			})
			return
		}
		if r.Header.Get("X-Amz-Target") == "" && r.ParseForm() == nil && r.FormValue("Action") == "Publish" && strings.Contains(r.FormValue("Message"), `"drop_source": true`) && sourceDropped.CompareAndSwap(false, true) {
			aws.ServeHTTP(httptest.NewRecorder(), r)
			connection, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = connection.Close()
			}
			return
		}
		aws.ServeHTTP(w, r)
	})
	callbackWork := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/2015-03-31/functions/stack-persist:live/invocations" {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "fixture read failed", 500)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			var payload struct {
				Chain   string
				Attempt int
			}
			_ = json.Unmarshal(body, &payload)
			if payload.Chain == "lost" && payload.Attempt == 1 && nestedDropped.CompareAndSwap(false, true) {
				lostReply(w, lostStarted, func() { aws.ServeHTTP(httptest.NewRecorder(), r.WithContext(context.WithoutCancel(r.Context()))) })
				return
			}
		}
		aws.ServeHTTP(w, r)
	})
	fixture.source.Config.Handler = fixture.owner.Wrap(devquiescence.Source, sourceWork, nil)
	fixture.callback.Config.Handler = fixture.owner.Wrap(devquiescence.Callback, callbackWork, cleanup)
	fixture.source.Start()
	fixture.callback.Start()
	workers, cancel := context.WithCancel(context.Background())
	fixture.cancelRequeue = cancel
	fixture.requeueDone = fixture.broker.StartRequeueLoop(workers)
	zero := 0
	fixture.gateway, err = gateway.New(gateway.Config{RetainedOwnerControlURL: fixture.control.URL + devquiescence.ControlPath,
		Authorizers: map[string]gateway.AuthorizerConfig{"jwt": {Type: "REQUEST", PayloadFormatVersion: "2.0", EnableSimpleResponses: true,
			InvokeURL: callbackURL + "/2015-03-31/functions/stack-authorize:live/invocations", TTL: &zero, IdentitySources: []string{"$request.header.Authorization"}}},
		Routes: []gateway.RouteConfig{{Path: "/api/work", Method: "POST", Authorizer: "jwt", Integration: gateway.IntegrationConfig{Type: "AWS_PROXY", PayloadFormatVersion: "2.0",
			InvokeURL: callbackURL + "/2015-03-31/functions/stack-gateway:live/invocations"}}}}, gateway.Options{Logger: zerolog.Nop()})
	require.NoError(t, err)
	fixture.edge = httptest.NewServer(fixture.gateway)
	return fixture
}

func (fixture *retainedStackFixture) close(t *testing.T) {
	t.Helper()
	fixture.closed.Do(func() {
		fixture.available.Store(true)
		for _, gate := range []string{"release-batches", "release-lost-1", "release-lost-2", "release-lost-3", "release-cognito", "release-scheduled", "release-cleanup", "release-shutdown"} {
			_ = os.WriteFile(filepath.Join(fixture.root, gate), nil, 0600)
		}
		barrierCtx, cancelBarrier := context.WithTimeout(context.Background(), 25*time.Second)
		var failures []error
		if fixture.owner != nil {
			fixture.owner.Shutdown()
			_, err := fixture.owner.Quiesce(barrierCtx)
			failures = append(failures, err)
			if err != nil && fixture.edge != nil {
				// Failed proofs still own cleanup. Cancel blocked client reads before
				// joining the gateway, keeping native callback/control peers alive.
				fixture.edge.CloseClientConnections()
			}
		}
		cancelBarrier()
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		defer cancel()
		if fixture.gateway != nil {
			failures = append(failures, fixture.gateway.Close())
		}
		if fixture.mappings != nil {
			failures = append(failures, fixture.mappings.Close(ctx))
		}
		if fixture.schedules != nil {
			failures = append(failures, fixture.schedules.Close(ctx))
		}
		if fixture.triggers != nil {
			failures = append(failures, fixture.triggers.Close(ctx))
		}
		if fixture.functions != nil {
			failures = append(failures, fixture.functions.DrainAsync(ctx))
		}
		if fixture.edge != nil {
			fixture.edge.Close()
		}
		fixture.source.Close()
		fixture.callback.Close()
		fixture.control.Close()
		if fixture.cancelRequeue != nil {
			fixture.cancelRequeue()
			<-fixture.requeueDone
		}
		if fixture.functions != nil {
			failures = append(failures, fixture.functions.Close(ctx))
		}
		if fixture.delivery != nil {
			failures = append(failures, fixture.delivery.ShutdownContext(ctx))
		}
		if fixture.proxy != nil {
			fixture.proxy.Close()
		}
		if fixture.emails != nil {
			failures = append(failures, fixture.emails.Close())
		}
		if fixture.sns != nil {
			failures = append(failures, fixture.sns.Close())
		}
		if fixture.store != nil {
			failures = append(failures, fixture.store.Close())
		}
		if err := errors.Join(failures...); err != nil {
			t.Errorf("retained stack owned join: %v", err)
		}
	})
}

func (fixture *retainedStackFixture) phase(t *testing.T, language, phase string) {
	t.Helper()
	environment := append(sdkEnvironment(t.TempDir(), "", "", ""), "STACK_ROOT="+fixture.root, "STACK_POOL="+fixture.pool,
		"STACK_SOURCE="+fixture.source.URL, "STACK_CALLBACK="+fixture.callback.URL, "STACK_CONTROL="+fixture.control.URL,
		"STACK_S3="+fixture.s3, "STACK_PHASE="+phase, "STACK_SES_CAPTURE="+filepath.Join(fixture.root, "ses.jsonl"))
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
	defer cancel()
	var output []byte
	var err error
	if language == "python" {
		output, err = runSDKProcess(ctx, fixture.python, fixturePath("python", "retained_stack_smoke.py"), environment)
	} else {
		output, err = runJavascriptSDKProcess(ctx, fixture.node, fixturePath("javascript", "retained_stack_smoke.mjs"), nil, environment)
	}
	t.Logf("actual retained-stack %s SDK %s (origin exited):\n%s", language, phase, output)
	require.NoError(t, err)
}

func (fixture *retainedStackFixture) rows(t *testing.T, chain, stage string) []retainedStackRow {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(fixture.root, "row-*.json"))
	require.NoError(t, err)
	var rows []retainedStackRow
	for _, filename := range files {
		data, err := os.ReadFile(filename)
		require.NoError(t, err)
		var row retainedStackRow
		require.NoError(t, json.Unmarshal(data, &row))
		if row.Chain == chain && (stage == "" || row.Stage == stage) {
			rows = append(rows, row)
		}
	}
	return rows
}

func (fixture *retainedStackFixture) release(t *testing.T, gate string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(fixture.root, gate), nil, 0600))
}

func (fixture *retainedStackFixture) controlCall(t *testing.T, operation string, input any, want int) devquiescence.Snapshot {
	t.Helper()
	body, err := json.Marshal(input)
	require.NoError(t, err)
	request, err := http.NewRequest(http.MethodPost, fixture.control.URL+devquiescence.ControlPath+operation, bytes.NewReader(body))
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 20 * time.Second}
	defer client.CloseIdleConnections()
	response, err := client.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, want, response.StatusCode, "%s", data)
	var snapshot devquiescence.Snapshot
	require.NoError(t, json.Unmarshal(data, &snapshot))
	require.Equal(t, "eventbus.retained-owner.v1", snapshot.SchemaVersion)
	require.NotContains(t, string(data), "Bearer ")
	require.NotContains(t, string(data), "privateChallengeParameters")
	return snapshot
}

func stackJSONObject(t *testing.T, filename string) map[string]any {
	t.Helper()
	data, err := os.ReadFile(filename)
	require.NoError(t, err)
	var value map[string]any
	require.NoError(t, json.Unmarshal(data, &value))
	return value
}

// A real received chunked root is held before integration Invoke. Native Lambda
// activity cannot account for this wait; the remote ingress lease must do so.
func (fixture *retainedStackFixture) delayedGateway(t *testing.T) func() {
	t.Helper()
	auth := stackJSONObject(t, filepath.Join(fixture.root, "auth.json"))
	resource := stackJSONObject(t, filepath.Join(fixture.root, "resources.json"))
	address := strings.TrimPrefix(fixture.edge.URL, "http://")
	connection, err := net.Dial("tcp", address)
	require.NoError(t, err)
	t.Cleanup(func() { _ = connection.Close() })
	require.NoError(t, connection.SetDeadline(time.Now().Add(30*time.Second)))
	_, err = fmt.Fprintf(connection, "POST /api/work HTTP/1.1\r\nHost: %s\r\nAuthorization: Bearer %s\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n", address, auth["idToken"])
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		for _, activity := range fixture.owner.Snapshot().Activities {
			if activity.Kind == "http.gateway" {
				return true
			}
		}
		return false
	}, 3*time.Second, 10*time.Millisecond)
	require.Empty(t, fixture.rows(t, "gateway", "gateway-started"))
	return func() {
		body, err := json.Marshal(map[string]any{"suite": resource["suite"], "chain": "gateway"})
		require.NoError(t, err)
		_, err = fmt.Fprintf(connection, "%x\r\n%s\r\n0\r\n\r\n", len(body), body)
		require.NoError(t, err)
		response, err := http.ReadResponse(bufio.NewReader(connection), nil)
		require.NoError(t, err)
		defer response.Body.Close()
		data, err := io.ReadAll(response.Body)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, response.StatusCode, "%s", data)
		require.JSONEq(t, `{"ok":true}`, string(data))
		require.NoError(t, connection.Close())
	}
}

func (fixture *retainedStackFixture) gatewayFenced(t *testing.T) {
	t.Helper()
	client := &http.Client{Timeout: 8 * time.Second}
	defer client.CloseIdleConnections()
	response, err := client.Post(fixture.edge.URL+"/api/work", "application/json", strings.NewReader(`{"chain":"unrelated"}`))
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
}

func TestRetainedStackNativeSDKRecovery(t *testing.T) {
	python, node := sdkPython(t), sdkNode(t)
	fixture := newRetainedStackFixture(t, python, node, newRetainedStackRustFS(t))
	peer := newRetainedSDKFixture(t, python, "B")
	fixture.phase(t, "javascript", "provision")
	fixture.phase(t, "python", "provision")
	require.Eventually(t, func() bool { return len(fixture.rows(t, "initial", "batch-started")) == 2 }, 3*time.Second, 10*time.Millisecond)
	initial := fixture.rows(t, "initial", "batch-started")
	require.Len(t, initial, 2, "native concurrency two must be achievable")
	for _, row := range initial {
		require.Len(t, row.IDs, 5)
		require.True(t, retainedSDKAlive(row.PID))
	}
	fixture.phase(t, "javascript", "schedules")
	finishGateway := fixture.delayedGateway(t)
	fixture.phase(t, "javascript", "lost-auth")
	require.Eventually(t, func() bool { return len(fixture.rows(t, "scheduled", "scheduled-started")) == 1 }, 3*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		failed, live := fixture.rows(t, "lost", "sns-failed"), fixture.rows(t, "lost", "persist-started")
		return len(failed) == 1 && len(live) >= 1 && !retainedSDKAlive(failed[0].PID) && retainedSDKAlive(live[0].PID)
	}, 2*time.Second, 10*time.Millisecond)
	dirty := fixture.controlCall(t, "/quiesce", map[string]int{"timeout_ms": 100}, http.StatusRequestTimeout)
	require.Equal(t, devquiescence.Draining, dirty.State)
	require.False(t, dirty.FixtureSafe)
	require.Greater(t, dirty.WorkCount, 0)
	fixture.gatewayFenced(t)
	resource := stackJSONObject(t, filepath.Join(fixture.root, "resources.json"))
	for _, batch := range initial {
		for _, arn := range batch.ARNs {
			require.Equal(t, resource["queue_arn"], arn, "mapping keeps the original queue identity")
		}
	}
	require.Eventually(t, func() bool {
		state, ok := fixture.delivery.Snapshot(resource["stream"].(string))
		return ok && state.PendingObjects > 0 && state.LastDeliveryError != ""
	}, 2*time.Second, 10*time.Millisecond, "accepted buffering must become retained pending delivery")
	fixture.release(t, "release-cognito")
	fixture.phase(t, "python", "source-refusals")
	fixture.release(t, "release-scheduled")
	fixture.release(t, "release-batches")
	fixture.release(t, "release-lost-1")
	finishGateway()
	require.Eventually(t, func() bool {
		for _, row := range fixture.rows(t, "lost", "") {
			if retainedSDKAlive(row.PID) {
				return false
			}
		}
		for _, record := range fixture.functions.AsyncSnapshot() {
			if record.FunctionName == "stack-sns:live" && record.State == "retrying" && record.Attempts == 1 {
				return true
			}
		}
		return false
	}, 2*time.Second, 10*time.Millisecond, "accepted async retry must stay joined while its actual children are absent")
	dirty = fixture.controlCall(t, "/quiesce", map[string]int{"timeout_ms": 100}, http.StatusRequestTimeout)
	require.False(t, dirty.FixtureSafe)
	require.Eventually(t, func() bool { return len(fixture.rows(t, "lost", "persist-started")) == 2 }, 6*time.Second, 10*time.Millisecond)
	fixture.release(t, "release-lost-2")
	failed := fixture.rows(t, "initial", "batch-failed")
	require.Len(t, failed, 1)
	require.False(t, retainedSDKAlive(failed[0].PID))
	queue := fixture.broker.GetQueueByARN(resource["queue_arn"].(string))
	pending, _, err := fixture.broker.SQSLambdaCustodyState(queue)
	require.NoError(t, err)
	require.True(t, pending, "failed whole-batch visibility remains retained without a child")
	require.Eventually(t, func() bool { return len(fixture.rows(t, "initial", "batch-completed")) == 2 }, 12*time.Second, 15*time.Millisecond)
	completed := fixture.rows(t, "initial", "batch-completed")
	var redelivered *retainedStackRow
	for index := range completed {
		if slices.Contains(completed[index].Counts, 2) {
			redelivered = &completed[index]
		}
	}
	require.NotNil(t, redelivered)
	require.ElementsMatch(t, failed[0].IDs, redelivered.IDs, "whole native batch must return with the same IDs")
	for _, receipt := range redelivered.Receipts {
		require.NotContains(t, failed[0].Receipts, receipt, "redelivery needs current receipts")
	}
	require.Eventually(t, func() bool {
		pending, _, err := fixture.broker.SQSLambdaCustodyState(queue)
		return err == nil && !pending
	}, 2*time.Second, 10*time.Millisecond, "native successful completion must release exact accepted-message custody")
	require.Empty(t, fixture.rows(t, "future", "scheduled-started"), "held epoch must pause future roots without deleting schedules")
	dirty = fixture.controlCall(t, "/quiesce", map[string]int{"timeout_ms": 100}, http.StatusRequestTimeout)
	require.False(t, dirty.FixtureSafe, "pending destination delivery remains accepted work after children finish")
	fixture.available.Store(true)
	held := fixture.controlCall(t, "/quiesce", map[string]int{"timeout_ms": 15000}, http.StatusOK)
	require.True(t, held.FixtureSafe)
	require.Zero(t, held.WorkCount)
	for _, chain := range []string{"initial", "lost", "scheduled", "gateway", "cognito"} {
		for _, row := range fixture.rows(t, chain, "") {
			require.False(t, retainedSDKAlive(row.PID), "safe follows actual child join")
		}
	}
	require.Len(t, fixture.rows(t, "cognito", "trigger-completed"), 1)
	fixture.phase(t, "python", "s3-proof")
	fixture.phase(t, "python", "held-refusals")
	fixture.phase(t, "python", "bad-auth")
	held = fixture.controlCall(t, "/quiesce", map[string]int{"timeout_ms": 5000}, http.StatusOK)
	require.True(t, held.FixtureSafe)
	require.Empty(t, fixture.rows(t, "cleanup", "cleanup-started"), "invalid real JWT must not start deletion")
	fixture.phase(t, "python", "cleanup")
	require.Eventually(t, func() bool {
		root, child := fixture.rows(t, "cleanup", "cleanup-returned"), fixture.rows(t, "cleanup", "cleanup-nested-started")
		return len(root) == 1 && len(child) == 1 && !retainedSDKAlive(root[0].PID) && retainedSDKAlive(child[0].PID)
	}, 3*time.Second, 10*time.Millisecond)
	require.False(t, fixture.owner.Snapshot().FixtureSafe)
	fixture.gatewayFenced(t)
	fixture.controlCall(t, "/resume", map[string]uint64{"generation": held.Generation}, http.StatusConflict)
	dirty = fixture.controlCall(t, "/quiesce", map[string]int{"timeout_ms": 100}, http.StatusRequestTimeout)
	require.False(t, dirty.FixtureSafe)
	fixture.release(t, "release-cleanup")
	held = fixture.controlCall(t, "/quiesce", map[string]int{"timeout_ms": 5000}, http.StatusOK)
	require.True(t, held.FixtureSafe)
	for _, row := range fixture.rows(t, "cleanup", "") {
		require.False(t, retainedSDKAlive(row.PID))
	}
	require.Len(t, fixture.rows(t, "cleanup", "batch-completed"), 1, "cleanup native Send must execute its owned mapping descendant")
	require.Len(t, fixture.rows(t, "cleanup", "cleanup-nested-completed"), 1)
	fixture.controlCall(t, "/resume", map[string]uint64{"generation": held.Generation + 1}, http.StatusConflict)
	opened := fixture.controlCall(t, "/resume", map[string]uint64{"generation": held.Generation}, http.StatusOK)
	require.Equal(t, held.Generation+1, opened.Generation)
	fixture.controlCall(t, "/resume", map[string]uint64{"generation": held.Generation}, http.StatusConflict)
	fixture.phase(t, "python", "resume")
	fixture.phase(t, "javascript", "resume")
	require.Eventually(t, func() bool {
		count := 0
		for _, row := range fixture.rows(t, "next", "batch-completed") {
			count += len(row.IDs)
		}
		return count == 5 && len(fixture.rows(t, "next", "sns-completed")) == 1 && len(fixture.rows(t, "future", "scheduled-completed")) == 1
	}, 5*time.Second, 15*time.Millisecond)
	held = fixture.controlCall(t, "/quiesce", map[string]int{"timeout_ms": 5000}, http.StatusOK)
	require.True(t, held.FixtureSafe)
	fixture.phase(t, "python", "s3-proof-next")
	fixture.controlCall(t, "/resume", map[string]uint64{"generation": held.Generation}, http.StatusOK)
	fixture.phase(t, "python", "shutdown")
	require.Eventually(t, func() bool { return len(fixture.rows(t, "shutdown", "batch-started")) >= 2 }, 3*time.Second, 10*time.Millisecond)
	fixture.owner.Shutdown()
	dirty = fixture.controlCall(t, "/quiesce", map[string]int{"timeout_ms": 100}, http.StatusRequestTimeout)
	require.Equal(t, devquiescence.Shutdown, dirty.State)
	runRetainedSDKPhase(t, peer, python, "origin", "peer")
	require.Eventually(t, func() bool {
		return len(retainedSDKRows(t, peer, "peer", "root-completed")) == 1 && peer.coordinator.Snapshot().WorkCount == 0
	}, 3*time.Second, 10*time.Millisecond)
	require.Equal(t, devquiescence.Open, peer.coordinator.Snapshot().State)
	fixture.release(t, "release-shutdown")
	joined := fixture.controlCall(t, "/quiesce", map[string]int{"timeout_ms": 8000}, http.StatusOK)
	require.Equal(t, devquiescence.Shutdown, joined.State)
	require.False(t, joined.FixtureSafe)
	require.Zero(t, joined.WorkCount)
	for _, row := range fixture.rows(t, "shutdown", "") {
		require.False(t, retainedSDKAlive(row.PID))
	}
	fixture.close(t) // all stores/captures close only after joined owners
}
