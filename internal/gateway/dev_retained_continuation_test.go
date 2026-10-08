package gateway

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/devquiescence"
	"github.com/stretchr/testify/require"
)

func newRetainedContinuationFixture(t *testing.T, authVersion string) *retainedGatewayFixture {
	t.Helper()
	fixture := newRetainedGatewayFixtureVersion(t, authVersion)
	cfg := fixture.gateway.config.clone()
	require.NoError(t, fixture.gateway.Close())
	cfg.RetainedOwnerContinuationPort = cfg.Port + 1
	service, err := New(cfg, Options{})
	require.NoError(t, err)
	fixture.gateway = service
	service.retained.mu.Lock()
	service.retained.budget = 100 * time.Millisecond
	service.retained.mu.Unlock()
	t.Cleanup(func() {
		if err := service.Close(); err != nil && !fixture.expectCloseFailure {
			t.Errorf("close continuation gateway: %v", err)
		}
	})
	return fixture
}

func retainedContinuationRequest(fixture *retainedGatewayFixture, body io.Reader, authorization string) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	request := httptest.NewRequest("POST", "/items/owned?repeat=one&repeat=two", body)
	if authorization != "" {
		request.Header.Set("Authorization", authorization)
	}
	fixture.gateway.RetainedContinuationHandler().ServeHTTP(response, request)
	return response
}

func beginContinuationParent(t *testing.T, fixture *retainedGatewayFixture) func(error) {
	t.Helper()
	release, _, err := fixture.owner.Load().BeginSource("lambda_invoke", "accepted-parent")
	require.NoError(t, err)
	t.Cleanup(func() { release(nil) })
	return release
}

func drainContinuationParent(t *testing.T, fixture *retainedGatewayFixture) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	snapshot, err := fixture.owner.Load().Quiesce(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, devquiescence.Draining, snapshot.State)
}

func closeContinuationGate(gate chan struct{}) {
	select {
	case <-gate:
	default:
		close(gate)
	}
}

func TestRetainedContinuationOwnsBodyAuthorizationAndIntegration(t *testing.T) {
	fixture := newRetainedContinuationFixture(t, "2.0")
	releaseParent := beginContinuationParent(t, fixture)
	drainContinuationParent(t, fixture)
	authEntered, authRelease := make(chan struct{}), make(chan struct{})
	integrationEntered, integrationRelease := make(chan struct{}), make(chan struct{})
	body := &retainedBody{entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() {
		closeContinuationGate(authRelease)
		closeContinuationGate(body.release)
		closeContinuationGate(integrationRelease)
	})
	transport := fixture.gateway.invokeClient.Transport
	fixture.gateway.invokeClient.Transport = retainedRoundTrip(func(request *http.Request) (*http.Response, error) {
		entered, release := integrationEntered, integrationRelease
		if strings.Contains(request.URL.Path, "/auth/") {
			entered, release = authEntered, authRelease
		}
		close(entered)
		select {
		case <-release:
		case <-request.Context().Done():
			return nil, request.Context().Err()
		}
		return transport.RoundTrip(request)
	})
	result := make(chan *httptest.ResponseRecorder, 1)
	go func() { result <- retainedContinuationRequest(fixture, body, "native-token") }()
	select {
	case <-authEntered:
	case <-time.After(time.Second):
		t.Fatal("continuation did not reach native authorization")
	}
	require.Equal(t, 2, fixture.owner.Load().Snapshot().WorkCount)
	require.Zero(t, body.reads.Load(), "lease must precede authorization and body read")
	fixture.mu.Lock()
	acquisitions := append([]devquiescence.SourceLeaseInput(nil), fixture.acquisitions...)
	fixture.mu.Unlock()
	require.Len(t, acquisitions, 1)
	require.Equal(t, retainedGatewayContinuationKind, acquisitions[0].Kind)
	publicBody := &retainedBody{}
	require.Equal(t, 503, retainedRequest(fixture, publicBody).Code)
	require.Zero(t, publicBody.reads.Load())
	close(authRelease)
	select {
	case <-body.entered:
	case <-time.After(time.Second):
		t.Fatal("authorized continuation did not reach body read")
	}
	require.Equal(t, 2, fixture.owner.Load().Snapshot().WorkCount)
	require.Equal(t, int32(1), fixture.authorizations.Load())
	require.Zero(t, fixture.invocations.Load())
	close(body.release)
	select {
	case <-integrationEntered:
	case <-time.After(time.Second):
		t.Fatal("continuation did not reach native integration")
	}
	releaseParent(nil)
	require.Equal(t, 1, fixture.owner.Load().Snapshot().WorkCount, "continuation owns its whole integration lifetime")
	drainContinuationParent(t, fixture)
	close(integrationRelease)
	select {
	case response := <-result:
		require.Equal(t, 200, response.Code)
		require.Equal(t, "owned", response.Body.String())
	case <-time.After(time.Second):
		t.Fatal("continuation did not join")
	}
	require.Equal(t, int32(1), fixture.invocations.Load())
	snapshot, err := fixture.owner.Load().Quiesce(context.Background())
	require.NoError(t, err)
	require.True(t, snapshot.FixtureSafe)
}

func TestRetainedContinuationRequiresLiveWorkAndNativeIdentity(t *testing.T) {
	fixture := newRetainedContinuationFixture(t, "2.0")
	unread := &retainedBody{}
	require.Equal(t, 503, retainedContinuationRequest(fixture, unread, "token").Code, "private listener cannot create an autonomous root")
	require.Zero(t, unread.reads.Load())
	releaseParent := beginContinuationParent(t, fixture)
	drainContinuationParent(t, fixture)
	unread = &retainedBody{}
	require.Equal(t, 401, retainedContinuationRequest(fixture, unread, "").Code, "continuation keeps native authorizer identity validation")
	require.Zero(t, unread.reads.Load())
	require.Zero(t, fixture.authorizations.Load())
	require.Zero(t, fixture.invocations.Load())
	require.Equal(t, 1, fixture.owner.Load().Snapshot().WorkCount)
	releaseParent(nil)
	snapshot, err := fixture.owner.Load().Quiesce(context.Background())
	require.NoError(t, err)
	require.True(t, snapshot.FixtureSafe)
	unread = &retainedBody{}
	require.Equal(t, 503, retainedContinuationRequest(fixture, unread, "token").Code)
	require.Zero(t, unread.reads.Load())
	response := httptest.NewRecorder()
	fixture.gateway.RetainedContinuationHandler().ServeHTTP(response, httptest.NewRequest("GET", "/health", nil))
	require.Equal(t, 200, response.Code, "private readiness shares the existing unleased exception")
	require.Zero(t, fixture.owner.Load().Snapshot().WorkCount)
}

func TestRetainedContinuationAcquisitionIsAuthoritativeAfterPrecheck(t *testing.T) {
	fixture := newRetainedContinuationFixture(t, "public")
	releaseParent := beginContinuationParent(t, fixture)
	owner := fixture.gateway.retained
	owner.monitorCancel()
	<-owner.monitorDone
	transport := owner.client.Transport
	owner.client.Transport = retainedRoundTrip(func(request *http.Request) (*http.Response, error) {
		if strings.HasSuffix(request.URL.Path, "/acquire") {
			releaseParent(nil)
		}
		return transport.RoundTrip(request)
	})
	owner.start()
	unread := &retainedBody{}
	require.Equal(t, 503, retainedContinuationRequest(fixture, unread, "").Code, "parent can finish between snapshot and acquisition")
	require.Zero(t, unread.reads.Load())
	require.Zero(t, fixture.invocations.Load())
	require.Zero(t, fixture.owner.Load().Snapshot().WorkCount)
}

func TestRetainedContinuationLostAcknowledgementsReconcileOneLease(t *testing.T) {
	for _, acknowledgement := range []string{"acquire", "release"} {
		t.Run(acknowledgement, func(t *testing.T) {
			fixture := newRetainedContinuationFixture(t, "public")
			beginContinuationParent(t, fixture)
			drainContinuationParent(t, fixture)
			if acknowledgement == "acquire" {
				fixture.acquireAfter.Store(1)
			} else {
				fixture.releaseAfter.Store(1)
			}
			require.Equal(t, 200, retainedContinuationRequest(fixture, nil, "").Code)
			require.Equal(t, 1, fixture.owner.Load().Snapshot().WorkCount)
			require.Equal(t, int32(1), fixture.invocations.Load())
			fixture.mu.Lock()
			defer fixture.mu.Unlock()
			for _, input := range fixture.acquisitions {
				require.Equal(t, retainedGatewayContinuationKind, input.Kind)
				require.Equal(t, fixture.acquisitions[0].RequestID, input.RequestID)
			}
		})
	}
}

func TestRetainedContinuationUncertainAcknowledgementsBlockExecution(t *testing.T) {
	for _, acknowledgement := range []string{"acquire", "release"} {
		t.Run(acknowledgement, func(t *testing.T) {
			fixture := newRetainedContinuationFixture(t, "public")
			beginContinuationParent(t, fixture)
			drainContinuationParent(t, fixture)
			defer fixture.acquireAfter.Store(0)
			defer fixture.releaseBefore.Store(false)
			if acknowledgement == "acquire" {
				fixture.acquireAfter.Store(100)
				unread := &retainedBody{}
				require.Equal(t, 503, retainedContinuationRequest(fixture, unread, "").Code)
				require.Zero(t, unread.reads.Load())
				require.Zero(t, fixture.invocations.Load())
			} else {
				fixture.releaseBefore.Store(true)
				require.Equal(t, 200, retainedContinuationRequest(fixture, nil, "").Code)
				require.Equal(t, int32(1), fixture.invocations.Load())
			}
			before := fixture.invocations.Load()
			unread := &retainedBody{}
			require.Equal(t, 503, retainedContinuationRequest(fixture, unread, "").Code)
			require.Zero(t, unread.reads.Load())
			require.Equal(t, before, fixture.invocations.Load())
			require.Equal(t, 2, fixture.owner.Load().Snapshot().WorkCount, "unknown lease cannot expire or attest held state")
			fixture.acquireAfter.Store(0)
			fixture.releaseBefore.Store(false)
			require.Equal(t, 200, retainedContinuationRequest(fixture, nil, "").Code, "reconcile unused ownership before a future continuation")
			require.Equal(t, 1, fixture.owner.Load().Snapshot().WorkCount)
		})
	}
}

func TestRetainedContinuationReceivedCandidateCannotCrossResumeGeneration(t *testing.T) {
	fixture := newRetainedContinuationFixture(t, "public")
	owner := fixture.gateway.retained
	owner.monitorCancel()
	<-owner.monitorDone
	entered, release := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() { closeContinuationGate(release) })
	transport := owner.client.Transport
	owner.client.Transport = retainedRoundTrip(func(request *http.Request) (*http.Response, error) {
		if request.Method == "GET" && request.Context().Value(retainedDelayKey{}) == true {
			close(entered)
			select {
			case <-release:
			case <-request.Context().Done():
				return nil, request.Context().Err()
			}
		}
		return transport.RoundTrip(request)
	})
	owner.mu.Lock()
	owner.budget = time.Second
	owner.mu.Unlock()
	owner.start()
	unread := &retainedBody{}
	request := httptest.NewRequest("POST", "/items/stale", unread).WithContext(context.WithValue(context.Background(), retainedDelayKey{}, true))
	result := make(chan int, 1)
	go func() {
		response := httptest.NewRecorder()
		fixture.gateway.RetainedContinuationHandler().ServeHTTP(response, request)
		result <- response.Code
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("candidate did not freeze its epoch before snapshot")
	}
	snapshot, err := fixture.owner.Load().Quiesce(context.Background())
	require.NoError(t, err)
	_, err = fixture.owner.Load().Resume(snapshot.Generation)
	require.NoError(t, err)
	beginContinuationParent(t, fixture)
	require.Eventually(t, func() bool { owner.mu.Lock(); defer owner.mu.Unlock(); return owner.generation == 2 }, time.Second, time.Millisecond)
	close(release)
	select {
	case status := <-result:
		require.Equal(t, 503, status)
	case <-time.After(time.Second):
		t.Fatal("stale continuation did not return")
	}
	require.Zero(t, unread.reads.Load())
	require.Zero(t, fixture.invocations.Load())
	require.Equal(t, 200, retainedContinuationRequest(fixture, nil, "").Code)
}

func TestRetainedContinuationShutdownKeepsAcceptedCallbacksAlive(t *testing.T) {
	fixture := newRetainedContinuationFixture(t, "2.0")
	fixture.gateway.retained.mu.Lock()
	fixture.gateway.retained.budget = 2 * time.Second
	fixture.gateway.retained.mu.Unlock()
	releaseParent := beginContinuationParent(t, fixture)
	require.Equal(t, devquiescence.Shutdown, fixture.owner.Load().Shutdown().State)
	body := &retainedBody{entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() { closeContinuationGate(body.release) })
	result := make(chan int, 1)
	go func() { result <- retainedContinuationRequest(fixture, body, "native-token").Code }()
	select {
	case <-body.entered:
	case <-time.After(time.Second):
		t.Fatal("shutdown continuation did not reach native handler")
	}
	releaseParent(nil)
	require.Equal(t, 1, fixture.owner.Load().Snapshot().WorkCount)
	select {
	case <-fixture.gateway.RetainedShutdownSignal():
		t.Fatal("shutdown fenced a still-owned continuation")
	case <-time.After(150 * time.Millisecond):
	}
	unread := &retainedBody{}
	require.Equal(t, 503, retainedRequest(fixture, unread).Code)
	require.Zero(t, unread.reads.Load())
	close(body.release)
	select {
	case status := <-result:
		require.Equal(t, 200, status)
	case <-time.After(time.Second):
		t.Fatal("accepted continuation did not join")
	}
	select {
	case <-fixture.gateway.RetainedShutdownSignal():
	case <-time.After(time.Second):
		t.Fatal("final ownership join did not notify listener")
	}
	require.Equal(t, int32(1), fixture.authorizations.Load())
	require.Equal(t, int32(1), fixture.invocations.Load())
	require.Zero(t, fixture.owner.Load().Snapshot().WorkCount)
	require.NoError(t, fixture.gateway.Close())
}

func TestRetainedContinuationShutdownJoinDeadlineFailsStrict(t *testing.T) {
	fixture := newRetainedContinuationFixture(t, "public")
	fixture.gateway.retained.mu.Lock()
	fixture.gateway.retained.budget = 150 * time.Millisecond
	fixture.gateway.retained.mu.Unlock()
	releaseParent := beginContinuationParent(t, fixture)
	fixture.owner.Load().Shutdown()
	select {
	case <-fixture.gateway.RetainedShutdownSignal():
	case <-time.After(time.Second):
		t.Fatal("unresolved shutdown work did not reach bounded failure")
	}
	require.Equal(t, 1, fixture.owner.Load().Snapshot().WorkCount, "deadline cannot expire accepted work")
	unread := &retainedBody{}
	require.Equal(t, 503, retainedContinuationRequest(fixture, unread, "").Code)
	require.Zero(t, unread.reads.Load())
	fixture.expectCloseFailure = true
	require.ErrorIs(t, fixture.gateway.Close(), errRetainedOwnership)
	releaseParent(nil)
	require.ErrorIs(t, fixture.gateway.Close(), errRetainedOwnership, "later join must not erase failed ownership evidence")
}

func TestRetainedContinuationChangedOwnerSignalsImmediately(t *testing.T) {
	fixture := newRetainedContinuationFixture(t, "public")
	beginContinuationParent(t, fixture)
	fresh := devquiescence.New()
	require.NoError(t, fresh.SetCallbackOrigin(fixture.callback.URL))
	fixture.owner.Store(fresh)
	select {
	case <-fixture.gateway.RetainedShutdownSignal():
	case <-time.After(time.Second):
		t.Fatal("changed owner did not notify listener")
	}
	fixture.expectCloseFailure = true
	require.ErrorIs(t, fixture.gateway.Close(), errRetainedOwnerChanged)
}

type retainedGoexitBody struct{}

func (retainedGoexitBody) Read([]byte) (int, error) { runtime.Goexit(); return 0, io.EOF }

type retainedGoexitWriter struct{ *httptest.ResponseRecorder }

func (retainedGoexitWriter) Write([]byte) (int, error) { runtime.Goexit(); return 0, nil }

func TestRetainedContinuationGoexitNeverConfirmsOwnership(t *testing.T) {
	for _, boundary := range []string{"public", "continuation"} {
		for _, exit := range []string{"body", "handler", "writer"} {
			t.Run(boundary+"/"+exit, func(t *testing.T) {
				fixture := newRetainedContinuationFixture(t, "public")
				if boundary == "continuation" {
					beginContinuationParent(t, fixture)
				}
				kind, handler := retainedGatewayRootKind, http.Handler(fixture.gateway)
				if boundary == "continuation" {
					kind, handler = retainedGatewayContinuationKind, fixture.gateway.RetainedContinuationHandler()
				}
				var body io.Reader
				var writer http.ResponseWriter = httptest.NewRecorder()
				if exit == "body" {
					body = retainedGoexitBody{}
				} else if exit == "writer" {
					writer = retainedGoexitWriter{httptest.NewRecorder()}
				}
				done := make(chan struct{})
				go func() {
					defer close(done)
					request := httptest.NewRequest("POST", "/items/owned", body)
					if exit == "handler" {
						fixture.gateway.retained.serve(writer, request, kind, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { runtime.Goexit() }))
					} else {
						handler.ServeHTTP(writer, request)
					}
				}()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Fatal("Goexit envelope did not retire its lease")
				}
				require.NotEmpty(t, fixture.owner.Load().Snapshot().EvidenceFailure)
				require.False(t, fixture.owner.Load().Snapshot().FixtureSafe)
				fixture.expectCloseFailure = true
				require.ErrorIs(t, fixture.gateway.Close(), errRetainedOwnership)
			})
		}
	}
}

func TestRetainedContinuationFeatureDisabledAndConfiguration(t *testing.T) {
	fixture := newRetainedGatewayFixture(t)
	require.Nil(t, fixture.gateway.RetainedContinuationHandler())
	require.Nil(t, (&Gateway{}).RetainedContinuationHandler())
	require.Nil(t, (*Gateway)(nil).RetainedContinuationHandler())
	base := fixture.gateway.config.clone()
	for _, test := range []struct {
		name       string
		port       int
		publicPort int
		control    string
	}{
		{"negative", -1, base.Port, base.RetainedOwnerControlURL},
		{"too-large", 65536, base.Port, base.RetainedOwnerControlURL},
		{"same-public", base.Port, base.Port, base.RetainedOwnerControlURL},
		{"same-default-public", 4180, 0, base.RetainedOwnerControlURL},
		{"missing-owner", base.Port + 1, base.Port, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := base.clone()
			cfg.Port, cfg.RetainedOwnerContinuationPort, cfg.RetainedOwnerControlURL = test.publicPort, test.port, test.control
			require.Error(t, cfg.Validate())
		})
	}
	cfg := base.clone()
	cfg.RetainedOwnerContinuationPort = 65535
	require.NoError(t, cfg.Validate())
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	data := "port: 4180\nretained_owner_continuation_port: 4181\nretained_owner_control_url: " + base.RetainedOwnerControlURL + "\nroutes:\n  - path: /items/{id}\n    method: POST\n    integration:\n      type: AWS_PROXY\n      payload_format_version: '2.0'\n      invoke_url: " + fixture.callback.URL + "/2015-03-31/functions/app/invocations\n"
	require.NoError(t, os.WriteFile(path, []byte(data), 0600))
	loaded, err := LoadConfig(path)
	require.NoError(t, err)
	require.Equal(t, 4181, loaded.RetainedOwnerContinuationPort)
}
