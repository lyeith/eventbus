package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/devquiescence"
	"github.com/stretchr/testify/require"
)

type retainedGatewayFixture struct {
	owner                        atomic.Pointer[devquiescence.Coordinator]
	gateway                      *Gateway
	control, callback            *httptest.Server
	acquireAfter, releaseAfter   atomic.Int32
	acquireBefore, releaseBefore atomic.Bool
	invocations                  atomic.Int32
	authorizations               atomic.Int32
	acquireAccepted, acquireAck  chan struct{}
	acquireOnce                  sync.Once
	acquireSkip                  atomic.Int32
	mu                           sync.Mutex
	acquisitions                 []devquiescence.SourceLeaseInput
	expectCloseFailure           bool
}

func newRetainedGatewayFixture(t *testing.T) *retainedGatewayFixture {
	t.Helper()
	return newRetainedGatewayFixtureVersion(t, "public")
}

func newRetainedGatewayFixtureVersion(t *testing.T, authVersion string) *retainedGatewayFixture {
	t.Helper()
	fixture := &retainedGatewayFixture{}
	fixture.owner.Store(devquiescence.New())
	fixture.callback = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fixture.owner.Load().Wrap(devquiescence.Callback, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.Contains(r.URL.Path, "/auth/") {
				fixture.authorizations.Add(1)
				_, _ = io.WriteString(w, `{"isAuthorized":true}`)
				return
			}
			fixture.invocations.Add(1)
			_, _ = io.WriteString(w, `{"statusCode":200,"body":"owned"}`)
		}), nil).ServeHTTP(w, r)
	}))
	t.Cleanup(fixture.callback.Close)
	require.NoError(t, fixture.owner.Load().SetCallbackOrigin(fixture.callback.URL))
	fixture.control = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		acquiring := strings.HasSuffix(r.URL.Path, "/acquire")
		releasing := strings.HasSuffix(r.URL.Path, "/release")
		if acquiring {
			data, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			r.Body = io.NopCloser(bytes.NewReader(data))
			var input devquiescence.SourceLeaseInput
			require.NoError(t, json.Unmarshal(data, &input))
			fixture.mu.Lock()
			fixture.acquisitions = append(fixture.acquisitions, input)
			fixture.mu.Unlock()
		}
		if acquiring && fixture.acquireBefore.Load() || releasing && fixture.releaseBefore.Load() {
			dropRetainedResponse(t, w)
			return
		}
		response := httptest.NewRecorder()
		devquiescence.NewHandler(fixture.owner.Load()).ServeHTTP(response, r)
		if acquiring && fixture.acquireAccepted != nil && !consumeRetainedDrop(&fixture.acquireSkip) {
			fixture.acquireOnce.Do(func() { close(fixture.acquireAccepted); <-fixture.acquireAck })
		}
		if acquiring && consumeRetainedDrop(&fixture.acquireAfter) || releasing && consumeRetainedDrop(&fixture.releaseAfter) {
			dropRetainedResponse(t, w)
			return
		}
		for key, values := range response.Header() {
			w.Header()[key] = values
		}
		w.WriteHeader(response.Code)
		_, _ = w.Write(response.Body.Bytes())
	}))
	t.Cleanup(fixture.control.Close)
	cfg := httpAPIConfig(fixture.callback.URL, authVersion, "2.0")
	if authVersion != "public" {
		configuration := cfg.Authorizers["auth"]
		configuration.EnableSimpleResponses = true
		configuration.IdentitySources = []string{"$request.header.Authorization"}
		cfg.Authorizers["auth"] = configuration
	}
	cfg.RetainedOwnerControlURL = fixture.control.URL + devquiescence.ControlPath
	gateway, err := New(cfg, Options{})
	require.NoError(t, err)
	fixture.gateway = gateway
	gateway.retained.mu.Lock()
	gateway.retained.budget = 100 * time.Millisecond
	gateway.retained.mu.Unlock()
	t.Cleanup(func() {
		err := gateway.Close()
		if !fixture.expectCloseFailure {
			require.NoError(t, err)
		}
	})
	return fixture
}

func consumeRetainedDrop(counter *atomic.Int32) bool {
	for {
		value := counter.Load()
		if value <= 0 {
			return false
		}
		if counter.CompareAndSwap(value, value-1) {
			return true
		}
	}
}

func dropRetainedResponse(t *testing.T, w http.ResponseWriter) {
	t.Helper()
	connection, _, err := w.(http.Hijacker).Hijack()
	require.NoError(t, err)
	require.NoError(t, connection.Close())
}

func retainedRequest(fixture *retainedGatewayFixture, body io.Reader) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	fixture.gateway.ServeHTTP(response, httptest.NewRequest("POST", "/items/owned", body))
	return response
}

type retainedBody struct {
	reads            atomic.Int32
	entered, release chan struct{}
	once             sync.Once
}

func (body *retainedBody) Read(buffer []byte) (int, error) {
	body.reads.Add(1)
	if body.entered != nil {
		body.once.Do(func() { close(body.entered) })
		<-body.release
	}
	return 0, io.EOF
}

func TestRetainedGatewayCountsBodyDelayAndKeepsAcceptedCallbacks(t *testing.T) {
	fixture := newRetainedGatewayFixture(t)
	body := &retainedBody{entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() {
		select {
		case <-body.release:
		default:
			close(body.release)
		}
	})
	result := make(chan *httptest.ResponseRecorder, 1)
	go func() { result <- retainedRequest(fixture, body) }()
	select {
	case <-body.entered:
	case <-time.After(time.Second):
		t.Fatal("root never reached body read")
	}
	require.Equal(t, 1, fixture.owner.Load().Snapshot().WorkCount)
	require.Zero(t, fixture.invocations.Load(), "body delay is before native Invoke")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	_, err := fixture.owner.Load().Quiesce(ctx)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	refused := &retainedBody{}
	require.Equal(t, 503, retainedRequest(fixture, refused).Code)
	require.Zero(t, refused.reads.Load())
	select {
	case <-fixture.gateway.RetainedShutdownSignal():
		t.Fatal("draining canceled accepted roots")
	default:
	}
	close(body.release)
	select {
	case response := <-result:
		require.Equal(t, 200, response.Code)
	case <-time.After(time.Second):
		t.Fatal("accepted callback did not join")
	}
	require.Equal(t, int32(1), fixture.invocations.Load())
	snapshot, err := fixture.owner.Load().Quiesce(context.Background())
	require.NoError(t, err)
	require.True(t, snapshot.FixtureSafe)
	_, err = fixture.owner.Load().Resume(snapshot.Generation)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return retainedRequest(fixture, nil).Code == 200 }, time.Second, 10*time.Millisecond)
}

func TestRetainedGatewayLostAcquireAcknowledgementReplaysOneLease(t *testing.T) {
	fixture := newRetainedGatewayFixture(t)
	fixture.acquireAfter.Store(1)
	require.Equal(t, 200, retainedRequest(fixture, nil).Code)
	require.Equal(t, int32(1), fixture.invocations.Load())
	require.Zero(t, fixture.owner.Load().Snapshot().WorkCount)
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	require.GreaterOrEqual(t, len(fixture.acquisitions), 2)
	require.Equal(t, fixture.acquisitions[0], fixture.acquisitions[1])
}

func TestRetainedGatewayLostReleaseAcknowledgementIsIdempotent(t *testing.T) {
	fixture := newRetainedGatewayFixture(t)
	fixture.releaseAfter.Store(1)
	require.Equal(t, 200, retainedRequest(fixture, nil).Code)
	require.Zero(t, fixture.owner.Load().Snapshot().WorkCount)
	require.NoError(t, fixture.gateway.Close())
}

func TestRetainedGatewayUncertainReleaseFencesRootsAndStrictClose(t *testing.T) {
	fixture := newRetainedGatewayFixture(t)
	fixture.releaseBefore.Store(true)
	require.Equal(t, 200, retainedRequest(fixture, nil).Code)
	require.Equal(t, 1, fixture.owner.Load().Snapshot().WorkCount)
	body := &retainedBody{}
	require.Equal(t, 503, retainedRequest(fixture, body).Code)
	require.Zero(t, body.reads.Load())
	require.Equal(t, int32(1), fixture.invocations.Load())
	require.ErrorIs(t, fixture.gateway.Close(), errRetainedOwnership)
	require.Equal(t, 1, fixture.owner.Load().Snapshot().WorkCount, "failed close cannot expire remote work")
	fixture.releaseBefore.Store(false)
	require.NoError(t, fixture.gateway.Close(), "same receipt must reconcile even after a failed close")
	require.Zero(t, fixture.owner.Load().Snapshot().WorkCount)
}

func TestRetainedGatewayUncertainAcquireNeverProcessesBody(t *testing.T) {
	fixture := newRetainedGatewayFixture(t)
	fixture.acquireAfter.Store(100)
	body := &retainedBody{}
	require.Equal(t, 503, retainedRequest(fixture, body).Code)
	require.Zero(t, body.reads.Load())
	require.Zero(t, fixture.invocations.Load())
	require.Equal(t, 1, fixture.owner.Load().Snapshot().WorkCount)
	fixture.acquireAfter.Store(0)
	require.NoError(t, fixture.gateway.Close())
	require.Zero(t, fixture.owner.Load().Snapshot().WorkCount)
	require.Zero(t, fixture.invocations.Load(), "reconciliation completes unused grants; it never launches old work")
}

func TestRetainedGatewayUnknownAcquireBeforeDeliveryRemainsNonProducing(t *testing.T) {
	fixture := newRetainedGatewayFixture(t)
	fixture.acquireBefore.Store(true)
	body := &retainedBody{}
	require.Equal(t, 503, retainedRequest(fixture, body).Code)
	require.Zero(t, body.reads.Load())
	snapshot, err := fixture.owner.Load().Quiesce(context.Background())
	require.NoError(t, err)
	require.True(t, snapshot.FixtureSafe, "ungranted candidates have no application ownership")
	fixture.acquireBefore.Store(false)
	require.NoError(t, fixture.gateway.Close())
	require.Zero(t, fixture.invocations.Load())
}

type retainedRoundTrip func(*http.Request) (*http.Response, error)

func (execute retainedRoundTrip) RoundTrip(request *http.Request) (*http.Response, error) {
	return execute(request)
}

type retainedDelayKey struct{}

func TestRetainedGatewayReceivedCandidateCannotCrossResumeGeneration(t *testing.T) {
	fixture := newRetainedGatewayFixture(t)
	owner := fixture.gateway.retained
	owner.monitorCancel()
	<-owner.monitorDone
	entered, release := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
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
	owner.start()
	owner.mu.Lock()
	owner.budget = time.Second
	owner.mu.Unlock()
	body := &retainedBody{}
	request := httptest.NewRequest("POST", "/items/old-generation", body)
	request = request.WithContext(context.WithValue(request.Context(), retainedDelayKey{}, true))
	result := make(chan int, 1)
	go func() {
		response := httptest.NewRecorder()
		fixture.gateway.ServeHTTP(response, request)
		result <- response.Code
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("candidate never entered control delay")
	}
	snapshot, err := fixture.owner.Load().Quiesce(context.Background())
	require.NoError(t, err)
	_, err = fixture.owner.Load().Resume(snapshot.Generation)
	require.NoError(t, err)
	require.Eventually(t, func() bool { owner.mu.Lock(); defer owner.mu.Unlock(); return owner.generation == 2 }, time.Second, 10*time.Millisecond)
	close(release)
	select {
	case status := <-result:
		require.Equal(t, 503, status)
	case <-time.After(time.Second):
		t.Fatal("candidate did not return")
	}
	require.Zero(t, body.reads.Load())
	require.Zero(t, fixture.invocations.Load())
	require.Equal(t, 200, retainedRequest(fixture, nil).Code, "a future envelope can use the same owner in its new epoch")
}

func TestRetainedGatewayOwnerRestartCannotRebind(t *testing.T) {
	fixture := newRetainedGatewayFixture(t)
	fresh := devquiescence.New()
	require.NoError(t, fresh.SetCallbackOrigin(fixture.callback.URL))
	fixture.owner.Store(fresh)
	body := &retainedBody{}
	require.Equal(t, 503, retainedRequest(fixture, body).Code)
	require.Zero(t, body.reads.Load())
	require.Zero(t, fixture.invocations.Load())
	require.ErrorIs(t, fixture.gateway.Close(), errRetainedOwnerChanged)
	fixture.expectCloseFailure = true
}

func TestRetainedGatewayBrokerShutdownNotifiesListenerWithoutExpiringRoot(t *testing.T) {
	fixture := newRetainedGatewayFixture(t)
	body := &retainedBody{entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() {
		select {
		case <-body.release:
		default:
			close(body.release)
		}
	})
	result := make(chan int, 1)
	go func() { result <- retainedRequest(fixture, body).Code }()
	select {
	case <-body.entered:
	case <-time.After(time.Second):
		t.Fatal("root never entered")
	}
	fixture.owner.Load().Shutdown()
	select {
	case <-fixture.gateway.RetainedShutdownSignal():
	case <-time.After(time.Second):
		t.Fatal("listener was not notified of irreversible broker shutdown")
	}
	require.Equal(t, 1, fixture.owner.Load().Snapshot().WorkCount)
	require.Equal(t, 503, retainedRequest(fixture, &retainedBody{}).Code)
	close(body.release)
	select {
	case status := <-result:
		require.Equal(t, 200, status)
	case <-time.After(time.Second):
		t.Fatal("accepted root was interrupted")
	}
	require.NoError(t, fixture.gateway.Close())
	require.Zero(t, fixture.owner.Load().Snapshot().WorkCount)
}

type retainedPanicReader struct{}

func (retainedPanicReader) Read([]byte) (int, error) { panic("controlled gateway body panic") }
func TestRetainedGatewayPanicPoisonsOwnershipEvidence(t *testing.T) {
	fixture := newRetainedGatewayFixture(t)
	require.Panics(t, func() { retainedRequest(fixture, retainedPanicReader{}) })
	snapshot, err := fixture.owner.Load().Quiesce(context.Background())
	require.ErrorIs(t, err, devquiescence.ErrEvidence)
	require.False(t, snapshot.FixtureSafe)
	require.NotEmpty(t, snapshot.EvidenceFailure)
	require.ErrorIs(t, fixture.gateway.Close(), errRetainedOwnership)
	fixture.expectCloseFailure = true
}

func TestRetainedGatewayConfigurationAndReadiness(t *testing.T) {
	for _, value := range []string{"https://127.0.0.1/__eventbus/dev/retained-owner", "http://localhost/__eventbus/dev/retained-owner", "http://192.0.2.1/__eventbus/dev/retained-owner", "http://user:secret@127.0.0.1/__eventbus/dev/retained-owner", "http://127.0.0.1/__eventbus/dev/retained-owner?query", "http://127.0.0.1/__eventbus/dev/retained-owner#fragment", "http://127.0.0.1/other", "http://127.0.0.1/__eventbus/dev/retained-owner/"} {
		require.Error(t, validateRetainedControlURL(value), value)
	}
	fixture := newRetainedGatewayFixture(t)
	cfg := fixture.gateway.config.clone()
	cfg.Routes[0].Integration.InvokeURL = fixture.control.URL + "/2015-03-31/functions/app/invocations"
	_, err := New(cfg, Options{})
	require.Error(t, err, "source origin is not a joined callback target")
	cfg = fixture.gateway.config.clone()
	_, err = New(cfg, Options{FrontendProxy: fixture.callback.URL})
	require.Error(t, err)
	cfg.Routes[0].Integration = IntegrationConfig{Type: "HTTP_PROXY", URI: fixture.callback.URL}
	require.Error(t, cfg.Validate())
	fixture.owner.Load().Shutdown()
	response := httptest.NewRecorder()
	fixture.gateway.ServeHTTP(response, httptest.NewRequest("GET", "/health", nil))
	require.Equal(t, 200, response.Code)
	require.Zero(t, fixture.owner.Load().Snapshot().WorkCount)
	require.Nil(t, (&Gateway{}).RetainedShutdownSignal())
}

func TestRetainedGatewayAcquiredRootKeepsAuthorizerAndIntegrationCallbacksWhileFenced(t *testing.T) {
	fixture := newRetainedGatewayFixtureVersion(t, "2.0")
	fixture.acquireAccepted, fixture.acquireAck = make(chan struct{}), make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-fixture.acquireAck:
		default:
			close(fixture.acquireAck)
		}
	})
	fixture.gateway.retained.mu.Lock()
	fixture.gateway.retained.budget = time.Second
	fixture.gateway.retained.mu.Unlock()
	request := httptest.NewRequest("POST", "/items/owned", nil)
	request.Header.Set("Authorization", "owned")
	result := make(chan int, 1)
	go func() {
		response := httptest.NewRecorder()
		fixture.gateway.ServeHTTP(response, request)
		result <- response.Code
	}()
	select {
	case <-fixture.acquireAccepted:
	case <-time.After(time.Second):
		t.Fatal("root did not acquire")
	}
	require.Equal(t, 1, fixture.owner.Load().Snapshot().WorkCount)
	require.Zero(t, fixture.authorizations.Load())
	require.Zero(t, fixture.invocations.Load())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	_, err := fixture.owner.Load().Quiesce(ctx)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	close(fixture.acquireAck)
	select {
	case status := <-result:
		require.Equal(t, 200, status)
	case <-time.After(time.Second):
		t.Fatal("accepted callbacks failed to join")
	}
	require.Equal(t, int32(1), fixture.authorizations.Load())
	require.Equal(t, int32(1), fixture.invocations.Load())
	snapshot, err := fixture.owner.Load().Quiesce(context.Background())
	require.NoError(t, err)
	require.True(t, snapshot.FixtureSafe)
}

func TestRetainedGatewayForcedHTTPShutdownJoinsCanceledBodyAndFreshCompletion(t *testing.T) {
	fixture := newRetainedGatewayFixture(t)
	server := httptest.NewServer(fixture.gateway)
	t.Cleanup(server.Close)
	connection, err := net.Dial("tcp", server.Listener.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = connection.Close() })
	_, err = io.WriteString(connection, "POST /items/owned HTTP/1.1\r\nHost: owned\r\nContent-Length: 100\r\n\r\nx")
	require.NoError(t, err)
	require.Eventually(t, func() bool { return fixture.owner.Load().Snapshot().WorkCount == 1 }, time.Second, time.Millisecond)
	require.Zero(t, fixture.invocations.Load(), "accepted envelope is still blocked before Invoke")
	fixture.owner.Load().Shutdown()
	select {
	case <-fixture.gateway.RetainedShutdownSignal():
	case <-time.After(time.Second):
		t.Fatal("broker shutdown was not observed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, server.Config.Shutdown(ctx), context.DeadlineExceeded)
	require.Equal(t, 1, fixture.owner.Load().Snapshot().WorkCount, "drain deadline cannot expire root ownership")
	require.NoError(t, server.Config.Close())
	require.NoError(t, fixture.gateway.Close(), "forced inbound close must join body reader and reconcile independent of its canceled request")
	require.Zero(t, fixture.owner.Load().Snapshot().WorkCount)
	require.Zero(t, fixture.invocations.Load())
}

func TestRetainedGatewayGrantAcknowledgementCannotBypassNewCompletionUncertainty(t *testing.T) {
	fixture := newRetainedGatewayFixture(t)
	fixture.acquireAccepted, fixture.acquireAck = make(chan struct{}), make(chan struct{})
	fixture.acquireSkip.Store(1)
	t.Cleanup(func() {
		select {
		case <-fixture.acquireAck:
		default:
			close(fixture.acquireAck)
		}
	})
	fixture.gateway.retained.mu.Lock()
	fixture.gateway.retained.budget = time.Second
	fixture.gateway.retained.mu.Unlock()
	body := &retainedBody{entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() {
		select {
		case <-body.release:
		default:
			close(body.release)
		}
	})
	first := make(chan int, 1)
	go func() { first <- retainedRequest(fixture, body).Code }()
	select {
	case <-body.entered:
	case <-time.After(time.Second):
		t.Fatal("first root did not reach body")
	}
	secondBody := &retainedBody{}
	second := make(chan int, 1)
	go func() { second <- retainedRequest(fixture, secondBody).Code }()
	select {
	case <-fixture.acquireAccepted:
	case <-time.After(time.Second):
		t.Fatal("second root did not acquire")
	}
	require.Equal(t, 2, fixture.owner.Load().Snapshot().WorkCount)
	fixture.releaseBefore.Store(true)
	fixture.gateway.retained.mu.Lock()
	fixture.gateway.retained.budget = 100 * time.Millisecond
	fixture.gateway.retained.mu.Unlock()
	close(body.release)
	select {
	case status := <-first:
		require.Equal(t, 200, status)
	case <-time.After(time.Second):
		t.Fatal("first root did not finish")
	}
	close(fixture.acquireAck)
	select {
	case status := <-second:
		require.Equal(t, 503, status)
	case <-time.After(2 * time.Second):
		t.Fatal("second root did not return")
	}
	require.Zero(t, secondBody.reads.Load(), "uncertainty created during grant cannot be bypassed by the ACK")
	require.Equal(t, int32(1), fixture.invocations.Load())
	require.Equal(t, 2, fixture.owner.Load().Snapshot().WorkCount)
	fixture.releaseBefore.Store(false)
	require.NoError(t, fixture.gateway.Close())
	require.Zero(t, fixture.owner.Load().Snapshot().WorkCount)
}
