package devquiescence

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHTTPControlsStrictValidationAndResume(t *testing.T) {
	c := New()
	handler := NewHandler(c)
	for _, scenario := range []struct {
		method, path, body string
		status             int
	}{
		{"GET", ControlPath, "", 200}, {"PUT", ControlPath, "", 405}, {"GET", ControlPath + "/quiesce", "", 405},
		{"POST", ControlPath + "/quiesce", `{"timeout_ms":0}`, 400}, {"POST", ControlPath + "/quiesce", `{"timeout_ms":300001}`, 400},
		{"POST", ControlPath + "/quiesce", `{"timeout_ms":10,"unknown":true}`, 400},
		{"POST", ControlPath + "/quiesce", `{"timeout_ms":10,"timeout_ms":20}`, 400}, {"POST", ControlPath + "/quiesce", `{"timeout_ms":10} {}`, 400},
		{"POST", ControlPath + "/quiesce", `{"timeout_ms":10}`, 200}, {"POST", ControlPath + "/resume", `{"generation":0}`, 400},
		{"POST", ControlPath + "/resume", `{"generation":99}`, 409}, {"POST", ControlPath + "/resume", `{"generation":1}`, 200},
		{"POST", ControlPath + "/resume", `{"generation":1}`, 409}, {"GET", ControlPath + "/unknown", "", 404},
	} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(scenario.method, scenario.path, strings.NewReader(scenario.body)))
		require.Equal(t, scenario.status, recorder.Code, scenario)
		require.Zero(t, c.Snapshot().WorkCount, "controls never participate in the joined work count")
	}
}

func TestHealthBypassesHeldFenceAndCleanupHandlerCannotStartWork(t *testing.T) {
	c := New()
	_, err := c.Quiesce(t.Context())
	require.NoError(t, err)
	called := 0
	wrapper := c.Wrap(Callback, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called++; require.Zero(t, c.Snapshot().WorkCount) }), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, CleanupOnly, RequestMode(r))
		_, err := c.BeginActivity("lambda.sync", "late")
		require.ErrorIs(t, err, ErrFenced)
		require.Equal(t, 1, c.Snapshot().CleanupEnvelopes)
	}))
	wrapper.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/health", nil))
	require.Equal(t, 1, called)
	wrapper.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/lambda", nil))
	require.True(t, c.Snapshot().FixtureSafe)
}

func TestCallbackCleanupRequiresCompletedEvidenceBarrier(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	c := New(func() error { once.Do(func() { close(entered); <-release }); return nil })
	joined := make(chan error, 1)
	go func() { _, err := c.Quiesce(t.Context()); joined <- err }()
	<-entered
	cleanupCalls := 0
	wrapper := c.Wrap(Callback, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("draining zero envelope became work") }), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cleanupCalls++
		require.Equal(t, CleanupOnly, RequestMode(r))
	}))
	rejected := httptest.NewRecorder()
	wrapper.ServeHTTP(rejected, httptest.NewRequest(http.MethodPost, "/exact-delete", nil))
	require.Equal(t, http.StatusServiceUnavailable, rejected.Code)
	require.Zero(t, cleanupCalls, "blocked evidence check has not authorized exact deletion")
	require.False(t, c.Snapshot().FixtureSafe)
	close(release)
	require.NoError(t, <-joined)
	wrapper.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/exact-delete", nil))
	require.Equal(t, 1, cleanupCalls, "successful held barrier permits cleanup")
}

func TestCallbackCleanupRequiresExplicitRetryAfterTimeout(t *testing.T) {
	c := New()
	complete, err := c.BeginActivity("lambda.async", "accepted")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = c.Quiesce(ctx)
	require.ErrorIs(t, err, context.Canceled)
	complete(nil)
	cleanupCalls := 0
	wrapper := c.Wrap(Callback, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("timed-out envelope became work") }), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { cleanupCalls++ }))
	rejected := httptest.NewRecorder()
	wrapper.ServeHTTP(rejected, httptest.NewRequest(http.MethodPost, "/exact-delete", nil))
	require.Equal(t, http.StatusServiceUnavailable, rejected.Code)
	require.Zero(t, cleanupCalls, "zero remaining work alone does not authorize exact deletion")
	_, err = c.Quiesce(t.Context())
	require.NoError(t, err)
	wrapper.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/exact-delete", nil))
	require.Equal(t, 1, cleanupCalls)
}
