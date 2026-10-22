package app

import (
	"context"
	"flag"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/devquiescence"
	"github.com/lyeith/eventbus/internal/ses"
	"github.com/stretchr/testify/require"
)

type devSESTrackingFunc func(http.ResponseWriter, *http.Request)

func (f devSESTrackingFunc) ServeDevTracking(w http.ResponseWriter, r *http.Request) { f(w, r) }

func TestCLIOptionalSESTracking(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want bool
	}{
		{nil, false}, {[]string{"--ses-tracking"}, true}, {[]string{"--ses-tracking=false"}, false},
	} {
		flags := flag.NewFlagSet("eventbus", flag.ContinueOnError)
		flags.SetOutput(io.Discard)
		cfg, err := readConfig(flags, tc.args)
		require.NoError(t, err)
		require.Equal(t, tc.want, cfg.sesTracking)
	}
}

func TestDevSESTrackingNamespacesStaySeparateFromNativeRoutes(t *testing.T) {
	var native, tracking int
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { native++; w.WriteHeader(204) })
	view := devSESTrackingFunc(func(w http.ResponseWriter, r *http.Request) { tracking++; w.WriteHeader(202) })
	router := withDevSES(next, &devSESOutcomeProbe{}, view)
	for _, path := range []string{
		ses.DevTrackingMessagesPrefix + "message",
		ses.DevTrackingMessagesPrefix + "message/html",
		ses.DevTrackingPixelPrefix + "token.gif",
		"/v2/email/outbound-emails",
		"/__eventbus/dev/ses/messages-lookalike/message",
		"/__eventbus/dev/ses/tracking-lookalike/token.gif",
	} {
		router.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil))
	}
	require.Equal(t, 3, tracking)
	require.Equal(t, 3, native)
	off := withDevSES(next, &devSESOutcomeProbe{}, nil)
	response := httptest.NewRecorder()
	off.ServeHTTP(response, httptest.NewRequest(http.MethodGet, ses.DevTrackingPixelPrefix+"token.gif", nil))
	require.Equal(t, 404, response.Code)
	require.Equal(t, 3, native, "disabled local resource must not enter AWS routing")
}

func TestRetainedSESPixelGETIsCountedAndFenced(t *testing.T) {
	owner := devquiescence.New()
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	pixel := devSESTrackingFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if mode := devquiescence.RequestMode(r); mode != devquiescence.Work {
			t.Errorf("pixel was dispatched with admission mode %q", mode)
		}
		close(entered)
		<-release
		w.WriteHeader(http.StatusOK)
	})
	router := withDevSES(http.NotFoundHandler(), &devSESOutcomeProbe{}, pixel)
	ingress, retained := newRetainedHTTP(owner, router, 4101, nil)
	request := func() *http.Request {
		return httptest.NewRequest(http.MethodGet, ses.DevTrackingPixelPrefix+"token.gif", nil)
	}
	finished := make(chan struct{})
	response := httptest.NewRecorder()
	go func() { ingress.ServeHTTP(response, request()); close(finished) }()
	var released bool
	t.Cleanup(func() {
		if !released {
			close(release)
		}
		<-finished
	})
	select {
	case <-entered:
	case <-finished:
		t.Fatalf("pixel was not dispatched: HTTP %d", response.Code)
	case <-time.After(2 * time.Second):
		t.Fatal("pixel handler did not start")
	}
	require.Equal(t, 1, owner.Snapshot().WorkCount, "GET can produce native SNS work and must retain its envelope")
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	snapshot, err := owner.Quiesce(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.False(t, snapshot.FixtureSafe)
	require.Equal(t, 1, snapshot.WorkCount)
	close(release)
	released = true
	<-finished
	require.Equal(t, 200, response.Code)
	held, err := owner.Quiesce(t.Context())
	require.NoError(t, err)
	require.True(t, held.FixtureSafe)
	for _, handler := range []http.Handler{ingress, retained.callbacks.Handler} {
		refused := httptest.NewRecorder()
		handler.ServeHTTP(refused, request())
		require.Equal(t, 503, refused.Code, "held sources and cleanup-only callbacks must refuse pixel publication")
	}
	require.EqualValues(t, 1, calls.Load())
}
