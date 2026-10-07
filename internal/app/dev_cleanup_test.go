package app

import (
	"bytes"
	"context"
	"github.com/lyeith/eventbus/internal/cognito"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/devquiescence"
	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
	"github.com/stretchr/testify/require"
)

func retainedCleanupTestFunctions(t *testing.T) *lambdaservice.Service {
	t.Helper()
	executable, err := os.Executable()
	require.NoError(t, err)
	service, err := lambdaservice.NewService(&lambdaservice.Config{Functions: map[string]lambdaservice.Function{
		"cleanup:live":  {Runtime: "provided", Command: []string{executable}, Timeout: time.Second},
		"cleanup:other": {Runtime: "provided", Command: []string{executable}, Timeout: time.Second},
	}}, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, service.Close(context.Background())) })
	return service
}

func TestRetainedCleanupDeclarationsValidateExactRegisteredTargets(t *testing.T) {
	functions := retainedCleanupTestFunctions(t)
	for _, declarations := range []string{
		"missing", "cleanup:live,", ",cleanup:live", "cleanup:live,cleanup:live",
		"cleanup:live,arn:aws:lambda:us-east-1:000000000000:function:cleanup:live",
		"arn:aws:lambda:us-west-2:000000000000:function:cleanup:live",
		"arn:aws:lambda:us-east-1:999999999999:function:cleanup:live",
		"999999999999:function:cleanup:live",
	} {
		_, err := retainedCleanupInvocations(devquiescence.New(), functions, declarations, "us-east-1", "000000000000", http.NotFoundHandler())
		require.Error(t, err, declarations)
	}
	_, err := retainedCleanupInvocations(devquiescence.New(), nil, "cleanup:live", "us-east-1", "000000000000", http.NotFoundHandler())
	require.ErrorContains(t, err, "lambda-functions")
	_, err = retainedCleanupInvocations(devquiescence.New(), functions, "cleanup:live, cleanup:other", "us-east-1", "000000000000", http.NotFoundHandler())
	require.NoError(t, err)
}

type retainedUnreadBody struct{ reads int }

func (body *retainedUnreadBody) Read([]byte) (int, error) { body.reads++; return 0, io.EOF }
func (*retainedUnreadBody) Close() error                  { return nil }

func TestRetainedCleanupKeepsNativeMetadataPayloadAndExplicitDescendantBarrier(t *testing.T) {
	functions := retainedCleanupTestFunctions(t)
	for _, tc := range []struct {
		name, path, mode string
		allow            bool
	}{
		{"default", "/2015-03-31/functions/cleanup:live/invocations", "", true},
		{"request response", "/2015-03-31/functions/cleanup:live/invocations", "RequestResponse", true},
		{"qualified", "/2015-03-31/functions/cleanup/invocations?Qualifier=live", "RequestResponse", true},
		{"arn", "/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:cleanup:live/invocations", "RequestResponse", true},
		{"foreign region", "/2015-03-31/functions/arn:aws:lambda:us-west-2:000000000000:function:cleanup:live/invocations", "RequestResponse", false},
		{"foreign account", "/2015-03-31/functions/arn:aws:lambda:us-east-1:999999999999:function:cleanup:live/invocations", "RequestResponse", false},
		{"partial arn", "/2015-03-31/functions/000000000000:function:cleanup:live/invocations", "RequestResponse", true},
		{"foreign partial arn", "/2015-03-31/functions/999999999999:function:cleanup:live/invocations", "RequestResponse", false},
		{"event", "/2015-03-31/functions/cleanup:live/invocations", "Event", false},
		{"dry run", "/2015-03-31/functions/cleanup:live/invocations", "DryRun", false},
		{"other qualifier", "/2015-03-31/functions/cleanup:other/invocations", "RequestResponse", false},
		{"wrong suffix", "/2015-03-31/functions/cleanup:live/invocations/", "RequestResponse", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner := devquiescence.New()
			held, err := owner.Quiesce(t.Context())
			require.NoError(t, err)
			payload := []byte("{ \"Authorization\":\"application-owned\", \"signed_scope\":\"owned-only\" }")
			var descendant func(error)
			called := false
			router := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				require.Equal(t, "AWS4-HMAC-SHA256 original-signature", r.Header.Get("Authorization"))
				raw, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				require.Equal(t, payload, raw, "cleanup admission must preserve the signed native payload")
				descendant, err = owner.BeginActivity("cleanup-descendant", "native-send")
				require.NoError(t, err)
				w.WriteHeader(http.StatusNoContent)
			})
			cleanup, err := retainedCleanupInvocations(owner, functions, "cleanup:live", "us-east-1", "000000000000", router)
			require.NoError(t, err)
			handler := owner.Wrap(devquiescence.Callback, router, cleanup)
			request := httptest.NewRequest(http.MethodPost, tc.path, bytes.NewReader(payload))
			request.Header.Set("Authorization", "AWS4-HMAC-SHA256 original-signature")
			request.Header.Set("X-Amz-Invocation-Type", tc.mode)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			require.Equal(t, tc.allow, called)
			if !tc.allow {
				require.Equal(t, http.StatusServiceUnavailable, response.Code)
				require.True(t, owner.Snapshot().FixtureSafe)
				return
			}
			require.Equal(t, http.StatusNoContent, response.Code)
			defer descendant(nil)
			pending := owner.Snapshot()
			require.Equal(t, devquiescence.Draining, pending.State)
			require.False(t, pending.FixtureSafe)
			require.Empty(t, pending.EvidenceFailure)
			_, err = owner.Resume(held.Generation)
			require.Error(t, err, "a native response cannot certify its pending descendant")
			refused := httptest.NewRecorder()
			owner.Wrap(devquiescence.Source, router, nil).ServeHTTP(refused, httptest.NewRequest("GET", "/", nil))
			require.Equal(t, http.StatusServiceUnavailable, refused.Code)
			descendant(nil)
			rejoined, err := owner.Quiesce(t.Context())
			require.NoError(t, err)
			require.True(t, rejoined.FixtureSafe)
			require.Equal(t, held.Generation, rejoined.Generation)
			_, err = owner.Resume(rejoined.Generation)
			require.NoError(t, err)
		})
	}
}

func TestRetainedCleanupMetadataDoesNotReadRequestBody(t *testing.T) {
	functions := retainedCleanupTestFunctions(t)
	for _, mode := range []string{"", "RequestResponse", "Event"} {
		body := &retainedUnreadBody{}
		request := httptest.NewRequest("POST", "/2015-03-31/functions/cleanup:live/invocations", strings.NewReader(""))
		request.Body = body
		request.Header.Set("X-Amz-Invocation-Type", mode)
		_, allowed := functions.DevRequestResponseTarget(request)
		require.Equal(t, mode != "Event", allowed)
		require.Zero(t, body.reads)
	}
}

// A foreign gateway lease is never inferred complete. A permanent failed
// shutdown may abandon it only with sticky uncertainty, without hanging cleanup
// or certifying fixtures safe.
func TestRetainedShutdownUnknownRemoteLeaseRetainsDirtyStore(t *testing.T) {
	owner := devquiescence.New()
	snapshot := owner.Snapshot()
	_, err := owner.AcquireSourceLease(devquiescence.SourceLeaseInput{
		OwnerID: snapshot.OwnerID, Generation: snapshot.Generation, RequestID: "lost-gateway", Kind: "http.gateway",
	})
	require.NoError(t, err)
	source := httptest.NewServer(owner.Wrap(devquiescence.Source, http.NotFoundHandler(), nil))
	peer := httptest.NewServer(owner.Wrap(devquiescence.Callback, http.NotFoundHandler(), nil))
	t.Cleanup(source.Close)
	t.Cleanup(peer.Close)
	store, err := cognito.OpenCognitoStore(filepath.Join(t.TempDir(), "retained.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	listener := newEventBusListener(source.Config, &eventBusLifecycle{store: store}, 20*time.Millisecond)
	listener.devRetained = &devRetainedHTTP{owner: owner, callbacks: peer.Config}
	stopped := make(chan error, 1)
	go func() { stopped <- listener.Shutdown(context.Background()) }()
	select {
	case err = <-stopped:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("unknown remote lease blocked permanent shutdown")
	}
	dirty := owner.Snapshot()
	require.Zero(t, dirty.WorkCount)
	require.False(t, dirty.FixtureSafe)
	require.NotEmpty(t, dirty.EvidenceFailure)
	require.NoError(t, readCognitoStore(context.Background(), store))
	_, err = owner.Resume(dirty.Generation)
	require.ErrorIs(t, err, devquiescence.ErrShutdown)
}

func TestRetainedCleanupFlagRequiresRetainedOwner(t *testing.T) {
	cfg := runTestConfig(t)
	cfg.retainedCleanupFunctions = "cleanup:live"
	require.ErrorContains(t, validateRetainedConfig(cfg), "retained-owner-callback-port")
	cfg.port, cfg.retainedCallbackPort = 4100, 4101
	require.NoError(t, validateRetainedConfig(cfg))
}
