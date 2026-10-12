package cognitotrigger

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type devTriggerReloadFunc func(context.Context, string, string) error

func (reload devTriggerReloadFunc) DevReloadTrigger(ctx context.Context, pool, trigger string) error {
	return reload(ctx, pool, trigger)
}

func TestDevTriggerReloadHTTPParsesSelectedTargetAndStaticSuccess(t *testing.T) {
	var pool, trigger string
	handler := NewDevReloadHandler(devTriggerReloadFunc(func(_ context.Context, selectedPool, selectedTrigger string) error {
		pool, trigger = selectedPool, selectedTrigger
		return nil
	}))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, devReloadPrefix+"owned-pool/triggers/"+CreateAuthChallenge+"/reload?ignored=PRIVATE", strings.NewReader(`{"ignored":"PRIVATE"}`)))
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, "owned-pool", pool)
	require.Equal(t, CreateAuthChallenge, trigger)
	require.JSONEq(t, `{"pool_id":"owned-pool","trigger":"CreateAuthChallenge","status":"reloaded"}`, response.Body.String())
	require.Equal(t, "application/json", response.Header().Get("Content-Type"))
	require.NotContains(t, response.Body.String(), "PRIVATE")
}

func TestDevTriggerReloadHTTPRefusesRoutesAndMethodsWithoutPortCall(t *testing.T) {
	handler := NewDevReloadHandler(devTriggerReloadFunc(func(context.Context, string, string) error {
		t.Fatal("invalid reload route called runtime")
		return nil
	}))
	for _, scenario := range []struct {
		method, path, code string
		status             int
	}{
		{http.MethodGet, devReloadPrefix + "owned-pool/triggers/" + DefineAuthChallenge + "/reload", "reload_requires_post", 405},
		{http.MethodPost, devReloadPrefix + "/triggers/" + DefineAuthChallenge + "/reload", "reload_route_not_found", 404},
		{http.MethodPost, devReloadPrefix + "owned-pool/triggers//reload", "reload_route_not_found", 404},
		{http.MethodPost, devReloadPrefix + "owned-pool/triggers/" + DefineAuthChallenge + "/reload/", "reload_route_not_found", 404},
		{http.MethodPost, devReloadPrefix + "owned%2Fpool/triggers/" + DefineAuthChallenge + "/reload", "reload_route_not_found", 404},
		{http.MethodPost, devReloadPrefix + "owned-pool/functions/" + DefineAuthChallenge + "/reload", "reload_route_not_found", 404},
		{http.MethodPost, "/2015-03-31/functions/cognito_trigger_1/invocations", "reload_route_not_found", 404},
		{http.MethodPost, "/__eventbus/dev/lambda/functions/cognito_trigger_1/reload", "reload_route_not_found", 404},
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(scenario.method, scenario.path, nil))
		require.Equal(t, scenario.status, response.Code, response.Body.String())
		require.JSONEq(t, `{"error":"`+scenario.code+`"}`, response.Body.String())
		if scenario.status == 405 {
			require.Equal(t, http.MethodPost, response.Header().Get("Allow"))
		}
	}
}

func TestDevTriggerReloadHTTPMapsStaticPrivateFailures(t *testing.T) {
	path := devReloadPrefix + "owned-pool/triggers/" + DefineAuthChallenge + "/reload"
	for _, scenario := range []struct {
		failure error
		status  int
		code    string
	}{
		{&DevReloadError{Kind: DevReloadNotConfigured}, 404, "trigger_not_configured"},
		{&DevReloadError{Kind: DevReloadUnsupported}, 503, "trigger_reload_unsupported"},
		{&DevReloadError{Kind: DevReloadClosed}, 503, "trigger_reload_closed"},
		{&DevReloadError{Kind: DevReloadWaitInterrupted}, 504, "reload_wait_interrupted"},
		{errors.Join(errors.New("PRIVATE_HANDLER_PATH"), context.DeadlineExceeded), 504, "reload_wait_interrupted"},
		{context.Canceled, 504, "reload_wait_interrupted"},
		{errors.New("PRIVATE_HANDLER_PATH"), 503, "reload_failed"},
	} {
		handler := NewDevReloadHandler(devTriggerReloadFunc(func(context.Context, string, string) error { return scenario.failure }))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, path, nil))
		require.Equal(t, scenario.status, response.Code)
		require.JSONEq(t, `{"error":"`+scenario.code+`"}`, response.Body.String())
		require.NotContains(t, response.Body.String(), "PRIVATE")
		require.NotContains(t, response.Body.String(), "cognito_trigger_")
	}
	response := httptest.NewRecorder()
	NewDevReloadHandler(nil).ServeHTTP(response, httptest.NewRequest(http.MethodPost, path, nil))
	require.Equal(t, 404, response.Code)
	require.JSONEq(t, `{"error":"trigger_not_configured"}`, response.Body.String())
}
