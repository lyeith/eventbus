package server_test

import (
	"github.com/lyeith/eventbus/internal/server"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWarmTuningDispatcherHasSeparateExactDevelopmentPort(t *testing.T) {
	var warm, async, native int
	handler := func(count *int, status int) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			*count++
			w.WriteHeader(status)
		})
	}
	router := server.New(server.Services{Lambda: handler(&native, 201), LambdaDev: handler(&async, 202), LambdaWarmDev: handler(&warm, 203)})
	for _, scenario := range []struct {
		path   string
		status int
	}{
		{"/__eventbus/dev/lambda/warm", 203},
		{"/__eventbus/dev/lambda/async/events", 202},
		{"/2015-03-31/functions/registered/invocations", 201},
		{"/__eventbus/dev/lambda/warming", 503},
		{"/__eventbus/dev/lambda/warm/extra", 503},
	} {
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest("POST", scenario.path, strings.NewReader("{}")))
		require.Equal(t, scenario.status, response.Code, scenario.path)
	}
	require.Equal(t, 1, warm)
	require.Equal(t, 1, async)
	require.Equal(t, 1, native)
	absent := httptest.NewRecorder()
	server.New(server.Services{Lambda: handler(&native, 201)}).ServeHTTP(absent, httptest.NewRequest("POST", "/__eventbus/dev/lambda/warm", strings.NewReader("{}")))
	require.Equal(t, http.StatusNotFound, absent.Code)
	require.Equal(t, 1, native)
}
