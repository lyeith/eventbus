package server_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/lyeith/eventbus/internal/server"
	"github.com/stretchr/testify/require"
)

func TestDevLambdaPortForwardsOnlyDevelopmentNamespace(t *testing.T) {
	type observed struct{ method, path, query, invocationType, body string }
	var native, development []observed
	port := func(records *[]observed, status int) http.Handler {
		return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			body, err := io.ReadAll(request.Body)
			require.NoError(t, err)
			*records = append(*records, observed{request.Method, request.URL.Path,
				request.URL.RawQuery, request.Header.Get("X-Amz-Invocation-Type"), string(body)})
			writer.WriteHeader(status)
		})
	}
	router := server.New(server.Services{Lambda: port(&native, 201), LambdaDev: port(&development, 202)})
	for _, scenario := range []struct {
		method, path, invocationType, body string
		status                             int
	}{
		{"GET", "/__eventbus/dev/lambda/async/events", "", "", 202},
		{"PUT", "/__eventbus/dev/lambda/async/events/request/completion-loss?owner=test", "", `{"function_name":"processor:live","attempt":1}`, 202},
		{"DELETE", "/__eventbus/dev/lambda/async/unknown", "", `{}`, 202},
		{"POST", "/2015-03-31/functions/processor/invocations?Qualifier=live", "Event", " { \"native\": true } \n", 201},
		{"GET", "/2015-03-31/functions/processor:live", "", "", 201},
	} {
		request := httptest.NewRequest(scenario.method, scenario.path, strings.NewReader(scenario.body))
		request.Header.Set("X-Amz-Invocation-Type", scenario.invocationType)
		request.Header.Set("X-Eventbus-Completion-Loss", "true")
		result := httptest.NewRecorder()
		router.ServeHTTP(result, request)
		require.Equal(t, scenario.status, result.Code)
		path, query, _ := strings.Cut(scenario.path, "?")
		want := observed{scenario.method, path, query, scenario.invocationType, scenario.body}
		if scenario.status == 202 {
			require.Equal(t, want, development[len(development)-1])
		} else {
			require.Equal(t, want, native[len(native)-1])
		}
	}
	require.Len(t, development, 3)
	require.Len(t, native, 2)
	result := httptest.NewRecorder()
	router.ServeHTTP(result, httptest.NewRequest("GET", "/__eventbus/dev/lambda/asyncity/events", nil))
	require.Equal(t, 405, result.Code)
	require.Len(t, development, 3, "a lookalike namespace must not reach the development port")
	require.Len(t, native, 2)
}

func TestDevLambdaPortAbsenceKeepsNativeRouteIndependent(t *testing.T) {
	nativeCalls, devCalls := 0, 0
	native := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		nativeCalls++
		writer.WriteHeader(204)
	})
	development := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		devCalls++
		writer.WriteHeader(204)
	})
	router := server.New(server.Services{Lambda: native})
	for _, method := range []string{"GET", "PUT", "DELETE"} {
		result := httptest.NewRecorder()
		router.ServeHTTP(result, httptest.NewRequest(method, "/__eventbus/dev/lambda/async/events", nil))
		require.Equal(t, 404, result.Code)
	}
	require.Zero(t, nativeCalls)
	result := httptest.NewRecorder()
	router.ServeHTTP(result, httptest.NewRequest("POST", "/2015-03-31/functions/processor/invocations", strings.NewReader(`{}`)))
	require.Equal(t, 204, result.Code)
	require.Equal(t, 1, nativeCalls)
	result = httptest.NewRecorder()
	server.New(server.Services{LambdaDev: development}).ServeHTTP(result,
		httptest.NewRequest("POST", "/2015-03-31/functions/processor/invocations", strings.NewReader(`{}`)))
	require.Equal(t, 404, result.Code)
	require.Equal(t, "ResourceNotFoundException", result.Header().Get("X-Amzn-ErrorType"))
	require.Zero(t, devCalls)
}
