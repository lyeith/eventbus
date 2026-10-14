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

func TestDevLambdaWorkerPortKeepsNativeAndPeerControlsIndependent(t *testing.T) {
	type observed struct{ method, path, query, body string }
	var native, async, warm, worker []observed
	port := func(records *[]observed, status int) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			*records = append(*records, observed{r.Method, r.URL.Path, r.URL.RawQuery, string(body)})
			w.WriteHeader(status)
		})
	}
	router := server.New(server.Services{
		Lambda: port(&native, 201), LambdaDev: port(&async, 202),
		LambdaWarmDev: port(&warm, 203), LambdaWorkerDev: port(&worker, 204),
	})
	for _, test := range []struct {
		method, path, body string
		status             int
		records            *[]observed
	}{
		{"GET", "/__eventbus/dev/lambda/invocations", "", 204, &worker},
		{"PUT", "/__eventbus/dev/lambda/invocations/request/worker-crash?owner=test", `{"function_name":"processor:live","attempt":1}`, 204, &worker},
		{"DELETE", "/__eventbus/dev/lambda/invocations/request/worker-crash", `{"function_name":"processor:live","attempt":1}`, 204, &worker},
		{"GET", "/__eventbus/dev/lambda/async/events", "", 202, &async},
		{"POST", "/__eventbus/dev/lambda/warm", `{"max_workers":2,"max_concurrency":4}`, 203, &warm},
		{"POST", "/2015-03-31/functions/processor/invocations", `{"native":true}`, 201, &native},
	} {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))
		r.Header.Set("X-Eventbus-Worker-Crash", "true")
		router.ServeHTTP(w, r)
		require.Equal(t, test.status, w.Code)
		path, query, _ := strings.Cut(test.path, "?")
		require.Equal(t, observed{test.method, path, query, test.body}, (*test.records)[len(*test.records)-1])
	}
	require.Len(t, worker, 3)
	require.Len(t, native, 1)
	require.Len(t, async, 1)
	require.Len(t, warm, 1)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest("GET", "/__eventbus/dev/lambda/invocations-lookalike", nil))
	require.Equal(t, http.StatusMethodNotAllowed, w.Code)
	require.Len(t, worker, 3, "lookalike namespace must not reach a fault control")
}

func TestDevLambdaWorkerPortAbsenceDoesNotExposeNativeRuntime(t *testing.T) {
	calls := 0
	native := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.WriteHeader(204) })
	router := server.New(server.Services{Lambda: native})
	for _, path := range []string{
		"/__eventbus/dev/lambda/invocations",
		"/__eventbus/dev/lambda/invocations/request/worker-crash",
	} {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest("PUT", path, nil))
		require.Equal(t, http.StatusNotFound, w.Code)
	}
	require.Zero(t, calls)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest("POST", "/2015-03-31/functions/processor/invocations", strings.NewReader(`{}`)))
	require.Equal(t, 204, w.Code)
	require.Equal(t, 1, calls)
}
