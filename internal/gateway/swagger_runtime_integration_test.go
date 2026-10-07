//go:build sdksmoke

package gateway_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/gateway"
	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/server"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// The opt-in SDK lane supplies frozen Express/Swagger dependencies. Missing
// dependencies fail this lane; normal gateway tests do not require npm packages.
func TestNativeGatewayUnchangedExpressSwagger(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)
	packagePath := filepath.Clean(filepath.Join(filepath.Dir(filename), "../../tests/sdk/javascript/package.json"))
	for _, dependency := range []string{"express", "swagger-ui-express"} {
		_, err := os.Stat(filepath.Join(filepath.Dir(packagePath), "node_modules", dependency, "package.json"))
		require.NoError(t, err, "install the frozen JavaScript SDK lane with npm ci --ignore-scripts")
	}
	directory := t.TempDir()
	source, err := os.ReadFile(filepath.Join(filepath.Dir(filename), "testdata", "swagger_handler.mjs"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(directory, "swagger_handler.mjs"), source, 0600))
	function := lambdaservice.Function{Runtime: "node", Handler: "swagger_handler.mjs#docs", Timeout: 10 * time.Second, Environment: map[string]string{"FRAMEWORK_PACKAGE_JSON": packagePath}}
	authorizer, fallback := function, function
	authorizer.Handler, fallback.Handler = "swagger_handler.mjs#authorize", "swagger_handler.mjs#fallback"
	functions, err := lambdaservice.NewService(&lambdaservice.Config{Functions: map[string]lambdaservice.Function{"docs": function, "authorize": authorizer, "fallback": fallback}}, directory)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, functions.Close(ctx))
	})
	aws := httptest.NewServer(server.New(server.Services{Lambda: functions}))
	t.Cleanup(aws.Close)
	for _, version := range []string{"1.0", "2.0"} {
		t.Run(version, func(t *testing.T) {
			zero := 0
			integration := gateway.IntegrationConfig{Type: "AWS_PROXY", InvokeURL: aws.URL + "/2015-03-31/functions/docs/invocations", PayloadFormatVersion: version, Timeout: 10 * time.Second}
			private := integration
			private.InvokeURL = aws.URL + "/2015-03-31/functions/fallback/invocations"
			cfg := gateway.Config{Stage: "$default", DevHealthPath: "/.eventbus/ready", Authorizers: map[string]gateway.AuthorizerConfig{"auth": {Type: "REQUEST", InvokeURL: aws.URL + "/2015-03-31/functions/authorize/invocations", PayloadFormatVersion: "2.0", EnableSimpleResponses: true, TTL: &zero, IdentitySources: []string{"$request.header.Authorization"}, Timeout: 10 * time.Second}}, Routes: []gateway.RouteConfig{
				{Path: "/{proxy+}", Method: "ANY", RouteKey: "$default", Authorizer: "auth", Integration: private},
				{Path: "/api/example/docs/{proxy+}", Method: "GET", Integration: integration},
				{Path: "/api/example/docs", Method: "GET", Integration: integration},
				{Path: "/api/example/docs/", Method: "GET", Integration: integration},
				{Path: "/api/example/docs/private/", Method: "GET", Authorizer: "auth", Integration: private},
			}}
			edge, err := gateway.New(cfg, gateway.Options{Logger: zerolog.Nop()})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, edge.Close()) })
			management := httptest.NewServer(edge)
			t.Cleanup(management.Close)
			target, err := url.Parse(management.URL)
			require.NoError(t, err)
			proxy := httputil.NewSingleHostReverseProxy(target)
			// Public ingress is application-owned. Its management guard executes
			// before the private gateway listener, without changing application paths.
			public := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasPrefix(r.URL.Path, "/.eventbus/") {
					http.NotFound(w, r)
					return
				}
				proxy.ServeHTTP(w, r)
			}))
			t.Cleanup(public.Close)
			client := public.Client()
			client.Timeout = 15 * time.Second
			client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
			get := func(path, credential string) (*http.Response, string) {
				t.Helper()
				request, err := http.NewRequest("GET", public.URL+path, nil)
				require.NoError(t, err)
				request.Header.Set("Authorization", credential)
				response, err := client.Do(request)
				require.NoError(t, err)
				body, err := io.ReadAll(response.Body)
				response.Body.Close()
				require.NoError(t, err)
				return response, string(body)
			}
			response, body := get("/api/example/docs", "")
			require.Equal(t, 301, response.StatusCode, body)
			require.Equal(t, "/api/example/docs/", response.Header.Get("Location"))
			require.Equal(t, "/api/example/docs", response.Header.Get("X-Fixture-Native-Path"))
			response, body = get(response.Header.Get("Location"), "")
			require.Equal(t, 200, response.StatusCode, body)
			require.Contains(t, body, "Swagger UI")
			require.Empty(t, response.Header.Get("Location"), "original slash must not redirect again")
			require.Equal(t, "/api/example/docs/", response.Header.Get("X-Fixture-Native-Path"))
			require.Equal(t, "GET /api/example/docs/", response.Header.Get("X-Fixture-Native-Route"))
			for _, asset := range []struct{ path, contentType, contains string }{
				{"index.html", "text/html", "Swagger UI"},
				{"swagger-ui-init.js", "application/javascript", "EventBus unchanged Swagger fixture"},
				{"swagger-ui.css", "text/css", ".swagger-ui"},
				{"swagger-ui-bundle.js", "text/javascript", "SwaggerUIBundle"},
				{"nested/page", "text/html", "Swagger UI"},
			} {
				path := "/api/example/docs/" + asset.path
				response, body = get(path, "")
				require.Equal(t, 200, response.StatusCode, path)
				require.Contains(t, response.Header.Get("Content-Type"), asset.contentType, path)
				require.Contains(t, body, asset.contains, path)
				require.Equal(t, path, response.Header.Get("X-Fixture-Native-Path"))
				require.Equal(t, "GET /api/example/docs/{proxy+}", response.Header.Get("X-Fixture-Native-Route"))
				require.Empty(t, response.Header.Get("X-Fixture-Default"))
			}
			for _, path := range []string{"/private", "/api/example/docs/private/"} {
				response, body = get(path, "")
				require.Equal(t, 401, response.StatusCode, body)
				response, body = get(path, "invalid")
				require.Equal(t, 403, response.StatusCode, body)
				response, body = get(path, "Bearer owned")
				require.Equal(t, 404, response.StatusCode, body)
				require.Equal(t, "selected", response.Header.Get("X-Fixture-Default"))
			}
			readiness, err := management.Client().Get(management.URL + "/.eventbus/ready")
			require.NoError(t, err)
			readiness.Body.Close()
			require.Equal(t, 200, readiness.StatusCode)
			for _, path := range []string{"/.eventbus/ready", "/.eventbus/ready/", "/%2eeventbus/ready"} {
				response, body = get(path, "Bearer owned")
				require.Equal(t, 404, response.StatusCode, body)
				require.Empty(t, response.Header.Get("X-Fixture-Default"), "public guard must reject before Lambda")
			}
		})
	}
}
