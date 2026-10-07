package gateway

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestDevHealthDefaultAndRequestBoundary(t *testing.T) {
	cfg := httpAPIConfig("http://127.0.0.1:1", "2.0", "2.0")
	cfg.Authorizers["auth"] = AuthorizerConfig{Type: "REQUEST", PayloadFormatVersion: "2.0", InvokeURL: "http://127.0.0.1:1/2015-03-31/functions/auth/invocations", TTL: intPointer(0), IdentitySources: []string{"$request.header.Authorization"}}
	cfg.Routes[0].Path, cfg.Routes[0].Method, cfg.Routes[0].RouteKey = "/{proxy+}", "ANY", "$default"
	edge := newTestGateway(t, cfg, Options{})
	for _, tc := range []struct {
		method, path string
		status       int
	}{
		{"GET", "/health", 200},
		{"GET", "/health?probe=1", 200},
		{"GET", "/%68ealth", 200},
		{"HEAD", "/health", 401},
		{"POST", "/health", 401},
		{"GET", "/health/", 401},
		{"GET", "/health%2f", 400},
		{"GET", "/%2e/health", 400},
	} {
		t.Run(tc.method+tc.path, func(t *testing.T) {
			response := httptest.NewRecorder()
			edge.ServeHTTP(response, httptest.NewRequest(tc.method, tc.path, nil))
			if response.Code != tc.status {
				t.Fatalf("response %d: %s", response.Code, response.Body.String())
			}
			if tc.status == 200 && response.Body.String() != `{"status":"healthy","service":"eventbus-gateway"}` {
				t.Fatalf("default readiness changed: %s", response.Body.String())
			}
		})
	}
	if cfg.DevHealthPath != "" {
		t.Fatal("gateway changed its caller's default configuration")
	}
	if err := edge.Close(); err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	edge.ServeHTTP(response, httptest.NewRequest("GET", "/health", nil))
	if response.Code != 503 {
		t.Fatalf("closed gateway still reports ready: %d", response.Code)
	}
}

func TestDevHealthValidationAndAPIMappingCollisions(t *testing.T) {
	for _, tc := range []struct {
		name, readiness, base, route, method, routeKey string
		wantError                                      string
	}{
		{name: "default", route: "/api"},
		{name: "private", readiness: "/.eventbus/ready", route: "/health"},
		{name: "root", readiness: "/", route: "/health"},
		{name: "literal GET collision", readiness: "/health", route: "/health", method: "GET", wantError: "conflicts"},
		{name: "literal ANY collision", readiness: "/health", route: "/health", method: "ANY", wantError: "conflicts"},
		{name: "default collision", route: "/health", wantError: "conflicts"},
		{name: "parameter collision", readiness: "/ready", route: "/{id}", wantError: "conflicts"},
		{name: "separate literal trailing slash", readiness: "/health", route: "/health/"},
		{name: "separate parameter trailing slash", readiness: "/ready", route: "/{id}/"},
		{name: "mapped separate trailing slash", readiness: "/edge/health", base: "/edge", route: "/health/"},
		{name: "POST only", readiness: "/health", route: "/health", method: "POST"},
		{name: "greedy fallback", readiness: "/.eventbus/ready", route: "/{proxy+}", method: "ANY"},
		{name: "default fallback", readiness: "/.eventbus/ready", route: "/{proxy+}", method: "ANY", routeKey: "$default"},
		{name: "mapped collision", readiness: "/edge/health", base: "/edge", route: "/health", wantError: "conflicts"},
		{name: "mapped root collision", readiness: "/edge", base: "/edge", route: "/", wantError: "conflicts"},
		{name: "outside mapping", readiness: "/health", base: "/edge", route: "/health"},
		{name: "mapping prefix boundary", readiness: "/edgewise/health", base: "/edge", route: "/health"},
		{name: "relative", readiness: "ready", route: "/app", wantError: "dev_health_path"},
		{name: "template", readiness: "/{ready}", route: "/app", wantError: "dev_health_path"},
		{name: "query", readiness: "/ready?probe=1", route: "/app", wantError: "dev_health_path"},
		{name: "fragment", readiness: "/ready#probe", route: "/app", wantError: "dev_health_path"},
		{name: "trailing slash", readiness: "/ready/", route: "/app", wantError: "dev_health_path"},
		{name: "dot segment", readiness: "/app/../ready", route: "/app", wantError: "dev_health_path"},
		{name: "double root slash", readiness: "//", route: "/app", wantError: "dev_health_path"},
		{name: "double slash", readiness: "/app//ready", route: "/app", wantError: "dev_health_path"},
		{name: "encoded", readiness: "/%72eady", route: "/app", wantError: "dev_health_path"},
		{name: "space", readiness: "/ready now", route: "/app", wantError: "dev_health_path"},
		{name: "control", readiness: "/ready\n", route: "/app", wantError: "dev_health_path"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			method := tc.method
			if method == "" {
				method = "GET"
			}
			cfg := Config{DevHealthPath: tc.readiness, BasePath: tc.base, Routes: []RouteConfig{{Path: tc.route, Method: method, RouteKey: tc.routeKey, Integration: IntegrationConfig{Type: "HTTP_PROXY", URI: "http://127.0.0.1:1"}}}}
			err := cfg.Validate()
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("want %q validation error, got %v", tc.wantError, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			before := cfg.DevHealthPath
			if err := cfg.Validate(); err != nil || before != cfg.DevHealthPath {
				t.Fatalf("validation not idempotent: %v", err)
			}
		})
	}
}

func TestDevHealthYAMLRecipe(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "gateway.yaml")
	data := "dev_health_path: /.eventbus/ready\nroutes:\n  - path: /health\n    method: GET\n    integration:\n      type: HTTP_PROXY\n      uri: http://127.0.0.1:1\n"
	if err := os.WriteFile(filename, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(filename)
	if err != nil {
		t.Fatal(err)
	}
	edge := newTestGateway(t, *cfg, Options{})
	cfg.DevHealthPath = "/changed"
	response := httptest.NewRecorder()
	edge.ServeHTTP(response, httptest.NewRequest("GET", "/.eventbus/ready", nil))
	if response.Code != 200 || !strings.Contains(response.Body.String(), "eventbus-gateway") {
		t.Fatalf("readiness recipe lost: %d %s", response.Code, response.Body.String())
	}
}

func TestDevHealthDoesNotShadowProtectedNativeApplication(t *testing.T) {
	for _, authVersion := range []string{"", "1.0", "2.0"} {
		for _, integrationVersion := range []string{"1.0", "2.0"} {
			for _, fallback := range []bool{false, true} {
				t.Run(fmt.Sprintf("auth=%s/integration=%s/default=%t", authVersion, integrationVersion, fallback), func(t *testing.T) {
					var authCalls, integrationCalls atomic.Int32
					endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						var event map[string]any
						if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
							t.Error(err)
							w.WriteHeader(500)
							return
						}
						if strings.Contains(r.URL.Path, "/auth/") {
							authCalls.Add(1)
							var credential string
							for key, value := range event["headers"].(map[string]any) {
								if strings.EqualFold(key, "Authorization") {
									credential, _ = value.(string)
								}
							}
							if credential == "" {
								w.Header().Set("X-Amz-Function-Error", "Unhandled")
								_, _ = io.WriteString(w, `{"errorMessage":"Unauthorized"}`)
								return
							}
							arnKey, pathKey := "methodArn", "path"
							if authVersion == "2.0" {
								arnKey, pathKey = "routeArn", "rawPath"
							}
							if event[pathKey] != "/health" {
								t.Errorf("authorizer application path changed: %v", event)
							}
							result := allowResponse(event[arnKey].(string), nil)
							if credential != "valid" {
								result["policyDocument"].(map[string]any)["Statement"].([]any)[0].(map[string]any)["Effect"] = "Deny"
							}
							_ = json.NewEncoder(w).Encode(result)
							return
						}
						integrationCalls.Add(1)
						payload, _ := json.Marshal(event)
						_ = json.NewEncoder(w).Encode(map[string]any{"statusCode": 201, "body": string(payload)})
					}))
					defer endpoint.Close()
					cfg := httpAPIConfig(endpoint.URL, authVersion, integrationVersion)
					cfg.DevHealthPath = "/.eventbus/ready"
					auth := cfg.Authorizers["auth"]
					auth.IdentitySources = []string{"$request.header.Authorization"}
					if authVersion == "" {
						auth.IdentitySources = []string{"method.request.header.Authorization"}
					}
					cfg.Authorizers["auth"] = auth
					cfg.Routes[0].Path, cfg.Routes[0].Method = "/health", "GET"
					if fallback {
						cfg.Routes[0].Path, cfg.Routes[0].Method, cfg.Routes[0].RouteKey = "/{proxy+}", "ANY", "$default"
					}
					edge := newTestGateway(t, cfg, Options{})
					response := httptest.NewRecorder()
					edge.ServeHTTP(response, httptest.NewRequest("GET", "/.eventbus/ready?probe=1", nil))
					if response.Code != 200 || authCalls.Load() != 0 || integrationCalls.Load() != 0 {
						t.Fatal("readiness reached native application owners")
					}
					for _, tc := range []struct {
						credential string
						status     int
					}{{"", 401}, {"invalid", 403}, {"valid", 201}} {
						request := httptest.NewRequest("GET", "/health?original=1", nil)
						if tc.credential != "" {
							request.Header.Set("Authorization", tc.credential)
						}
						response := httptest.NewRecorder()
						edge.ServeHTTP(response, request)
						if response.Code != tc.status {
							t.Fatalf("credential %q returned %d: %s", tc.credential, response.Code, response.Body.String())
						}
						if tc.status != 201 {
							if integrationCalls.Load() != 0 {
								t.Fatal("refused application request reached integration")
							}
							continue
						}
						var event map[string]any
						if err := json.Unmarshal(response.Body.Bytes(), &event); err != nil {
							t.Fatal(err)
						}
						if event["version"] != integrationVersion {
							t.Fatalf("integration format changed: %v", event)
						}
						pathKey := "path"
						if integrationVersion == "2.0" {
							pathKey = "rawPath"
						}
						if event[pathKey] != "/health" || event["queryStringParameters"].(map[string]any)["original"] != "1" {
							t.Fatalf("application request path/query changed: %v", event)
						}
					}
					if authCalls.Load() < 2 || integrationCalls.Load() != 1 {
						t.Fatalf("native owners skipped: auth=%d integration=%d", authCalls.Load(), integrationCalls.Load())
					}
				})
			}
		}
	}
}
