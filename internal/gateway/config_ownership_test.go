package gateway

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestGatewayOwnsResolvedConfigurationAfterCallerMutation(t *testing.T) {
	var invocations atomic.Int32
	authorizer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		invocations.Add(1)
		var event requestEvent
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			t.Error(err)
		}
		if event.StageVariables["version"] != "v1" || event.Headers["X-User-Id"] != "" {
			t.Errorf("caller changed the authorization event: %+v", event)
		}
		_ = json.NewEncoder(w).Encode(allowResponse(event.MethodARN, nil))
	}))
	defer authorizer.Close()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/files" || r.Header.Get("X-Version") != "v1" || r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" || r.Header.Get("X-User-Id") != "" || r.Header.Get("X-Keep") != "retained" {
			t.Errorf("caller changed the integration: path=%s headers=%v", r.URL.Path, r.Header)
		}
		_, _ = io.WriteString(w, "ok")
	}))
	defer backend.Close()
	cfg := fixture(authorizer.URL+"/2015-03-31/functions/auth/invocations", backend.URL)
	ttl := 300
	cfg.Authorizers["auth"] = AuthorizerConfig{Type: "REQUEST", InvokeURL: authorizer.URL + "/2015-03-31/functions/auth/invocations", TTL: &ttl, IdentitySources: []string{"method.request.header.Authorization"}, Timeout: time.Second}
	cfg.RemoveHeaders = []string{"X-User-Id"}
	cfg.LogRedactions = []string{"/api/{proxy+}"}
	cfg.Routes[0].Integration.RequestParameters = map[string]string{"integration.request.header.X-Version": "stageVariables.version"}
	gateway := newTestGateway(t, cfg, Options{})
	configurationBefore := ownershipJSON(t, gateway.config)
	authorizerBefore := ownershipJSON(t, gateway.authorizers["auth"].config)
	routeBefore := ownershipJSON(t, gateway.routes[0].config)

	// Mutate every caller-owned collection and pointer after construction.
	// Compiled routes and cached authorizers must keep their original recipe.
	cfg.StageVariables["version"] = "mutated"
	ttl = 0
	changedAuthorizer := cfg.Authorizers["auth"]
	changedAuthorizer.IdentitySources[0] = "method.request.header.X-Mutated"
	changedAuthorizer.InvokeURL = "http://127.0.0.1:1/2015-03-31/functions/changed/invocations"
	cfg.Authorizers["auth"] = changedAuthorizer
	delete(cfg.Authorizers, "auth")
	cfg.Routes[0].Integration.RequestParameters["integration.request.header.X-Version"] = "'mutated'"
	cfg.Routes[0].Integration.RemoveHeaders[0] = "X-Keep"
	cfg.Routes[0].Integration.RemoveHeaders[1] = "X-Keep"
	cfg.Routes[0].Path = "/changed/{proxy+}"
	cfg.Routes[0].Method = "POST"
	cfg.Routes[0].Authorizer = "changed"
	cfg.Routes[0].Integration.URI = "http://127.0.0.1:1/{proxy}"
	cfg.LogRedactions[0] = "/changed/{proxy+}"
	cfg.RemoveHeaders[0] = "X-Keep"

	if got := ownershipJSON(t, gateway.config); got != configurationBefore {
		t.Fatalf("caller mutated gateway configuration:\nbefore %s\nafter  %s", configurationBefore, got)
	}
	if got := ownershipJSON(t, gateway.authorizers["auth"].config); got != authorizerBefore {
		t.Fatalf("caller mutated authorizer configuration:\nbefore %s\nafter  %s", authorizerBefore, got)
	}
	if got := ownershipJSON(t, gateway.routes[0].config); got != routeBefore {
		t.Fatalf("caller mutated compiled route configuration:\nbefore %s\nafter  %s", routeBefore, got)
	}
	for attempt := 0; attempt < 2; attempt++ {
		request := httptest.NewRequest(http.MethodGet, "http://edge/api/files", nil)
		request.Header.Set("Authorization", "Bearer owned")
		request.Header.Set("Cookie", "owned=value")
		request.Header.Set("X-User-Id", "spoofed")
		request.Header.Set("X-Keep", "retained")
		response := httptest.NewRecorder()
		gateway.ServeHTTP(response, request)
		if response.Code != http.StatusOK || response.Body.String() != "ok" {
			t.Fatalf("request after caller mutation: %d %s", response.Code, response.Body.String())
		}
	}
	if invocations.Load() != 1 {
		t.Fatalf("caller changed the authorizer TTL or identity sources: invocations=%d", invocations.Load())
	}
}

func TestGatewayDefaultsDoNotMutateCallerConfiguration(t *testing.T) {
	cfg := Config{
		Authorizers: map[string]AuthorizerConfig{"auth": {Type: "REQUEST", InvokeURL: "http://127.0.0.1:1/2015-03-31/functions/auth/invocations", IdentitySources: []string{"method.request.header.Authorization"}}},
		Routes:      []RouteConfig{{Path: "/api/{proxy+}", Authorizer: "auth", Integration: IntegrationConfig{Type: "HTTP_PROXY", URI: "http://127.0.0.1:1/{proxy}"}}},
	}
	before := ownershipJSON(t, cfg)
	gateway := newTestGateway(t, cfg, Options{})
	if got := ownershipJSON(t, cfg); got != before {
		t.Fatalf("defaults mutated the caller:\nbefore %s\nafter  %s", before, got)
	}
	authorizer := gateway.authorizers["auth"].config
	if gateway.config.Port != 4180 || gateway.config.Region != "us-east-1" || gateway.config.AccountID != "000000000000" || gateway.config.APIID != "local" || gateway.config.Stage != "dev" || gateway.routes[0].config.Method != "ANY" || authorizer.TTL == nil || *authorizer.TTL != 300 || authorizer.Timeout != 10*time.Second {
		t.Fatalf("resolved defaults changed: config=%+v authorizer=%+v", gateway.config, authorizer)
	}
}

func ownershipJSON(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}
