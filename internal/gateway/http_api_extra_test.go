package gateway

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestNativeHTTPAuthorizerPolicyAndSimpleFailures(t *testing.T) {
	for _, test := range []struct {
		name, payload string
		simple        bool
		functionError bool
		want          int
	}{
		{name: "direct Unauthorized", payload: `{"errorMessage":"Unauthorized"}`, want: 401},
		{name: "function Unauthorized", payload: `{"errorMessage":"Unauthorized"}`, functionError: true, want: 401},
		{name: "simple disabled", payload: `{"isAuthorized":true}`, want: 500},
		{name: "simple null", payload: `{"isAuthorized":null}`, simple: true, want: 500},
		{name: "simple denied", payload: `{"isAuthorized":false}`, simple: true, want: 403},
		{name: "malformed", payload: `{"principalId":"p","policyDocument":{}}`, want: 500},
	} {
		t.Run(test.name, func(t *testing.T) {
			var integrationCalls atomic.Int32
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.Contains(r.URL.Path, "/auth/") {
					if test.functionError {
						w.Header().Set("X-Amz-Function-Error", "Unhandled")
					}
					_, _ = io.WriteString(w, test.payload)
				} else {
					integrationCalls.Add(1)
					_, _ = io.WriteString(w, `{"ok":true}`)
				}
			}))
			defer endpoint.Close()
			cfg := httpAPIConfig(endpoint.URL, "2.0", "2.0")
			auth := cfg.Authorizers["auth"]
			auth.IdentitySources = nil
			auth.EnableSimpleResponses = test.simple
			cfg.Authorizers["auth"] = auth
			gateway := newTestGateway(t, cfg, Options{})
			response := httptest.NewRecorder()
			gateway.ServeHTTP(response, httptest.NewRequest("POST", "/items/1", nil))
			if response.Code != test.want || integrationCalls.Load() != 0 {
				t.Fatalf("fail closed: %d %d %s", response.Code, integrationCalls.Load(), response.Body.String())
			}
		})
	}
}

func TestNativeAuthorizerWorksWithExistingPrivateHTTPProxyMapping(t *testing.T) {
	authorizer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var event map[string]any
		_ = json.NewDecoder(r.Body).Decode(&event)
		_ = json.NewEncoder(w).Encode(map[string]any{"isAuthorized": true, "context": map[string]any{"tenant": "trusted", "number": 42, "boolean": true, "nested": map[string]string{"private": "value"}}})
	}))
	defer authorizer.Close()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Tenant") != "trusted" || r.Header.Get("X-Number") != "42" || r.Header.Get("X-Boolean") != "true" {
			t.Errorf("trusted scalar mappings %v", r.Header)
		}
		if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			t.Error("private credentials escaped HTTP proxy boundary")
		}
		_, _ = io.WriteString(w, "ok")
	}))
	defer backend.Close()
	cfg := fixture(authorizer.URL+"/2015-03-31/functions/auth/invocations", backend.URL)
	auth := cfg.Authorizers["auth"]
	auth.PayloadFormatVersion = "2.0"
	auth.EnableSimpleResponses = true
	auth.IdentitySources = []string{"$request.header.Authorization"}
	cfg.Authorizers["auth"] = auth
	cfg.Routes[0].Integration.RequestParameters = map[string]string{"integration.request.header.X-Tenant": "context.authorizer.tenant", "integration.request.header.X-Number": "context.authorizer.number", "integration.request.header.X-Boolean": "context.authorizer.boolean"}
	gateway := newTestGateway(t, cfg, Options{})
	request := httptest.NewRequest("POST", "/api/owned", nil)
	request.Header.Set("Authorization", "valid")
	request.Header.Set("Cookie", "private")
	request.Header.Set("X-Tenant", "spoof")
	request.Header.Set("Connection", "X-Tenant")
	response := httptest.NewRecorder()
	gateway.ServeHTTP(response, request)
	if response.Code != 200 || response.Body.String() != "ok" {
		t.Fatalf("native authorizer/private HTTP mapping %d %s", response.Code, response.Body.String())
	}
}

func TestDefaultRouteDoesNotShadowSpecificRoute(t *testing.T) {
	var keys []string
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var event map[string]any
		_ = json.NewDecoder(r.Body).Decode(&event)
		keys = append(keys, event["routeKey"].(string))
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer endpoint.Close()
	cfg := httpAPIConfig(endpoint.URL, "public", "2.0")
	specific := cfg.Routes[0]
	cfg.Routes[0].Path = "/{proxy+}"
	cfg.Routes[0].Method = "ANY"
	cfg.Routes[0].RouteKey = "$default"
	cfg.Routes = append(cfg.Routes, specific)
	gateway := newTestGateway(t, cfg, Options{})
	for _, request := range []*http.Request{httptest.NewRequest("POST", "/items/1", nil), httptest.NewRequest("GET", "/items/1", nil), httptest.NewRequest("GET", "/", nil)} {
		response := httptest.NewRecorder()
		gateway.ServeHTTP(response, request)
		if response.Code != 200 {
			t.Fatal(response.Code)
		}
	}
	if strings.Join(keys, ",") != "POST /items/{id},$default,$default" {
		t.Fatalf("wrong native route precedence: %v", keys)
	}
}

func TestBinaryRequestClassificationAndNoBodyResponses(t *testing.T) {
	for _, test := range []struct {
		body, contentType string
		binary            bool
	}{
		{"plain", "text/plain; charset=utf-8", false}, {`{"x":1}`, "application/json", false}, {"xml", "application/fixture+xml", false}, {"ascii", "application/octet-stream", true}, {string([]byte{255}), "text/plain", true}, {"ascii", "invalid mime type", true},
	} {
		_, binary := requestBody([]byte(test.body), test.contentType)
		if binary != test.binary {
			t.Fatalf("%q %q binary=%t", test.body, test.contentType, binary)
		}
	}
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"statusCode":204,"body":"discarded","headers":{"Content-Length":"999","Connection":"X-Private","X-Private":"secret"}}`)
	}))
	defer endpoint.Close()
	cfg := httpAPIConfig(endpoint.URL, "public", "2.0")
	cfg.Routes[0].Method = "ANY"
	gateway := newTestGateway(t, cfg, Options{})
	response := httptest.NewRecorder()
	gateway.ServeHTTP(response, httptest.NewRequest("HEAD", "/items/1", nil))
	if response.Code != 204 || response.Body.Len() != 0 || response.Header().Get("Content-Length") != "" || response.Header().Get("X-Private") != "" {
		t.Fatalf("function controlled HTTP framing: %d %v %s", response.Code, response.Header(), response.Body.String())
	}
}
