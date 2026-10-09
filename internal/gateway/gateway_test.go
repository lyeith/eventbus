package gateway

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func intPointer(value int) *int { return &value }
func allowResponse(arn string, context map[string]any) map[string]any {
	return map[string]any{"principalId": "canonical-principal", "policyDocument": map[string]any{"Version": "2012-10-17", "Statement": []any{map[string]any{"Effect": "Allow", "Action": "execute-api:Invoke", "Resource": arn}}}, "context": context}
}
func fixture(invokeURL, backendURL string) Config {
	return Config{Region: "us-east-1", AccountID: "123456789012", APIID: "fixture", Stage: "local", StageVariables: map[string]string{"version": "v1"}, Authorizers: map[string]AuthorizerConfig{"auth": {Type: "REQUEST", InvokeURL: invokeURL, TTL: intPointer(0), Timeout: time.Second}}, Routes: []RouteConfig{{Path: "/api/{proxy+}", Method: "ANY", Authorizer: "auth", Integration: IntegrationConfig{Type: "HTTP_PROXY", URI: backendURL + "/{proxy}", RemoveHeaders: []string{"Cookie", "Authorization"}}}}}
}
func newTestGateway(t *testing.T, cfg Config, options Options) *Gateway {
	t.Helper()
	gateway, err := New(cfg, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := gateway.Close(); err != nil {
			t.Error(err)
		}
	})
	return gateway
}

func TestInvokeContractAndPrivateIntegrationMapping(t *testing.T) {
	var event requestEvent
	authorizer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/2015-03-31/functions/auth/invocations" || r.URL.Query().Get("Qualifier") != "test" || r.Header.Get("X-Amz-Invocation-Type") != "RequestResponse" {
			t.Errorf("wrong Invoke request: %s %s %v", r.Method, r.URL, r.Header)
		}
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			t.Error(err)
		}
		if event.Type != "REQUEST" || event.Path != "/api/files/items/a" || event.Resource != "/api/{proxy+}" || event.HTTPMethod != "PATCH" || event.MethodARN != "arn:aws:execute-api:us-east-1:123456789012:fixture/local/PATCH/api/files/items/a" {
			t.Errorf("incorrect event: %+v", event)
		}
		if event.PathParameters["proxy"] != "files/items/a" || len(event.MultiValueQueryStringParameters["tag"]) != 2 || event.StageVariables["version"] != "v1" {
			t.Errorf("incorrect parameters: %+v", event)
		}
		if event.Headers["X-User-Id"] != "" || event.Headers["Cookie"] != "auth_token=secret" || event.RequestContext.RequestID == "spoofed" || event.RequestContext.Identity.SourceIP != "192.0.2.1" || event.RequestContext.Path != "/local/api/files/items/a" || event.RequestContext.Protocol != "HTTP/1.1" || event.RequestContext.RequestTimeEpoch == 0 {
			t.Errorf("invalid boundary event: %+v", event)
		}
		_ = json.NewEncoder(w).Encode(allowResponse(event.MethodARN, map[string]any{"gatewayPath": "private/tenant/object", "number": 42, "boolean": true}))
	}))
	defer authorizer.Close()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/private/tenant/object" || r.URL.Query().Get("version") != "v1" || len(r.URL.Query()["tag"]) != 2 {
			t.Errorf("invalid mapped URL: %s", r.URL)
		}
		if r.Header.Get("Cookie") != "" || r.Header.Get("Authorization") != "" || r.Header.Get("X-User-Id") != "" {
			t.Errorf("credentials escaped: %v", r.Header)
		}
		if r.Header.Get("X-Principal") != "canonical-principal" || r.Header.Get("X-Number") != "42" || r.Header.Get("X-Boolean") != "true" || r.Header.Get("X-Request-Id") != event.RequestContext.RequestID {
			t.Errorf("invalid context mappings: %v", r.Header)
		}
		body, _ := io.ReadAll(r.Body)
		if string(body) != "request body" {
			t.Errorf("body %q", body)
		}
		w.Header().Add("X-Request-Id", "backend-spoof")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, "ok")
	}))
	defer backend.Close()
	cfg := fixture(authorizer.URL+"/2015-03-31/functions/auth/invocations?Qualifier=test", backend.URL)
	cfg.RemoveHeaders = []string{"X-User-Id"}
	cfg.Routes[0].Integration.RequestParameters = map[string]string{"integration.request.path.proxy": "context.authorizer.gatewayPath", "integration.request.header.X-Principal": "context.authorizer.principalId", "integration.request.header.X-Request-Id": "context.requestId", "integration.request.header.X-Number": "context.authorizer.number", "integration.request.header.X-Boolean": "context.authorizer.boolean", "integration.request.querystring.version": "stageVariables.version"}
	gateway := newTestGateway(t, cfg, Options{})
	request := httptest.NewRequest(http.MethodPatch, "http://edge/api/files/items/a?tag=a&tag=b", strings.NewReader("request body"))
	request.Header.Set("Cookie", "auth_token=secret")
	request.Header.Set("Authorization", "Bearer secret")
	request.Header.Set("X-User-Id", "attacker")
	request.Header.Set("X-Request-Id", "spoofed")
	response := httptest.NewRecorder()
	gateway.ServeHTTP(response, request)
	if response.Code != http.StatusCreated || response.Body.String() != "ok" {
		t.Fatalf("response %d %s", response.Code, response.Body.String())
	}
	if ids := response.Header().Values("X-Request-Id"); len(ids) != 1 || ids[0] != event.RequestContext.RequestID {
		t.Errorf("backend request ID was not removed: %v", ids)
	}
	if strings.Contains(response.Body.String(), "canonical-principal") {
		t.Fatal("authorizer context leaked to client")
	}
}

func TestAuthorizerFailuresDoNotReachBackend(t *testing.T) {
	for _, tc := range []struct {
		name          string
		status        int
		functionError string
		payload       string
		want          int
	}{
		{"unauthorized", 200, "Unhandled", `{"errorMessage":"Unauthorized"}`, 401},
		{"other function error", 200, "Unhandled", `{"errorMessage":"private secret"}`, 500},
		{"invoke service error", 403, "", `{"message":"secret"}`, 500},
		{"malformed", 200, "", `{`, 500},
		{"missing principal", 200, "", `{"policyDocument":{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}}`, 500},
		{"explicit deny", 200, "", `{"principalId":"p","policyDocument":{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"*","Resource":"*"}]}}`, 403},
		{"implicit deny", 200, "", `{"principalId":"p","policyDocument":{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}}`, 403},
		{"unknown condition", 200, "", `{"principalId":"p","policyDocument":{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*","Condition":{"StringEquals":{"aws:SourceIp":"x"}}}]}}`, 500},
		{"object context", 200, "", `{"principalId":"p","policyDocument":{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]},"context":{"private":{"value":"secret"}}}`, 500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			authorizer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.functionError != "" {
					w.Header().Set("X-Amz-Function-Error", tc.functionError)
				}
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.payload)
			}))
			defer authorizer.Close()
			var backendCalls atomic.Int32
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { backendCalls.Add(1) }))
			defer backend.Close()
			gateway := newTestGateway(t, fixture(authorizer.URL+"/2015-03-31/functions/auth/invocations", backend.URL), Options{})
			response := httptest.NewRecorder()
			gateway.ServeHTTP(response, httptest.NewRequest("GET", "/api/item", nil))
			if response.Code != tc.want || backendCalls.Load() != 0 {
				t.Fatalf("status=%d calls=%d body=%s", response.Code, backendCalls.Load(), response.Body.String())
			}
			if strings.Contains(response.Body.String(), "secret") {
				t.Fatal("private failure leaked")
			}
		})
	}
}

func TestNoCacheAndPublicRoutes(t *testing.T) {
	var calls atomic.Int32
	authorizer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var event requestEvent
		_ = json.NewDecoder(r.Body).Decode(&event)
		_ = json.NewEncoder(w).Encode(allowResponse(event.MethodARN, nil))
	}))
	defer authorizer.Close()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "ok") }))
	defer backend.Close()
	cfg := fixture(authorizer.URL+"/2015-03-31/functions/auth/invocations", backend.URL)
	cfg.Authorizers["auth"] = AuthorizerConfig{Type: "REQUEST", InvokeURL: authorizer.URL + "/2015-03-31/functions/auth/invocations", TTL: intPointer(0), IdentitySources: []string{"method.request.header.Authorization"}}
	cfg.Routes = append(cfg.Routes, RouteConfig{Path: "/api/public", Method: "GET", Integration: IntegrationConfig{Type: "HTTP_PROXY", URI: backend.URL + "/public"}})
	gateway := newTestGateway(t, cfg, Options{})
	for _, path := range []string{"/api/a", "/api/a", "/api/public"} {
		response := httptest.NewRecorder()
		gateway.ServeHTTP(response, httptest.NewRequest("GET", path, nil))
		if response.Code != 200 {
			t.Fatalf("%s: %d", path, response.Code)
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("TTL0 must invoke with missing identity source, public must bypass: %d", calls.Load())
	}
	bypass := newTestGateway(t, cfg, Options{NoAuth: true})
	response := httptest.NewRecorder()
	bypass.ServeHTTP(response, httptest.NewRequest("GET", "/api/a", nil))
	if response.Code != 200 || calls.Load() != 2 {
		t.Fatalf("explicit no-auth failed")
	}
}

func TestCacheUsesIdentityButEvaluatesCurrentARN(t *testing.T) {
	var calls atomic.Int32
	authorizer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var event requestEvent
		_ = json.NewDecoder(r.Body).Decode(&event)
		_ = json.NewEncoder(w).Encode(allowResponse(event.MethodARN, nil))
	}))
	defer authorizer.Close()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "ok") }))
	defer backend.Close()
	cfg := fixture(authorizer.URL+"/2015-03-31/functions/auth/invocations", backend.URL)
	auth := cfg.Authorizers["auth"]
	auth.TTL = intPointer(30)
	auth.IdentitySources = []string{"method.request.header.Authorization", "method.request.querystring.tenant"}
	cfg.Authorizers["auth"] = auth
	gateway := newTestGateway(t, cfg, Options{})
	now := time.Now()
	gateway.authorizers["auth"].now = func() time.Time { return now }
	invoke := func(path, token string) int {
		request := httptest.NewRequest("GET", path, nil)
		if token != "" {
			request.Header.Set("Authorization", token)
		}
		response := httptest.NewRecorder()
		gateway.ServeHTTP(response, request)
		return response.Code
	}
	if status := invoke("/api/a?tenant=x", ""); status != 401 || calls.Load() != 0 {
		t.Fatalf("missing identity: status=%d calls=%d", status, calls.Load())
	}
	if invoke("/api/a?tenant=x", "token") != 200 || invoke("/api/a?tenant=x", "token") != 200 || calls.Load() != 1 {
		t.Fatal("cache miss/hit failed")
	}
	if status := invoke("/api/b?tenant=x", "token"); status != 403 || calls.Load() != 1 {
		t.Fatalf("cached policy must evaluate new ARN: status=%d calls=%d", status, calls.Load())
	}
	if invoke("/api/b?tenant=y", "token") != 200 || calls.Load() != 2 {
		t.Fatal("ordered identity cache failed")
	}
	now = now.Add(31 * time.Second)
	if invoke("/api/b?tenant=x", "token") != 200 || calls.Load() != 3 {
		t.Fatal("cache expiry failed")
	}
}

func TestMappingCannotChangeTargetHostOrInjectQuery(t *testing.T) {
	for _, value := range []string{"safe/nested", "safe/item?admin=true#fragment", "//attacker/path", "../escape", "a/../escape", "a\\escape"} {
		t.Run(value, func(t *testing.T) {
			cfg := IntegrationConfig{Type: "HTTP_PROXY", URI: "http://backend/{proxy}", RequestParameters: map[string]string{"integration.request.path.proxy": "context.authorizer.path"}}
			request := httptest.NewRequest("GET", "/source?original=yes", nil)
			target, _, err := compileTestIntegration(t, cfg).mapRequest(mappingInput{request: request, authorizer: authorizerResponse{Context: map[string]string{"path": value}}})
			if strings.HasPrefix(value, "//") || strings.Contains(value, "..") || strings.Contains(value, "\\") {
				if err == nil {
					t.Fatal("ambiguous mapping accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if target.Host != "backend" || target.RawQuery != "original=yes" || target.Fragment != "" {
				t.Fatalf("URI injection: %+v", target)
			}
			if value == "safe/nested" && target.Path != "/safe/nested" {
				t.Fatal("slashes lost")
			}
			if strings.Contains(value, "?") && !strings.Contains(target.String(), "%3F") {
				t.Fatal("query delimiter not escaped")
			}
		})
	}
}

func TestGatewayLogsOnlyTemplatesAndNoSecrets(t *testing.T) {
	var logs bytes.Buffer
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "ok") }))
	defer backend.Close()
	cfg := Config{Routes: []RouteConfig{{Path: "/invite/{token}", Method: "GET", Integration: IntegrationConfig{Type: "HTTP_PROXY", URI: backend.URL + "/{token}"}}}}
	logger := testLogger(&logs)
	gateway := newTestGateway(t, cfg, Options{Logger: logger})
	response := httptest.NewRecorder()
	gateway.ServeHTTP(response, httptest.NewRequest("GET", "/invite/bearer-secret?jwt=query-secret", strings.NewReader("body-secret")))
	if strings.Contains(logs.String(), "bearer-secret") || strings.Contains(logs.String(), "query-secret") || strings.Contains(logs.String(), "body-secret") {
		t.Fatal("secret logged")
	}
	if !strings.Contains(logs.String(), "/invite/{token}") {
		t.Fatalf("route template absent: %s", logs.String())
	}
}
