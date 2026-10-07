package gateway

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func httpAPIConfig(endpoint, authorizerVersion, integrationVersion string) Config {
	cfg := Config{Region: "us-east-1", AccountID: "123456789012", APIID: "native", Stage: "$default", StageVariables: map[string]string{"tenant": "fixture"}, Routes: []RouteConfig{{Path: "/items/{id}", Method: "POST", Integration: IntegrationConfig{Type: "AWS_PROXY", InvokeURL: endpoint + "/2015-03-31/functions/app/invocations?Qualifier=live", PayloadFormatVersion: integrationVersion, Timeout: time.Second}}}}
	if authorizerVersion != "public" {
		cfg.Authorizers = map[string]AuthorizerConfig{"auth": {Type: "REQUEST", InvokeURL: endpoint + "/2015-03-31/functions/auth/invocations", PayloadFormatVersion: authorizerVersion, TTL: intPointer(0), IdentitySources: []string{"$request.header.Authorization", "$request.querystring.token", "$context.routeKey", "$stageVariables.tenant"}, Timeout: time.Second}}
		cfg.Routes[0].Authorizer = "auth"
	}
	return cfg
}

func TestNativeHTTPFormatsSelectedIndependently(t *testing.T) {
	for _, authVersion := range []string{"1.0", "2.0"} {
		for _, integrationVersion := range []string{"1.0", "2.0"} {
			t.Run(authVersion+"/"+integrationVersion, func(t *testing.T) {
				var authEvent, integration map[string]any
				endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Header.Get("X-Amz-Invocation-Type") != "RequestResponse" {
						t.Error("missing synchronous Invoke contract")
					}
					if err := json.NewDecoder(r.Body).Decode(&integration); err != nil {
						t.Error(err)
						w.WriteHeader(500)
						return
					}
					if strings.Contains(r.URL.Path, "/auth/") {
						authEvent, integration = integration, nil
						arnKey := "methodArn"
						if authVersion == "2.0" {
							arnKey = "routeArn"
						}
						context := map[string]any{"tenant": "trusted", "count": 17, "enabled": true}
						if authVersion == "2.0" {
							context["roles"] = []string{"reader", "writer"}
							context["nested"] = map[string]any{"region": "local"}
						}
						_ = json.NewEncoder(w).Encode(allowResponse(authEvent[arnKey].(string), context))
						return
					}
					if r.URL.Path != "/2015-03-31/functions/app/invocations" || r.URL.RawQuery != "Qualifier=live" {
						t.Errorf("function alias changed: %s", r.URL)
					}
					output := map[string]any{"statusCode": 201, "body": base64.StdEncoding.EncodeToString([]byte{0, 255, 1}), "isBase64Encoded": true, "headers": map[string]string{"Content-Type": "application/octet-stream", "X-Request-Id": "function-spoof"}}
					if integrationVersion == "1.0" {
						output["multiValueHeaders"] = map[string][]string{"Set-Cookie": {"a=1", "b=2"}, "X-Multi": {"one", "two"}}
					} else {
						output["cookies"] = []string{"a=1", "b=2"}
					}
					_ = json.NewEncoder(w).Encode(output)
				}))
				defer endpoint.Close()
				cfg := httpAPIConfig(endpoint.URL, authVersion, integrationVersion)
				cfg.Routes[0].Integration.RemoveHeaders = []string{"X-Remove"}
				gateway := newTestGateway(t, cfg, Options{})
				request := httptest.NewRequest("POST", "http://native.example/items/42?token=a&token=b&x=hello%20world&x=second", bytes.NewReader([]byte{0, 255, 1}))
				request.RemoteAddr = "192.0.2.4:1234"
				request.Header.Set("Content-Type", "application/octet-stream")
				request.Header.Set("aUtHoRiZaTiOn", "valid")
				request.Header.Add("X-Repeated", "first")
				request.Header.Add("X-Repeated", "second")
				request.Header.Add("Cookie", "a=1; b=2")
				request.Header.Add("Cookie", "c=3")
				request.Header.Set("X-Remove", "hidden")
				request.Header.Set("X-Tenant", "spoof")
				request.Header.Set("X-Forwarded-For", "203.0.113.9")
				response := httptest.NewRecorder()
				gateway.ServeHTTP(response, request)
				if response.Code != 201 || !bytes.Equal(response.Body.Bytes(), []byte{0, 255, 1}) {
					t.Fatalf("HTTP result %d %q", response.Code, response.Body.Bytes())
				}
				if !reflect.DeepEqual(response.Header().Values("Set-Cookie"), []string{"a=1", "b=2"}) {
					t.Fatalf("cookies lost: %v", response.Header())
				}
				if response.Header().Get("X-Request-Id") == "function-spoof" {
					t.Fatal("function replaced trusted request ID")
				}
				if authEvent["version"] != authVersion || integration["version"] != integrationVersion {
					t.Fatalf("coupled formats auth=%v integration=%v", authEvent, integration)
				}
				if integration["body"] != "AP8B" || integration["isBase64Encoded"] != true {
					t.Fatalf("binary input changed: %v", integration)
				}
				headers := integration["headers"].(map[string]any)
				if _, ok := headers["x-remove"]; ok {
					t.Fatal("integration remove_headers ignored")
				}
				context := integration["requestContext"].(map[string]any)
				if context["stage"] != "$default" || context["accountId"] != "123456789012" || context["apiId"] != "native" {
					t.Fatalf("context %v", context)
				}
				authorization := context["authorizer"].(map[string]any)
				if integrationVersion == "2.0" {
					if _, ok := integration["multiValueHeaders"]; ok {
						t.Fatal("v2 retained v1 fields")
					}
					if headers["x-repeated"] != "first,second" || integration["rawQueryString"] != "token=a&token=b&x=hello%20world&x=second" {
						t.Fatalf("v2 repeated input %v", integration)
					}
					if context["http"].(map[string]any)["sourceIp"] != "192.0.2.4" {
						t.Fatal("forwarded address was trusted")
					}
					authorization = authorization["lambda"].(map[string]any)
				} else {
					if context["path"] != "/items/42" {
						t.Fatalf("$default leaked into path: %v", context)
					}
					if !reflect.DeepEqual(integration["multiValueHeaders"].(map[string]any)["x-repeated"], []any{"first", "second"}) {
						t.Fatal("v1 repeated headers lost")
					}
				}
				if authorization["tenant"] != "trusted" || authorization["count"] != float64(17) || authorization["enabled"] != true || authorization["principalId"] != "canonical-principal" {
					t.Fatalf("native authorizer context lost: %v", authorization)
				}
				if authVersion == "2.0" && !reflect.DeepEqual(authorization["roles"], []any{"reader", "writer"}) {
					t.Fatalf("structured context lost: %v", authorization)
				}
				if authVersion == "2.0" {
					if authEvent["routeKey"] != "POST /items/{id}" || authEvent["routeArn"] != "arn:aws:execute-api:us-east-1:123456789012:native/$default/POST/items/42" {
						t.Fatalf("native authorizer route %v", authEvent)
					}
					if !reflect.DeepEqual(authEvent["identitySource"], []any{"valid", "a,b", "POST /items/{id}", "fixture"}) {
						t.Fatalf("identity input %v", authEvent)
					}
				} else if authEvent["identitySource"] != "valid,a,b,POST /items/{id},fixture" {
					t.Fatalf("1.0 native identity %v", authEvent)
				}
			})
		}
	}
}

func TestNativeIdentityRequiredWithoutCacheAndCaseRules(t *testing.T) {
	var calls atomic.Int32
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = io.WriteString(w, `{"isAuthorized":true}`)
	}))
	defer endpoint.Close()
	cfg := httpAPIConfig(endpoint.URL, "2.0", "2.0")
	auth := cfg.Authorizers["auth"]
	auth.EnableSimpleResponses = true
	cfg.Authorizers["auth"] = auth
	gateway := newTestGateway(t, cfg, Options{})
	for _, query := range []string{"", "?Token=a", "?token="} {
		request := httptest.NewRequest("POST", "/items/1"+query, nil)
		request.Header.Set("AUTHORIZATION", "valid")
		response := httptest.NewRecorder()
		gateway.ServeHTTP(response, request)
		if response.Code != 401 {
			t.Fatalf("missing identity %q returned %d", query, response.Code)
		}
	}
	request := httptest.NewRequest("POST", "/items/1?token=a", nil)
	response := httptest.NewRecorder()
	gateway.ServeHTTP(response, request)
	if response.Code != 401 || calls.Load() != 0 {
		t.Fatalf("missing header invoked authorizer: %d %d", response.Code, calls.Load())
	}
}

func TestNativeSimpleResponseCacheAndIAMRouteScope(t *testing.T) {
	for _, simple := range []bool{false, true} {
		for _, routeIdentity := range []bool{false, true} {
			t.Run(fmt.Sprintf("simple=%t/route=%t", simple, routeIdentity), func(t *testing.T) {
				var calls atomic.Int32
				endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					var event map[string]any
					_ = json.NewDecoder(r.Body).Decode(&event)
					if strings.Contains(r.URL.Path, "/auth/") {
						calls.Add(1)
						if simple {
							_ = json.NewEncoder(w).Encode(map[string]any{"isAuthorized": !strings.HasPrefix(event["rawPath"].(string), "/denied"), "context": map[string]any{"list": []int{1, 2}}})
						} else {
							_ = json.NewEncoder(w).Encode(allowResponse(event["routeArn"].(string), nil))
						}
					} else {
						_, _ = io.WriteString(w, `{"ok":true}`)
					}
				}))
				defer endpoint.Close()
				cfg := httpAPIConfig(endpoint.URL, "2.0", "2.0")
				auth := cfg.Authorizers["auth"]
				auth.TTL = intPointer(300)
				auth.EnableSimpleResponses = simple
				auth.IdentitySources = []string{"$request.header.Authorization"}
				if routeIdentity {
					auth.IdentitySources = append(auth.IdentitySources, "$context.routeKey")
				}
				cfg.Authorizers["auth"] = auth
				cfg.Routes[0].Path = "/first"
				cfg.Routes = append(cfg.Routes, cfg.Routes[0])
				cfg.Routes[1].Path = "/denied"
				gateway := newTestGateway(t, cfg, Options{})
				for index, path := range []string{"/first", "/first", "/denied", "/denied"} {
					request := httptest.NewRequest("POST", path, nil)
					request.Header.Set("Authorization", "token")
					response := httptest.NewRecorder()
					gateway.ServeHTTP(response, request)
					want := 200
					if index >= 2 && (simple && routeIdentity || !simple && !routeIdentity) {
						want = 403
					}
					if response.Code != want {
						t.Fatalf("%s returned %d want %d (%s)", path, response.Code, want, response.Body.String())
					}
				}
				wantCalls := int32(1)
				if routeIdentity {
					wantCalls = 2
				}
				if calls.Load() != wantCalls {
					t.Fatalf("cache calls %d want %d", calls.Load(), wantCalls)
				}
			})
		}
	}
}

func TestDefaultRouteStagePublicAndManagementIsolation(t *testing.T) {
	var calls atomic.Int32
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/2015-03-31/functions/app/invocations" {
			t.Errorf("request selected unowned function: %s", r.URL)
		}
		var event map[string]any
		_ = json.NewDecoder(r.Body).Decode(&event)
		if event["routeKey"] != "$default" || event["requestContext"].(map[string]any)["stage"] != "$default" {
			t.Errorf("default event %v", event)
		}
		if _, ok := event["requestContext"].(map[string]any)["authorizer"]; ok {
			t.Error("public route invented authorizer context")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"path": event["rawPath"]})
	}))
	defer endpoint.Close()
	cfg := httpAPIConfig(endpoint.URL, "public", "2.0")
	cfg.Routes[0].Path = "/{proxy+}"
	cfg.Routes[0].Method = "ANY"
	cfg.Routes[0].RouteKey = "$default"
	gateway := newTestGateway(t, cfg, Options{})
	for _, path := range []string{"/", "/ordinary", "/2015-03-31/functions/other/invocations", "/?Action=CreateQueue"} {
		response := httptest.NewRecorder()
		gateway.ServeHTTP(response, httptest.NewRequest("POST", path, strings.NewReader(`{"Action":"CreateTopic"}`)))
		if response.Code != 200 {
			t.Fatalf("default %s status %d", path, response.Code)
		}
	}
	if calls.Load() != 4 {
		t.Fatalf("management-shaped requests bypassed selected route: %d", calls.Load())
	}
	cfg.Routes[0].RouteKey = ""
	cfg.Routes[0].Path = "/public"
	cfg.Routes[0].Method = "POST"
	specific := newTestGateway(t, cfg, Options{})
	response := httptest.NewRecorder()
	specific.ServeHTTP(response, httptest.NewRequest("POST", "/2015-03-31/functions/other/invocations", nil))
	if response.Code != 403 || calls.Load() != 4 {
		t.Fatalf("management endpoint exposed: %d %d", response.Code, calls.Load())
	}
}

func TestNativeContextScalarMappingIsIndependent(t *testing.T) {
	result, err := parseConfiguredAuthorizerResponse([]byte(`{"isAuthorized":true,"context":{"tenant":"real","count":7,"enabled":true,"list":[1,2],"map":{"a":1}}}`), AuthorizerConfig{PayloadFormatVersion: "2.0", EnableSimpleResponses: true})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Context, map[string]string{"tenant": "real", "count": "7", "enabled": "true"}) {
		t.Fatalf("scalar projection %v", result.Context)
	}
	if _, ok := result.NativeContext["list"].([]any); !ok {
		t.Fatal("native array lost")
	}
	for _, payload := range []string{`{"isAuthorized":true}`, `{"principalId":"p","policyDocument":{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":"execute-api:Invoke","Resource":"*"}},"context":{"map":{}}}`} {
		if _, err := parseAuthorizerResponse([]byte(payload)); err == nil {
			t.Fatal("legacy REST accepted native v2 response")
		}
	}
}

func TestLambdaProxyResponseFormatsAndMalformedResults(t *testing.T) {
	valid := []struct {
		version, payload, body string
		status                 int
	}{
		{"2.0", `{"ok":true}`, `{"ok":true}`, 200}, {"2.0", `"hello"`, "hello", 200}, {"2.0", `null`, "null", 200}, {"2.0", `[1,2]`, `[1,2]`, 200},
		{"2.0", `{"statusCode":202,"body":"accepted","cookies":["a=1","b=2"]}`, "accepted", 202},
		{"1.0", `{"statusCode":201,"body":"AP8=","isBase64Encoded":true,"headers":{"X-Multi":"a"},"multiValueHeaders":{"x-multi":["a","b"]}}`, string([]byte{0, 255}), 201},
	}
	for _, test := range valid {
		response, err := parseProxyResponse([]byte(test.payload), test.version)
		if err != nil || response.status != test.status || string(response.body) != test.body {
			t.Fatalf("%s %s: %v %v", test.version, test.payload, response, err)
		}
		if test.version == "1.0" && !reflect.DeepEqual(response.headers.Values("X-Multi"), []string{"a", "b"}) {
			t.Fatalf("repeated response values %v", response.headers)
		}
	}
	for _, version := range []string{"1.0", "2.0"} {
		for _, payload := range []string{`{`, `{"statusCode":null}`, `{"statusCode":"200"}`, `{"statusCode":200.1}`, `{"statusCode":99}`, `{"statusCode":200,"body":{}}`, `{"statusCode":200,"headers":{"X-Test":null}}`, `{"statusCode":200,"multiValueHeaders":{"X-Test":[null]}}`, `{"statusCode":200,"cookies":[null]}`, `{"statusCode":200,"body":"bad!","isBase64Encoded":true}`, `{"statusCode":200,"headers":{"X-Test":"bad\r\nInjected: yes"}}`, `{"statusCode":200,"cookies":["a=1\n"]}`} {
			if _, err := parseProxyResponse([]byte(payload), version); err == nil {
				t.Fatalf("%s accepted malformed result %s", version, payload)
			}
		}
	}
	if _, err := parseProxyResponse([]byte(`{"ok":true}`), "1.0"); err == nil {
		t.Fatal("v1 inferred v2 response")
	}
}

func TestLambdaProxyInvokeFailuresAndPayloadBudgets(t *testing.T) {
	cases := []struct {
		name          string
		invokeStatus  int
		result        string
		functionError bool
		delay         time.Duration
		body          string
		want          int
	}{
		{name: "invoke rejected", invokeStatus: 404, result: `{"message":"missing alias"}`, want: 500},
		{name: "function error", invokeStatus: 200, result: `{"errorMessage":"secret failure"}`, functionError: true, want: 502},
		{name: "malformed", invokeStatus: 200, result: `{"statusCode":200,"body":{}}`, want: 502},
		{name: "timeout", invokeStatus: 200, result: `{"ok":true}`, delay: 60 * time.Millisecond, want: 504},
		{name: "result budget", invokeStatus: 200, result: strings.Repeat("x", maxInvokePayload+1), want: 502},
		{name: "body budget", body: strings.Repeat("x", maxInvokePayload+1), want: 413},
		{name: "envelope budget", body: strings.Repeat("x", maxInvokePayload-1), want: 413},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int32
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if test.delay > 0 {
					time.Sleep(test.delay)
				}
				if test.functionError {
					w.Header().Set("X-Amz-Function-Error", "Unhandled")
				}
				w.WriteHeader(test.invokeStatus)
				_, _ = io.WriteString(w, test.result)
			}))
			defer endpoint.Close()
			cfg := httpAPIConfig(endpoint.URL, "public", "2.0")
			cfg.Routes[0].Integration.Timeout = 2 * time.Second
			if test.delay > 0 {
				cfg.Routes[0].Integration.Timeout = 20 * time.Millisecond
			}
			gateway := newTestGateway(t, cfg, Options{})
			request := httptest.NewRequest("POST", "/items/1", strings.NewReader(test.body))
			request.Header.Set("Content-Type", "text/plain")
			response := httptest.NewRecorder()
			gateway.ServeHTTP(response, request)
			if response.Code != test.want {
				t.Fatalf("status %d want %d: %s", response.Code, test.want, response.Body.String())
			}
			if strings.Contains(response.Body.String(), "secret") || strings.Contains(response.Body.String(), "missing alias") {
				t.Fatal("function details exposed")
			}
			if test.want == 413 && calls.Load() != 0 {
				t.Fatal("oversized event invoked Lambda")
			}
		})
	}
}

func TestHTTPAPIConfigValidation(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"authorizer version", func(c *Config) { a := c.Authorizers["auth"]; a.PayloadFormatVersion = "3.0"; c.Authorizers["auth"] = a }},
		{"native identity syntax", func(c *Config) {
			a := c.Authorizers["auth"]
			a.IdentitySources = []string{"method.request.header.Authorization"}
			c.Authorizers["auth"] = a
		}},
		{"simple v1", func(c *Config) {
			a := c.Authorizers["auth"]
			a.PayloadFormatVersion = "1.0"
			a.EnableSimpleResponses = true
			c.Authorizers["auth"] = a
		}},
		{"native authorizer timeout", func(c *Config) { a := c.Authorizers["auth"]; a.Timeout = 11 * time.Second; c.Authorizers["auth"] = a }},
		{"integration version", func(c *Config) { c.Routes[0].Integration.PayloadFormatVersion = "" }},
		{"integration endpoint", func(c *Config) {
			c.Routes[0].Integration.InvokeURL = "http://localhost/2015-03-31/functions/app/other/invocations"
		}},
		{"request mapping", func(c *Config) {
			c.Routes[0].Integration.RequestParameters = map[string]string{"integration.request.header.X-Test": "'value'"}
		}},
		{"route key", func(c *Config) { c.Routes[0].RouteKey = "$default" }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			cfg := httpAPIConfig("http://localhost:1234", "2.0", "2.0")
			test.mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("invalid recipe accepted")
			}
		})
	}
}
