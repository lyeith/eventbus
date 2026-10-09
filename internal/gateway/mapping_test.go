package gateway

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func compileTestIntegration(t *testing.T, config IntegrationConfig) *compiledIntegration {
	t.Helper()
	plan, err := compileIntegration(config)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func TestCompiledIntegrationPreservesSourcesPrecedenceAndOwnedOutput(t *testing.T) {
	cfg := IntegrationConfig{
		Type: "HTTP_PROXY", URI: "http://backend/base/{proxy}/{suffix}/{proxy}?shadow=recipe&tag=recipe1&tag=recipe2",
		RemoveHeaders: []string{"Authorization", "Cookie"},
		RequestParameters: map[string]string{
			"integration.request.path.proxy":          "context.authorizer.privatePath",
			"integration.request.path.suffix":         "stageVariables.suffix",
			"integration.request.header.X-Principal":  "context.authorizer.principalId",
			"integration.request.header.X-Source":     "method.request.header.X-Source",
			"integration.request.header.X-Ordered":    "'first'",
			"integration.request.header.x-ordered":    "'last'",
			"integration.request.header.X-Request-Id": "context.requestId",
			"integration.request.querystring.shadow":  "'mapped'",
			"integration.request.querystring.z-copy":  "method.request.querystring.shadow",
		},
	}
	plan := compileTestIntegration(t, cfg)
	request := httptest.NewRequest("GET", "/source?shadow=caller1&shadow=caller2&tag=caller&keep=yes", nil)
	request.Header["X-Source"] = []string{"one", "two"}
	request.Header.Set("Authorization", "private")
	request.Header.Set("Cookie", "private")
	request.Header.Set("Connection", "X-Principal, X-Source")
	request.Header.Set("X-Principal", "spoof")
	beforeURL, beforeHeader := *request.URL, request.Header.Clone()
	parameters := map[string]string{"proxy": "caller", "unused": "retained"}
	input := mappingInput{request: request, parameters: parameters, requestID: "owned-id",
		authorizer: authorizerResponse{PrincipalID: "principal", Context: map[string]string{"privatePath": "private/a b?x#fragment"}},
		config:     Config{StageVariables: map[string]string{"suffix": "final"}}}
	cfg.URI = "http://changed.invalid"
	cfg.RemoveHeaders[0] = "X-Source"
	cfg.RequestParameters["integration.request.path.proxy"] = "'changed'"
	for attempt := 0; attempt < 2; attempt++ {
		target, headers, err := plan.mapRequest(input)
		if err != nil {
			t.Fatal(err)
		}
		expectedPath := "/base/private/a b?x#fragment/final/private/a b?x#fragment"
		expectedQuery := "keep=yes&shadow=mapped&tag=recipe1&tag=recipe2&z-copy=caller1%2Ccaller2"
		if target.Host != "backend" || target.Path != expectedPath || target.RawQuery != expectedQuery || target.Fragment != "" || target.RawPath != "" {
			t.Fatalf("static URI or original source precedence changed: %s", target)
		}
		if !strings.Contains(target.String(), "a%20b%3Fx%23fragment") {
			t.Fatalf("mapped URL delimiters lost escaping: %s", target)
		}
		if headers.Get("Authorization") != "" || headers.Get("Cookie") != "" || headers.Get("X-Principal") != "principal" ||
			headers.Get("X-Source") != "one,two" || headers.Get("X-Ordered") != "last" || headers.Get("X-Request-Id") != "owned-id" {
			t.Fatalf("header removal/source/order changed: %v", headers)
		}
		target.Path, target.Host, target.RawQuery = "/mutated", "mutated", "mutated=true"
		headers["X-Source"][0] = "mutated"
	}
	if *request.URL != beforeURL || !reflect.DeepEqual(request.Header, beforeHeader) || !reflect.DeepEqual(parameters, map[string]string{"proxy": "caller", "unused": "retained"}) {
		t.Fatal("compiled mapping mutated caller-owned request or path parameters")
	}
}

func TestCompiledIntegrationPreservesSequentialRepeatedPlaceholders(t *testing.T) {
	plan := compileTestIntegration(t, IntegrationConfig{URI: "http://backend/{first}/{second}/{first}"})
	target, _, err := plan.mapRequest(mappingInput{request: httptest.NewRequest("GET", "/", nil), parameters: map[string]string{"first": "a{second}", "second": "b"}})
	if err != nil || target.Path != "/ab/b/ab" {
		t.Fatalf("sequential placeholder semantics changed: target=%v err=%v", target, err)
	}
}

func TestCompiledIntegrationStillRejectsMissingOrUnsafeSources(t *testing.T) {
	for _, source := range []string{"method.request.path.missing", "method.request.header.X-Missing", "method.request.querystring.missing", "context.authorizer.missing", "context.authorizer.principalId", "stageVariables.missing"} {
		t.Run(source, func(t *testing.T) {
			plan := compileTestIntegration(t, IntegrationConfig{URI: "http://backend", RequestParameters: map[string]string{"integration.request.header.X-Mapped": source}})
			if _, _, err := plan.mapRequest(mappingInput{request: httptest.NewRequest("GET", "/", nil)}); err == nil {
				t.Fatal("absent source became an empty mapped header")
			}
		})
	}
	plan := compileTestIntegration(t, IntegrationConfig{URI: "http://backend", RequestParameters: map[string]string{"integration.request.header.X-Mapped": "context.authorizer.value"}})
	for _, value := range []string{"value\r\nInjected: true", "value\x00"} {
		if _, _, err := plan.mapRequest(mappingInput{request: httptest.NewRequest("GET", "/", nil), authorizer: authorizerResponse{Context: map[string]string{"value": value}}}); err == nil {
			t.Fatal("unsafe header source accepted")
		}
	}
}

func TestGatewayCompiledProxyStateIsIsolatedAcrossConcurrentRequests(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(struct {
			Path, Query, Source, Static, Private string
		}{r.URL.EscapedPath(), r.URL.RawQuery, r.Header.Get("X-Source"), r.Header.Get("X-Static"), r.Header.Get("Authorization") + r.Header.Get("Cookie")})
	}))
	defer backend.Close()
	cfg := Config{BasePath: "/edge", StageVariables: map[string]string{"version": "v1"}, Routes: []RouteConfig{
		{Path: "/api/{proxy+}", Method: "GET", Integration: IntegrationConfig{
			Type: "HTTP_PROXY", URI: backend.URL + "/base/{proxy}?fixed=recipe",
			RemoveHeaders: []string{"Authorization", "Cookie"},
			RequestParameters: map[string]string{
				"integration.request.header.X-Source":     "method.request.header.X-Source",
				"integration.request.header.X-Static":     "'owned'",
				"integration.request.querystring.version": "stageVariables.version",
				"integration.request.querystring.copy":    "method.request.querystring.value",
			},
		}},
	}}
	gateway := newTestGateway(t, cfg, Options{})
	cfg.Routes[0].Integration.URI = "http://changed.invalid"
	cfg.Routes[0].Integration.RequestParameters["integration.request.header.X-Static"] = "'changed'"
	const workers, operations = 4, 20
	failures := make(chan error, workers*operations)
	var joined sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		joined.Add(1)
		go func(worker int) {
			defer joined.Done()
			for operation := 0; operation < operations; operation++ {
				value := fmt.Sprintf("w%d-%d", worker, operation)
				path := "/edge/api/" + value + "/a%20b/"
				request := httptest.NewRequest("GET", path+"?fixed=caller&value="+value, nil)
				request.Header.Set("X-Source", value)
				request.Header.Set("Authorization", "private")
				request.Header.Set("Cookie", "private")
				beforeURL, beforeHeader := *request.URL, request.Header.Clone()
				response := httptest.NewRecorder()
				gateway.ServeHTTP(response, request)
				var got struct{ Path, Query, Source, Static, Private string }
				err := json.Unmarshal(response.Body.Bytes(), &got)
				expected := url.Values{"fixed": {"recipe"}, "value": {value}, "copy": {value}, "version": {"v1"}}.Encode()
				if err != nil || response.Code != 200 || got.Path != "/base/"+value+"/a%20b/" || got.Query != expected ||
					got.Source != value || got.Static != "owned" || got.Private != "" || *request.URL != beforeURL || !reflect.DeepEqual(request.Header, beforeHeader) {
					failures <- fmt.Errorf("request %s: status=%d output=%+v err=%v", value, response.Code, got, err)
				}
			}
		}(worker)
	}
	joined.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
}

func TestCompiledRedactionsPreserveOriginalPathOrderAndPrivacy(t *testing.T) {
	var logs bytes.Buffer
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	defer backend.Close()
	cfg := Config{BasePath: "/edge", LogRedactions: []string{"/edge/{proxy+}", "/edge/invite/{token}", "/invite/{token}"},
		Routes: []RouteConfig{{Path: "/invite/private-token", Method: "GET", Integration: IntegrationConfig{Type: "HTTP_PROXY", URI: backend.URL}}}}
	gateway := newTestGateway(t, cfg, Options{Logger: testLogger(&logs)})
	cfg.LogRedactions[0] = "/changed/{proxy+}"
	request := httptest.NewRequest("GET", "/edge/invite/private-token?jwt=query-private", strings.NewReader("body-private"))
	request.Header.Set("Authorization", "header-private")
	response := httptest.NewRecorder()
	gateway.ServeHTTP(response, request)
	var record map[string]any
	if err := json.Unmarshal(logs.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if response.Code != 204 || record["route"] != "/edge/{proxy+}" {
		t.Fatalf("redaction changed full path or declaration precedence: status=%d log=%s", response.Code, logs.String())
	}
	for _, private := range []string{"private-token", "query-private", "body-private", "header-private"} {
		if strings.Contains(logs.String(), private) {
			t.Fatalf("private request entered compiled redaction log: %s", logs.String())
		}
	}
}
