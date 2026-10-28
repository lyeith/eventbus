package gateway

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

func TestRequestRoutePathDecodesOnlyOriginalComponents(t *testing.T) {
	for _, tc := range []struct {
		wire, decoded string
		parts         []string
	}{
		{"/", "/", []string{}},
		{"/items/Type/example%2Fhistory", "/items/Type/example/history", []string{"items", "Type", "example/history"}},
		{"/items/a%5cb", "/items/a\\b", []string{"items", "a\\b"}},
		{"/items/%252F", "/items/%2F", []string{"items", "%2F"}},
		{"/items/%252e", "/items/%2e", []string{"items", "%2e"}},
		{"/items/a%20b/%E9%9B%AA/", "/items/a b/雪/", []string{"items", "a b", "雪", ""}},
	} {
		t.Run(tc.wire, func(t *testing.T) {
			requestURL, err := url.ParseRequestURI(tc.wire)
			if err != nil {
				t.Fatal(err)
			}
			before := *requestURL
			decoded, parts, valid := requestRoutePath("", requestURL)
			if !valid || decoded != tc.decoded || !reflect.DeepEqual(parts, tc.parts) || *requestURL != before {
				t.Fatalf("decoded=%q parts=%q valid=%t URL=%+v", decoded, parts, valid, requestURL)
			}
			if got := rawAPIPath("", requestURL.EscapedPath()); got != tc.wire {
				t.Fatalf("raw path changed: %q", got)
			}
		})
	}
	for _, tc := range []struct {
		wire         string
		valid        bool
		decoded, raw string
		parts        []string
	}{
		{"/edge/v1/items/a%2fb", true, "/items/a/b", "/items/a%2fb", []string{"items", "a/b"}},
		{"/%65dge/v1/items/%252F", true, "/items/%2F", "/items/%252F", []string{"items", "%2F"}},
		{"/edge/v1", true, "/", "/", []string{}},
		{"/edge/v1/", true, "/", "/", []string{}},
		{"/edge%2Fv1/items/x", false, "", "", nil},
		{"/edge/v1%2Fitems/x", false, "", "", nil},
		{"/edge/v10/items/x", false, "", "", nil},
	} {
		t.Run("mapping"+tc.wire, func(t *testing.T) {
			requestURL, err := url.ParseRequestURI(tc.wire)
			if err != nil {
				t.Fatal(err)
			}
			decoded, parts, valid := requestRoutePath("/edge/v1", requestURL)
			if valid != tc.valid || decoded != tc.decoded || !reflect.DeepEqual(parts, tc.parts) {
				t.Fatalf("decoded=%q parts=%q valid=%t", decoded, parts, valid)
			}
			if valid && rawAPIPath("/edge/v1", requestURL.EscapedPath()) != tc.raw {
				t.Fatalf("mapping changed raw path: %q", rawAPIPath("/edge/v1", requestURL.EscapedPath()))
			}
		})
	}
}

func TestRequestRoutePathRetainsFramingAndCanonicalValidation(t *testing.T) {
	for _, wire := range []string{
		"//", "/a//b", "/a//", "/a/.", "/a/..", "/a/%2e", "/a/%2E%2e/",
		"/a/%00", "/a/%1f", "/a/%7f", "/a/space here", "/a/back\\slash",
	} {
		t.Run(wire, func(t *testing.T) {
			requestURL, err := url.ParseRequestURI(wire)
			if err != nil {
				return
			} // The HTTP parser may reject framing first.
			if _, _, valid := requestRoutePath("", requestURL); valid {
				t.Fatalf("invalid framing or ambiguous components accepted: %+v", requestURL)
			}
		})
	}
	for _, requestURL := range []*url.URL{
		nil,
		{Path: ""},
		{Path: "relative"},
		{Path: "/a/b", RawPath: "/a/%2"},
		{Path: "/a/b", RawPath: "/a/%2Fdifferent"},
		{Path: "/a/b", RawPath: "/a/b?query"},
		{Path: "/a/b", RawPath: "/a/b#fragment"},
	} {
		if _, _, valid := requestRoutePath("", requestURL); valid {
			t.Fatalf("invalid URL accepted: %+v", requestURL)
		}
	}
}

func TestComponentMatchingAndPolicyIdentityKeepEncodedSeparatorsDistinct(t *testing.T) {
	requestURL, err := url.ParseRequestURI("/items/private%2Fadmin")
	if err != nil {
		t.Fatal(err)
	}
	decoded, parts, valid := requestRoutePath("", requestURL)
	if !valid {
		t.Fatal("encoded component rejected")
	}
	for _, tc := range []struct {
		pattern string
		want    map[string]string
		matches bool
	}{
		{"/items/private/admin", nil, false},
		{"/items/{id}", map[string]string{"id": "private/admin"}, true},
		{"/items/{proxy+}", map[string]string{"proxy": "private/admin"}, true},
	} {
		template, err := parseTemplate(tc.pattern)
		if err != nil {
			t.Fatal(err)
		}
		parameters, matched := template.matchSegments(decoded, parts)
		if matched != tc.matches || !reflect.DeepEqual(parameters, tc.want) {
			t.Fatalf("%s matched=%t parameters=%v", tc.pattern, matched, parameters)
		}
	}
	for _, tc := range []struct {
		parts    []string
		identity string
	}{
		{[]string{}, "/"},
		{[]string{"items", "private/admin"}, "/items/private%2Fadmin"},
		{[]string{"items", "private", "admin"}, "/items/private/admin"},
		{[]string{"items", "%2F"}, "/items/%252F"},
		{[]string{"items", "a\\b"}, "/items/a%5Cb"},
		{[]string{"items", "*?雪 space"}, "/items/%2A%3F雪 space"},
	} {
		if got := routePathIdentity(tc.parts); got != tc.identity {
			t.Fatalf("identity %q => %q, want %q", tc.parts, got, tc.identity)
		}
	}
}

func TestEscapedPathCapturesRetainRegularAndGreedyProvenance(t *testing.T) {
	for _, tc := range []struct {
		pattern, wire string
		want          map[string]string
	}{
		{"/", "/", map[string]string{}},
		{"/items/{id}", "/items/a%2fb%5C%252F", map[string]string{"id": "a%2fb%5C%252F"}},
		{"/items/{proxy+}", "/items/a%2fb/c%5Cd/", map[string]string{"proxy": "a%2fb/c%5Cd/"}},
	} {
		template, err := parseTemplate(tc.pattern)
		if err != nil {
			t.Fatal(err)
		}
		if got := requestEscapedParameters(template, tc.wire); !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("captures=%v want %v", got, tc.want)
		}
	}
}

func TestHTTPProxyPreservesEscapedCaptureProvenanceAndOriginalQuery(t *testing.T) {
	type observed struct{ Path, Query string }
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(observed{r.URL.EscapedPath(), r.URL.RawQuery})
	}))
	defer backend.Close()
	cfg := Config{BasePath: "/edge/v1", Routes: []RouteConfig{
		{Path: "/items/{source}", Method: "GET", Integration: IntegrationConfig{
			Type: "HTTP_PROXY", URI: backend.URL + "/private/{renamed}",
			RequestParameters: map[string]string{"integration.request.path.renamed": "method.request.path.source"},
		}},
		{Path: "/greedy/{proxy+}", Method: "GET", Integration: IntegrationConfig{
			Type: "HTTP_PROXY", URI: backend.URL + "/private/{proxy}",
		}},
	}}
	gateway := newTestGateway(t, cfg, Options{})
	for _, tc := range []struct{ incoming, outgoing string }{
		{"/edge/v1/items/a%2fb%5C%252F", "/private/a%2fb%5C%252F"},
		{"/%65dge/v1/items/%252e", "/private/%252e"},
		{"/edge/v1/items/a%20b%E9%9B%AA", "/private/a%20b%E9%9B%AA"},
		{"/edge/v1/greedy/a%2fb/c%5Cd/", "/private/a%2fb/c%5Cd/"},
	} {
		t.Run(tc.incoming, func(t *testing.T) {
			const query = "z=last&x=one%20two&x=one+two&encoded=%2f&empty=&flag"
			request := httptest.NewRequest("GET", tc.incoming+"?"+query, nil)
			request.Header.Set("X-Fixture", "original")
			beforeURL, beforeHeader := *request.URL, request.Header.Clone()
			response := httptest.NewRecorder()
			gateway.ServeHTTP(response, request)
			var got observed
			err := json.Unmarshal(response.Body.Bytes(), &got)
			if response.Code != 200 || err != nil || got.Path != tc.outgoing || got.Query != query {
				t.Fatalf("status=%d target=%+v body=%s err=%v", response.Code, got, response.Body, err)
			}
			if *request.URL != beforeURL || !reflect.DeepEqual(request.Header, beforeHeader) {
				t.Fatal("proxy mapping mutated caller-owned input")
			}
		})
	}
}

func TestHTTPProxyEscapedCaptureCannotCreateAnotherPlaceholder(t *testing.T) {
	plan := compileTestIntegration(t, IntegrationConfig{URI: "http://backend/{first}/{second}"})
	for _, raw := range []string{"a%7Bsecond%7D", "a{second}"} {
		decoded, err := url.PathUnescape(raw)
		if err != nil {
			t.Fatal(err)
		}
		target, _, err := plan.mapRequest(mappingInput{
			request:           httptest.NewRequest("GET", "/", nil),
			parameters:        map[string]string{"first": decoded, "second": "owned"},
			escapedParameters: map[string]string{"first": raw},
		})
		if err != nil || target.Path != "/a{second}/owned" {
			t.Fatalf("captured data reinterpreted as template: raw=%q target=%v err=%v", raw, target, err)
		}
	}
}

func TestEncodedSeparatorCannotBorrowNeighboringAuthorizerPolicy(t *testing.T) {
	var appCalls atomic.Int32
	const privilegedARN = "arn:aws:execute-api:us-east-1:123456789012:native/$default/GET/items/private/admin"
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/auth/") {
			_, _ = io.Copy(io.Discard, r.Body)
			_ = json.NewEncoder(w).Encode(allowResponse(privilegedARN, nil))
			return
		}
		appCalls.Add(1)
		_, _ = io.WriteString(w, `{"statusCode":204,"body":""}`)
	}))
	defer endpoint.Close()
	cfg := httpAPIConfig(endpoint.URL, "2.0", "2.0")
	auth := cfg.Authorizers["auth"]
	auth.IdentitySources = []string{"$request.header.Authorization"}
	cfg.Authorizers["auth"] = auth
	cfg.Routes[0].Path, cfg.Routes[0].Method = "/items/{id}", "GET"
	privileged := cfg.Routes[0]
	privileged.Path = "/items/private/admin"
	cfg.Routes = append(cfg.Routes, privileged)
	gateway := newTestGateway(t, cfg, Options{})
	for _, tc := range []struct {
		wire   string
		status int
	}{
		{"/items/private/admin", 204},
		{"/items/private%2Fadmin", 403},
		{"/items/private%252Fadmin", 403},
	} {
		request := httptest.NewRequest("GET", tc.wire, nil)
		request.Header.Set("Authorization", "fixture")
		response := httptest.NewRecorder()
		gateway.ServeHTTP(response, request)
		if response.Code != tc.status {
			t.Fatalf("%s returned %d: %s", tc.wire, response.Code, response.Body)
		}
	}
	if appCalls.Load() != 1 {
		t.Fatalf("neighboring policy admitted %d integrations", appCalls.Load())
	}
}

func TestEscapedPathRedactionUsesOriginalComponents(t *testing.T) {
	var logs bytes.Buffer
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	defer backend.Close()
	gateway := newTestGateway(t, Config{
		LogRedactions: []string{"/items/private/admin", "/items/{token}"},
		Routes:        []RouteConfig{{Path: "/items/{id}", Method: "GET", Integration: IntegrationConfig{Type: "HTTP_PROXY", URI: backend.URL}}},
	}, Options{Logger: testLogger(&logs)})
	response := httptest.NewRecorder()
	gateway.ServeHTTP(response, httptest.NewRequest("GET", "/items/private%2Fadmin", nil))
	if response.Code != 204 {
		t.Fatalf("status=%d: %s", response.Code, response.Body)
	}
	var record map[string]any
	if err := json.Unmarshal(logs.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record["route"] != "/items/{token}" || strings.Contains(logs.String(), "private") {
		t.Fatalf("redaction matched decoded structural alias or exposed component: %s", logs.String())
	}
}
