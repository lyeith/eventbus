//go:build performance

package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/testperf"
	"github.com/rs/zerolog"
)

const performanceGatewaySamples = 20
const performanceGatewayBatch = 10

type performanceGatewayTransport func(*http.Request) (*http.Response, error)

func (transport performanceGatewayTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

// This replaces only downstream I/O. Real ServeHTTP, sort/match, mapping and
// proxy response handling remain in the native owner; no fixture matcher exists.
func performanceRouteGateway(t *testing.T, routes []RouteConfig, base string) *Gateway {
	t.Helper()
	gateway := newTestGateway(t, Config{BasePath: base, Routes: routes}, Options{Logger: zerolog.Nop()})
	transport := performanceGatewayTransport(func(request *http.Request) (*http.Response, error) {
		if request.Body != nil {
			defer request.Body.Close()
		}
		return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{
			"X-Performance-Target": []string{request.URL.Path},
		}, Body: http.NoBody, Request: request}, nil
	})
	for index := range gateway.routes {
		gateway.routes[index].proxy.Transport = transport
	}
	return gateway
}

func performanceRoute(path, method, target string) RouteConfig {
	return RouteConfig{Path: path, Method: method, Integration: IntegrationConfig{
		Type: "HTTP_PROXY", URI: "http://performance.invalid" + target,
	}}
}

func performanceRouteMeasurement(t *testing.T, gateway *Gateway, name, method, path, target string, status int) {
	t.Helper()
	request := httptest.NewRequest(method, path, nil)
	beforeURL := *request.URL
	check := func(response *httptest.ResponseRecorder) {
		if response.Code != status || response.Header().Get("X-Performance-Target") != target {
			t.Fatalf("native route changed: method=%s path=%s status=%d target=%q", method, path, response.Code, response.Header().Get("X-Performance-Target"))
		}
	}
	// Semantic verification is outside each measured batch.
	response := httptest.NewRecorder()
	gateway.ServeHTTP(response, request)
	check(response)
	samples := make([]float64, 0, performanceGatewaySamples)
	for sample := 0; sample < performanceGatewaySamples; sample++ {
		started := time.Now()
		for operation := 0; operation < performanceGatewayBatch; operation++ {
			response = httptest.NewRecorder()
			gateway.ServeHTTP(response, request)
		}
		samples = append(samples, float64(time.Since(started))/float64(time.Millisecond)/performanceGatewayBatch)
		check(response)
	}
	if *request.URL != beforeURL {
		t.Fatal("matching rewrote the fixture's original path")
	}
	t.Logf("PERFORMANCE_GATEWAY_FIXTURE case=%s routes=%d operations_per_sample=%d native=ServeHTTP downstream=bounded_transport setup=excluded", name, len(gateway.routes), performanceGatewayBatch)
	testperf.Report(t, name, "serve_http_per_op_ms", samples)
}

func TestPerformanceReviewGatewayLiteralRoutes(t *testing.T) {
	for _, count := range []int{10, 100, 1000} {
		t.Run(fmt.Sprintf("routes_%d", count), func(t *testing.T) {
			routes := make([]RouteConfig, 0, count)
			for index := 0; index < count; index++ {
				path := fmt.Sprintf("/routes/r%04d", index)
				routes = append(routes, performanceRoute(path, "GET", path))
			}
			gateway := performanceRouteGateway(t, routes, "")
			// Equal specificity preserves declaration order. This is observed
			// against the owner's compiled ordering, never recreated here.
			for index := range routes {
				if gateway.routes[index].config.Path != routes[index].Path {
					t.Fatalf("stable literal precedence changed at %d", index)
				}
			}
			for _, position := range []struct {
				name  string
				index int
			}{
				{"first", 0}, {"middle", count / 2}, {"last", count - 1},
			} {
				path := routes[position.index].Path
				performanceRouteMeasurement(t, gateway, fmt.Sprintf("routes_%d_literal_%s", count, position.name), "GET", path, path, 204)
			}
			performanceRouteMeasurement(t, gateway, fmt.Sprintf("routes_%d_miss", count), "GET", "/routes/missing", "", 403)
		})
	}
}

func TestPerformanceReviewGatewayMixedPrecedence(t *testing.T) {
	for _, count := range []int{10, 100, 1000} {
		t.Run(fmt.Sprintf("routes_%d", count), func(t *testing.T) {
			// Broad routes deliberately precede specific declarations.
			routes := []RouteConfig{
				performanceRoute("/{proxy+}", "ANY", "/target/default"),
				performanceRoute("/docs/{proxy+}", "GET", "/target/greedy/{proxy}"),
				performanceRoute("/{section}/index.html", "GET", "/target/full/{section}"),
				performanceRoute("/docs/exact.html", "GET", "/target/literal"),
				performanceRoute("/docs/", "GET", "/target/directory/"),
				performanceRoute("/docs/{id}/", "GET", "/target/parameter/{id}/"),
				performanceRoute("/docs/index.html/", "GET", "/target/literal-slash/"),
				performanceRoute("/method", "ANY", "/target/any"),
				performanceRoute("/method", "GET", "/target/get"),
			}
			routes[0].RouteKey = "$default"
			for len(routes) < count {
				path := fmt.Sprintf("/routes/r%04d", len(routes))
				routes = append(routes, performanceRoute(path, "GET", path))
			}
			gateway := performanceRouteGateway(t, routes, "/edge")
			for _, check := range []struct {
				method, path, target string
				status               int
			}{
				{"GET", "/edge/docs/exact.html", "/target/literal", 204},
				{"GET", "/edge/docs/index.html", "/target/full/docs", 204},
				{"GET", "/edge/docs/index.html/", "/target/literal-slash/", 204},
				{"GET", "/edge/docs/example/", "/target/parameter/example/", 204},
				{"GET", "/edge/docs/", "/target/directory/", 204},
				{"GET", "/edge/docs", "/target/default", 204},
				{"GET", "/edge/docs/assets/", "/target/parameter/assets/", 204},
				{"GET", "/edge/docs/assets/nested.js", "/target/greedy/assets/nested.js", 204},
				{"GET", "/edge/method", "/target/get", 204},
				{"DELETE", "/edge/method", "/target/any", 204},
				{"GET", "/edge/", "/target/default", 204},
				{"GET", "/edgex/docs/exact.html", "", 403},
				{"GET", "/edge/docs//", "", 400},
			} {
				response := httptest.NewRecorder()
				gateway.ServeHTTP(response, httptest.NewRequest(check.method, check.path, nil))
				if response.Code != check.status || response.Header().Get("X-Performance-Target") != check.target {
					t.Fatalf("native precedence/path contract %+v: status=%d target=%q", check, response.Code, response.Header().Get("X-Performance-Target"))
				}
			}
			for _, measured := range []struct{ name, path, target string }{
				{"full_parameter", "/edge/docs/index.html", "/target/full/docs"},
				{"greedy", "/edge/docs/assets/nested.js", "/target/greedy/assets/nested.js"},
				{"default", "/edge/missing", "/target/default"},
			} {
				performanceRouteMeasurement(t, gateway, fmt.Sprintf("routes_%d_%s", count, measured.name), "GET", measured.path, measured.target, 204)
			}
		})
	}
}

type performanceCacheOwner struct {
	gateway           *Gateway
	authorizer        *lambdaAuthorizer
	now               time.Time
	calls, openBodies int
}
type performanceCacheBody struct {
	io.Reader
	owner *performanceCacheOwner
}

func (body *performanceCacheBody) Close() error {
	body.owner.openBodies--
	return nil
}

func newPerformanceCacheOwner(t *testing.T, wildcard bool) *performanceCacheOwner {
	t.Helper()
	cfg := fixture("http://performance.invalid/2015-03-31/functions/auth/invocations", "http://performance.invalid")
	cfg.Authorizers["auth"] = AuthorizerConfig{Type: "REQUEST", InvokeURL: "http://performance.invalid/2015-03-31/functions/auth/invocations",
		TTL: intPointer(60), Timeout: time.Second, IdentitySources: []string{"method.request.header.Authorization"}}
	gateway := newTestGateway(t, cfg, Options{Logger: zerolog.Nop()})
	owner := &performanceCacheOwner{gateway: gateway, authorizer: gateway.authorizers["auth"], now: time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC)}
	owner.authorizer.now = func() time.Time { return owner.now }
	resource := owner.event("seed", "a").MethodARN
	if wildcard {
		resource = strings.TrimSuffix(resource, "a") + "*"
	}
	owner.authorizer.client = &http.Client{Transport: performanceGatewayTransport(func(request *http.Request) (*http.Response, error) {
		defer request.Body.Close()
		owner.calls++
		var event requestEvent
		if err := json.NewDecoder(request.Body).Decode(&event); err != nil {
			return nil, err
		}
		payload := allowResponse(resource, nil)
		payload["principalId"] = event.Headers["Authorization"]
		data, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		owner.openBodies++
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: &performanceCacheBody{Reader: strings.NewReader(string(data)), owner: owner}, Request: request}, nil
	})}
	return owner
}

func (owner *performanceCacheOwner) event(token, suffix string) requestEvent {
	request := httptest.NewRequest("GET", "/api/"+suffix, nil)
	if token != "" {
		request.Header.Set("Authorization", token)
	}
	return newRequestEvent(owner.gateway.config, owner.gateway.routes[0].config, request, map[string]string{"proxy": suffix}, "performance-request")
}

// Prefill through real authorize/admission, not a copied cache-key classifier.
// Snapshot restoration controls occupancy and expiry between single misses;
// immutable cached responses are not changed by this fixture.
func (owner *performanceCacheOwner) prefill(t *testing.T) map[string]cachedAuthorization {
	t.Helper()
	for index := 0; index < maxAuthorizerCache; index++ {
		token := fmt.Sprintf("seed-%04d", index)
		response, status := owner.authorizer.authorize(context.Background(), owner.event(token, "a"))
		if status != 0 || response.PrincipalID != token || owner.openBodies != 0 {
			t.Fatalf("native prefill %d: status=%d principal=%q", index, status, response.PrincipalID)
		}
		owner.now = owner.now.Add(time.Millisecond)
	}
	if len(owner.authorizer.cache) != maxAuthorizerCache {
		t.Fatalf("native prefill occupancy=%d", len(owner.authorizer.cache))
	}
	snapshot := make(map[string]cachedAuthorization, maxAuthorizerCache)
	for key, value := range owner.authorizer.cache {
		snapshot[key] = value
	}
	return snapshot
}

func (owner *performanceCacheOwner) restore(snapshot map[string]cachedAuthorization) {
	owner.authorizer.cache = make(map[string]cachedAuthorization, len(snapshot))
	for key, value := range snapshot {
		owner.authorizer.cache[key] = value
	}
}

func TestPerformanceReviewGatewayAuthorizerCache(t *testing.T) {
	for _, policy := range []string{"exact", "wildcard"} {
		t.Run(policy, func(t *testing.T) {
			owner := newPerformanceCacheOwner(t, policy == "wildcard")
			full := owner.prefill(t)
			validNow := owner.now
			var firstExpiry time.Time
			for _, entry := range full {
				if entry.response.PrincipalID == "seed-0000" {
					firstExpiry = entry.expires
				}
			}
			if firstExpiry.IsZero() {
				t.Fatal("native oldest expiry not found")
			}
			// Missing identity never invokes/adopts a policy, and an entry
			// remains valid strictly before its real TTL expiry boundary.
			beforeCalls := owner.calls
			_, status := owner.authorizer.authorize(context.Background(), owner.event("", "a"))
			if status != 401 || owner.calls != beforeCalls || len(owner.authorizer.cache) != maxAuthorizerCache {
				t.Fatal("missing identity changed native cache admission")
			}
			owner.now = firstExpiry.Add(-time.Nanosecond)
			_, status = owner.authorizer.authorize(context.Background(), owner.event("seed-0000", "a"))
			if status != 0 || owner.calls != beforeCalls {
				t.Fatal("unexpired identity was treated as a miss")
			}

			for _, mode := range []string{"empty_miss", "full_hit", "full_policy_check", "full_new_miss", "full_expired_identity", "full_all_expired"} {
				t.Run(mode, func(t *testing.T) {
					event := owner.event("measured", "a")
					expectedStatus, expectedCalls, expectedSize := 0, 1, 1
					batch := 1
					now := validNow
					snapshot := full
					switch mode {
					case "empty_miss":
						snapshot = nil
					case "full_hit":
						event, expectedCalls, expectedSize, batch = owner.event("seed-4095", "a"), 0, maxAuthorizerCache, performanceGatewayBatch
					case "full_policy_check":
						event, expectedCalls, expectedSize, batch = owner.event("seed-4095", "b"), 0, maxAuthorizerCache, performanceGatewayBatch
						if policy == "exact" {
							expectedStatus = 403
						}
					case "full_new_miss":
						expectedSize = maxAuthorizerCache
					case "full_expired_identity":
						event, now, expectedSize = owner.event("seed-0000", "a"), firstExpiry, maxAuthorizerCache
					case "full_all_expired":
						now = validNow.Add(61 * time.Second)
					}
					samples := make([]float64, 0, performanceGatewaySamples)
					for sample := 0; sample < performanceGatewaySamples; sample++ {
						owner.now = now
						owner.restore(snapshot)
						beforeCalls := owner.calls
						var response authorizerResponse
						var status int
						started := time.Now()
						for operation := 0; operation < batch; operation++ {
							response, status = owner.authorizer.authorize(context.Background(), event)
						}
						samples = append(samples, float64(time.Since(started))/float64(time.Millisecond)/float64(batch))
						if status != expectedStatus || response.PrincipalID != event.Headers["Authorization"] ||
							owner.calls-beforeCalls != expectedCalls || len(owner.authorizer.cache) != expectedSize || owner.openBodies != 0 {
							t.Fatalf("cache native outcome/occupancy: mode=%s status=%d calls=%d size=%d principal=%q open=%d",
								mode, status, owner.calls-beforeCalls, len(owner.authorizer.cache), response.PrincipalID, owner.openBodies)
						}
						if mode == "full_new_miss" {
							for _, entry := range owner.authorizer.cache {
								if entry.response.PrincipalID == "seed-0000" {
									t.Fatal("full cache retained its strictly earliest-expiring entry")
								}
							}
						}
						if mode == "full_expired_identity" {
							for _, entry := range owner.authorizer.cache {
								if entry.response.PrincipalID == "seed-0000" && !entry.expires.Equal(now.Add(60*time.Second)) {
									t.Fatal("expiry boundary was treated as a valid hit")
								}
							}
						}
					}
					name := "cache_" + policy + "_" + mode
					t.Logf("PERFORMANCE_GATEWAY_FIXTURE case=%s initial_occupancy=%d operations_per_sample=%d policy=native_IAM downstream=bounded_transport setup=excluded", name, len(snapshot), batch)
					testperf.Report(t, name, "authorize_per_op_ms", samples)
				})
			}
			if err := owner.gateway.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPerformanceReviewGatewayCacheCanceledMissRetiresExchange(t *testing.T) {
	owner := newPerformanceCacheOwner(t, false)
	full := owner.prefill(t)
	owner.restore(full)
	owner.authorizer.config.Timeout = time.Minute // Caller cancellation owns this gated exchange.
	started, stopped := make(chan struct{}), make(chan struct{})
	owner.authorizer.client = &http.Client{Transport: performanceGatewayTransport(func(request *http.Request) (*http.Response, error) {
		defer request.Body.Close()
		defer close(stopped)
		close(started)
		<-request.Context().Done()
		return nil, request.Context().Err()
	})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan int, 1)
	go func() {
		_, status := owner.authorizer.authorize(ctx, owner.event("canceled-miss", "a"))
		done <- status
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("native miss never entered the HTTP exchange")
	}
	// An owned Invoke cannot retain the cache lock while awaiting I/O. A
	// concurrent known hit still uses the same real policy evaluator.
	if !owner.authorizer.mu.TryLock() {
		cancel()
		<-done
		t.Fatal("native Invoke retained the cache mutex while awaiting I/O")
	}
	owner.authorizer.mu.Unlock()
	_, hitStatus := owner.authorizer.authorize(context.Background(), owner.event("seed-4095", "a"))
	if hitStatus != 0 {
		t.Fatalf("independent hit blocked or failed: %d", hitStatus)
	}
	cancel()
	select {
	case status := <-done:
		if status != 500 {
			t.Fatalf("canceled native authorizer status=%d", status)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled native Invoke did not return")
	}
	select {
	case <-stopped:
	default:
		t.Fatal("authorizer returned before its owned transport joined")
	}
	if !errors.Is(ctx.Err(), context.Canceled) || !reflect.DeepEqual(owner.authorizer.cache, full) {
		t.Fatal("canceled miss changed native cache admission")
	}
	if err := owner.gateway.Close(); err != nil {
		t.Fatal(err)
	}
}
