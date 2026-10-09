package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type flightTestResult struct {
	response authorizerResponse
	status   int
}

func waitForAuthorizerWaiters(t *testing.T, authorizer *lambdaAuthorizer, wanted int) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		authorizer.mu.Lock()
		actual := 0
		for _, flight := range authorizer.flights {
			actual += flight.waiters
		}
		authorizer.mu.Unlock()
		if actual == wanted {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("shared authorizer waiters=%d want=%d", actual, wanted)
		default:
			runtime.Gosched()
		}
	}
}

func awaitFlightResult(t *testing.T, results <-chan flightTestResult) flightTestResult {
	t.Helper()
	select {
	case result := <-results:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("owned authorizer request did not join")
		return flightTestResult{}
	}
}

func flightEvent(gateway *Gateway, token, suffix string) requestEvent {
	request := httptest.NewRequest("GET", "/api/"+suffix, nil)
	request.Header.Set("Authorization", token)
	return newRequestEvent(gateway.config, gateway.routes[0].config, request, map[string]string{"proxy": suffix}, "request-"+suffix)
}

func flightGateway(t *testing.T, endpoint string, ttl int) *Gateway {
	t.Helper()
	cfg := fixture(endpoint+"/2015-03-31/functions/auth/invocations", "http://127.0.0.1:1")
	auth := cfg.Authorizers["auth"]
	auth.TTL, auth.Timeout = intPointer(ttl), 30*time.Second
	auth.IdentitySources = []string{"method.request.header.Authorization"}
	cfg.Authorizers["auth"] = auth
	return newTestGateway(t, cfg, Options{})
}

func flightAuthorize(authorizer *lambdaAuthorizer, ctx context.Context, event requestEvent) <-chan flightTestResult {
	result := make(chan flightTestResult, 1)
	go func() {
		response, status := authorizer.authorize(ctx, event)
		result <- flightTestResult{response, status}
	}()
	return result
}

func TestAuthorizerFlightSharesResponseAndEvaluatesEachCurrentARN(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) != 1 {
			t.Error("same identity created another native Invoke")
			w.WriteHeader(500)
			return
		}
		var event requestEvent
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			t.Error(err)
		}
		if r.Header.Get("X-Amz-Invocation-Type") != "RequestResponse" || event.Type != "REQUEST" {
			t.Error("shared work bypassed the native authorizer/Invoke contract")
		}
		close(started)
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		prefix := event.MethodARN[:len(event.MethodARN)-len("public")]
		_ = json.NewEncoder(w).Encode(map[string]any{
			"principalId": "shared",
			"policyDocument": map[string]any{"Version": "2012-10-17", "Statement": []any{
				map[string]any{"Effect": "Allow", "Action": "execute-api:Invoke", "Resource": prefix + "*"},
				map[string]any{"Effect": "Deny", "Action": "execute-api:Invoke", "Resource": prefix + "private"},
			}},
		})
	}))
	t.Cleanup(endpoint.Close)
	gateway := flightGateway(t, endpoint.URL, 30)
	authorizer := gateway.authorizers["auth"]
	first := flightAuthorize(authorizer, t.Context(), flightEvent(gateway, "same", "public"))
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("native Invoke did not start")
	}
	second := flightAuthorize(authorizer, t.Context(), flightEvent(gateway, "same", "private"))
	waitForAuthorizerWaiters(t, authorizer, 2)
	close(release)
	if result := awaitFlightResult(t, first); result.status != 0 || result.response.PrincipalID != "shared" {
		t.Fatalf("shared Allow: %+v", result)
	}
	if result := awaitFlightResult(t, second); result.status != 403 || result.response.PrincipalID != "shared" {
		t.Fatalf("shared explicit Deny for current ARN: %+v", result)
	}
	if _, status := authorizer.authorize(t.Context(), flightEvent(gateway, "same", "private")); status != 403 || calls.Load() != 1 {
		t.Fatalf("cached explicit Deny=%d native invocations=%d", status, calls.Load())
	}
}

func TestAuthorizerFlightLeaderCancellationDoesNotCancelWaitingCaller(t *testing.T) {
	started, release, cancelled := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var event requestEvent
		_ = json.NewDecoder(r.Body).Decode(&event)
		close(started)
		select {
		case <-release:
			_ = json.NewEncoder(w).Encode(allowResponse(event.MethodARN, nil))
		case <-r.Context().Done():
			close(cancelled)
		}
	}))
	t.Cleanup(endpoint.Close)
	gateway := flightGateway(t, endpoint.URL, 30)
	authorizer := gateway.authorizers["auth"]
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	event := flightEvent(gateway, "same", "public")
	first := flightAuthorize(authorizer, ctx, event)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("native Invoke did not start")
	}
	second := flightAuthorize(authorizer, t.Context(), event)
	waitForAuthorizerWaiters(t, authorizer, 2)
	cancel()
	if result := awaitFlightResult(t, first); result.status != 500 {
		t.Fatalf("cancelled first caller status=%d", result.status)
	}
	waitForAuthorizerWaiters(t, authorizer, 1)
	select {
	case <-cancelled:
		t.Fatal("leader cancellation killed surviving caller's shared Invoke")
	default:
	}
	close(release)
	if result := awaitFlightResult(t, second); result.status != 0 {
		t.Fatalf("surviving caller status=%d", result.status)
	}
	if _, status := authorizer.authorize(t.Context(), event); status != 0 || calls.Load() != 1 {
		t.Fatalf("surviving response was not cached: status=%d calls=%d", status, calls.Load())
	}
}

func TestAuthorizerFlightLastCancellationAndCloseJoin(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(fmt.Sprintf("close_%t", shutdown), func(t *testing.T) {
			started, stopped := make(chan struct{}), make(chan struct{})
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Consume the request so the server sees the client's disconnect.
				var event requestEvent
				_ = json.NewDecoder(r.Body).Decode(&event)
				close(started)
				<-r.Context().Done()
				close(stopped)
			}))
			t.Cleanup(endpoint.Close)
			gateway := flightGateway(t, endpoint.URL, 30)
			authorizer := gateway.authorizers["auth"]
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			result := flightAuthorize(authorizer, ctx, flightEvent(gateway, "cancelled", "public"))
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("native Invoke did not start")
			}
			if shutdown {
				joined := make(chan error, 2)
				for range 2 {
					go func() { joined <- gateway.Close() }()
				}
				for range 2 {
					select {
					case err := <-joined:
						if err != nil {
							t.Fatal(err)
						}
					case <-time.After(5 * time.Second):
						t.Fatal("concurrent Close did not join the authorizer worker")
					}
				}
			} else {
				cancel()
			}
			if completed := awaitFlightResult(t, result); completed.status != 500 {
				t.Fatalf("cancelled native exchange status=%d", completed.status)
			}
			select {
			case <-stopped:
			case <-time.After(5 * time.Second):
				t.Fatal("cancelled native HTTP exchange remained active")
			}
			authorizer.mu.Lock()
			flights, cached := len(authorizer.flights), len(authorizer.cache)
			authorizer.mu.Unlock()
			if flights != 0 || cached != 0 {
				t.Fatalf("cancelled work retained: flights=%d cached=%d", flights, cached)
			}
		})
	}
}

func TestAuthorizerFlightCachesNativeDenialsButNotFailures(t *testing.T) {
	for _, test := range []struct {
		name, payload, functionError string
		httpStatus, status           int
		simple, cached               bool
	}{
		{"deny", `{"principalId":"p","policyDocument":{"Version":"2012-10-17","Statement":{"Effect":"Deny","Action":"*","Resource":"*"}}}`, "", 200, 403, false, true},
		{"simple false", `{"isAuthorized":false,"context":{"reason":"native"}}`, "", 200, 403, true, true},
		{"malformed", `{`, "", 200, 500, false, false},
		{"function failure", `{"errorMessage":"failed"}`, "Unhandled", 200, 500, false, false},
		{"unauthorized", `{"errorMessage":"Unauthorized"}`, "Unhandled", 200, 401, false, false},
		{"invoke failure", `{"message":"failed"}`, "", 403, 500, false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			started, release := make(chan struct{}), make(chan struct{})
			var calls atomic.Int32
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) == 1 {
					close(started)
					select {
					case <-release:
					case <-r.Context().Done():
						return
					}
				}
				if test.functionError != "" {
					w.Header().Set("X-Amz-Function-Error", test.functionError)
				}
				w.WriteHeader(test.httpStatus)
				_, _ = w.Write([]byte(test.payload))
			}))
			t.Cleanup(endpoint.Close)
			gateway := flightGateway(t, endpoint.URL, 30)
			authorizer := gateway.authorizers["auth"]
			if test.simple {
				authorizer.config.PayloadFormatVersion = "2.0"
				authorizer.config.EnableSimpleResponses = true
				authorizer.config.IdentitySources = []string{"$request.header.Authorization"}
			}
			event := flightEvent(gateway, "same", "public")
			first := flightAuthorize(authorizer, t.Context(), event)
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("native Invoke did not start")
			}
			second := flightAuthorize(authorizer, t.Context(), event)
			waitForAuthorizerWaiters(t, authorizer, 2)
			close(release)
			for _, results := range []<-chan flightTestResult{first, second} {
				if result := awaitFlightResult(t, results); result.status != test.status {
					t.Fatalf("shared native outcome=%d want=%d", result.status, test.status)
				}
			}
			if _, status := authorizer.authorize(t.Context(), event); status != test.status {
				t.Fatalf("subsequent native outcome=%d want=%d", status, test.status)
			}
			wanted := int32(2)
			if test.cached {
				wanted = 1
			}
			if calls.Load() != wanted {
				t.Fatalf("native invocations=%d want=%d cached=%t", calls.Load(), wanted, test.cached)
			}
		})
	}
}

func TestAuthorizerFlightTTLZeroKeepsIndependentInvocations(t *testing.T) {
	started, release := make(chan struct{}, 2), make(chan struct{})
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var event requestEvent
		_ = json.NewDecoder(r.Body).Decode(&event)
		started <- struct{}{}
		select {
		case <-release:
			_ = json.NewEncoder(w).Encode(allowResponse(event.MethodARN, nil))
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(endpoint.Close)
	gateway := flightGateway(t, endpoint.URL, 0)
	authorizer := gateway.authorizers["auth"]
	events := []requestEvent{flightEvent(gateway, "", "one"), flightEvent(gateway, "", "two")}
	results := make([]<-chan flightTestResult, len(events))
	for i, event := range events {
		results[i] = flightAuthorize(authorizer, t.Context(), event)
	}
	for range 2 {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("TTL0 incorrectly coalesced native invocations or required identity")
		}
	}
	close(release)
	for _, result := range results {
		if result := awaitFlightResult(t, result); result.status != 0 {
			t.Fatalf("TTL0 native status=%d", result.status)
		}
	}
	if len(authorizer.flights) != 0 || len(authorizer.cache) != 0 {
		t.Fatal("TTL0 retained shared work or cached authorization")
	}
}

func TestAuthorizerFlightBoundsKeepOverflowRequestOwned(t *testing.T) {
	for _, identities := range []bool{true, false} {
		t.Run(fmt.Sprintf("identities_%t", identities), func(t *testing.T) {
			limit := maxAuthorizerFlightWaiters
			if identities {
				limit = maxAuthorizerFlights
			}
			started, release := make(chan struct{}, limit+1), make(chan struct{})
			var calls atomic.Int32
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				var event requestEvent
				_ = json.NewDecoder(r.Body).Decode(&event)
				started <- struct{}{}
				select {
				case <-release:
					_ = json.NewEncoder(w).Encode(allowResponse(event.MethodARN, nil))
				case <-r.Context().Done():
				}
			}))
			t.Cleanup(endpoint.Close)
			gateway := flightGateway(t, endpoint.URL, 30)
			authorizer := gateway.authorizers["auth"]
			var joined sync.WaitGroup
			results := make(chan flightTestResult, limit+1)
			begin := func(index int) {
				joined.Add(1)
				go func() {
					defer joined.Done()
					token := "same"
					if identities {
						token = fmt.Sprintf("identity-%d", index)
					}
					response, status := authorizer.authorize(t.Context(), flightEvent(gateway, token, "public"))
					results <- flightTestResult{response, status}
				}()
			}
			for index := range limit {
				begin(index)
			}
			if identities {
				for range limit {
					select {
					case <-started:
					case <-time.After(5 * time.Second):
						t.Fatal("bounded identity exchanges did not start")
					}
				}
			} else {
				waitForAuthorizerWaiters(t, authorizer, limit)
				<-started
			}
			begin(limit)
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("overflow request was rejected or retained as a shared waiter")
			}
			authorizer.mu.Lock()
			flights, waiters := len(authorizer.flights), 0
			for _, flight := range authorizer.flights {
				waiters += flight.waiters
			}
			authorizer.mu.Unlock()
			wantedFlights := 1
			if identities {
				wantedFlights = limit
			}
			if flights != wantedFlights || waiters != limit {
				t.Fatalf("bounded ownership: flights=%d waiters=%d want=%d/%d", flights, waiters, wantedFlights, limit)
			}
			close(release)
			joined.Wait()
			for range limit + 1 {
				if result := awaitFlightResult(t, results); result.status != 0 {
					t.Fatalf("accepted overflow status=%d", result.status)
				}
			}
			wantedCalls := int32(2)
			if identities {
				wantedCalls = int32(limit + 1)
			}
			if calls.Load() != wantedCalls {
				t.Fatalf("actual native calls=%d want=%d", calls.Load(), wantedCalls)
			}
		})
	}
}
