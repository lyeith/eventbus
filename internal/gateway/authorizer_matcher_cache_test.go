package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestParsedIAMMatchersPreserveNativeGlobGrammar(t *testing.T) {
	const prefix = "arn:aws:execute-api:us-east-1:123456789012:api/dev/GET/"
	for _, test := range []struct {
		name, action, resource, suffix string
		status                         int
	}{
		{"action case", "EXECUTE-API:INVOKE", prefix + "docs/a", "docs/a", 0},
		{"action wildcard", "execute-api:*", prefix + "docs/a", "docs/a", 0},
		{"wildcard spans slash and newline", "execute-api:Invoke", prefix + "docs/*", "docs/a/\nb", 0},
		{"question matches Unicode rune", "execute-api:Invoke", prefix + "docs/?", "docs/α", 0},
		{"question matches newline", "execute-api:Invoke", prefix + "docs/a?b", "docs/a\nb", 0},
		{"question rejects two runes", "execute-api:Invoke", prefix + "docs/?", "docs/αβ", 403},
		{"regexp metacharacters are literal", "execute-api:Invoke", prefix + "docs/[a].(b)+$", "docs/[a].(b)+$", 0},
		{"resource remains case sensitive", "execute-api:Invoke", prefix + "docs/A", "docs/a", 403},
		{"resource anchored at end", "execute-api:Invoke", prefix + "docs/a", "docs/a/more", 403},
		{"decoded replacement rune", "execute-api:Invoke", prefix + "docs/�", "docs/" + string([]byte{0xff}), 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			data, err := json.Marshal(map[string]any{"principalId": "native", "policyDocument": map[string]any{
				"Version": "2012-10-17", "Statement": map[string]any{"Effect": "Allow", "Action": test.action, "Resource": test.resource},
			}})
			if err != nil {
				t.Fatal(err)
			}
			response, err := parseAuthorizerResponse(data)
			if err != nil {
				t.Fatal(err)
			}
			if status := policyStatus(response, prefix+test.suffix); status != test.status {
				t.Fatalf("parsed native policy status=%d, want %d", status, test.status)
			}
		})
	}
}

func TestParsedIAMPolicyReusesMatchersForCurrentARNAndExplicitDeny(t *testing.T) {
	const prefix = "arn:aws:execute-api:us-east-1:123456789012:api/dev/GET/"
	data, err := json.Marshal(map[string]any{"principalId": "native", "policyDocument": map[string]any{
		"Version": "2012-10-17",
		"Statement": []any{
			map[string]any{"Effect": "Allow", "Action": "execute-api:Invoke", "Resource": prefix + "docs/*"},
			map[string]any{"Effect": "Deny", "Action": "execute-api:Invoke", "Resource": prefix + "docs/private/*"},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	response, err := parseAuthorizerResponse(data)
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	for index := 0; index < 8; index++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for attempt := 0; attempt < 50; attempt++ {
				for _, request := range []struct {
					arn    string
					status int
				}{
					{prefix + "docs/public", 0}, {prefix + "docs/private/object", 403}, {prefix + "other", 403},
				} {
					if status := policyStatus(response, request.arn); status != request.status {
						t.Errorf("shared parsed policy changed current ARN: status=%d want=%d", status, request.status)
						return
					}
				}
			}
		}()
	}
	workers.Wait()
}

func TestCoalescedSameIdentityCacheInsertionRetainsOtherEntries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gates := [1]chan struct{}{make(chan struct{})}

	started := make(chan int, 2)
	exited := make(chan int, 2)
	var calls atomic.Int32
	endpoint := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		index := int(calls.Add(1)) - 1
		if index >= len(gates) {
			t.Error("unexpected additional native authorizer invocation")
			writer.WriteHeader(500)
			return
		}
		defer func() { exited <- index }()
		var event requestEvent
		if err := json.NewDecoder(request.Body).Decode(&event); err != nil {
			t.Error(err)
			writer.WriteHeader(500)
			return
		}
		started <- index
		select {
		case <-gates[index]:
		case <-request.Context().Done():
			return
		}
		response := allowResponse(event.MethodARN, nil)
		response["principalId"] = "new-identity"
		if err := json.NewEncoder(writer).Encode(response); err != nil {
			t.Error(err)
		}
	}))
	t.Cleanup(endpoint.Close)
	cfg := fixture(endpoint.URL+"/2015-03-31/functions/auth/invocations", "http://127.0.0.1:1")
	auth := cfg.Authorizers["auth"]
	auth.TTL, auth.Timeout = intPointer(30), 30*time.Second
	auth.IdentitySources = []string{"method.request.header.Authorization"}
	cfg.Authorizers["auth"] = auth
	gateway := newTestGateway(t, cfg, Options{})
	t.Cleanup(func() {
		cancel()
		for _, gate := range gates {
			select {
			case <-gate:
			default:
				close(gate)
			}
		}
	})
	authorizer := gateway.authorizers["auth"]
	now := time.Now()
	authorizer.now = func() time.Time { return now }
	request := httptest.NewRequest("GET", "/api/object", nil)
	request.Header.Set("Authorization", "same-new-identity")
	event := newRequestEvent(gateway.config, gateway.routes[0].config, request, map[string]string{"proxy": "object"}, "native-request")
	data, err := json.Marshal(allowResponse(event.MethodARN, nil))
	if err != nil {
		t.Fatal(err)
	}
	existing, err := parseAuthorizerResponse(data)
	if err != nil {
		t.Fatal(err)
	}
	// These are preexisting private cache slots. The shared new key is produced
	// solely by real identity extraction/admission and actual HTTP Invoke.
	for index := 0; index < maxAuthorizerCache; index++ {
		authorizer.cache[fmt.Sprintf("existing-%04d", index)] = cachedAuthorization{
			response: existing, expires: now.Add(time.Duration(index+1) * time.Second),
		}
	}
	type result struct {
		response authorizerResponse
		status   int
	}
	results := make(chan result, 2)
	for index := 0; index < 2; index++ {
		go func() {
			response, status := authorizer.authorize(ctx, event)
			results <- result{response, status}
		}()
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent native misses did not enter the shared exchange")
	}
	waitForAuthorizerWaiters(t, authorizer, 2)
	assertSlots := func() {
		t.Helper()
		authorizer.mu.Lock()
		defer authorizer.mu.Unlock()
		if len(authorizer.cache) != maxAuthorizerCache {
			t.Fatalf("cache occupancy=%d, want %d", len(authorizer.cache), maxAuthorizerCache)
		}
		if _, exists := authorizer.cache["existing-0000"]; exists {
			t.Fatal("first new key retained the earliest-expiring slot")
		}
		for index := 1; index < maxAuthorizerCache; index++ {
			key := fmt.Sprintf("existing-%04d", index)
			entry, exists := authorizer.cache[key]
			if !exists || !reflect.DeepEqual(entry.response, existing) || !entry.expires.Equal(now.Add(time.Duration(index+1)*time.Second)) {
				t.Fatalf("same-key replacement evicted or changed unrelated slot %s", key)
			}
		}
	}
	close(gates[0])
	for index := 0; index < 2; index++ {
		select {
		case completed := <-results:
			if completed.status != 0 || completed.response.PrincipalID != "new-identity" {
				t.Fatalf("native concurrent outcome: status=%d principal=%q", completed.status, completed.response.PrincipalID)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("released native authorizer exchange did not join")
		}
		assertSlots()
	}
	for index := 0; index < 1; index++ {
		select {
		case <-exited:
		case <-time.After(5 * time.Second):
			t.Fatal("native authorizer handler did not join")
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("same-identity native misses=%d, want 1", calls.Load())
	}
	_, status := authorizer.authorize(ctx, event)
	if status != 0 || calls.Load() != 1 {
		t.Fatal("replaced identity was not cached")
	}
	if err := gateway.Close(); err != nil {
		t.Fatal(err)
	}
}
