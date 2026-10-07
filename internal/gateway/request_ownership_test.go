package gateway

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestNativeIdentityNamesAndOneIncomingRequestContext(t *testing.T) {
	for _, version := range []string{"1.0", "2.0"} {
		t.Run(version, func(t *testing.T) {
			var authContext map[string]any
			var authorizerCalls atomic.Int32
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var event map[string]any
				_ = json.NewDecoder(r.Body).Decode(&event)
				context := event["requestContext"].(map[string]any)
				if strings.Contains(r.URL.Path, "/auth/") {
					authorizerCalls.Add(1)
					authContext = context
					if event["headers"].(map[string]any)["x.org-key"] != "valid" {
						t.Error("legal dotted header identity lost")
					}
					time.Sleep(25 * time.Millisecond)
					_ = json.NewEncoder(w).Encode(allowResponse(event["routeArn"].(string), nil))
					return
				}
				epochKey := "timeEpoch"
				if version == "1.0" {
					epochKey = "requestTimeEpoch"
				}
				if context[epochKey] != authContext["timeEpoch"] || context["requestId"] != authContext["requestId"] {
					t.Errorf("integration rebuilt incoming request metadata: %v vs %v", context, authContext)
				}
				if version == "1.0" && (context["extendedRequestId"] != context["requestId"] || context["domainPrefix"] == nil || context["resourceId"] != nil) {
					t.Errorf("native 1.0 context shape %v", context)
				}
				if _, present := event["headers"].(map[string]any)["x.org-key"]; present {
					t.Error("integration header removal changed shared metadata contract")
				}
				_, _ = io.WriteString(w, `{"statusCode":200,"body":"ok"}`)
			}))
			defer endpoint.Close()
			cfg := httpAPIConfig(endpoint.URL, "2.0", version)
			auth := cfg.Authorizers["auth"]
			auth.IdentitySources = []string{"$request.header.X.Org-Key", "$request.querystring.token.value"}
			cfg.Authorizers["auth"] = auth
			cfg.Routes[0].Integration.RemoveHeaders = []string{"X.Org-Key"}
			edge := newTestGateway(t, cfg, Options{})
			missing := httptest.NewRequest("POST", "/items/1?Token.value=secret", nil)
			missing.Header.Set("X.ORG-KEY", "valid")
			response := httptest.NewRecorder()
			edge.ServeHTTP(response, missing)
			if response.Code != 401 || authorizerCalls.Load() != 0 {
				t.Fatalf("dotted query case was ignored: %d", response.Code)
			}
			request := httptest.NewRequest("POST", "/items/1?token.value=secret", nil)
			request.Header.Set("X.ORG-KEY", "valid")
			response = httptest.NewRecorder()
			edge.ServeHTTP(response, request)
			if response.Code != 200 || authorizerCalls.Load() != 1 {
				t.Fatalf("legal native identity failed: %d %s", response.Code, response.Body.String())
			}
		})
	}
}
