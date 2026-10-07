package gateway

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

func TestNativeTrailingSlashRoutesPreservePathAndAuthorization(t *testing.T) {
	for _, authVersion := range []string{"1.0", "2.0"} {
		for _, integrationVersion := range []string{"1.0", "2.0"} {
			for _, fallback := range []bool{false, true} {
				t.Run(fmt.Sprintf("auth=%s/integration=%s/default=%t", authVersion, integrationVersion, fallback), func(t *testing.T) {
					authorizations := make(chan map[string]any, 4)
					var integrationCalls atomic.Int32
					endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						var event map[string]any
						if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
							t.Error(err)
							w.WriteHeader(500)
							return
						}
						if strings.Contains(r.URL.Path, "/auth/") {
							authorizations <- event
							arn := "methodArn"
							if authVersion == "2.0" {
								arn = "routeArn"
							}
							_ = json.NewEncoder(w).Encode(allowResponse(event[arn].(string), map[string]any{"scope": "private"}))
							return
						}
						integrationCalls.Add(1)
						data, _ := json.Marshal(event)
						_ = json.NewEncoder(w).Encode(map[string]any{"statusCode": 200, "body": string(data)})
					}))
					defer endpoint.Close()
					cfg := httpAPIConfig(endpoint.URL, authVersion, integrationVersion)
					cfg.DevHealthPath = "/.eventbus/ready"
					cfg.BasePath = "/edge"
					auth := cfg.Authorizers["auth"]
					auth.IdentitySources = []string{"$request.header.Authorization"}
					cfg.Authorizers["auth"] = auth
					integration := cfg.Routes[0].Integration
					broad := RouteConfig{Path: "/api/{proxy+}", Method: "ANY", Authorizer: "auth", Integration: integration}
					if fallback {
						broad.Path, broad.RouteKey = "/{proxy+}", "$default"
					}
					// Deliberately configure broader routes first; selection must use specificity.
					cfg.Routes = []RouteConfig{broad,
						{Path: "/api/docs/{proxy+}", Method: "GET", Integration: integration},
						{Path: "/api/docs", Method: "GET", Integration: integration},
						{Path: "/api/docs/", Method: "GET", Integration: integration},
						{Path: "/api/docs/private/", Method: "GET", Authorizer: "auth", Integration: integration},
						{Path: "/api/{section}/admin/", Method: "GET", Authorizer: "auth", Integration: integration},
						{Path: "/api/only", Method: "GET", Integration: integration},
						{Path: "/api/locked", Method: "GET", Integration: integration},
						{Path: "/api/locked/", Method: "GET", Authorizer: "auth", Integration: integration},
					}
					edge := newTestGateway(t, cfg, Options{})
					for _, tc := range []struct {
						method, path, route, proxy, section, credential string
						status                                          int
					}{
						{method: "GET", path: "/api/docs", route: "/api/docs", status: 200},
						{method: "GET", path: "/api/docs/", route: "/api/docs/", status: 200},
						{method: "GET", path: "/api/docs/index.html", route: "/api/docs/{proxy+}", proxy: "index.html", status: 200},
						{method: "GET", path: "/api/docs/assets/nested.js", route: "/api/docs/{proxy+}", proxy: "assets/nested.js", status: 200},
						{method: "GET", path: "/api/docs/assets/", route: "/api/docs/{proxy+}", proxy: "assets/", status: 200},
						{method: "GET", path: "/api/docs/private/", status: 401},
						{method: "GET", path: "/api/docs/private/", route: "/api/docs/private/", credential: "valid", status: 200},
						{method: "GET", path: "/api/docs/admin/", status: 401},
						{method: "GET", path: "/api/docs/admin/", route: "/api/{section}/admin/", section: "docs", credential: "valid", status: 200},
						{method: "GET", path: "/api/only/", status: 401},
						{method: "POST", path: "/api/docs/", status: 401},
						{method: "GET", path: "/api/locked", route: "/api/locked", status: 200},
						{method: "GET", path: "/api/locked/", status: 401},
						{method: "GET", path: "/api/locked/", route: "/api/locked/", credential: "valid", status: 200},
						{method: "GET", path: "/api/docs//", status: 400},
						{method: "GET", path: "/api/docs/%2f", status: 400},
						{method: "GET", path: "/api/%2e/docs/", status: 400},
					} {
						request := httptest.NewRequest(tc.method, "/edge"+tc.path+"?source=original", nil)
						request.Header.Set("Authorization", tc.credential)
						response := httptest.NewRecorder()
						before := integrationCalls.Load()
						edge.ServeHTTP(response, request)
						if response.Code != tc.status {
							t.Fatalf("%s %s: %d %s", tc.method, tc.path, response.Code, response.Body.String())
						}
						if tc.status != 200 {
							if integrationCalls.Load() != before {
								t.Fatalf("refused request reached integration: %s", tc.path)
							}
							continue
						}
						var event map[string]any
						if err := json.Unmarshal(response.Body.Bytes(), &event); err != nil {
							t.Fatal(err)
						}
						assertTrailingSlashEvent(t, event, integrationVersion, tc.path, tc.route)
						parameters := event["pathParameters"].(map[string]any)
						wantParameters := map[string]any{}
						if tc.proxy != "" {
							wantParameters["proxy"] = tc.proxy
						}
						if tc.section != "" {
							wantParameters["section"] = tc.section
						}
						if !reflect.DeepEqual(parameters, wantParameters) {
							t.Fatalf("path parameters %v, want %v", parameters, wantParameters)
						}
						if tc.credential != "" {
							authorized := <-authorizations
							assertTrailingSlashEvent(t, authorized, authVersion, tc.path, tc.route)
							arnKey := "methodArn"
							if authVersion == "2.0" {
								arnKey = "routeArn"
							}
							if !strings.HasSuffix(authorized[arnKey].(string), "/GET"+tc.path) {
								t.Fatalf("authorizer ARN lost slash: %v", authorized)
							}
						} else if _, ok := event["requestContext"].(map[string]any)["authorizer"]; ok {
							t.Fatalf("public route gained authorization: %v", event)
						}
					}
					before := integrationCalls.Load()
					response := httptest.NewRecorder()
					edge.ServeHTTP(response, httptest.NewRequest("GET", "/.eventbus/ready", nil))
					if response.Code != 200 || integrationCalls.Load() != before || len(authorizations) != 0 {
						t.Fatal("readiness escaped its harness reservation")
					}
				})
			}
		}
	}
}

func assertTrailingSlashEvent(t *testing.T, event map[string]any, version, path, route string) {
	t.Helper()
	requestContext := event["requestContext"].(map[string]any)
	if version == "2.0" {
		if event["rawPath"] != path || requestContext["http"].(map[string]any)["path"] != path || event["routeKey"] != "GET "+route {
			t.Fatalf("v2 path/route rewritten: %v", event)
		}
	} else if event["path"] != "/edge"+path || requestContext["path"] != "/edge"+path || event["resource"] != route {
		t.Fatalf("v1 path/route rewritten: %v", event)
	}
}
