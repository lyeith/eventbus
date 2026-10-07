package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAPIMappingBasePathNativeFormats(t *testing.T) {
	for _, authVersion := range []string{"1.0", "2.0"} {
		for _, integrationVersion := range []string{"1.0", "2.0"} {
			t.Run(authVersion+"/"+integrationVersion, func(t *testing.T) {
				endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					var event map[string]any
					_ = json.NewDecoder(r.Body).Decode(&event)
					authorizer := strings.Contains(r.URL.Path, "/auth/")
					if event["pathParameters"].(map[string]any)["id"] != "a b" {
						t.Errorf("mapped path parameters: %v", event)
					}
					if event["version"] == "1.0" {
						if event["path"] != "/edge/v1/items/a b" || event["requestContext"].(map[string]any)["path"] != "/edge/v1/items/a b" {
							t.Errorf("v1 mapping path lost: %v", event)
						}
					} else {
						if event["rawPath"] != "/items/a%20b" || event["requestContext"].(map[string]any)["http"].(map[string]any)["path"] != "/items/a b" {
							t.Errorf("v2 mapping leaked into native path: %v", event)
						}
					}
					if authorizer {
						key := "methodArn"
						if authVersion == "2.0" {
							key = "routeArn"
						}
						arn := event[key].(string)
						if !strings.HasSuffix(arn, "/release/POST/items/a b") {
							t.Errorf("mapping leaked into route ARN: %s", arn)
						}
						_ = json.NewEncoder(w).Encode(allowResponse(arn, nil))
					} else {
						_ = json.NewEncoder(w).Encode(map[string]any{"statusCode": 200, "body": "ok"})
					}
				}))
				defer endpoint.Close()
				cfg := httpAPIConfig(endpoint.URL, authVersion, integrationVersion)
				cfg.BasePath = "/edge/v1"
				cfg.Stage = "release"
				edge := newTestGateway(t, cfg, Options{})
				for _, path := range []string{"/edge/v1/items/a%20b?token=x", "/%65dge/v1/items/a%20b?token=x"} {
					request := httptest.NewRequest("POST", path, nil)
					request.Header.Set("Authorization", "valid")
					response := httptest.NewRecorder()
					edge.ServeHTTP(response, request)
					if response.Code != 200 {
						t.Fatalf("mapping response %d %s", response.Code, response.Body.String())
					}
				}
				for _, path := range []string{"/items/a", "/edge/v10/items/a"} {
					response := httptest.NewRecorder()
					edge.ServeHTTP(response, httptest.NewRequest("POST", path, nil))
					if response.Code != 403 {
						t.Fatalf("outside mapping admitted: %s %d", path, response.Code)
					}
				}
			})
		}
	}
	for _, path := range []string{"/", "/trailing/", "/a/../b", "/a//b", "relative", "/bad?query", "/bad{param}"} {
		cfg := httpAPIConfig("http://localhost", "public", "2.0")
		cfg.BasePath = path
		if err := cfg.Validate(); err == nil {
			t.Fatalf("invalid base_path accepted %q", path)
		}
	}
}
