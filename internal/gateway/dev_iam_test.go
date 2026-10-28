package gateway

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func localIAMFixtureConfig() Config {
	return Config{
		AccountID: "111111111111",
		IAMCredentials: map[string]DevIAMCredential{"LOCAL_SERVICE": {
			SecretAccessKey: "fixture-secret", PrincipalARN: "arn:aws:iam::222222222222:user/service", AccountID: "222222222222",
		}},
		Authorizers: map[string]AuthorizerConfig{"request": {
			Type: "REQUEST", InvokeURL: "http://127.0.0.1:1/2015-03-31/functions/auth/invocations", TTL: intPointer(0),
		}},
		Routes: []RouteConfig{{Path: "/items/{id}", Method: "POST", AuthorizationType: "AWS_IAM",
			Integration: IntegrationConfig{Type: "HTTP_PROXY", URI: "http://127.0.0.1:1/{id}"}}},
	}
}

func TestLocalIAMConfigurationRejectsAmbiguousAuthority(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*Config)
	}{
		{"missing credentials", func(cfg *Config) { cfg.IAMCredentials = nil }},
		{"missing secret", func(cfg *Config) {
			value := cfg.IAMCredentials["LOCAL_SERVICE"]
			value.SecretAccessKey = ""
			cfg.IAMCredentials["LOCAL_SERVICE"] = value
		}},
		{"ARN account disagrees", func(cfg *Config) {
			value := cfg.IAMCredentials["LOCAL_SERVICE"]
			value.AccountID = "333333333333"
			cfg.IAMCredentials["LOCAL_SERVICE"] = value
		}},
		{"nonprincipal ARN", func(cfg *Config) {
			value := cfg.IAMCredentials["LOCAL_SERVICE"]
			value.PrincipalARN = "arn:aws:s3::222222222222:bucket"
			cfg.IAMCredentials["LOCAL_SERVICE"] = value
		}},
		{"empty ARN principal", func(cfg *Config) {
			value := cfg.IAMCredentials["LOCAL_SERVICE"]
			value.PrincipalARN = "arn:aws:iam::222222222222:user/"
			cfg.IAMCredentials["LOCAL_SERVICE"] = value
		}},
		{"token framing", func(cfg *Config) {
			value := cfg.IAMCredentials["LOCAL_SERVICE"]
			value.SessionToken = "private-token\r\n"
			cfg.IAMCredentials["LOCAL_SERVICE"] = value
		}},
		{"two auth owners", func(cfg *Config) { cfg.Routes[0].Authorizer = "request" }},
		{"NONE with REQUEST", func(cfg *Config) {
			cfg.Routes[0].AuthorizationType, cfg.Routes[0].Authorizer = "NONE", "request"
		}},
		{"CUSTOM without REQUEST", func(cfg *Config) { cfg.Routes[0].AuthorizationType = "CUSTOM" }},
		{"unknown auth type", func(cfg *Config) { cfg.Routes[0].AuthorizationType = "AWS_JWT" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := localIAMFixtureConfig()
			tc.edit(&cfg)
			err := cfg.Validate()
			require.Error(t, err)
			require.NotContains(t, err.Error(), "fixture-secret")
			require.NotContains(t, err.Error(), "private-token")
		})
	}
	for _, tc := range []struct{ explicit, authorizer, resolved string }{
		{"", "", "NONE"}, {"", "request", "CUSTOM"}, {"NONE", "", "NONE"},
		{"CUSTOM", "request", "CUSTOM"}, {"AWS_IAM", "", "AWS_IAM"},
	} {
		cfg := localIAMFixtureConfig()
		cfg.Routes[0].AuthorizationType, cfg.Routes[0].Authorizer = tc.explicit, tc.authorizer
		require.NoError(t, cfg.Validate())
		require.Equal(t, tc.resolved, routeAuthorizationType(cfg.Routes[0]))
	}
}

func TestLocalIAMYAMLRecipeLoadsNativeRouteAndExplicitCaller(t *testing.T) {
	file := filepath.Join(t.TempDir(), "gateway.yaml")
	require.NoError(t, os.WriteFile(file, []byte(`account_id: "111111111111"
iam_credentials:
  LOCAL_SERVICE:
    secret_access_key: fixture-secret
    principal_arn: arn:aws:sts::222222222222:assumed-role/service/local-session
    account_id: "222222222222"
    user_id: "AROAEXAMPLE:local-session"
    caller_id: "AROAEXAMPLE:local-session"
routes:
  - path: /{proxy+}
    method: ANY
    route_key: $default
    authorization_type: AWS_IAM
    integration:
      type: AWS_PROXY
      invoke_url: http://127.0.0.1:1/2015-03-31/functions/app/invocations
      payload_format_version: "2.0"
`), 0600))
	cfg, err := LoadConfig(file)
	require.NoError(t, err)
	require.Equal(t, "AWS_IAM", routeAuthorizationType(cfg.Routes[0]))
	require.Equal(t, "222222222222", cfg.IAMCredentials["LOCAL_SERVICE"].AccountID)
	require.Equal(t, "111111111111", cfg.AccountID)
}

func TestIAMHTTPProxyReusesVerifiedBytesAndOwnedCredentials(t *testing.T) {
	body := []byte{0, 0xff, 'o', 'r', 'i', 'g', 'i', 'n', 'a', 'l', '\n'}
	const query = "tag=two&tag=one&space=a%20b&plus=%2B"
	var calls atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		data, err := io.ReadAll(r.Body)
		assert.NoError(t, err)
		assert.Equal(t, body, data)
		assert.Equal(t, "/prefix/a%2Fb", r.URL.EscapedPath())
		assert.Equal(t, query, r.URL.RawQuery)
		assert.Empty(t, r.Header.Get("Authorization"), "recipe still controls forwarded credentials")
		assert.Empty(t, r.Header.Get("X-Signed-Removed"))
		w.WriteHeader(http.StatusAccepted)
	}))
	defer backend.Close()
	cfg := localIAMFixtureConfig()
	cfg.BasePath = "/edge/v1"
	cfg.RemoveHeaders = []string{"X-Signed-Removed"}
	cfg.Routes[0].Integration.URI = backend.URL + "/prefix/{id}"
	cfg.Routes[0].Integration.RemoveHeaders = []string{"Authorization"}
	edge := newTestGateway(t, cfg, Options{})
	original := cfg.IAMCredentials["LOCAL_SERVICE"]
	changed := original
	changed.SecretAccessKey, changed.PrincipalARN = "changed", "arn:aws:iam::222222222222:user/changed"
	cfg.IAMCredentials["LOCAL_SERVICE"] = changed
	delete(cfg.IAMCredentials, "LOCAL_SERVICE")
	require.Equal(t, original, edge.config.IAMCredentials["LOCAL_SERVICE"], "gateway owns the recipe independently of its caller")
	frontend := httptest.NewServer(edge)
	defer frontend.Close()
	client := frontend.Client()
	client.Timeout = 3 * time.Second
	for _, framing := range []string{"content-length", "chunked"} {
		t.Run(framing, func(t *testing.T) {
			request, err := http.NewRequest(http.MethodPost, frontend.URL+"/edge/v1/items/a%2Fb?"+query, bytes.NewReader(body))
			require.NoError(t, err)
			if framing == "chunked" {
				request.ContentLength = -1
			}
			request.Header.Set("Content-Type", "application/octet-stream")
			request.Header.Set("X-Signed-Removed", "verify-before-removal")
			request.Header.Set("X-Request-Id", "client-id-signed-before-gateway-replacement")
			digest := sha256.Sum256(body)
			require.NoError(t, v4.NewSigner().SignHTTP(t.Context(), aws.Credentials{AccessKeyID: "LOCAL_SERVICE", SecretAccessKey: original.SecretAccessKey},
				request, hex.EncodeToString(digest[:]), "execute-api", "us-east-1", time.Now()))
			request.URL.RawQuery = query
			response, err := client.Do(request)
			require.NoError(t, err)
			defer response.Body.Close()
			require.Equal(t, http.StatusAccepted, response.StatusCode)
		})
	}
	unsigned, err := http.NewRequest(http.MethodPost, frontend.URL+"/edge/v1/items/a%2Fb?"+query, bytes.NewReader(body))
	require.NoError(t, err)
	response, err := client.Do(unsigned)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusForbidden, response.StatusCode)
	require.EqualValues(t, 2, calls.Load(), "unsigned requests cannot reach the HTTP integration")
}

func TestIAMNoAuthProxyRetainsExistingBodyLimitContract(t *testing.T) {
	// Only authenticated IAM adds a bounded hashing step. Explicit NoAuth keeps
	// the proxy path streaming and never invents a verified caller.
	const size = maxIAMRequestBody + 1
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count, err := io.Copy(io.Discard, r.Body)
		assert.NoError(t, err)
		assert.EqualValues(t, size, count)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer backend.Close()
	cfg := localIAMFixtureConfig()
	cfg.Routes[0].Integration.URI = backend.URL + "/{id}"
	edge := newTestGateway(t, cfg, Options{NoAuth: true})
	request := httptest.NewRequest(http.MethodPost, "http://gateway.test/items/large", strings.NewReader(strings.Repeat("x", size)))
	response := httptest.NewRecorder()
	edge.ServeHTTP(response, request)
	require.Equal(t, http.StatusAccepted, response.Code, response.Body.String())
}

func TestReadinessDoesNotInheritEncodedApplicationSeparators(t *testing.T) {
	cfg := localIAMFixtureConfig()
	cfg.DevHealthPath = "/ready/private"
	cfg.Routes[0].Path, cfg.Routes[0].Method, cfg.Routes[0].RouteKey = "/{proxy+}", "ANY", "$default"
	cfg.Routes[0].Integration.URI = "http://127.0.0.1:1/unreached"
	edge := newTestGateway(t, cfg, Options{})
	for _, tc := range []struct {
		path   string
		status int
	}{
		{"/ready/private", http.StatusOK},
		{"/ready/%70rivate", http.StatusOK},
		{"/ready%2Fprivate", http.StatusForbidden},
		{"/ready%2fprivate", http.StatusForbidden},
	} {
		response := httptest.NewRecorder()
		edge.ServeHTTP(response, httptest.NewRequest(http.MethodGet, tc.path, nil))
		require.Equal(t, tc.status, response.Code, tc.path+": "+response.Body.String())
	}
}
