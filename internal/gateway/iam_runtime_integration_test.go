package gateway_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/lyeith/eventbus/internal/gateway"
	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/server"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// Source and release checks share the same requests and application fixture.
// The optional binary owns only its gateway listener; Lambda stays with the
// existing test service and native process owner below.
func nativeIAMGateway(t *testing.T, cfg gateway.Config, noAuth bool) (string, *http.Client) {
	t.Helper()
	binary := os.Getenv("EVENTBUS_SMOKE_GATEWAY_BINARY")
	if binary == "" {
		edge, err := gateway.New(cfg, gateway.Options{NoAuth: noAuth, Logger: zerolog.Nop()})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, edge.Close()) })
		frontend := httptest.NewServer(edge)
		t.Cleanup(frontend.Close)
		client := frontend.Client()
		client.Timeout = 10 * time.Second
		return frontend.URL, client
	}
	require.True(t, filepath.IsAbs(binary), "EVENTBUS_SMOKE_GATEWAY_BINARY must select an absolute artifact")
	info, err := os.Stat(binary)
	require.NoError(t, err)
	require.True(t, info.Mode().IsRegular())
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := reserved.Addr().String()
	cfg.Port = reserved.Addr().(*net.TCPAddr).Port
	require.NoError(t, reserved.Close())
	cfg.DevHealthPath = "/.eventbus/iam-smoke-ready"
	directory := t.TempDir()
	configuration, err := yaml.Marshal(cfg)
	require.NoError(t, err)
	configPath := filepath.Join(directory, "gateway.yaml")
	require.NoError(t, os.WriteFile(configPath, configuration, 0600))
	args := []string{"--config", configPath}
	if noAuth {
		args = append(args, "--no-auth")
	}
	command := exec.Command(binary, args...)
	command.Dir, command.WaitDelay = directory, time.Second
	logPath := filepath.Join(directory, "gateway.log")
	output, err := os.OpenFile(logPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	require.NoError(t, err)
	t.Cleanup(func() { _ = output.Close() })
	command.Stdout, command.Stderr = output, output
	require.NoError(t, command.Start())
	done := make(chan struct{})
	var waitError error
	go func() { waitError = command.Wait(); close(done) }()
	transport := http.DefaultTransport.(*http.Transport).Clone()
	client := &http.Client{Transport: transport, Timeout: time.Second}
	t.Cleanup(func() {
		_ = command.Process.Signal(syscall.SIGTERM)
		select {
		case <-done:
			if waitError != nil {
				t.Errorf("owned gateway artifact did not exit cleanly after SIGTERM: %v", waitError)
			}
		case <-time.After(12 * time.Second):
			_ = command.Process.Kill()
			<-done
			t.Error("owned gateway artifact exceeded its shutdown budget")
		}
		transport.CloseIdleConnections()
		_ = output.Close()
		listener, err := net.Listen("tcp", address)
		if err != nil {
			t.Errorf("owned gateway listener survived its child join: %v", err)
		} else if err = listener.Close(); err != nil {
			t.Errorf("close rebound gateway fixture listener: %v", err)
		}
		if t.Failed() {
			log, _ := os.ReadFile(logPath)
			t.Logf("owned gateway artifact output:\n%s", log)
		}
	})
	assertRunning := func() {
		select {
		case <-done:
			t.Fatalf("owned gateway artifact exited before readiness: %v", waitError)
		default:
		}
	}
	origin := "http://" + address
	deadline := time.Now().Add(5 * time.Second)
	for {
		assertRunning()
		response, err := client.Get(origin + cfg.DevHealthPath)
		ready := false
		if err == nil {
			var health struct {
				Status  string `json:"status"`
				Service string `json:"service"`
			}
			decodeError := json.NewDecoder(response.Body).Decode(&health)
			_ = response.Body.Close()
			ready = decodeError == nil && response.StatusCode == http.StatusOK && health.Status == "healthy" && health.Service == "eventbus-gateway"
		}
		assertRunning()
		if ready {
			client.Timeout = 10 * time.Second
			return origin, client
		}
		if time.Now().After(deadline) {
			t.Fatal("owned gateway artifact did not become ready")
		}
		select {
		case <-done:
			t.Fatalf("owned gateway artifact exited before readiness: %v", waitError)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// This test signs actual HTTP ingress with the maintained AWS SDK. The existing
// registered application runs unchanged in its own native provided-runtime
// process; no event or authenticated identity is submitted directly to Lambda.
func TestNativeGatewaySDKVerifiedIAMIdentityAndEncodedComponents(t *testing.T) {
	const region, ownerAccount, callerAccount = "eu-west-1", "111111111111", "222222222222"
	const principal = "arn:aws:iam::222222222222:user/service/native"
	const userID = "AIDAOWNEDNATIVESERVICE"
	const originalQuery = "repeat=two&repeat=one&plus=a%2Bb&literal=%252F&space=a%20b"
	credentials := aws.Credentials{AccessKeyID: "AKIAIOSFODNN7EXAMPLE", SecretAccessKey: "owned-native-static-secret"}
	temporary := aws.Credentials{AccessKeyID: "ASIAIOSFODNN7EXAMPLE", SecretAccessKey: "owned-native-session-secret", SessionToken: "owned-session/token+value=="}
	directory := t.TempDir()
	executable, err := os.Executable()
	require.NoError(t, err)
	provided := lambdaservice.Function{Runtime: "provided", Command: []string{executable, "-test.run=^TestNativeGatewayProvidedRuntime$"},
		Timeout: 5 * time.Second, Environment: map[string]string{"EVENTBUS_NATIVE_GATEWAY_PROVIDED": "1"}}
	diagnosticPath := filepath.Join(directory, "invocations.jsonl")
	functions, err := lambdaservice.NewService(&lambdaservice.Config{
		Functions:      map[string]lambdaservice.Function{"iam-app:live": provided, "iam-request": provided},
		DevDiagnostics: &lambdaservice.DevDiagnosticsConfig{LogPath: diagnosticPath},
	}, directory)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, functions.Close(ctx), "native runtime owner must join its actual workers")
	})
	native := httptest.NewServer(server.New(server.Services{Lambda: functions}))
	t.Cleanup(native.Close)
	invocations := func(t *testing.T) map[string]int {
		t.Helper()
		counts := map[string]int{}
		data, err := os.ReadFile(diagnosticPath)
		if errors.Is(err, os.ErrNotExist) {
			return counts
		}
		require.NoError(t, err)
		lines := bytes.Split(data, []byte{'\n'})
		for _, line := range lines[:len(lines)-1] {
			var record struct {
				RequestID          string `json:"request_id"`
				FunctionName       string `json:"function_name"`
				State              string `json:"state"`
				OwnershipConfirmed bool   `json:"ownership_confirmed"`
			}
			require.NoError(t, json.Unmarshal(line, &record))
			require.NotEmpty(t, record.RequestID)
			require.Equal(t, "succeeded", record.State)
			require.True(t, record.OwnershipConfirmed)
			counts[record.FunctionName]++
		}
		return counts
	}
	for _, version := range []string{"2.0", "1.0"} {
		t.Run(version, func(t *testing.T) {
			zero := 0
			integration := gateway.IntegrationConfig{Type: "AWS_PROXY", PayloadFormatVersion: version,
				InvokeURL: native.URL + "/2015-03-31/functions/iam-app/invocations?Qualifier=live", Timeout: 5 * time.Second}
			cfg := gateway.Config{Region: region, AccountID: ownerAccount, APIID: "owned-iam", Stage: "$default", BasePath: "/edge/v1",
				// Verification must precede removal and generated correlation IDs.
				RemoveHeaders: []string{"X-Signed-Removed", "X-Amz-Date", "X-Amz-Security-Token"},
				IAMCredentials: map[string]gateway.DevIAMCredential{
					credentials.AccessKeyID: {SecretAccessKey: credentials.SecretAccessKey, PrincipalARN: principal, AccountID: callerAccount, UserID: userID, CallerID: userID},
					temporary.AccessKeyID:   {SecretAccessKey: temporary.SecretAccessKey, SessionToken: temporary.SessionToken, PrincipalARN: principal, AccountID: callerAccount, UserID: userID, CallerID: userID},
				},
				Authorizers: map[string]gateway.AuthorizerConfig{"request": {Type: "REQUEST", PayloadFormatVersion: "2.0", TTL: &zero,
					InvokeURL: native.URL + "/2015-03-31/functions/iam-request/invocations", IdentitySources: []string{"$request.header.Authorization"}, Timeout: 5 * time.Second}},
				Routes: []gateway.RouteConfig{
					{Path: "/anonymous", Method: "POST", AuthorizationType: "NONE", Integration: integration},
					{Path: "/custom", Method: "POST", AuthorizationType: "CUSTOM", Authorizer: "request", Integration: integration},
					{Path: "/items/Type/example/history", Method: "POST", AuthorizationType: "CUSTOM", Authorizer: "request", Integration: integration},
					{Path: "/parameters/{id}", Method: "POST", AuthorizationType: "AWS_IAM", Integration: integration},
					{Path: "/{proxy+}", Method: "ANY", RouteKey: "$default", AuthorizationType: "AWS_IAM", Integration: integration},
				}}
			origin, client := nativeIAMGateway(t, cfg, false)
			body := []byte{0, 0xff, 'b', 'o', 'd', 'y', '\n'}
			request := func(t *testing.T, path string, payload []byte) *http.Request {
				t.Helper()
				r, err := http.NewRequest(http.MethodPost, origin+path+"?"+originalQuery, bytes.NewReader(payload))
				require.NoError(t, err)
				r.Header.Set("Content-Type", "application/octet-stream")
				r.Header.Set("X-Signed-Removed", "original-signed-value")
				r.Header.Set("X-Request-Id", "original-client-request-id")
				r.Header.Set("X-Correlation-Id", "original-client-correlation")
				r.Header.Set("X-User-Arn", "arn:aws:iam::999999999999:user/spoof")
				r.Header.Set("X-Amzn-User-Arn", "arn:aws:iam::999999999999:user/spoof")
				return r
			}
			sign := func(t *testing.T, r *http.Request, payload []byte, selected aws.Credentials, stamp time.Time) {
				t.Helper()
				hash := sha256.Sum256(payload)
				rawQuery := r.URL.RawQuery
				require.NoError(t, v4.NewSigner().SignHTTP(t.Context(), selected, r, hex.EncodeToString(hash[:]), "execute-api", region, stamp))
				// SigV4 sorts query pairs; their wire order remains application data.
				// Restore the equivalent original spelling after SDK canonicalization.
				r.URL.RawQuery = rawQuery
			}
			send := func(t *testing.T, r *http.Request, status int) map[string]any {
				t.Helper()
				response, err := client.Do(r)
				require.NoError(t, err)
				defer response.Body.Close()
				data, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				require.Equal(t, status, response.StatusCode, string(data))
				if status != http.StatusOK {
					return nil
				}
				var event map[string]any
				require.NoError(t, json.Unmarshal(data, &event), "real native app must receive and echo a valid integration event")
				require.Equal(t, version, event["version"])
				require.Equal(t, response.Header.Get("X-Request-Id"), event["requestContext"].(map[string]any)["requestId"])
				return event
			}
			assertIAM := func(t *testing.T, event map[string]any, access string) {
				t.Helper()
				context := event["requestContext"].(map[string]any)
				require.Equal(t, ownerAccount, context["accountId"], "API owner account must remain distinct from verified caller")
				identity := context["identity"]
				if version == "2.0" {
					identity = context["authorizer"].(map[string]any)["iam"]
				}
				trusted := identity.(map[string]any)
				require.Equal(t, access, trusted["accessKey"])
				require.Equal(t, callerAccount, trusted["accountId"])
				require.Equal(t, principal, trusted["userArn"])
				if version == "2.0" {
					require.Equal(t, userID, trusted["userId"])
					require.Equal(t, userID, trusted["callerId"])
				} else {
					require.Equal(t, userID, trusted["user"])
					require.Equal(t, userID, trusted["caller"])
				}
			}
			assertNoIAM := func(t *testing.T, event map[string]any) {
				t.Helper()
				context := event["requestContext"].(map[string]any)
				if version == "2.0" {
					if authorizer, ok := context["authorizer"].(map[string]any); ok {
						require.NotContains(t, authorizer, "iam")
					}
				} else {
					identity := context["identity"].(map[string]any)
					for _, field := range []string{"accessKey", "accountId", "userArn", "caller", "user"} {
						require.Empty(t, identity[field], "route must not invent verified IAM field %s", field)
					}
				}
			}
			paths := []string{"/items/Type/example%2Fhistory"}
			if version == "2.0" {
				paths = append(paths, "/items/Type/literal%252Fhistory", "/items/Type/space%20value", "/items/Type/%E6%9D%B1%E4%BA%AC", "/items/Type/back%5Cslash")
			}
			for _, path := range paths {
				t.Run(path, func(t *testing.T) {
					r := request(t, "/edge/v1"+path, body)
					sign(t, r, body, credentials, time.Now())
					event := send(t, r, http.StatusOK)
					assertIAM(t, event, credentials.AccessKeyID)
					require.Equal(t, base64.StdEncoding.EncodeToString(body), event["body"])
					require.Equal(t, true, event["isBase64Encoded"])
					headers := event["headers"].(map[string]any)
					for _, removed := range []string{"x-signed-removed", "x-amz-date", "x-amz-security-token"} {
						require.NotContains(t, headers, removed, "signature must be verified before configured header removal")
					}
					require.NotEqual(t, "original-client-request-id", headers["x-request-id"])
					require.NotEqual(t, "original-client-correlation", headers["x-correlation-id"])
					if version == "2.0" {
						require.Equal(t, path, event["rawPath"])
						require.Equal(t, "$default", event["routeKey"], "encoded separator cannot borrow neighboring REQUEST route permissions")
						require.Equal(t, originalQuery, event["rawQueryString"])
						require.Equal(t, "two,one", event["queryStringParameters"].(map[string]any)["repeat"])
					} else {
						decoded, err := url.PathUnescape("/edge/v1" + path)
						require.NoError(t, err)
						require.Equal(t, decoded, event["path"])
						require.Equal(t, []any{"two", "one"}, event["multiValueQueryStringParameters"].(map[string]any)["repeat"])
					}
				})
			}
			t.Run("encoded-parameter-and-base", func(t *testing.T) {
				r := request(t, "/%65dge/v1/parameters/example%2Fhistory", body)
				sign(t, r, body, credentials, time.Now())
				event := send(t, r, http.StatusOK)
				assertIAM(t, event, credentials.AccessKeyID)
				require.Equal(t, "example/history", event["pathParameters"].(map[string]any)["id"])
				if version == "2.0" {
					require.Equal(t, "/parameters/example%2Fhistory", event["rawPath"])
				}
			})
			t.Run("verified-session-and-spoofed-body", func(t *testing.T) {
				payload := []byte(`{"requestContext":{"authorizer":{"iam":{"userArn":"arn:aws:iam::999999999999:user/spoof"}}}}`)
				r := request(t, "/edge/v1/service", payload)
				r.Header.Set("Content-Type", "application/json")
				sign(t, r, payload, temporary, time.Now())
				event := send(t, r, http.StatusOK)
				assertIAM(t, event, temporary.AccessKeyID)
				require.Equal(t, string(payload), event["body"])
			})
			for _, scenario := range []struct {
				name   string
				mutate func(*http.Request)
			}{
				{"unsigned-spoof", func(r *http.Request) { r.Header.Del("Authorization") }},
				{"bearer-is-not-IAM", func(r *http.Request) { r.Header.Set("Authorization", "Bearer owned") }},
				{"query-tamper", func(r *http.Request) { r.URL.RawQuery += "&changed=value" }},
				{"body-tamper", func(r *http.Request) {
					changed := bytes.Clone(body)
					changed[0] = 1
					r.Body = io.NopCloser(bytes.NewReader(changed))
				}},
				{"path-tamper", func(r *http.Request) {
					r.URL.Path = "/edge/v1/items/Type/changed/history"
					r.URL.RawPath = "/edge/v1/items/Type/changed%2Fhistory"
				}},
				{"host-tamper", func(r *http.Request) { r.Host = "different.example.invalid" }},
				{"method-tamper", func(r *http.Request) { r.Method = http.MethodDelete }},
				{"signed-header-tamper", func(r *http.Request) { r.Header.Set("X-Signed-Removed", "tampered-value") }},
				{"unknown-key", func(r *http.Request) {
					r.Header.Set("Authorization", strings.Replace(r.Header.Get("Authorization"), credentials.AccessKeyID, "AKIAUNKNOWN7EXAMPLE", 1))
				}},
			} {
				t.Run(scenario.name, func(t *testing.T) {
					r := request(t, "/edge/v1/items/Type/example%2Fhistory", body)
					sign(t, r, body, credentials, time.Now())
					scenario.mutate(r)
					before := invocations(t)
					send(t, r, http.StatusForbidden)
					require.Equal(t, before, invocations(t), "invalid IAM request must not launch an application or authorizer")
				})
			}
			for _, skew := range []time.Duration{-10 * time.Minute, 10 * time.Minute} {
				t.Run("date-skew-"+skew.String(), func(t *testing.T) {
					r := request(t, "/edge/v1/service", body)
					sign(t, r, body, credentials, time.Now().Add(skew))
					before := invocations(t)
					send(t, r, http.StatusForbidden)
					require.Equal(t, before, invocations(t))
				})
			}
			t.Run("missing-session-token", func(t *testing.T) {
				r := request(t, "/edge/v1/service", body)
				sign(t, r, body, temporary, time.Now())
				r.Header.Del("X-Amz-Security-Token")
				before := invocations(t)
				send(t, r, http.StatusForbidden)
				require.Equal(t, before, invocations(t))
			})
			t.Run("REQUEST-and-anonymous-independent", func(t *testing.T) {
				for _, signed := range []bool{false, true} {
					r := request(t, "/edge/v1/anonymous", body)
					if signed {
						sign(t, r, body, credentials, time.Now())
					}
					assertNoIAM(t, send(t, r, http.StatusOK))
				}
				r := request(t, "/edge/v1/custom", body)
				r.Header.Set("Authorization", "Bearer owned")
				event := send(t, r, http.StatusOK)
				assertNoIAM(t, event)
				authorizer := event["requestContext"].(map[string]any)["authorizer"].(map[string]any)
				if version == "2.0" {
					authorizer = authorizer["lambda"].(map[string]any)
				}
				require.Equal(t, "native-user", authorizer["principalId"])
				r = request(t, "/edge/v1/custom", body)
				sign(t, r, body, credentials, time.Now())
				before := invocations(t)
				send(t, r, http.StatusForbidden)
				after := invocations(t)
				require.Equal(t, before["iam-app:live"], after["iam-app:live"], "IAM credentials cannot bypass a REQUEST route")
				require.Equal(t, before["iam-request"]+1, after["iam-request"])
			})
			t.Run("NoAuth-does-not-invent-identity", func(t *testing.T) {
				bypassOrigin, _ := nativeIAMGateway(t, cfg, true)
				r := request(t, "/edge/v1/service", body)
				target, err := url.Parse(bypassOrigin)
				require.NoError(t, err)
				r.URL.Host, r.Host = target.Host, target.Host
				assertNoIAM(t, send(t, r, http.StatusOK))
			})
		})
	}
}
