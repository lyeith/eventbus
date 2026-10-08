package app

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/lyeith/eventbus/internal/devquiescence"
	"github.com/lyeith/eventbus/internal/gateway"
	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestDevGatewayContinuationProvidedProcess(t *testing.T) {
	if os.Getenv("EVENTBUS_DEV_GATEWAY_CONTINUATION_PROCESS") != "1" {
		return
	}
	if err := devGatewayContinuationInvocation(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	os.Exit(0)
}

// This external provided-runtime fixture owns application policy, never retained
// admission. Its REQUEST authorizer validates an owned RS256 fixture token; cold
// Cognito-issued JWT/JWKS provenance is covered by the separate SDK acceptance.
func devGatewayContinuationInvocation() error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 15 * time.Second}
	defer client.CloseIdleConnections()
	runtime := "http://" + os.Getenv("AWS_LAMBDA_RUNTIME_API") + "/2018-06-01/runtime/invocation/"
	next, err := http.NewRequestWithContext(ctx, http.MethodGet, runtime+"next", nil)
	if err != nil {
		return err
	}
	response, err := client.Do(next)
	if err != nil {
		return err
	}
	payload, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	requestID := response.Header.Get("Lambda-Runtime-Aws-Request-Id")
	if err != nil || response.StatusCode != 200 || requestID == "" {
		return errors.New("invalid native Runtime API admission")
	}
	var event map[string]any
	if err := json.Unmarshal(payload, &event); err != nil {
		return err
	}
	root := os.Getenv("DEV_GATEWAY_ROOT")
	var result any
	switch os.Getenv("DEV_GATEWAY_ROLE") {
	case "cleanup":
		token, err := os.ReadFile(filepath.Join(root, "token"))
		if err != nil {
			return err
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, os.Getenv("DEV_GATEWAY_URL")+"/private/cleanup", bytes.NewBufferString(`{"suite":"configured"}`))
		if err != nil {
			return err
		}
		request.Header.Set("Authorization", "Bearer "+string(token))
		request.Header.Set("Content-Type", "application/json")
		downstream, err := client.Do(request)
		if err != nil {
			return err
		}
		body, err := io.ReadAll(downstream.Body)
		_ = downstream.Body.Close()
		if err != nil || downstream.StatusCode != 200 || string(body) != `{"cleaned":true}` {
			return fmt.Errorf("authenticated cleanup returned %d: %s", downstream.StatusCode, body)
		}
		result = map[string]bool{"completed": true}
	case "authorizer":
		if event["type"] != "REQUEST" || event["version"] != "2.0" {
			return errors.New("expected native REQUEST authorizer event")
		}
		encodedKey, err := os.ReadFile(filepath.Join(root, "public.pem"))
		if err != nil {
			return err
		}
		block, _ := pem.Decode(encodedKey)
		if block == nil {
			return errors.New("missing owned public key")
		}
		key, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return err
		}
		header, _ := event["headers"].(map[string]any)["authorization"].(string)
		claims := jwt.MapClaims{}
		token, parseErr := jwt.ParseWithClaims(strings.TrimPrefix(header, "Bearer "), claims, func(*jwt.Token) (any, error) { return key, nil },
			jwt.WithValidMethods([]string{"RS256"}), jwt.WithIssuer("owned-gateway-fixture"),
			jwt.WithAudience("owned-cleanup-app"), jwt.WithExpirationRequired())
		allowed := parseErr == nil && token != nil && token.Valid && strings.HasPrefix(header, "Bearer ") &&
			claims["sub"] == "owned-cleanup-user" && claims["token_use"] == "id"
		effect := "Deny"
		if allowed {
			effect = "Allow"
		}
		result = map[string]any{"principalId": "owned-cleanup-user", "policyDocument": map[string]any{
			"Version": "2012-10-17", "Statement": []any{map[string]any{"Action": "execute-api:Invoke", "Effect": effect, "Resource": event["routeArn"]}}},
			"context": map[string]any{"authenticated": allowed}}
	case "integration":
		trusted := event["requestContext"].(map[string]any)["authorizer"].(map[string]any)["lambda"].(map[string]any)
		if trusted["authenticated"] != true || event["body"] != `{"suite":"configured"}` || event["rawPath"] != "/private/cleanup" {
			return errors.New("native authenticated cleanup payload changed")
		}
		if err := sqsManualSettlementSync(filepath.Join(root, "business-effect"), []byte("cleaned")); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(root, "integration-started"), []byte(requestID), 0600); err != nil {
			return err
		}
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			if _, err := os.Stat(filepath.Join(root, "release-integration")); err == nil {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
			}
		}
		result = map[string]any{"statusCode": 200, "headers": map[string]string{"Content-Type": "application/json"}, "body": `{"cleaned":true}`}
	default:
		return errors.New("unknown external fixture role")
	}
	body, err := json.Marshal(result)
	if err != nil {
		return err
	}
	reply, err := http.NewRequestWithContext(ctx, http.MethodPost, runtime+requestID+"/response", bytes.NewReader(body))
	if err != nil {
		return err
	}
	ack, err := client.Do(reply)
	if err != nil {
		return err
	}
	_ = ack.Body.Close()
	if ack.StatusCode != http.StatusAccepted {
		return fmt.Errorf("native runtime response returned %d", ack.StatusCode)
	}
	return nil
}

func TestProductionDeclaredCleanupCallsAuthenticatedGatewayContinuation(t *testing.T) {
	cfg := runTestConfig(t)
	cfg.port = retainedTestPort(t)
	for cfg.retainedCallbackPort == 0 || cfg.retainedCallbackPort == cfg.port {
		cfg.retainedCallbackPort = retainedTestPort(t)
	}
	cfg.lambdaFunctions = filepath.Join(cfg.workDir, "functions.yaml")
	flags := flag.NewFlagSet("declared-gateway-cleanup", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	parsed, err := readConfig(flags, []string{"--port", strconv.Itoa(cfg.port), "--retained-owner-callback-port", strconv.Itoa(cfg.retainedCallbackPort),
		"--lambda-functions", cfg.lambdaFunctions, "--retained-owner-cleanup-functions", "declared-cleanup:live"})
	require.NoError(t, err)
	cfg.retainedCleanupFunctions = parsed.retainedCleanupFunctions
	source, callback := fmt.Sprintf("http://127.0.0.1:%d", cfg.port), fmt.Sprintf("http://127.0.0.1:%d", cfg.retainedCallbackPort)
	private := httptest.NewUnstartedServer(nil)
	privateURL := "http://" + private.Listener.Addr().String()
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	require.NoError(t, err)
	publicDER, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(cfg.workDir, "public.pem"), pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: publicDER}), 0600))
	token, err := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{"iss": "owned-gateway-fixture", "aud": "owned-cleanup-app",
		"sub": "owned-cleanup-user", "token_use": "id", "iat": time.Now().Unix(), "exp": time.Now().Add(time.Hour).Unix()}).SignedString(key)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(cfg.workDir, "token"), []byte(token), 0600))
	executable, err := os.Executable()
	require.NoError(t, err)
	registered := map[string]lambdaservice.Function{}
	for name, role := range map[string]string{"declared-cleanup:live": "cleanup", "declared-authorizer:live": "authorizer", "declared-integration:live": "integration"} {
		registered[name] = lambdaservice.Function{Runtime: "provided", Command: []string{executable, "-test.run=^TestDevGatewayContinuationProvidedProcess$"},
			Timeout: 20 * time.Second, Environment: map[string]string{"EVENTBUS_DEV_GATEWAY_CONTINUATION_PROCESS": "1",
				"DEV_GATEWAY_ROLE": role, "DEV_GATEWAY_ROOT": cfg.workDir, "DEV_GATEWAY_URL": privateURL}}
	}
	recipe, err := yaml.Marshal(lambdaservice.Config{Functions: registered})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(cfg.lambdaFunctions, recipe, 0600))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg) }()
	client := &http.Client{Timeout: 30 * time.Second}
	defer client.CloseIdleConnections()
	var edge *gateway.Gateway
	t.Cleanup(func() {
		_ = os.WriteFile(filepath.Join(cfg.workDir, "release-integration"), nil, 0600)
		if edge != nil {
			if err := edge.Close(); err != nil {
				t.Errorf("private gateway cleanup: %v", err)
			}
		}
		private.Close()
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(35 * time.Second):
			t.Error("production declared cleanup did not join")
		}
	})
	require.Eventually(t, func() bool {
		response, err := client.Get(source + "/health")
		if err != nil {
			return false
		}
		_ = response.Body.Close()
		return response.StatusCode == 200
	}, 5*time.Second, 10*time.Millisecond)
	zero := 0
	edge, err = gateway.New(gateway.Config{Stage: "$default", RetainedOwnerControlURL: source + devquiescence.ControlPath,
		RetainedOwnerContinuationPort: private.Listener.Addr().(*net.TCPAddr).Port,
		Authorizers: map[string]gateway.AuthorizerConfig{"auth": {Type: "REQUEST", PayloadFormatVersion: "2.0",
			InvokeURL: callback + "/2015-03-31/functions/declared-authorizer:live/invocations", IdentitySources: []string{"$request.header.Authorization"}, TTL: &zero}},
		Routes: []gateway.RouteConfig{{Path: "/private/cleanup", Method: "POST", Authorizer: "auth", Integration: gateway.IntegrationConfig{
			Type: "AWS_PROXY", PayloadFormatVersion: "2.0", InvokeURL: callback + "/2015-03-31/functions/declared-integration:live/invocations", Timeout: 15 * time.Second}}},
	}, gateway.Options{Logger: zerolog.Nop()})
	require.NoError(t, err)
	private.Config.Handler = edge.RetainedContinuationHandler()
	private.Start()
	held := retainedControl(t, client, source, "/quiesce", `{"timeout_ms":10000}`, 200)
	require.True(t, held.FixtureSafe)
	cleanup := make(chan retainedResponse, 1)
	go func() {
		cleanup <- retainedRequest(client, http.MethodPost, callback+"/2015-03-31/functions/declared-cleanup:live/invocations", `{"suite":"configured"}`, "application/json")
	}()
	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(cfg.workDir, "integration-started"))
		return err == nil
	}, 8*time.Second, 10*time.Millisecond, "actual app declaration admits cleanup and native authenticated continuation")
	effect, err := os.ReadFile(filepath.Join(cfg.workDir, "business-effect"))
	require.NoError(t, err)
	require.Equal(t, "cleaned", string(effect))
	retainedHTTP(t, client, http.MethodPost, source+"/2015-03-31/functions/declared-cleanup:live/invocations", `{"suite":"forbidden-root"}`, "application/json", 503)
	barrier := make(chan retainedResponse, 1)
	go func() {
		barrier <- retainedRequest(client, http.MethodPost, source+devquiescence.ControlPath+"/quiesce", `{"timeout_ms":10000}`, "application/json")
	}()
	live := retainedControl(t, client, source, "", "", 200)
	require.Equal(t, devquiescence.Draining, live.State)
	require.Positive(t, live.WorkCount)
	require.False(t, live.FixtureSafe, "persisted business effects do not certify native cleanup completion")
	select {
	case response := <-barrier:
		t.Fatalf("persisted effect preceded actual native cleanup join: %+v", response)
	default:
	}
	require.NoError(t, os.WriteFile(filepath.Join(cfg.workDir, "release-integration"), nil, 0600))
	select {
	case response := <-cleanup:
		require.NoError(t, response.err)
		require.Equal(t, 200, response.status, string(response.body))
		require.JSONEq(t, `{"completed":true}`, string(response.body))
	case <-time.After(15 * time.Second):
		t.Fatal("configured native cleanup failed to join its authenticated gateway call")
	}
	joined := retainedFullStackBarrier(t, barrier)
	require.Equal(t, held.Generation, joined.Generation)
}
