package gateway

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/stretchr/testify/require"
)

func iamLifecycleRequest(t *testing.T, key, secret, method, path string, body []byte, tokens ...string) *http.Request {
	t.Helper()
	request := httptest.NewRequest(method, "http://gateway.test"+path, bytes.NewReader(body))
	digest := sha256.Sum256(body)
	token := ""
	if len(tokens) != 0 {
		token = tokens[0]
	}
	require.NoError(t, v4.NewSigner().SignHTTP(t.Context(), aws.Credentials{AccessKeyID: key, SecretAccessKey: secret, SessionToken: token}, request, hex.EncodeToString(digest[:]), "execute-api", "us-east-1", iamTestTime))
	return request
}

func iamLifecycleGateway(t *testing.T, fixture DevIAMCredential) (*Gateway, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(backend.Close)
	cfg := localIAMFixtureConfig()
	cfg.APIID, cfg.Stage = "api", "prod"
	cfg.IAMCredentials = map[string]DevIAMCredential{"LOCAL_SERVICE": fixture}
	cfg.Routes[0].Path, cfg.Routes[0].Method = "/{proxy+}", "ANY"
	cfg.Routes[0].Integration.URI = backend.URL + "/{proxy}"
	edge := newTestGateway(t, cfg, Options{})
	edge.iam.now = func() time.Time { return iamTestTime }
	return edge, &calls
}

func iamLifecycleFixture() DevIAMCredential {
	return DevIAMCredential{SecretAccessKey: "fixture-secret", PrincipalARN: "arn:aws:iam::222222222222:user/service", AccountID: "222222222222", UserID: "AIDAOWNED"}
}

func serveIAMLifecycle(edge *Gateway, request *http.Request) int {
	response := httptest.NewRecorder()
	edge.ServeHTTP(response, request)
	return response.Code
}

func TestIAMNativePermissionsUseOwnerARNAndDetachedPrincipalFacts(t *testing.T) {
	fixture := iamLifecycleFixture()
	resources := []string{"arn:aws:execute-api:us-east-1:111111111111:api/prod/GET/items/${aws:PrincipalTag/item}"}
	tags := map[string]string{"item": "allowed"}
	fixture.PrincipalTags = tags
	fixture.Policy = map[string]any{"Version": "2012-10-17", "Statement": []any{map[string]any{
		"Effect": "Allow", "Action": "execute-api:Invoke", "Resource": resources,
		"Condition": map[string]any{"StringEquals": map[string]any{"aws:PrincipalAccount": "222222222222", "aws:PrincipalArn": fixture.PrincipalARN}, "Bool": map[string]any{"aws:SecureTransport": "false"}},
	}}}
	edge, calls := iamLifecycleGateway(t, fixture)
	resources[0] = "*"
	tags["item"] = "blocked"
	for _, tc := range []struct {
		method, path string
		status       int
	}{
		{"GET", "/items/allowed", 202}, {"POST", "/items/allowed", 403}, {"GET", "/items/blocked", 403},
		{"GET", "/items/allowed%2Fhidden", 403},
	} {
		require.Equal(t, tc.status, serveIAMLifecycle(edge, iamLifecycleRequest(t, "LOCAL_SERVICE", "fixture-secret", tc.method, tc.path, nil)), tc.path)
	}
	require.EqualValues(t, 1, calls.Load())
}

type iamBlockedBody struct {
	source           *bytes.Reader
	entered, release chan struct{}
	once             sync.Once
}

func (body *iamBlockedBody) Read(data []byte) (int, error) {
	body.once.Do(func() { close(body.entered) })
	<-body.release
	return body.source.Read(data)
}
func (body *iamBlockedBody) Close() error { return nil }

func TestIAMExpiryAndFixtureReplacementFenceBlockedAuthentication(t *testing.T) {
	for _, mode := range []string{"expiry", "removal", "same-key-replacement"} {
		t.Run(mode, func(t *testing.T) {
			fixture := iamLifecycleFixture()
			fixture.ExpiresAt = iamTestTime.Add(time.Second)
			edge, calls := iamLifecycleGateway(t, fixture)
			var current atomic.Int64
			current.Store(iamTestTime.UnixNano())
			edge.iam.now = func() time.Time { return time.Unix(0, current.Load()) }
			data := []byte("native body")
			request := iamLifecycleRequest(t, "LOCAL_SERVICE", "fixture-secret", "POST", "/items/allowed", data)
			body := &iamBlockedBody{source: bytes.NewReader(data), entered: make(chan struct{}), release: make(chan struct{})}
			request.Body = body
			done := make(chan int, 1)
			go func() { done <- serveIAMLifecycle(edge, request) }()
			released := false
			defer func() {
				if !released {
					close(body.release)
				}
			}()
			select {
			case <-body.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("valid signed request never reached body hashing")
			}
			switch mode {
			case "expiry":
				current.Store(fixture.ExpiresAt.UnixNano())
			case "removal":
				require.NoError(t, edge.DevReplaceIAMCredentials(nil))
			case "same-key-replacement":
				require.NoError(t, edge.DevReplaceIAMCredentials(map[string]DevIAMCredential{"LOCAL_SERVICE": fixture}))
			}
			close(body.release)
			released = true
			select {
			case status := <-done:
				require.Equal(t, 403, status)
			case <-time.After(3 * time.Second):
				t.Fatal("revoked authentication did not finish")
			}
			require.Zero(t, calls.Load(), "retired authentication cannot reach integration")
		})
	}
}

func TestIAMAtomicFixtureRotationInvalidCandidateAndExplicitDenyAll(t *testing.T) {
	fixture := iamLifecycleFixture()
	edge, calls := iamLifecycleGateway(t, fixture)
	invoke := func(key, secret string) int {
		return serveIAMLifecycle(edge, iamLifecycleRequest(t, key, secret, "GET", "/items/allowed", nil))
	}
	invalid := fixture
	invalid.Policy = map[string]any{"Statement": []any{map[string]any{"Effect": "Allow", "Action": "*", "Resource": "*", "Unknown": "must reject"}}}
	require.Error(t, edge.DevReplaceIAMCredentials(map[string]DevIAMCredential{"LOCAL_SERVICE": invalid}))
	require.Equal(t, 202, invoke("LOCAL_SERVICE", "fixture-secret"), "failed candidate preserves original grant")
	next := fixture
	next.SecretAccessKey = "replacement-secret"
	require.NoError(t, edge.DevReplaceIAMCredentials(map[string]DevIAMCredential{"LOCAL_SERVICE": fixture, "NEXT_SERVICE": next}))
	require.Equal(t, 202, invoke("NEXT_SERVICE", "replacement-secret"))
	require.Equal(t, 202, invoke("LOCAL_SERVICE", "fixture-secret"))
	require.NoError(t, edge.DevReplaceIAMCredentials(map[string]DevIAMCredential{"LOCAL_SERVICE": next}))
	require.Equal(t, 403, invoke("LOCAL_SERVICE", "fixture-secret"))
	require.Equal(t, 202, invoke("LOCAL_SERVICE", "replacement-secret"))
	denied := next
	denied.Policy = map[string]any{"Version": "2012-10-17", "Statement": []any{}}
	require.NoError(t, edge.DevReplaceIAMCredentials(map[string]DevIAMCredential{"LOCAL_SERVICE": denied}))
	require.Equal(t, 403, invoke("LOCAL_SERVICE", "replacement-secret"), "explicit empty policy is implicit deny")
	require.EqualValues(t, 4, calls.Load())
}

func TestIAMExpiredCredentialRefusesBeforeReadingBody(t *testing.T) {
	fixture := iamLifecycleFixture()
	fixture.ExpiresAt = iamTestTime
	edge, calls := iamLifecycleGateway(t, fixture)
	request := iamLifecycleRequest(t, "LOCAL_SERVICE", "fixture-secret", "POST", "/items/allowed", []byte("body"))
	body := &retainedBody{}
	request.Body = io.NopCloser(body)
	require.Equal(t, 403, serveIAMLifecycle(edge, request))
	require.Zero(t, body.reads.Load())
	require.Zero(t, calls.Load())
}

func TestIAMConcurrentFixtureRotationOwnsWholeRecords(t *testing.T) {
	fixture := iamLifecycleFixture()
	edge, _ := iamLifecycleGateway(t, fixture)
	first, err := compileDevIAMCredentials(map[string]DevIAMCredential{"LOCAL_SERVICE": fixture})
	require.NoError(t, err)
	next := fixture
	next.SecretAccessKey = "next"
	next.UserID = "NEXT"
	second, err := compileDevIAMCredentials(map[string]DevIAMCredential{"LOCAL_SERVICE": next})
	require.NoError(t, err)
	var workers sync.WaitGroup
	for range 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for range 200 {
				record, ok := edge.iam.credentials.lookup("LOCAL_SERVICE", iamTestTime)
				if !ok || !(record.secretAccessKey == "fixture-secret" && record.identity.UserID == "AIDAOWNED" || record.secretAccessKey == "next" && record.identity.UserID == "NEXT") {
					t.Error("credential generation mixed signing material and identity")
					return
				}
			}
		}()
	}
	for range 100 {
		edge.iam.credentials.replace(second)
		edge.iam.credentials.replace(first)
	}
	workers.Wait()
}

func TestIAMSessionTokenRotationRevokesOldToken(t *testing.T) {
	fixture := iamLifecycleFixture()
	fixture.SessionToken = "first-token"
	edge, calls := iamLifecycleGateway(t, fixture)
	invoke := func(token string) int {
		return serveIAMLifecycle(edge, iamLifecycleRequest(t, "LOCAL_SERVICE", "fixture-secret", "GET", "/items/allowed", nil, token))
	}
	require.Equal(t, 202, invoke("first-token"))
	fixture.SessionToken = "second-token"
	require.NoError(t, edge.DevReplaceIAMCredentials(map[string]DevIAMCredential{"LOCAL_SERVICE": fixture}))
	require.Equal(t, 403, invoke("first-token"))
	require.Equal(t, 202, invoke("second-token"))
	require.EqualValues(t, 2, calls.Load())
}
