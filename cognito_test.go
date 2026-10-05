package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newCognitoTestStore returns a CognitoStore backed by a fresh per-test
// SQLite file under t.TempDir, so tests are fully isolated.
func newCognitoTestStore(t *testing.T) (*CognitoStore, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cognito-test.db")
	store, err := OpenCognitoStore(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store, path
}

// newCognitoTestServer returns a real *Server wired with a Cognito store and
// fronted by httptest. Convenient for end-to-end JSON-1.1 dispatch tests.
//
// Default TTLs (1h access, 24h refresh) match what the eventbus binary uses
// in main.go. Tests that need a non-default TTL go through
// newCognitoTestServerWithTTL below.
func newCognitoTestServer(t *testing.T) (*Server, *httptest.Server, *CognitoStore) {
	return newCognitoTestServerWithTTL(t, time.Hour, 24*time.Hour)
}

// newCognitoTestServerWithTTL is the explicit-TTL variant used by tests that
// assert on `exp - iat`. Pass zero/negative durations to fall back to the
// SetCognito defaults.
func newCognitoTestServerWithTTL(t *testing.T, accessTTL, refreshTTL time.Duration) (*Server, *httptest.Server, *CognitoStore) {
	t.Helper()
	store, _ := newCognitoTestStore(t)
	broker := NewBroker("us-east-1", "000000000000", 0)
	fm := NewFirehoseManager("us-east-1", "000000000000", "http://localhost:9000", "test", "test")
	server := NewServer(broker, fm, NewSSMStore(), NewSecretsStore("us-east-1", "000000000000"))
	server.SetCognito(store, "http://localhost:4100", "", accessTTL, refreshTTL)
	ts := httptest.NewServer(server)
	t.Cleanup(ts.Close)
	return server, ts, store
}

func TestIsCognitoTarget(t *testing.T) {
	cases := []struct {
		target string
		want   bool
	}{
		{"AWSCognitoIdentityProviderService.InitiateAuth", true},
		{"AWSCognitoIdentityProviderService.AdminCreateUser", true},
		{"Firehose_20150804.PutRecord", false},
		{"AmazonSQS.SendMessage", false},
		{"", false},
		{"AWSCognitoIdentityProviderService", false},
	}
	for _, c := range cases {
		t.Run(c.target, func(t *testing.T) {
			assert.Equal(t, c.want, isCognitoTarget(c.target))
		})
	}
}

func TestExtractCognitoAction(t *testing.T) {
	assert.Equal(t, "InitiateAuth", extractCognitoAction("AWSCognitoIdentityProviderService.InitiateAuth"))
	assert.Equal(t, "", extractCognitoAction("Firehose_20150804.PutRecord"))
	assert.Equal(t, "", extractCognitoAction(""))
}

// As of GO-COGNITO-5, every entry in CognitoActions is a real handler:
//   - GetUser / AdminCreateUser / AdminDeleteUser  (GO-COGNITO-2)
//   - InitiateAuth                                  (GO-COGNITO-3)
//   - RespondToAuthChallenge / AdminInitiateAuth    (GO-COGNITO-5)
//
// The placeholder branch in handleCognitoJSON only fires for actions
// listed in CognitoActions but missing from the dispatch switch. Once
// every action is wired, the only remaining placeholder path is unknown
// actions — covered by TestCognitoDispatch_UnknownAction below.

func TestCognitoDispatch_UnknownAction(t *testing.T) {
	_, ts, _ := newCognitoTestServer(t)

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/", bytes.NewBufferString(`{}`))
	require.NoError(t, err)
	req.Header.Set("X-Amz-Target", "AWSCognitoIdentityProviderService.MadeUpOp")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	var env map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&env))
	assert.Equal(t, "InvalidAction", env["__type"])
}

// TestCognitoErrorEnvelopeShape pins down the exact wire shape to prevent
// regressions: lowercase `message`, content type application/x-amz-json-1.1.
// The provider-side error mappers in cognito.py / cognito.go grep on these
// exact characters.
func TestCognitoErrorEnvelopeShape(t *testing.T) {
	rec := httptest.NewRecorder()
	cognitoJSONError(rec, http.StatusUnauthorized, "NotAuthorizedException", "bad creds")

	resp := rec.Result()
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	assert.Equal(t, "application/x-amz-json-1.1", resp.Header.Get("Content-Type"))

	body, _ := io.ReadAll(resp.Body)
	assert.JSONEq(t, `{"__type":"NotAuthorizedException","message":"bad creds"}`, string(body))
}

func TestCognitoStore_PoolUserRoundTrip(t *testing.T) {
	store, _ := newCognitoTestStore(t)
	ctx := t.Context()

	require.NoError(t, store.UpsertPool(ctx, "p1", "us-east-1"))
	exists, err := store.PoolExists(ctx, "p1")
	require.NoError(t, err)
	assert.True(t, exists)

	require.NoError(t, store.UpsertClient(ctx, "c1", "p1", ""))

	sub, err := store.UpsertUser(ctx, "p1", "alice@example.com", "hash1", false)
	require.NoError(t, err)
	assert.NotEmpty(t, sub)

	require.NoError(t, store.SetUserAttribute(ctx, sub, "email", "alice@example.com"))
	require.NoError(t, store.SetUserAttribute(ctx, sub, "email_verified", "true"))
	attrs, err := store.LoadUserAttributes(ctx, sub)
	require.NoError(t, err)
	assert.Equal(t, "alice@example.com", attrs["email"])
	assert.Equal(t, "true", attrs["email_verified"])

	// Idempotency: re-upsert with new password keeps the same sub.
	sub2, err := store.UpsertUser(ctx, "p1", "alice@example.com", "hash2", true)
	require.NoError(t, err)
	assert.Equal(t, sub, sub2)
}

// TestHealth_PublishesCognitoURLs is the operator-visible contract: after
// SetCognito, the /health JSON includes the configured issuer + JWKS bases.
func TestHealth_PublishesCognitoURLs(t *testing.T) {
	_, ts, _ := newCognitoTestServer(t)
	resp, err := http.Get(ts.URL + "/health")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	var body map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "healthy", body["status"])
	assert.Equal(t, "http://localhost:4100", body["cognito_issuer"])
	assert.Equal(t, "http://localhost:4100", body["cognito_jwks_url"])
}

// TestHealth_OmitsCognitoURLsWhenStoreUnset verifies the legacy /health shape
// (pre-Cognito) survives if no SetCognito call was made — keeps any older
// callers that probe /health from getting noise.
func TestHealth_OmitsCognitoURLsWhenStoreUnset(t *testing.T) {
	broker := NewBroker("us-east-1", "000000000000", 0)
	fm := NewFirehoseManager("us-east-1", "000000000000", "http://localhost:9000", "test", "test")
	server := NewServer(broker, fm, NewSSMStore(), NewSecretsStore("us-east-1", "000000000000"))
	ts := httptest.NewServer(server)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/health")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	var body map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	_, hasIssuer := body["cognito_issuer"]
	_, hasJWKS := body["cognito_jwks_url"]
	assert.False(t, hasIssuer)
	assert.False(t, hasJWKS)
}

// helpers ----------------------------------------------------------------

// signTestAccessToken mints an RS256 access token signed with the pool's
// real persisted signing key. Reused across phases — phase 3 will share
// this once InitiateAuth lands; until then any test that needs a
// well-formed token (GetUser, refresh, etc.) goes through this helper so
// claim shape stays consistent.
//
// `iss` MUST be `<issuerBase>/<poolID>` to match what VerifyAccessToken
// expects; pass an explicit issuerBase so each test that wires its own
// httptest server can pin the URL it actually serves on.
func signTestAccessToken(t *testing.T, store *CognitoStore, issuerBase, poolID, sub, email string, ttl time.Duration) string {
	t.Helper()
	signing, err := store.EnsureSigningKey(context.Background(), poolID)
	require.NoError(t, err)

	now := time.Now()
	claims := jwt.MapClaims{
		"sub":       sub,
		"email":     email,
		"iss":       issuerBase + "/" + poolID,
		"aud":       "test-client",
		"iat":       now.Unix(),
		"exp":       now.Add(ttl).Unix(),
		"token_use": "access",
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = signing.Kid
	signed, err := tok.SignedString(signing.Private)
	require.NoError(t, err)
	return signed
}
