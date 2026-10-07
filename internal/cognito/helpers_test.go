package cognito

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
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

// newCognitoTestServer runs only the Cognito handler against owned SQLite state.
func newCognitoTestServer(t *testing.T) (*Handler, *httptest.Server, *CognitoStore) {
	return newCognitoTestServerWithTTL(t, time.Hour, 24*time.Hour)
}

func newCognitoTestServerWithTTL(t *testing.T, accessTTL, refreshTTL time.Duration) (*Handler, *httptest.Server, *CognitoStore) {
	t.Helper()
	store, _ := newCognitoTestStore(t)
	handler := NewHandler(store, Options{DevProfile: DevProfileLegacyFixtures, IssuerBase: "http://localhost:4100", AccessTokenTTL: accessTTL, RefreshTokenTTL: refreshTTL})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/.well-known/jwks.json") {
			handler.ServeJWKS(w, r)
			return
		}
		handler.ServeAction(w, r, Action(r.Header.Get("X-Amz-Target")))
	}))
	t.Cleanup(server.Close)
	return handler, server, store
}

// signTestAccessToken uses the pool's real persisted signing key.
// `iss` MUST be `<issuerBase>/<poolID>` to match what VerifyAccessToken
// expects; pass an explicit issuerBase so each test that wires its own
// httptest server can pin the URL it actually serves on.
func signTestAccessToken(t *testing.T, store *CognitoStore, issuerBase, poolID, sub, email string, ttl time.Duration) string {
	t.Helper()
	require.NoError(t, store.UpsertPool(t.Context(), poolID, "us-east-1"))
	signing, err := store.EnsureSigningKey(context.Background(), poolID)
	require.NoError(t, err)

	clientID := "test-client-" + poolID
	require.NoError(t, store.UpsertClient(context.Background(), clientID, poolID, ""))
	now := store.now()
	claims := jwt.MapClaims{
		"sub":       sub,
		"email":     email,
		"iss":       issuerBase + "/" + poolID,
		"aud":       clientID,
		"client_id": clientID,
		"scope":     "aws.cognito.signin.user.admin",
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
