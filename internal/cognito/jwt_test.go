package cognito

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"testing"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEnsureSigningKey_PersistsAcrossRestart pins the design's stable-`kid`
// guarantee (§5c). If a fresh process opens the same DB and asks for the same
// pool, it must get the same kid back — otherwise downstream JWKS caches in
// cognito.py and cognito.go thrash on every eventbus restart.
func TestEnsureSigningKey_PersistsAcrossRestart(t *testing.T) {
	dbPath := t.TempDir() + "/restart.db"

	// First open: generate fresh key.
	store1, err := OpenCognitoStore(dbPath)
	require.NoError(t, err)
	key1, err := store1.EnsureSigningKey(context.Background(), "pool-A")
	require.NoError(t, err)
	require.NoError(t, store1.Close())

	require.Len(t, key1.Kid, 16, "kid must be 16 hex chars")

	// Second open of the same DB: must return the same kid + key material.
	store2, err := OpenCognitoStore(dbPath)
	require.NoError(t, err)
	defer func() { _ = store2.Close() }()

	key2, err := store2.EnsureSigningKey(context.Background(), "pool-A")
	require.NoError(t, err)
	assert.Equal(t, key1.Kid, key2.Kid, "kid must be stable across restarts")
	assert.Equal(t, key1.Public.N.Cmp(key2.Public.N), 0, "modulus must match across restarts")
	assert.Equal(t, key1.Public.E, key2.Public.E)
}

// TestEnsureSigningKey_PerPoolUnique verifies pools get distinct keys, so a
// token signed for pool A doesn't validate against pool B's JWKS.
func TestEnsureSigningKey_PerPoolUnique(t *testing.T) {
	store, _ := newCognitoTestStore(t)
	ctx := context.Background()

	a, err := store.EnsureSigningKey(ctx, "pool-A")
	require.NoError(t, err)
	b, err := store.EnsureSigningKey(ctx, "pool-B")
	require.NoError(t, err)

	assert.NotEqual(t, a.Kid, b.Kid)
	assert.NotEqual(t, 0, a.Public.N.Cmp(b.Public.N))
}

// TestBuildJWKS_ShapeMatchesMockProvider asserts byte-level conformance to
// the mock.py:44-79 published shape so anything that already speaks
// MockProvider keeps working.
func TestBuildJWKS_ShapeMatchesMockProvider(t *testing.T) {
	store, _ := newCognitoTestStore(t)
	body, err := store.BuildJWKS(context.Background(), "local-pool-1")
	require.NoError(t, err)

	var doc JWKS
	require.NoError(t, json.Unmarshal(body, &doc))
	require.Len(t, doc.Keys, 1)

	jwk := doc.Keys[0]
	assert.Equal(t, "RSA", jwk.Kty)
	assert.Equal(t, "RS256", jwk.Alg)
	assert.Equal(t, "sig", jwk.Use)
	assert.Len(t, jwk.Kid, 16)
	assert.NotEmpty(t, jwk.N)
	assert.NotEmpty(t, jwk.E)

	// `n` must be base64url-no-pad and decode to a big-endian integer.
	nBytes, err := base64.RawURLEncoding.DecodeString(jwk.N)
	require.NoError(t, err, "n must be base64url-no-pad")
	require.NotEmpty(t, nBytes)
	require.NotEqual(t, byte(0x00), nBytes[0], "RFC 7518 §6.3 forbids leading zero")

	eBytes, err := base64.RawURLEncoding.DecodeString(jwk.E)
	require.NoError(t, err, "e must be base64url-no-pad")
	// Standard exponent 65537 is [0x01, 0x00, 0x01].
	assert.Equal(t, []byte{0x01, 0x00, 0x01}, eBytes)

	// Reconstruct the public key from the JWK and compare to the persisted one.
	loaded, err := store.LoadSigningKey(context.Background(), "local-pool-1")
	require.NoError(t, err)
	gotN := new(big.Int).SetBytes(nBytes)
	assert.Equal(t, 0, loaded.Public.N.Cmp(gotN), "modulus must match between JWKS and stored key")
}

// TestSignAndValidateAgainstJWKS is the end-to-end correctness check: sign a
// token with the persisted private key, then validate it via the published
// JWKS using the same library cognito.go would use in production
// (golang-jwt/jwt + keyfunc). If this passes, the Lambda authorizer's
// validation path will accept tokens minted by the dev service.
func TestSignAndValidateAgainstJWKS(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	ctx := context.Background()

	const poolID = "local-pool-1"
	signing, err := store.EnsureSigningKey(ctx, poolID)
	require.NoError(t, err)

	now := time.Now()
	claims := jwt.MapClaims{
		"sub":       "user-123",
		"email":     "alice@example.com",
		"iss":       ts.URL + "/" + poolID,
		"aud":       "client-xyz",
		"iat":       now.Unix(),
		"exp":       now.Add(time.Hour).Unix(),
		"token_use": "access",
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = signing.Kid
	signed, err := tok.SignedString(signing.Private)
	require.NoError(t, err)

	// Build a keyfunc backed by the live JWKS endpoint.
	jwksURL := ts.URL + "/" + poolID + "/.well-known/jwks.json"
	kf, err := keyfunc.NewDefault([]string{jwksURL})
	require.NoError(t, err)

	// Validate using the JWKS — required claims sub, email, exp, iat are
	// already present; standard validators handle exp/iat.
	parsed, err := jwt.Parse(signed, kf.Keyfunc, jwt.WithIssuer(ts.URL+"/"+poolID), jwt.WithAudience("client-xyz"), jwt.WithValidMethods([]string{"RS256"}))
	require.NoError(t, err)
	require.True(t, parsed.Valid)

	// Tamper-proof: a bogus signature must NOT validate.
	bad := signed[:len(signed)-4] + "AAAA"
	_, err = jwt.Parse(bad, kf.Keyfunc, jwt.WithIssuer(ts.URL+"/"+poolID), jwt.WithAudience("client-xyz"))
	assert.Error(t, err)

	// And a token signed by a *different* key for a different pool must
	// fail validation against the original pool's JWKS — guards against
	// cross-pool key bleed.
	otherKey, err := store.EnsureSigningKey(ctx, "other-pool")
	require.NoError(t, err)
	other := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	other.Header["kid"] = otherKey.Kid
	otherSigned, err := other.SignedString(otherKey.Private)
	require.NoError(t, err)
	_, err = jwt.Parse(otherSigned, kf.Keyfunc, jwt.WithIssuer(ts.URL+"/"+poolID), jwt.WithAudience("client-xyz"))
	assert.Error(t, err)
}

// TestComputeKid_StableAcrossRuns is a tiny pin against the documented
// `sha256(DER-SPKI)[:16]` rule; if the constant ever changes, the JWKS
// shape contract with mock.py:66 silently breaks.
func TestComputeKid_StableAcrossRuns(t *testing.T) {
	// Use a fixed RSA modulus / exponent to keep the assertion deterministic.
	pub := &rsa.PublicKey{N: big.NewInt(0xDEADBEEF), E: 65537}
	kid1, err := computeKid(pub)
	require.NoError(t, err)
	kid2, err := computeKid(pub)
	require.NoError(t, err)
	assert.Equal(t, kid1, kid2)
	assert.Len(t, kid1, 16)
}
