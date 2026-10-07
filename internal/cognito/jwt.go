// JWT signing and verification use one persisted RSA key pair per pool.
// Stable key IDs keep downstream JWKS caches valid across emulator restarts.
package cognito

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	rsaKeySize        = 2048
	rsaPublicExponent = 65537
	kidPrefixLen      = 16 // matches MockProvider mock.py:66
)

// SigningKey is a per-pool RSA keypair plus its derived `kid`.
type SigningKey struct {
	PoolID    string
	Kid       string
	Private   *rsa.PrivateKey
	Public    *rsa.PublicKey
	CreatedAt int64
}

// JWK is the JSON shape served at /.well-known/jwks.json. Field order in JSON
// is alphabetical via encoding/json, which matches the MockProvider output
// ordering closely enough that downstream consumers (PyJWT PyJWKClient,
// keyfunc.NewJWKSet) parse it identically.
type JWK struct {
	Kty string `json:"kty"`
	Alg string `json:"alg"`
	Use string `json:"use"`
	Kid string `json:"kid"`
	N   string `json:"n"`
	E   string `json:"e"`
}

// JWKS wraps a slice of JWKs.
type JWKS struct {
	Keys []JWK `json:"keys"`
}

// EnsureSigningKey returns the existing pool key if one is persisted, else
// generates and stores a new RSA-2048 pair atomically. The mutex on
// CognitoStore protects against two concurrent first-use callers generating
// duplicate keys for the same pool and against pool deletion. Key generation
// never provisions resources; the native API or development seed owns pools.
func (s *CognitoStore) EnsureSigningKey(ctx context.Context, poolID string) (*SigningKey, error) {
	if poolID == "" {
		return nil, errors.New("pool id required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	var parent string
	if err := s.db.QueryRowContext(ctx, `SELECT id FROM pools WHERE id=?`, poolID).Scan(&parent); err != nil {
		return nil, err
	}
	if key, err := s.loadSigningKeyLocked(ctx, poolID); err == nil {
		return key, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}

	priv, err := rsa.GenerateKey(rand.Reader, rsaKeySize)
	if err != nil {
		return nil, fmt.Errorf("generate rsa key: %w", err)
	}
	pub := &priv.PublicKey

	kid, err := computeKid(pub)
	if err != nil {
		return nil, err
	}

	privPEM, err := encodePrivateKeyPEM(priv)
	if err != nil {
		return nil, err
	}
	pubPEM, err := encodePublicKeyPEM(pub)
	if err != nil {
		return nil, err
	}

	now := s.now().Unix()
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO signing_keys (pool_id, kid, private_pem, public_pem, created_at)
		VALUES (?, ?, ?, ?, ?)
	`, poolID, kid, privPEM, pubPEM, now); err != nil {
		return nil, fmt.Errorf("persist signing key: %w", err)
	}

	return &SigningKey{
		PoolID:    poolID,
		Kid:       kid,
		Private:   priv,
		Public:    pub,
		CreatedAt: now,
	}, nil
}

// LoadSigningKey returns the persisted key for poolID without generating one.
// Returns sql.ErrNoRows if absent.
func (s *CognitoStore) LoadSigningKey(ctx context.Context, poolID string) (*SigningKey, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadSigningKeyLocked(ctx, poolID)
}

func (s *CognitoStore) loadSigningKeyLocked(ctx context.Context, poolID string) (*SigningKey, error) {
	var (
		kid       string
		privPEM   string
		pubPEM    string
		createdAt int64
	)
	err := s.db.QueryRowContext(ctx, `
		SELECT kid, private_pem, public_pem, created_at FROM signing_keys WHERE pool_id = ?
	`, poolID).Scan(&kid, &privPEM, &pubPEM, &createdAt)
	if err != nil {
		return nil, err
	}
	priv, err := decodePrivateKeyPEM(privPEM)
	if err != nil {
		return nil, fmt.Errorf("decode private pem: %w", err)
	}
	pub, err := decodePublicKeyPEM(pubPEM)
	if err != nil {
		return nil, fmt.Errorf("decode public pem: %w", err)
	}
	return &SigningKey{
		PoolID:    poolID,
		Kid:       kid,
		Private:   priv,
		Public:    pub,
		CreatedAt: createdAt,
	}, nil
}

// BuildJWKS returns the JSON document published at
// /{pool}/.well-known/jwks.json. Shape matches mock.py:44-79 exactly.
func (s *CognitoStore) BuildJWKS(ctx context.Context, poolID string) ([]byte, error) {
	key, err := s.EnsureSigningKey(ctx, poolID)
	if err != nil {
		return nil, err
	}
	jwk := PublicKeyToJWK(key.Public, key.Kid)
	doc := JWKS{Keys: []JWK{jwk}}
	return json.Marshal(doc)
}

// PublicKeyToJWK converts an RSA public key + kid into the JWK shape used in
// the published document. Modulus and exponent are big-endian, base64url
// without padding (RFC 7518 §6.3).
func PublicKeyToJWK(pub *rsa.PublicKey, kid string) JWK {
	n := pub.N.Bytes()
	// Right-strip leading zero bytes (unlikely with our keys, but conform to RFC).
	// RFC 7518 says the integer's big-endian representation MUST NOT have any
	// leading zero octets.
	for len(n) > 0 && n[0] == 0x00 {
		n = n[1:]
	}
	// Exponent (typically 65537 = 0x010001 = 3 bytes).
	e := bigEndianBytes(pub.E)
	return JWK{
		Kty: "RSA",
		Alg: "RS256",
		Use: "sig",
		Kid: kid,
		N:   base64.RawURLEncoding.EncodeToString(n),
		E:   base64.RawURLEncoding.EncodeToString(e),
	}
}

func bigEndianBytes(n int) []byte {
	if n == 0 {
		return []byte{0}
	}
	// Find minimum bytes to represent n big-endian without leading zeros.
	// AWS / RFC convention: 65537 → [0x01, 0x00, 0x01].
	bits := 0
	for x := n; x > 0; x >>= 8 {
		bits++
	}
	out := make([]byte, bits)
	for i := bits - 1; i >= 0; i-- {
		out[i] = byte(n & 0xff)
		n >>= 8
	}
	return out
}

func computeKid(pub *rsa.PublicKey) (string, error) {
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return "", fmt.Errorf("marshal pkix: %w", err)
	}
	sum := sha256.Sum256(der)
	return hex.EncodeToString(sum[:])[:kidPrefixLen], nil
}

// --- PEM encode/decode helpers ------------------------------------------

func encodePrivateKeyPEM(priv *rsa.PrivateKey) (string, error) {
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return "", err
	}
	block := &pem.Block{Type: "PRIVATE KEY", Bytes: der}
	return string(pem.EncodeToMemory(block)), nil
}

func encodePublicKeyPEM(pub *rsa.PublicKey) (string, error) {
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return "", err
	}
	block := &pem.Block{Type: "PUBLIC KEY", Bytes: der}
	return string(pem.EncodeToMemory(block)), nil
}

func decodePrivateKeyPEM(s string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(s))
	if block == nil {
		return nil, errors.New("no PEM block found in private key")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		// Fall back to PKCS#1 just in case some external tool wrote one.
		if k1, err1 := x509.ParsePKCS1PrivateKey(block.Bytes); err1 == nil {
			return k1, nil
		}
		return nil, err
	}
	rsaKey, ok := key.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("expected RSA private key")
	}
	return rsaKey, nil
}

// VerifyAccessToken parses and verifies a JWT minted by this service. It
// extracts the `iss` claim WITHOUT trusting the signature, derives the pool
// id from the issuer suffix (must match `<issuerBase>/<pool-id>`), then
// verifies the signature against that pool's persisted public key. Real
// Cognito uses the same `iss` → JWKS-URL convention.
//
// On success returns the parsed claims map. Errors are bare so the caller
// can map them all to NotAuthorizedException without leaking internal
// classification — Cognito's GetUser also collapses signature/expiry/format
// problems into a single NotAuthorizedException response.
//
// `token_use` is required to be `"access"`; refresh and id tokens MUST NOT
// be accepted on GetUser. `exp` is enforced by the JWT library.
func VerifyAccessToken(ctx context.Context, store *CognitoStore, issuerBase, tokenString string) (jwt.MapClaims, error) {
	if store == nil {
		return nil, errors.New("nil cognito store")
	}
	if tokenString == "" {
		return nil, errors.New("empty access token")
	}

	// First parse without verifying so we can read the `iss` claim and
	// figure out which pool's signing key to use. We do NOT trust any
	// claim until the second parse with the real keyfunc completes.
	parser := jwt.NewParser(jwt.WithValidMethods([]string{"RS256"}))
	unverified, _, err := parser.ParseUnverified(tokenString, jwt.MapClaims{})
	if err != nil {
		return nil, fmt.Errorf("parse unverified: %w", err)
	}
	claims, ok := unverified.Claims.(jwt.MapClaims)
	if !ok {
		return nil, errors.New("claims not a map")
	}

	issRaw, _ := claims["iss"].(string)
	if issRaw == "" {
		return nil, errors.New("missing iss claim")
	}
	prefix := strings.TrimRight(issuerBase, "/") + "/"
	if !strings.HasPrefix(issRaw, prefix) {
		return nil, fmt.Errorf("iss %q does not start with %q", issRaw, prefix)
	}
	poolID := strings.TrimPrefix(issRaw, prefix)
	if poolID == "" || strings.Contains(poolID, "/") {
		return nil, fmt.Errorf("invalid pool id derived from iss %q", issRaw)
	}

	signing, err := store.LoadSigningKey(ctx, poolID)
	if err != nil {
		return nil, fmt.Errorf("load signing key for pool %q: %w", poolID, err)
	}

	// Now verify with the real keyfunc + standard validators.
	verified, err := jwt.Parse(
		tokenString,
		func(t *jwt.Token) (interface{}, error) { return signing.Public, nil },
		jwt.WithValidMethods([]string{"RS256"}),
		jwt.WithIssuer(issRaw),
		jwt.WithTimeFunc(store.now),
	)
	if err != nil {
		return nil, fmt.Errorf("verify: %w", err)
	}
	if !verified.Valid {
		return nil, errors.New("token not valid")
	}
	verifiedClaims, ok := verified.Claims.(jwt.MapClaims)
	if !ok {
		return nil, errors.New("verified claims not a map")
	}

	tokenUse, _ := verifiedClaims["token_use"].(string)
	if tokenUse != "access" {
		return nil, fmt.Errorf("token_use %q is not %q", tokenUse, "access")
	}
	if sub, _ := verifiedClaims["sub"].(string); sub == "" {
		return nil, errors.New("missing sub claim")
	}
	return verifiedClaims, nil
}

func decodePublicKeyPEM(s string) (*rsa.PublicKey, error) {
	block, _ := pem.Decode([]byte(s))
	if block == nil {
		return nil, errors.New("no PEM block found in public key")
	}
	key, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	pub, ok := key.(*rsa.PublicKey)
	if !ok {
		return nil, errors.New("expected RSA public key")
	}
	return pub, nil
}

// --- Token issuance  --------------------------------------

// tokenGrant is the authentication a token belongs to. AuthTime is when the
// person authenticated (password and any challenge); OriginJTI is the jti of
// the refresh token issued with that authentication. Every access token
// minted from the grant, at sign-in or by REFRESH_TOKEN_AUTH, carries both,
// so a refresh renews iat and expiry but never the authentication time, and revoking the
// refresh token (RevokeToken) also revokes its access tokens, as in Cognito.
type tokenGrant struct {
	AuthTime    time.Time
	OriginJTI   string
	AuthVersion int64
}

const authVersionClaim = "eventbus:auth_version"

func (grant tokenGrant) valid() error {
	if grant.AuthTime.IsZero() {
		return errors.New("authentication time required")
	}
	if strings.TrimSpace(grant.OriginJTI) == "" {
		return errors.New("origin jti required")
	}
	if grant.AuthVersion < 0 {
		return errors.New("invalid authentication version")
	}
	return nil
}

// SignAccessToken mints an RS256 access token for the given user/client. The
// `iss` claim is `<issuerBase>/<poolID>` (matches what the JWKS path implies)
// and `aud` is the client id the SDK called with. Required claims for the
// Lambda authorizer are sub/email/exp/iat (lambda_authorizer.py:74); the
// extras (`username`, `client_id`, `auth_time`, `origin_jti`, `jti`,
// `token_use`) match what real Cognito emits so platform code reading any of
// these keeps working.
//
// `auth_time` is the grant's authentication time, not the mint time (`iat`).
//
// `jti` is a ULID — collision-resistant and the project standard for sortable
// IDs (GO_TECHSPEC.md). `kid` is set in the JWT header so consumers can pick
// the right JWKS entry.
func SignAccessToken(
	ctx context.Context,
	store *CognitoStore,
	issuerBase, poolID, clientID, sub, email string,
	grant tokenGrant,
	ttl time.Duration,
) (string, error) {
	if store == nil {
		return "", errors.New("nil cognito store")
	}
	if err := grant.valid(); err != nil {
		return "", err
	}
	user, err := store.LookupUserBySub(ctx, sub)
	if err != nil {
		return "", fmt.Errorf("load token identity: %w", err)
	}
	if user.PoolID != poolID {
		return "", errors.New("user does not belong to token pool")
	}
	signing, err := store.EnsureSigningKey(ctx, poolID)
	if err != nil {
		return "", fmt.Errorf("ensure signing key: %w", err)
	}
	now := store.now()
	claims := jwt.MapClaims{
		"sub":            sub,
		"email":          user.Email,
		"iss":            strings.TrimRight(issuerBase, "/") + "/" + poolID,
		"aud":            clientID,
		"iat":            now.Unix(),
		"exp":            now.Add(ttl).Unix(),
		"token_use":      "access",
		"username":       user.Username,
		"client_id":      clientID,
		"auth_time":      grant.AuthTime.Unix(),
		"origin_jti":     grant.OriginJTI,
		"jti":            newJTI(),
		authVersionClaim: grant.AuthVersion,
	}
	client, err := store.LookupClient(ctx, clientID)
	if err != nil {
		return "", fmt.Errorf("load access token client: %w", err)
	}
	if client.PoolID != poolID {
		return "", errors.New("client does not belong to token pool")
	}
	claims["scope"] = "aws.cognito.signin.user.admin"
	if client.Native {
		delete(claims, "email")
		delete(claims, "aud")
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = signing.Kid
	signed, err := tok.SignedString(signing.Private)
	if err != nil {
		return "", fmt.Errorf("sign access token: %w", err)
	}
	return signed, nil
}

// SignIDToken exposes user attributes with the Cognito identity-token claims.
// Reserved claims always come from the authenticated account and grant; a
// mutable user attribute cannot replace the issuer, audience or token purpose.
func SignIDToken(ctx context.Context, store *CognitoStore, issuerBase, poolID, clientID string, user *CognitoUser, grant tokenGrant, ttl time.Duration) (string, error) {
	if store == nil || user == nil {
		return "", errors.New("store and user required")
	}
	if user.PoolID != poolID {
		return "", errors.New("user does not belong to token pool")
	}
	if err := grant.valid(); err != nil {
		return "", err
	}
	attributes, err := store.LoadUserAttributes(ctx, user.Sub)
	if err != nil {
		return "", fmt.Errorf("load ID token attributes: %w", err)
	}
	client, err := store.LookupClient(ctx, clientID)
	if err != nil {
		return "", fmt.Errorf("load ID token client: %w", err)
	}
	if client.PoolID != poolID {
		return "", errors.New("client does not belong to token pool")
	}
	projected := client.Native || client.ReadAttributes != nil
	if projected {
		pool, err := store.LookupPool(ctx, poolID)
		if err != nil {
			return "", err
		}
		attributes = FilterClientReadAttributes(pool.SchemaAttributes, client.ReadAttributes, attributes)
	}
	signing, err := store.EnsureSigningKey(ctx, poolID)
	if err != nil {
		return "", fmt.Errorf("ensure signing key: %w", err)
	}
	claims := jwt.MapClaims{}
	for name, value := range attributes {
		switch name {
		case "email_verified", "phone_number_verified":
			if verified, err := strconv.ParseBool(value); err == nil {
				claims[name] = verified
			}
		default:
			claims[name] = value
		}
	}
	if user.Email != "" && !projected {
		claims["email"] = user.Email
	}
	now := store.now()
	for name, value := range (jwt.MapClaims{
		"sub":              user.Sub,
		"iss":              strings.TrimRight(issuerBase, "/") + "/" + poolID,
		"aud":              clientID,
		"iat":              now.Unix(),
		"exp":              now.Add(ttl).Unix(),
		"token_use":        "id",
		"cognito:username": user.Username,
		"auth_time":        grant.AuthTime.Unix(),
		"origin_jti":       grant.OriginJTI,
		"jti":              newJTI(),
		authVersionClaim:   grant.AuthVersion,
	}) {
		claims[name] = value
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = signing.Kid
	signed, err := tok.SignedString(signing.Private)
	if err != nil {
		return "", fmt.Errorf("sign ID token: %w", err)
	}
	return signed, nil
}

// SignRefreshToken mints an RS256 refresh token. `email` is intentionally
// omitted from the refresh-token claims — the platform never reads anything
// from the refresh token's body other than verifying it round-trips
// (cognito.py:99 falls back to the existing refresh on REFRESH_TOKEN_AUTH).
// Keeping it tighter than the access token reduces info leakage if the
// refresh ever shows up in a log.
//
// Its `jti` is the grant's OriginJTI and its `auth_time` the grant's
// authentication time, so every access token minted from it (grantOf)
// belongs to the same authentication.
func SignRefreshToken(
	ctx context.Context,
	store *CognitoStore,
	issuerBase, poolID, clientID, sub string,
	grant tokenGrant,
	ttl time.Duration,
) (string, error) {
	if store == nil {
		return "", errors.New("nil cognito store")
	}
	if err := grant.valid(); err != nil {
		return "", err
	}
	signing, err := store.EnsureSigningKey(ctx, poolID)
	if err != nil {
		return "", fmt.Errorf("ensure signing key: %w", err)
	}
	now := store.now()
	claims := jwt.MapClaims{
		"sub":            sub,
		"iss":            strings.TrimRight(issuerBase, "/") + "/" + poolID,
		"aud":            clientID,
		"iat":            now.Unix(),
		"exp":            now.Add(ttl).Unix(),
		"token_use":      "refresh",
		"client_id":      clientID,
		"auth_time":      grant.AuthTime.Unix(),
		"jti":            grant.OriginJTI,
		authVersionClaim: grant.AuthVersion,
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = signing.Kid
	signed, err := tok.SignedString(signing.Private)
	if err != nil {
		return "", fmt.Errorf("sign refresh token: %w", err)
	}
	return signed, nil
}

// VerifyRefreshToken parses and verifies a refresh JWT minted by this service.
// Mirrors VerifyAccessToken's `iss` → pool resolution but enforces
// `token_use=="refresh"` and `aud == expectedClientID`. All failures collapse
// to a bare error so the caller maps them uniformly to NotAuthorizedException
// — matches both `cognito.go:419-422` (expired refresh) and
// `cognito.go:1099-1101` (bad signature) which the providers already squash
// to the same TokenExpiredError downstream.
func VerifyRefreshToken(
	ctx context.Context,
	store *CognitoStore,
	issuerBase, expectedClientID, tokenString string,
) (jwt.MapClaims, error) {
	if store == nil {
		return nil, errors.New("nil cognito store")
	}
	if tokenString == "" {
		return nil, errors.New("empty refresh token")
	}

	parser := jwt.NewParser(jwt.WithValidMethods([]string{"RS256"}))
	unverified, _, err := parser.ParseUnverified(tokenString, jwt.MapClaims{})
	if err != nil {
		return nil, fmt.Errorf("parse unverified: %w", err)
	}
	claims, ok := unverified.Claims.(jwt.MapClaims)
	if !ok {
		return nil, errors.New("claims not a map")
	}

	issRaw, _ := claims["iss"].(string)
	if issRaw == "" {
		return nil, errors.New("missing iss claim")
	}
	prefix := strings.TrimRight(issuerBase, "/") + "/"
	if !strings.HasPrefix(issRaw, prefix) {
		return nil, fmt.Errorf("iss %q does not start with %q", issRaw, prefix)
	}
	poolID := strings.TrimPrefix(issRaw, prefix)
	if poolID == "" || strings.Contains(poolID, "/") {
		return nil, fmt.Errorf("invalid pool id derived from iss %q", issRaw)
	}

	signing, err := store.LoadSigningKey(ctx, poolID)
	if err != nil {
		return nil, fmt.Errorf("load signing key for pool %q: %w", poolID, err)
	}

	verified, err := jwt.Parse(
		tokenString,
		func(t *jwt.Token) (interface{}, error) { return signing.Public, nil },
		jwt.WithValidMethods([]string{"RS256"}),
		jwt.WithIssuer(issRaw),
		jwt.WithTimeFunc(store.now),
		jwt.WithAudience(expectedClientID),
	)
	if err != nil {
		return nil, fmt.Errorf("verify: %w", err)
	}
	if !verified.Valid {
		return nil, errors.New("token not valid")
	}
	verifiedClaims, ok := verified.Claims.(jwt.MapClaims)
	if !ok {
		return nil, errors.New("verified claims not a map")
	}

	tokenUse, _ := verifiedClaims["token_use"].(string)
	if tokenUse != "refresh" {
		return nil, fmt.Errorf("token_use %q is not %q", tokenUse, "refresh")
	}
	if clientID, _ := verifiedClaims["client_id"].(string); clientID != expectedClientID {
		return nil, errors.New("refresh token client does not match")
	}
	if sub, _ := verifiedClaims["sub"].(string); sub == "" {
		return nil, errors.New("missing sub claim")
	}
	return verifiedClaims, nil
}

// grantOf returns the grant a verified refresh token belongs to. A token
// minted before refresh tokens carried `auth_time` falls back to its `iat`:
// this service mints refresh tokens only when a person authenticates and
// never rotates them, so `iat` is that authentication's time. A token with
// neither is refused rather than given a fresh age.
func grantOf(claims jwt.MapClaims) (tokenGrant, error) {
	jti, _ := claims["jti"].(string)
	if strings.TrimSpace(jti) == "" {
		return tokenGrant{}, errors.New("refresh token carries no jti")
	}
	version, err := authVersionOf(claims)
	if err != nil {
		return tokenGrant{}, err
	}
	for _, name := range []string{"auth_time", "iat"} {
		if value, ok := claims[name].(float64); ok && value > 0 {
			return tokenGrant{AuthTime: time.Unix(int64(value), 0).UTC(), OriginJTI: jti, AuthVersion: version}, nil
		}
	}
	return tokenGrant{}, errors.New("refresh token carries no authentication time")
}

// Earlier grants omitted the private account version and represent version
// zero. Present values must be non-negative integers, never coerced strings or
// truncated fractions. Verification callers compare this to current state.
func authVersionOf(claims jwt.MapClaims) (int64, error) {
	value, present := claims[authVersionClaim]
	if !present {
		return 0, nil
	}
	switch value := value.(type) {
	case int64:
		if value >= 0 {
			return value, nil
		}
	case float64:
		if value >= 0 && value < math.Exp2(63) && value == math.Trunc(value) {
			return int64(value), nil
		}
	case json.Number:
		if version, err := value.Int64(); err == nil && version >= 0 {
			return version, nil
		}
	}
	return 0, errors.New("invalid authentication version claim")
}

// errTokenRevoked is a token that verified but was revoked by GlobalSignOut,
// AdminUserGlobalSignOut or RevokeToken.
var errTokenRevoked = errors.New("token has been revoked")

// checkNotRevoked checks the legacy timestamp cutoff for version-zero accounts
// and per-grant refresh revocation. Versioned accounts use the callers' exact
// AuthVersion comparison, allowing fresh authentication in the same second as
// global sign-out. Offline JWT verification does not consult either state.
func (s *CognitoStore) checkNotRevoked(ctx context.Context, user *CognitoUser, authTime int64, originJTI string) error {
	if user.AuthVersion == 0 && user.TokensRevokedBefore > 0 && authTime <= user.TokensRevokedBefore {
		return errTokenRevoked
	}
	revoked, err := s.RefreshTokenRevoked(ctx, originJTI)
	if err != nil {
		return err
	}
	if revoked {
		return errTokenRevoked
	}
	return nil
}

// newJTI returns a fresh ULID for use as a token's `jti` claim. ULIDs are
// already in use for `sub` (users.go:newULIDSub) so reusing the
// same dependency is consistent.
func newJTI() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Astronomically unlikely on a healthy host; fall back to a constant
		// so we never panic on a token mint. Tests never see this path.
		return "00000000000000000000000000"
	}
	// 16 random bytes hex-encoded → 32 chars. Compact and parseable by every
	// downstream JWT consumer we care about.
	return hex.EncodeToString(b[:])
}
