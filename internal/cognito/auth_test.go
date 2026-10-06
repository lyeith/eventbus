// Password and refresh authentication through the Cognito wire protocol.
package cognito

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
)

// seedClientPoolUser creates a pool, registers a single client, and inserts
// a user with a bcrypt-hashed password. Returns the user's sub for the
// caller's assertions. Centralised so each test starts from the same shape.
func seedClientPoolUser(t *testing.T, store *CognitoStore, poolID, clientID, email, password string) string {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, store.UpsertPool(ctx, poolID, "us-east-1"))
	require.NoError(t, store.UpsertClient(ctx, clientID, poolID, ""))

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	require.NoError(t, err)
	sub, err := store.UpsertUser(ctx, poolID, email, string(hash), false)
	require.NoError(t, err)
	require.NoError(t, store.SetUserAttribute(ctx, sub, "email", email))
	require.NoError(t, store.SetUserAttribute(ctx, sub, "email_verified", "true"))
	require.NoError(t, store.SetUserAttribute(ctx, sub, "sub", sub))
	return sub
}

// readAuthResult unwraps the AuthenticationResult sub-object from a
// successful InitiateAuth response. Fails the test if the shape is wrong.
func readAuthResult(t *testing.T, body map[string]interface{}) map[string]interface{} {
	t.Helper()
	auth, ok := body["AuthenticationResult"].(map[string]interface{})
	require.True(t, ok, "AuthenticationResult missing or wrong type: %v", body)
	return auth
}

// parseClaimsUnverified parses a JWT WITHOUT verifying its signature so the
// test can inspect the claim shape directly. Verification correctness is
// covered separately via the JWKS-roundtrip test below.
func parseClaimsUnverified(t *testing.T, token string) jwt.MapClaims {
	t.Helper()
	parser := jwt.NewParser(jwt.WithValidMethods([]string{"RS256"}))
	parsed, _, err := parser.ParseUnverified(token, jwt.MapClaims{})
	require.NoError(t, err)
	claims, ok := parsed.Claims.(jwt.MapClaims)
	require.True(t, ok)
	return claims
}

// --- USER_PASSWORD_AUTH -------------------------------------------------

func TestInitiateAuth_UserPasswordAuth_HappyPath(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "local-pool-1"
		clientID = "local-client-1"
		email    = "alice@example.com"
		password = "TempPass1!"
	)
	sub := seedClientPoolUser(t, store, poolID, clientID, email, password)

	status, body := postCognito(t, ts.URL, "InitiateAuth", map[string]interface{}{
		"AuthFlow": "USER_PASSWORD_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]string{
			"USERNAME": email,
			"PASSWORD": password,
		},
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)

	// Cognito omits ChallengeName/Session for non-challenge responses; both
	// providers test for presence (cognito.py:285) / non-empty
	// (cognito.go:892), so we MUST NOT emit them on success.
	_, hasChallenge := body["ChallengeName"]
	_, hasSession := body["Session"]
	assert.False(t, hasChallenge, "ChallengeName must be absent on success")
	assert.False(t, hasSession, "Session must be absent on success")

	auth := readAuthResult(t, body)
	access, _ := auth["AccessToken"].(string)
	refresh, _ := auth["RefreshToken"].(string)
	id, _ := auth["IdToken"].(string)
	require.NotEmpty(t, access)
	require.NotEmpty(t, refresh)
	require.NotEmpty(t, id)
	assert.Equal(t, "Bearer", auth["TokenType"])
	assert.Equal(t, float64(3600), auth["ExpiresIn"], "default access TTL is 1h")

	// Access token claim shape — every field the Lambda authorizer or
	// platform code reads must be present.
	accessClaims := parseClaimsUnverified(t, access)
	assert.Equal(t, sub, accessClaims["sub"])
	assert.Equal(t, email, accessClaims["email"])
	assert.Equal(t, "http://localhost:4100/"+poolID, accessClaims["iss"])
	assert.Equal(t, clientID, accessClaims["aud"])
	assert.Equal(t, "access", accessClaims["token_use"])
	assert.Equal(t, email, accessClaims["username"])
	assert.Equal(t, clientID, accessClaims["client_id"])
	assert.Equal(t, float64(0), accessClaims[authVersionClaim])
	idClaims := parseClaimsUnverified(t, id)
	assert.Equal(t, sub, idClaims["sub"])
	assert.Equal(t, email, idClaims["email"])
	assert.Equal(t, true, idClaims["email_verified"])
	assert.Equal(t, email, idClaims["cognito:username"])
	assert.Equal(t, clientID, idClaims["aud"])
	assert.Equal(t, "id", idClaims["token_use"])
	assert.Equal(t, accessClaims["auth_time"], idClaims["auth_time"])
	assert.Equal(t, accessClaims["origin_jti"], idClaims["origin_jti"])
	require.Contains(t, accessClaims, "iat")
	require.Contains(t, accessClaims, "exp")
	require.Contains(t, accessClaims, "auth_time")
	require.Contains(t, accessClaims, "jti")
	jti, _ := accessClaims["jti"].(string)
	assert.NotEmpty(t, jti)

	// Refresh token claim shape — narrower (no email).
	refreshClaims := parseClaimsUnverified(t, refresh)
	assert.Equal(t, sub, refreshClaims["sub"])
	assert.Equal(t, "http://localhost:4100/"+poolID, refreshClaims["iss"])
	assert.Equal(t, clientID, refreshClaims["aud"])
	assert.Equal(t, "refresh", refreshClaims["token_use"])
	assert.Equal(t, clientID, refreshClaims["client_id"])
	require.Contains(t, refreshClaims, "iat")
	require.Contains(t, refreshClaims, "exp")
	require.Contains(t, refreshClaims, "jti")
	_, hasEmail := refreshClaims["email"]
	assert.False(t, hasEmail, "refresh token should NOT carry email")

	// jti uniqueness across the two tokens (no shared/static value).
	assert.NotEqual(t, accessClaims["jti"], refreshClaims["jti"])
}

func TestInitiateAuth_UserPasswordAuth_WrongPassword(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	seedClientPoolUser(t, store, "local-pool-1", "local-client-1", "alice@example.com", "Right!")

	status, body := postCognito(t, ts.URL, "InitiateAuth", map[string]interface{}{
		"AuthFlow": "USER_PASSWORD_AUTH",
		"ClientId": "local-client-1",
		"AuthParameters": map[string]string{
			"USERNAME": "alice@example.com",
			"PASSWORD": "Wrong!",
		},
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NotAuthorizedException", body["__type"])
}

func TestInitiateAuth_UserPasswordAuth_UnknownUser(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	// Pool/client exist but user does not.
	require.NoError(t, store.UpsertPool(t.Context(), "local-pool-1", "us-east-1"))
	require.NoError(t, store.UpsertClient(t.Context(), "local-client-1", "local-pool-1", ""))

	status, body := postCognito(t, ts.URL, "InitiateAuth", map[string]interface{}{
		"AuthFlow": "USER_PASSWORD_AUTH",
		"ClientId": "local-client-1",
		"AuthParameters": map[string]string{
			"USERNAME": "ghost@example.com",
			"PASSWORD": "anything",
		},
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "UserNotFoundException", body["__type"])
}

func TestInitiateAuth_UserPasswordAuth_MissingClientId(t *testing.T) {
	_, ts, _ := newCognitoTestServer(t)

	status, body := postCognito(t, ts.URL, "InitiateAuth", map[string]interface{}{
		"AuthFlow": "USER_PASSWORD_AUTH",
		"ClientId": "no-such-client",
		"AuthParameters": map[string]string{
			"USERNAME": "alice@example.com",
			"PASSWORD": "anything",
		},
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "ResourceNotFoundException", body["__type"])
	assert.Contains(t, body["message"], "no-such-client")
}

// TestInitiateAuth_UserPasswordAuth_MFAEnabledIssuesChallenge pins the
// Cognito contract: an mfa_enabled user gets a SOFTWARE_TOKEN_MFA
// challenge response (no AuthenticationResult) carrying an opaque Session
// the client carries through RespondToAuthChallenge.
func TestInitiateAuth_UserPasswordAuth_MFAEnabledIssuesChallenge(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "local-pool-1"
		clientID = "local-client-1"
		email    = "mfa@example.com"
		password = "TempPass1!"
	)
	require.NoError(t, store.UpsertPool(t.Context(), poolID, "us-east-1"))
	require.NoError(t, store.UpsertClient(t.Context(), clientID, poolID, ""))

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	require.NoError(t, err)
	_, err = store.UpsertUser(t.Context(), poolID, email, string(hash), true) // mfa_enabled=true
	require.NoError(t, err)

	status, body := postCognito(t, ts.URL, "InitiateAuth", map[string]interface{}{
		"AuthFlow": "USER_PASSWORD_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]string{
			"USERNAME": email,
			"PASSWORD": password,
		},
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)

	// Challenge response: ChallengeName + Session present, no
	// AuthenticationResult. Both providers test for this exact shape
	// (cognito.py:285, cognito.go:892).
	assert.Equal(t, "SOFTWARE_TOKEN_MFA", body["ChallengeName"])
	session, _ := body["Session"].(string)
	assert.NotEmpty(t, session, "Session must be present on challenge response")
	_, hasAuth := body["AuthenticationResult"]
	assert.False(t, hasAuth, "AuthenticationResult must NOT appear on challenge response")

	params, ok := body["ChallengeParameters"].(map[string]interface{})
	require.True(t, ok, "ChallengeParameters missing or wrong type")
	assert.Equal(t, email, params["USERNAME"])
	assert.Equal(t, email, params["USER_ID_FOR_SRP"])
}

// TestInitiateAuth_UserPasswordAuth_TTLHonored pins that --access-token-ttl
// is plumbed through SetCognito to the issued token. exp - iat must equal
// the configured TTL.
func TestInitiateAuth_UserPasswordAuth_TTLHonored(t *testing.T) {
	const customAccess = 5 * time.Minute
	const customRefresh = 12 * time.Hour
	_, ts, store := newCognitoTestServerWithTTL(t, customAccess, customRefresh)

	seedClientPoolUser(t, store, "local-pool-1", "local-client-1", "alice@example.com", "TempPass1!")

	status, body := postCognito(t, ts.URL, "InitiateAuth", map[string]interface{}{
		"AuthFlow": "USER_PASSWORD_AUTH",
		"ClientId": "local-client-1",
		"AuthParameters": map[string]string{
			"USERNAME": "alice@example.com",
			"PASSWORD": "TempPass1!",
		},
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)

	auth := readAuthResult(t, body)
	assert.Equal(t, float64(customAccess.Seconds()), auth["ExpiresIn"])

	accessClaims := parseClaimsUnverified(t, auth["AccessToken"].(string))
	accessExp, _ := accessClaims["exp"].(float64)
	accessIat, _ := accessClaims["iat"].(float64)
	assert.Equal(t, int64(customAccess.Seconds()), int64(accessExp-accessIat),
		"access token exp - iat must match --access-token-ttl")

	refreshClaims := parseClaimsUnverified(t, auth["RefreshToken"].(string))
	refreshExp, _ := refreshClaims["exp"].(float64)
	refreshIat, _ := refreshClaims["iat"].(float64)
	assert.Equal(t, int64(customRefresh.Seconds()), int64(refreshExp-refreshIat),
		"refresh token exp - iat must match --refresh-token-ttl")
}

// TestInitiateAuth_UserPasswordAuth_AccessTokenValidatesAgainstJWKS is the
// cross-language correctness pin: the dev service's emitted access token
// must validate cleanly against the published JWKS using the same library
// the Lambda authorizer uses (golang-jwt + keyfunc). Mirrors
// TestSignAndValidateAgainstJWKS but for InitiateAuth-emitted tokens.
func TestInitiateAuth_UserPasswordAuth_AccessTokenValidatesAgainstJWKS(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "local-pool-1"
		clientID = "local-client-1"
		email    = "alice@example.com"
		password = "TempPass1!"
	)
	seedClientPoolUser(t, store, poolID, clientID, email, password)

	status, body := postCognito(t, ts.URL, "InitiateAuth", map[string]interface{}{
		"AuthFlow": "USER_PASSWORD_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]string{
			"USERNAME": email,
			"PASSWORD": password,
		},
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	auth := readAuthResult(t, body)
	access := auth["AccessToken"].(string)

	// Build a keyfunc backed by the live JWKS endpoint.
	jwksURL := ts.URL + "/" + poolID + "/.well-known/jwks.json"
	kf, err := keyfunc.NewDefault([]string{jwksURL})
	require.NoError(t, err)

	// Note: the issuer in the token is "http://localhost:4100/<poolID>"
	// (the SetCognito issuerBase wired by newCognitoTestServer), NOT
	// ts.URL — that's the same divergence handled in TestGetUser_HappyPath.
	parsed, err := jwt.Parse(
		access,
		kf.Keyfunc,
		jwt.WithIssuer("http://localhost:4100/"+poolID),
		jwt.WithAudience(clientID),
		jwt.WithValidMethods([]string{"RS256"}),
	)
	require.NoError(t, err)
	require.True(t, parsed.Valid)
}

// --- REFRESH_TOKEN_AUTH -------------------------------------------------

func TestInitiateAuth_RefreshTokenAuth_HappyPath(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "local-pool-1"
		clientID = "local-client-1"
		email    = "alice@example.com"
		password = "TempPass1!"
	)
	seedClientPoolUser(t, store, poolID, clientID, email, password)

	// 1. USER_PASSWORD_AUTH to get a refresh token.
	loginStatus, loginBody := postCognito(t, ts.URL, "InitiateAuth", map[string]interface{}{
		"AuthFlow": "USER_PASSWORD_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]string{
			"USERNAME": email,
			"PASSWORD": password,
		},
	})
	require.Equal(t, http.StatusOK, loginStatus)
	loginAuth := readAuthResult(t, loginBody)
	originalAccess := loginAuth["AccessToken"].(string)
	refresh := loginAuth["RefreshToken"].(string)

	originalAccessClaims := parseClaimsUnverified(t, originalAccess)
	originalExp, _ := originalAccessClaims["exp"].(float64)

	// Sleep just past the second-resolution boundary so iat and therefore
	// exp on the refreshed token are strictly later. JWT exp is unix
	// seconds — without this the two tokens could share an iat under load.
	time.Sleep(1100 * time.Millisecond)

	// 2. REFRESH_TOKEN_AUTH — must mint a fresh access token, NO refresh
	// token in the response .
	refreshStatus, refreshBody := postCognito(t, ts.URL, "InitiateAuth", map[string]interface{}{
		"AuthFlow": "REFRESH_TOKEN_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]string{
			"REFRESH_TOKEN": refresh,
		},
	})
	require.Equal(t, http.StatusOK, refreshStatus, "body=%v", refreshBody)

	refreshAuth := readAuthResult(t, refreshBody)
	newAccess, _ := refreshAuth["AccessToken"].(string)
	require.NotEmpty(t, newAccess)
	assert.NotEqual(t, originalAccess, newAccess, "refreshed access token must be distinct")
	assert.Equal(t, "Bearer", refreshAuth["TokenType"])
	assert.Equal(t, float64(3600), refreshAuth["ExpiresIn"])

	// New access token must have later exp than original. Both have the
	// same TTL, so iat shifting forward shifts exp forward too.
	newClaims := parseClaimsUnverified(t, newAccess)
	newExp, _ := newClaims["exp"].(float64)
	assert.Greater(t, newExp, originalExp, "new access exp must be later than original")
	assert.Equal(t, "access", newClaims["token_use"])
	assert.Equal(t, email, newClaims["email"], "refresh path must look up user → email for new access")
	idClaims := parseClaimsUnverified(t, refreshAuth["IdToken"].(string))
	assert.Equal(t, "id", idClaims["token_use"])
	assert.Equal(t, originalAccessClaims["auth_time"], idClaims["auth_time"])
	assert.Equal(t, originalAccessClaims["origin_jti"], idClaims["origin_jti"])
}

// TestInitiateAuth_RefreshTokenAuth_DoesNotRotate verifies that the
// REFRESH_TOKEN_AUTH response MUST NOT include a RefreshToken field. Both
// platform providers fall back to the original (cognito.py:99,
// cognito.go:442); echoing it back would be noise, AWS itself omits it.
func TestInitiateAuth_RefreshTokenAuth_DoesNotRotate(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "local-pool-1"
		clientID = "local-client-1"
		email    = "alice@example.com"
		password = "TempPass1!"
	)
	seedClientPoolUser(t, store, poolID, clientID, email, password)

	loginStatus, loginBody := postCognito(t, ts.URL, "InitiateAuth", map[string]interface{}{
		"AuthFlow":       "USER_PASSWORD_AUTH",
		"ClientId":       clientID,
		"AuthParameters": map[string]string{"USERNAME": email, "PASSWORD": password},
	})
	require.Equal(t, http.StatusOK, loginStatus)
	refresh := readAuthResult(t, loginBody)["RefreshToken"].(string)

	refreshStatus, refreshBody := postCognito(t, ts.URL, "InitiateAuth", map[string]interface{}{
		"AuthFlow":       "REFRESH_TOKEN_AUTH",
		"ClientId":       clientID,
		"AuthParameters": map[string]string{"REFRESH_TOKEN": refresh},
	})
	require.Equal(t, http.StatusOK, refreshStatus)

	auth := readAuthResult(t, refreshBody)
	_, hasRefresh := auth["RefreshToken"]
	assert.False(t, hasRefresh, "REFRESH_TOKEN_AUTH response MUST NOT carry a RefreshToken (design §3f)")
}

// TestInitiateAuth_RefreshTokenAuth_ExpiredRefresh signs a refresh JWT with
// a past `exp` (using SignRefreshToken's underlying primitives directly so
// we don't have to wait), and asserts NotAuthorizedException is returned.
func TestInitiateAuth_RefreshTokenAuth_ExpiredRefresh(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "local-pool-1"
		clientID = "local-client-1"
	)
	require.NoError(t, store.UpsertPool(t.Context(), poolID, "us-east-1"))
	require.NoError(t, store.UpsertClient(t.Context(), clientID, poolID, ""))
	signing, err := store.EnsureSigningKey(t.Context(), poolID)
	require.NoError(t, err)

	// Hand-craft a refresh JWT with iat/exp in the past.
	now := time.Now()
	claims := jwt.MapClaims{
		"sub":       "abc",
		"iss":       "http://localhost:4100/" + poolID,
		"aud":       clientID,
		"iat":       now.Add(-2 * time.Hour).Unix(),
		"exp":       now.Add(-1 * time.Hour).Unix(),
		"token_use": "refresh",
		"client_id": clientID,
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = signing.Kid
	expired, err := tok.SignedString(signing.Private)
	require.NoError(t, err)

	status, body := postCognito(t, ts.URL, "InitiateAuth", map[string]interface{}{
		"AuthFlow":       "REFRESH_TOKEN_AUTH",
		"ClientId":       clientID,
		"AuthParameters": map[string]string{"REFRESH_TOKEN": expired},
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NotAuthorizedException", body["__type"])
}

func TestInitiateAuth_RefreshTokenAuth_MalformedRefresh(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	require.NoError(t, store.UpsertPool(t.Context(), "local-pool-1", "us-east-1"))
	require.NoError(t, store.UpsertClient(t.Context(), "local-client-1", "local-pool-1", ""))

	status, body := postCognito(t, ts.URL, "InitiateAuth", map[string]interface{}{
		"AuthFlow":       "REFRESH_TOKEN_AUTH",
		"ClientId":       "local-client-1",
		"AuthParameters": map[string]string{"REFRESH_TOKEN": "not-a-jwt-at-all"},
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NotAuthorizedException", body["__type"])
}

// TestInitiateAuth_RefreshTokenAuth_WrongTokenUse asserts an access token
// presented as REFRESH_TOKEN must fail verification — the `token_use`
// claim is the only thing that distinguishes the two and we MUST enforce
// it strictly to prevent token-class confusion.
func TestInitiateAuth_RefreshTokenAuth_WrongTokenUse(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "local-pool-1"
		clientID = "local-client-1"
		email    = "alice@example.com"
		password = "TempPass1!"
	)
	seedClientPoolUser(t, store, poolID, clientID, email, password)

	loginStatus, loginBody := postCognito(t, ts.URL, "InitiateAuth", map[string]interface{}{
		"AuthFlow":       "USER_PASSWORD_AUTH",
		"ClientId":       clientID,
		"AuthParameters": map[string]string{"USERNAME": email, "PASSWORD": password},
	})
	require.Equal(t, http.StatusOK, loginStatus)
	access := readAuthResult(t, loginBody)["AccessToken"].(string)

	// Pass the access token where REFRESH_TOKEN is expected.
	status, body := postCognito(t, ts.URL, "InitiateAuth", map[string]interface{}{
		"AuthFlow":       "REFRESH_TOKEN_AUTH",
		"ClientId":       clientID,
		"AuthParameters": map[string]string{"REFRESH_TOKEN": access},
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NotAuthorizedException", body["__type"])
}

// TestInitiateAuth_RefreshTokenAuth_MissingClientId pins the same
// ResourceNotFoundException path the password flow has, just on the refresh
// branch — the dispatch resolves ClientId BEFORE branching on AuthFlow, so
// any unknown client id fails the same way regardless of flow.
func TestInitiateAuth_RefreshTokenAuth_MissingClientId(t *testing.T) {
	_, ts, _ := newCognitoTestServer(t)

	status, body := postCognito(t, ts.URL, "InitiateAuth", map[string]interface{}{
		"AuthFlow":       "REFRESH_TOKEN_AUTH",
		"ClientId":       "no-such-client",
		"AuthParameters": map[string]string{"REFRESH_TOKEN": "anything"},
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "ResourceNotFoundException", body["__type"])
}

// Unsupported flows, admin-only entry points and missing SRP parameters use
// the same AWS InvalidParameterException envelope.
func TestInitiateAuth_BadAuthFlow(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	require.NoError(t, store.UpsertPool(t.Context(), "local-pool-1", "us-east-1"))
	require.NoError(t, store.UpsertClient(t.Context(), "local-client-1", "local-pool-1", ""))

	for _, flow := range []string{"USER_SRP_AUTH", "ADMIN_NO_SRP_AUTH", "BOGUS_FLOW"} {
		t.Run(flow, func(t *testing.T) {
			status, body := postCognito(t, ts.URL, "InitiateAuth", map[string]interface{}{
				"AuthFlow":       flow,
				"ClientId":       "local-client-1",
				"AuthParameters": map[string]string{},
			})
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidParameterException", body["__type"])
		})
	}
}

// TestInitiateAuth_MalformedJSON asserts the readCognitoJSON path. Any
// malformed body must surface InvalidParameterException, not panic.
func TestInitiateAuth_MalformedJSON(t *testing.T) {
	_, ts, _ := newCognitoTestServer(t)

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader("{not-json"))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSCognitoIdentityProviderService.InitiateAuth")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	var body map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "InvalidParameterException", body["__type"])
}
