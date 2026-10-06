// Tests that authentication age survives refresh (organisation backend design
// V2, second review item 6): a refreshed access token renews `exp` and `iat`
// but keeps the original `auth_time`, as real Cognito's refreshed tokens do.
// Without this the local stack cannot prove that the principal's session
// cutoff (judged on auth_time) outlasts refreshing, or that a refresh
// extends the session (judged on iat).
package cognito

import (
	"net/http"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func refreshWith(t *testing.T, baseURL, clientID, refresh string) jwt.MapClaims {
	t.Helper()
	status, body := postCognito(t, baseURL, "InitiateAuth", map[string]interface{}{
		"AuthFlow":       "REFRESH_TOKEN_AUTH",
		"ClientId":       clientID,
		"AuthParameters": map[string]string{"REFRESH_TOKEN": refresh},
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	return parseClaimsUnverified(t, readAuthResult(t, body)["AccessToken"].(string))
}

func claimUnix(t *testing.T, claims jwt.MapClaims, name string) int64 {
	t.Helper()
	value, ok := claims[name].(float64)
	require.True(t, ok, "claim %s missing or not numeric: %v", name, claims[name])
	return int64(value)
}

// A password sign-in stamps one authentication time on both tokens.
func TestAuthTime_PasswordSignInStampsBothTokens(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "local-pool-1"
		clientID = "local-client-1"
		email    = "alice@example.com"
		password = "TempPass1!"
	)
	seedClientPoolUser(t, store, poolID, clientID, email, password)

	before := time.Now().Unix()
	status, body := postCognito(t, ts.URL, "InitiateAuth", map[string]interface{}{
		"AuthFlow":       "USER_PASSWORD_AUTH",
		"ClientId":       clientID,
		"AuthParameters": map[string]string{"USERNAME": email, "PASSWORD": password},
	})
	require.Equal(t, http.StatusOK, status)
	result := readAuthResult(t, body)
	access := parseClaimsUnverified(t, result["AccessToken"].(string))
	refresh := parseClaimsUnverified(t, result["RefreshToken"].(string))

	authTime := claimUnix(t, access, "auth_time")
	assert.GreaterOrEqual(t, authTime, before)
	assert.Equal(t, authTime, claimUnix(t, refresh, "auth_time"))
}

// Refresh keeps the authentication time of the refresh token it was minted
// from, however long ago that was, while `iat` and `exp` move forward.
func TestAuthTime_RefreshPreservesOriginalAuthentication(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "local-pool-1"
		clientID = "local-client-1"
		email    = "alice@example.com"
	)
	sub := seedClientPoolUser(t, store, poolID, clientID, email, "TempPass1!")

	authenticatedAt := time.Now().Add(-11 * time.Hour).Truncate(time.Second)
	refresh, err := SignRefreshToken(
		t.Context(), store, "http://localhost:4100", poolID, clientID, sub,
		tokenGrant{AuthTime: authenticatedAt, OriginJTI: newJTI()}, 24*time.Hour,
	)
	require.NoError(t, err)

	first := refreshWith(t, ts.URL, clientID, refresh)
	assert.Equal(t, authenticatedAt.Unix(), claimUnix(t, first, "auth_time"),
		"a refreshed token must carry the original authentication time")
	assert.Greater(t, claimUnix(t, first, "iat"), authenticatedAt.Unix(),
		"iat is the mint time, not the authentication time")

	// Refreshing again does not move it either: refresh is not authentication.
	second := refreshWith(t, ts.URL, clientID, refresh)
	assert.Equal(t, authenticatedAt.Unix(), claimUnix(t, second, "auth_time"))
}

// A refresh token minted before refresh tokens carried `auth_time` falls back
// to its `iat`, the moment it was issued at authentication, never to now.
func TestAuthTime_LegacyRefreshTokenUsesItsIssueTime(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "local-pool-1"
		clientID = "local-client-1"
		email    = "alice@example.com"
	)
	sub := seedClientPoolUser(t, store, poolID, clientID, email, "TempPass1!")
	signing, err := store.EnsureSigningKey(t.Context(), poolID)
	require.NoError(t, err)

	issuedAt := time.Now().Add(-3 * time.Hour).Truncate(time.Second)
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"sub":       sub,
		"iss":       "http://localhost:4100/" + poolID,
		"aud":       clientID,
		"iat":       issuedAt.Unix(),
		"exp":       time.Now().Add(time.Hour).Unix(),
		"token_use": "refresh",
		"client_id": clientID,
		"jti":       newJTI(),
	})
	tok.Header["kid"] = signing.Kid
	legacy, err := tok.SignedString(signing.Private)
	require.NoError(t, err)

	claims := refreshWith(t, ts.URL, clientID, legacy)
	assert.Equal(t, issuedAt.Unix(), claimUnix(t, claims, "auth_time"))
}

// A challenge completion is an authentication: it starts a new age.
func TestAuthTime_ChallengeCompletionStartsNewAuthentication(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "local-pool-1"
		clientID = "local-client-1"
		email    = "mfa@example.com"
		password = "TempPass1!"
	)
	seedMFAUser(t, store, poolID, clientID, email, password)
	session := initiateMFAChallenge(t, ts.URL, clientID, email, password)

	before := time.Now().Unix()
	status, body := postCognito(t, ts.URL, "RespondToAuthChallenge", map[string]interface{}{
		"ClientId":      clientID,
		"ChallengeName": "SOFTWARE_TOKEN_MFA",
		"Session":       session,
		"ChallengeResponses": map[string]string{
			"USERNAME":                email,
			"SOFTWARE_TOKEN_MFA_CODE": "123456",
		},
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	result := readAuthResult(t, body)
	access := parseClaimsUnverified(t, result["AccessToken"].(string))
	refresh := parseClaimsUnverified(t, result["RefreshToken"].(string))
	assert.GreaterOrEqual(t, claimUnix(t, access, "auth_time"), before)
	assert.Equal(t, claimUnix(t, access, "auth_time"), claimUnix(t, refresh, "auth_time"))
}
