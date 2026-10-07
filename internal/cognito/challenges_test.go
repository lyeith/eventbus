// MFA, new-password and admin-authentication wire contracts.
package cognito

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
)

// --- cognito_session.go unit tests --------------------------------------

func TestEncodeChallengeSession_RoundTrip(t *testing.T) {
	pem := []byte("-----BEGIN PRIVATE KEY-----\nfake\n-----END PRIVATE KEY-----\n")
	wire, prefix, err := EncodeChallengeSession(pem)
	require.NoError(t, err)
	assert.NotEmpty(t, wire)

	// Verifying the wire string with the same key recovers the prefix.
	got, err := VerifyChallengeSession(wire, pem)
	require.NoError(t, err)
	assert.Equal(t, prefix, got)
}

func TestVerifyChallengeSession_BadHMAC(t *testing.T) {
	pem := []byte("the-real-key")
	wire, _, err := EncodeChallengeSession(pem)
	require.NoError(t, err)

	// Different key → HMAC mismatch.
	_, err = VerifyChallengeSession(wire, []byte("other-key"))
	assert.Error(t, err)
}

func TestVerifyChallengeSession_Malformed(t *testing.T) {
	pem := []byte("k")
	_, err := VerifyChallengeSession("not-base64!@#", pem)
	assert.Error(t, err, "malformed base64 must fail")

	_, err = VerifyChallengeSession("", pem)
	assert.Error(t, err, "empty session must fail")

	// Valid base64 but wrong length.
	_, err = VerifyChallengeSession("YWFhYWFh", pem) // "aaaaaa"
	assert.Error(t, err, "wrong length must fail")
}

// --- challenge_sessions DB tests ---------------------------------------

func TestChallengeSessions_RoundTrip(t *testing.T) {
	store, _ := newCognitoTestStore(t)
	ctx := t.Context()
	require.NoError(t, store.UpsertPool(ctx, "pool-1", "us-east-1"))
	require.NoError(t, store.CreateUser(ctx, "sub-1", "pool-1", "session@example.test", "hash", false))

	require.NoError(t, store.CreateChallengeSession(ctx, "sess-1", "sub-1", "pool-1", "client-1", "SOFTWARE_TOKEN_MFA", time.Minute))
	row, err := store.LookupChallengeSession(ctx, "sess-1")
	require.NoError(t, err)
	assert.Equal(t, "sub-1", row.Sub)
	assert.Equal(t, "pool-1", row.PoolID)
	assert.Equal(t, "client-1", row.ClientID)
	assert.Equal(t, "SOFTWARE_TOKEN_MFA", row.ChallengeName)
	assert.False(t, row.Used)

	require.NoError(t, store.MarkChallengeSessionUsed(ctx, "sess-1"))
	row, err = store.LookupChallengeSession(ctx, "sess-1")
	require.NoError(t, err)
	assert.True(t, row.Used)
}

func TestChallengeSessionCleanup(t *testing.T) {
	store, _ := newCognitoTestStore(t)
	ctx := t.Context()
	require.NoError(t, store.UpsertPool(ctx, "p", "us-east-1"))
	require.NoError(t, store.CreateUser(ctx, "s", "p", "cleanup@example.test", "hash", false))

	// One that's already expired (negative TTL → expires_at < now immediately).
	require.NoError(t, store.CreateChallengeSession(ctx, "old", "s", "p", "c", "SOFTWARE_TOKEN_MFA", -time.Hour))
	// One that's still valid.
	require.NoError(t, store.CreateChallengeSession(ctx, "fresh", "s", "p", "c", "SOFTWARE_TOKEN_MFA", time.Hour))

	deleted, err := store.DeleteExpiredChallengeSessions(ctx, time.Now().Unix())
	require.NoError(t, err)
	assert.Equal(t, 1, deleted)

	_, err = store.LookupChallengeSession(ctx, "old")
	assert.Error(t, err, "expired row should be gone")
	_, err = store.LookupChallengeSession(ctx, "fresh")
	assert.NoError(t, err, "fresh row should remain")
}

// --- helpers -----------------------------------------------------------

// seedMFAUser registers a pool/client/user with mfa_enabled=true. Returns
// the user's sub for the caller's assertions.
func seedMFAUser(t *testing.T, store *CognitoStore, poolID, clientID, email, password string) string {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, store.UpsertPool(ctx, poolID, "us-east-1"))
	require.NoError(t, store.UpsertClient(ctx, clientID, poolID, ""))

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	require.NoError(t, err)
	sub, err := store.UpsertUser(ctx, poolID, email, string(hash), true) // mfa_enabled=true
	require.NoError(t, err)
	require.NoError(t, store.SetUserAttribute(ctx, sub, "email", email))
	require.NoError(t, store.SetUserAttribute(ctx, sub, "email_verified", "true"))
	require.NoError(t, store.SetUserAttribute(ctx, sub, "sub", sub))
	return sub
}

// seedSessionRow inserts a challenge_sessions row directly and returns the
// wire-encoded Session string the client would carry. Bypasses
// InitiateAuth so tests can pre-populate sessions for non-MFA challenge
// types (e.g. NEW_PASSWORD_REQUIRED is not currently issued by any
// InitiateAuth path; it's set up here for direct testing).
func seedSessionRow(t *testing.T, store *CognitoStore, sub, poolID, clientID, challengeName string, ttl time.Duration) string {
	t.Helper()
	ctx := context.Background()
	signing, err := store.EnsureSigningKey(ctx, poolID)
	require.NoError(t, err)
	privPEM, err := encodePrivateKeyPEM(signing.Private)
	require.NoError(t, err)
	wire, prefix, err := EncodeChallengeSession([]byte(privPEM))
	require.NoError(t, err)
	dbKey := challengeSessionDBKey(prefix)
	require.NoError(t, store.CreateChallengeSession(ctx, dbKey, sub, poolID, clientID, challengeName, ttl))
	return wire
}

// initiateMFAChallenge runs USER_PASSWORD_AUTH for an MFA-enabled user
// and returns the Session string from the resulting challenge response.
func initiateMFAChallenge(t *testing.T, baseURL, clientID, email, password string) string {
	t.Helper()
	status, body := postCognito(t, baseURL, "InitiateAuth", map[string]interface{}{
		"AuthFlow": "USER_PASSWORD_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]string{
			"USERNAME": email,
			"PASSWORD": password,
		},
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	require.Equal(t, "SOFTWARE_TOKEN_MFA", body["ChallengeName"])
	session, _ := body["Session"].(string)
	require.NotEmpty(t, session)
	return session
}

// --- RespondToAuthChallenge: SOFTWARE_TOKEN_MFA -------------------------

func TestRespondToAuthChallenge_SoftwareTokenMFA_HappyPath(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "local-pool-1"
		clientID = "local-client-1"
		email    = "mfa@example.com"
		password = "TempPass1!"
	)
	sub := seedMFAUser(t, store, poolID, clientID, email, password)
	session := initiateMFAChallenge(t, ts.URL, clientID, email, password)

	status, body := postCognito(t, ts.URL, "RespondToAuthChallenge", map[string]interface{}{
		"ChallengeName": "SOFTWARE_TOKEN_MFA",
		"ClientId":      clientID,
		"Session":       session,
		"ChallengeResponses": map[string]string{
			"USERNAME":                email,
			"SOFTWARE_TOKEN_MFA_CODE": "123456",
		},
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)

	auth, ok := body["AuthenticationResult"].(map[string]interface{})
	require.True(t, ok, "AuthenticationResult missing")
	access, _ := auth["AccessToken"].(string)
	refresh, _ := auth["RefreshToken"].(string)
	require.NotEmpty(t, access)
	require.NotEmpty(t, refresh)
	assert.Equal(t, "Bearer", auth["TokenType"])
	assert.Equal(t, float64(3600), auth["ExpiresIn"])

	// Access token claims sanity check.
	parser := jwt.NewParser(jwt.WithValidMethods([]string{"RS256"}))
	parsed, _, err := parser.ParseUnverified(access, jwt.MapClaims{})
	require.NoError(t, err)
	claims, _ := parsed.Claims.(jwt.MapClaims)
	assert.Equal(t, sub, claims["sub"])
	assert.Equal(t, email, claims["email"])
	assert.Equal(t, "access", claims["token_use"])
}

func TestRespondToAuthChallenge_SmsMFA_HappyPath(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "local-pool-1"
		clientID = "local-client-1"
		email    = "sms@example.com"
		password = "TempPass1!"
	)
	sub := seedMFAUser(t, store, poolID, clientID, email, password)

	// SMS_MFA is not auto-issued by InitiateAuth (which always picks
	// SOFTWARE_TOKEN_MFA today). Seed a row directly so the response path
	// can be exercised.
	session := seedSessionRow(t, store, sub, poolID, clientID, "SMS_MFA", time.Minute)

	status, body := postCognito(t, ts.URL, "RespondToAuthChallenge", map[string]interface{}{
		"ChallengeName": "SMS_MFA",
		"ClientId":      clientID,
		"Session":       session,
		"ChallengeResponses": map[string]string{
			"USERNAME":     email,
			"SMS_MFA_CODE": "654321",
		},
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)

	auth, ok := body["AuthenticationResult"].(map[string]interface{})
	require.True(t, ok)
	assert.NotEmpty(t, auth["AccessToken"])
	assert.NotEmpty(t, auth["RefreshToken"])
}

// --- RespondToAuthChallenge: NEW_PASSWORD_REQUIRED ---------------------

func TestRespondToAuthChallenge_NewPasswordRequired_HappyPath(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID      = "local-pool-1"
		clientID    = "local-client-1"
		email       = "newpw@example.com"
		oldPassword = "Temp!"
		newPassword = "ShinyNew1!"
	)
	ctx := t.Context()
	require.NoError(t, store.UpsertPool(ctx, poolID, "us-east-1"))
	require.NoError(t, store.UpsertClient(ctx, clientID, poolID, ""))
	hash, err := bcrypt.GenerateFromPassword([]byte(oldPassword), bcrypt.MinCost)
	require.NoError(t, err)
	sub, err := store.UpsertUser(ctx, poolID, email, string(hash), false)
	require.NoError(t, err)

	// A NEW_PASSWORD_REQUIRED session belongs to an invited account.
	require.NoError(t, store.SetUserPassword(ctx, sub, oldPassword, "FORCE_CHANGE_PASSWORD"))
	// Pre-populate a NEW_PASSWORD_REQUIRED session row.
	session := seedSessionRow(t, store, sub, poolID, clientID, "NEW_PASSWORD_REQUIRED", time.Minute)

	status, body := postCognito(t, ts.URL, "RespondToAuthChallenge", map[string]interface{}{
		"ChallengeName": "NEW_PASSWORD_REQUIRED",
		"ClientId":      clientID,
		"Session":       session,
		"ChallengeResponses": map[string]string{
			"USERNAME":     email,
			"NEW_PASSWORD": newPassword,
		},
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)

	auth, ok := body["AuthenticationResult"].(map[string]interface{})
	require.True(t, ok)
	assert.NotEmpty(t, auth["AccessToken"])
	assert.NotEmpty(t, auth["RefreshToken"])

	// Verify the password was actually rotated — old password no longer
	// authenticates, new one does.
	wrongStatus, _ := postCognito(t, ts.URL, "InitiateAuth", map[string]interface{}{
		"AuthFlow": "USER_PASSWORD_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]string{
			"USERNAME": email,
			"PASSWORD": oldPassword,
		},
	})
	assert.Equal(t, http.StatusBadRequest, wrongStatus, "old password must no longer work")

	rightStatus, rightBody := postCognito(t, ts.URL, "InitiateAuth", map[string]interface{}{
		"AuthFlow": "USER_PASSWORD_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]string{
			"USERNAME": email,
			"PASSWORD": newPassword,
		},
	})
	require.Equal(t, http.StatusOK, rightStatus, "new password must authenticate; body=%v", rightBody)
}

func TestRespondToAuthChallenge_NewPasswordRequired_EmptyPassword(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "local-pool-1"
		clientID = "local-client-1"
		email    = "newpw@example.com"
	)
	ctx := t.Context()
	require.NoError(t, store.UpsertPool(ctx, poolID, "us-east-1"))
	require.NoError(t, store.UpsertClient(ctx, clientID, poolID, ""))
	sub, err := store.UpsertUser(ctx, poolID, email, "", false)
	require.NoError(t, err)
	session := seedSessionRow(t, store, sub, poolID, clientID, "NEW_PASSWORD_REQUIRED", time.Minute)

	status, body := postCognito(t, ts.URL, "RespondToAuthChallenge", map[string]interface{}{
		"ChallengeName": "NEW_PASSWORD_REQUIRED",
		"ClientId":      clientID,
		"Session":       session,
		"ChallengeResponses": map[string]string{
			"USERNAME":     email,
			"NEW_PASSWORD": "",
		},
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterException", body["__type"])
}

// --- RespondToAuthChallenge: error paths --------------------------------

func TestRespondToAuthChallenge_WrongCode(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "local-pool-1"
		clientID = "local-client-1"
		email    = "mfa@example.com"
		password = "TempPass1!"
	)
	seedMFAUser(t, store, poolID, clientID, email, password)
	session := initiateMFAChallenge(t, ts.URL, clientID, email, password)

	cases := []string{"abcdef", "12345", "1234567", "", "1 2345"}
	for _, code := range cases {
		t.Run(code, func(t *testing.T) {
			status, body := postCognito(t, ts.URL, "RespondToAuthChallenge", map[string]interface{}{
				"ChallengeName": "SOFTWARE_TOKEN_MFA",
				"ClientId":      clientID,
				"Session":       session,
				"ChallengeResponses": map[string]string{
					"USERNAME":                email,
					"SOFTWARE_TOKEN_MFA_CODE": code,
				},
			})
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "CodeMismatchException", body["__type"])
		})
	}
}

func TestRespondToAuthChallenge_ExpiredSession(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "local-pool-1"
		clientID = "local-client-1"
		email    = "mfa@example.com"
		password = "TempPass1!"
	)
	sub := seedMFAUser(t, store, poolID, clientID, email, password)

	// Negative TTL → row's expires_at is already in the past.
	session := seedSessionRow(t, store, sub, poolID, clientID, "SOFTWARE_TOKEN_MFA", -time.Hour)

	status, body := postCognito(t, ts.URL, "RespondToAuthChallenge", map[string]interface{}{
		"ChallengeName": "SOFTWARE_TOKEN_MFA",
		"ClientId":      clientID,
		"Session":       session,
		"ChallengeResponses": map[string]string{
			"USERNAME":                email,
			"SOFTWARE_TOKEN_MFA_CODE": "123456",
		},
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "ExpiredCodeException", body["__type"])
}

func TestRespondToAuthChallenge_Replay(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "local-pool-1"
		clientID = "local-client-1"
		email    = "mfa@example.com"
		password = "TempPass1!"
	)
	seedMFAUser(t, store, poolID, clientID, email, password)
	session := initiateMFAChallenge(t, ts.URL, clientID, email, password)

	first, _ := postCognito(t, ts.URL, "RespondToAuthChallenge", map[string]interface{}{
		"ChallengeName": "SOFTWARE_TOKEN_MFA",
		"ClientId":      clientID,
		"Session":       session,
		"ChallengeResponses": map[string]string{
			"USERNAME":                email,
			"SOFTWARE_TOKEN_MFA_CODE": "123456",
		},
	})
	require.Equal(t, http.StatusOK, first)

	// Same Session, same code: must fail with NotAuthorizedException.
	status, body := postCognito(t, ts.URL, "RespondToAuthChallenge", map[string]interface{}{
		"ChallengeName": "SOFTWARE_TOKEN_MFA",
		"ClientId":      clientID,
		"Session":       session,
		"ChallengeResponses": map[string]string{
			"USERNAME":                email,
			"SOFTWARE_TOKEN_MFA_CODE": "123456",
		},
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NotAuthorizedException", body["__type"])
}

func TestRespondToAuthChallenge_ForgedSession(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "local-pool-1"
		clientID = "local-client-1"
	)
	require.NoError(t, store.UpsertPool(t.Context(), poolID, "us-east-1"))
	require.NoError(t, store.UpsertClient(t.Context(), clientID, poolID, ""))
	// Need a signing key wired up so VerifyChallengeSession has a key
	// to compare against.
	_, err := store.EnsureSigningKey(t.Context(), poolID)
	require.NoError(t, err)

	// Forge a session: generate one with a different "key" so the HMAC
	// fails the real key's check.
	forged, _, err := EncodeChallengeSession([]byte("not-the-real-key"))
	require.NoError(t, err)

	status, body := postCognito(t, ts.URL, "RespondToAuthChallenge", map[string]interface{}{
		"ChallengeName": "SOFTWARE_TOKEN_MFA",
		"ClientId":      clientID,
		"Session":       forged,
		"ChallengeResponses": map[string]string{
			"USERNAME":                "anyone@example.com",
			"SOFTWARE_TOKEN_MFA_CODE": "123456",
		},
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NotAuthorizedException", body["__type"])
}

func TestRespondToAuthChallenge_UnknownSession(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "local-pool-1"
		clientID = "local-client-1"
	)
	require.NoError(t, store.UpsertPool(t.Context(), poolID, "us-east-1"))
	require.NoError(t, store.UpsertClient(t.Context(), clientID, poolID, ""))
	signing, err := store.EnsureSigningKey(t.Context(), poolID)
	require.NoError(t, err)
	privPEM, err := encodePrivateKeyPEM(signing.Private)
	require.NoError(t, err)

	// Encode with the real key — HMAC verifies — but never insert into
	// challenge_sessions, so the DB lookup returns sql.ErrNoRows.
	wire, _, err := EncodeChallengeSession([]byte(privPEM))
	require.NoError(t, err)

	status, body := postCognito(t, ts.URL, "RespondToAuthChallenge", map[string]interface{}{
		"ChallengeName": "SOFTWARE_TOKEN_MFA",
		"ClientId":      clientID,
		"Session":       wire,
		"ChallengeResponses": map[string]string{
			"USERNAME":                "anyone@example.com",
			"SOFTWARE_TOKEN_MFA_CODE": "123456",
		},
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NotAuthorizedException", body["__type"])
}

func TestRespondToAuthChallenge_ChallengeTypeMismatch(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "local-pool-1"
		clientID = "local-client-1"
		email    = "mfa@example.com"
		password = "TempPass1!"
	)
	sub := seedMFAUser(t, store, poolID, clientID, email, password)

	// Stored as SOFTWARE_TOKEN_MFA, request claims SMS_MFA.
	session := seedSessionRow(t, store, sub, poolID, clientID, "SOFTWARE_TOKEN_MFA", time.Minute)

	status, body := postCognito(t, ts.URL, "RespondToAuthChallenge", map[string]interface{}{
		"ChallengeName": "SMS_MFA",
		"ClientId":      clientID,
		"Session":       session,
		"ChallengeResponses": map[string]string{
			"USERNAME":     email,
			"SMS_MFA_CODE": "123456",
		},
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NotAuthorizedException", body["__type"])
}

func TestRespondToAuthChallenge_UnsupportedChallengeName(t *testing.T) {
	_, ts, _ := newCognitoTestServer(t)
	status, body := postCognito(t, ts.URL, "RespondToAuthChallenge", map[string]interface{}{
		"ChallengeName":      "BOGUS_CHALLENGE",
		"ClientId":           "any",
		"Session":            "any",
		"ChallengeResponses": map[string]string{},
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterException", body["__type"])
}

func TestRespondToAuthChallenge_UnknownClient(t *testing.T) {
	_, ts, _ := newCognitoTestServer(t)
	status, body := postCognito(t, ts.URL, "RespondToAuthChallenge", map[string]interface{}{
		"ChallengeName":      "SOFTWARE_TOKEN_MFA",
		"ClientId":           "no-such-client",
		"Session":            "anything",
		"ChallengeResponses": map[string]string{},
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "ResourceNotFoundException", body["__type"])
}

// --- AdminInitiateAuth --------------------------------------------------

func TestAdminInitiateAuth_AdminNoSrpAuth_HappyPath(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "local-pool-1"
		clientID = "local-client-1"
		email    = "magic@example.com"
		password = "AdminGenerated1!"
	)
	// A user without MFA gets tokens straight away; an MFA user is
	// challenged (TestAdminInitiateAuth_MFAUserIsChallenged).
	sub := seedClientPoolUser(t, store, poolID, clientID, email, password)

	status, body := postCognito(t, ts.URL, "AdminInitiateAuth", map[string]interface{}{
		"UserPoolId": poolID,
		"ClientId":   clientID,
		"AuthFlow":   "ADMIN_NO_SRP_AUTH",
		"AuthParameters": map[string]string{
			"USERNAME": email,
			"PASSWORD": password,
		},
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)

	// No challenge — straight tokens.
	_, hasChallenge := body["ChallengeName"]
	_, hasSession := body["Session"]
	assert.False(t, hasChallenge, "AdminInitiateAuth issues no challenge to a user without MFA")
	assert.False(t, hasSession)

	auth, ok := body["AuthenticationResult"].(map[string]interface{})
	require.True(t, ok)
	access, _ := auth["AccessToken"].(string)
	refresh, _ := auth["RefreshToken"].(string)
	require.NotEmpty(t, access)
	require.NotEmpty(t, refresh)
	assert.Equal(t, "Bearer", auth["TokenType"])
	assert.Equal(t, float64(3600), auth["ExpiresIn"])

	// Sub must match.
	parser := jwt.NewParser(jwt.WithValidMethods([]string{"RS256"}))
	parsed, _, err := parser.ParseUnverified(access, jwt.MapClaims{})
	require.NoError(t, err)
	claims, _ := parsed.Claims.(jwt.MapClaims)
	assert.Equal(t, sub, claims["sub"])
	assert.Equal(t, email, claims["email"])
}

func TestAdminInitiateAuth_UnknownUser(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "local-pool-1"
		clientID = "local-client-1"
	)
	require.NoError(t, store.UpsertPool(t.Context(), poolID, "us-east-1"))
	require.NoError(t, store.UpsertClient(t.Context(), clientID, poolID, ""))

	status, body := postCognito(t, ts.URL, "AdminInitiateAuth", map[string]interface{}{
		"UserPoolId": poolID,
		"ClientId":   clientID,
		"AuthFlow":   "ADMIN_NO_SRP_AUTH",
		"AuthParameters": map[string]string{
			"USERNAME": "ghost@example.com",
			"PASSWORD": "anything",
		},
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "UserNotFoundException", body["__type"])
}

func TestAdminInitiateAuth_WrongPassword(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "local-pool-1"
		clientID = "local-client-1"
		email    = "alice@example.com"
		password = "Right!"
	)
	seedClientPoolUser(t, store, poolID, clientID, email, password)

	status, body := postCognito(t, ts.URL, "AdminInitiateAuth", map[string]interface{}{
		"UserPoolId": poolID,
		"ClientId":   clientID,
		"AuthFlow":   "ADMIN_NO_SRP_AUTH",
		"AuthParameters": map[string]string{
			"USERNAME": email,
			"PASSWORD": "Wrong!",
		},
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NotAuthorizedException", body["__type"])
}

func TestAdminInitiateAuth_UnsupportedAuthFlow(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "local-pool-1"
		clientID = "local-client-1"
	)
	require.NoError(t, store.UpsertPool(t.Context(), poolID, "us-east-1"))
	require.NoError(t, store.UpsertClient(t.Context(), clientID, poolID, ""))

	for _, flow := range []string{"USER_PASSWORD_AUTH", "REFRESH_TOKEN_AUTH", "USER_SRP_AUTH", ""} {
		t.Run(flow, func(t *testing.T) {
			status, body := postCognito(t, ts.URL, "AdminInitiateAuth", map[string]interface{}{
				"UserPoolId": poolID,
				"ClientId":   clientID,
				"AuthFlow":   flow,
				"AuthParameters": map[string]string{
					"USERNAME": "anyone@example.com",
					"PASSWORD": "anything",
				},
			})
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidParameterException", body["__type"])
		})
	}
}

func TestAdminInitiateAuth_UnknownPool(t *testing.T) {
	_, ts, _ := newCognitoTestServer(t)
	status, body := postCognito(t, ts.URL, "AdminInitiateAuth", map[string]interface{}{
		"UserPoolId": "no-such-pool",
		"ClientId":   "no-client",
		"AuthFlow":   "ADMIN_NO_SRP_AUTH",
		"AuthParameters": map[string]string{
			"USERNAME": "anyone@example.com",
			"PASSWORD": "anything",
		},
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "ResourceNotFoundException", body["__type"])
}

func TestAdminInitiateAuth_UnknownClient(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	require.NoError(t, store.UpsertPool(t.Context(), "real-pool", "us-east-1"))

	status, body := postCognito(t, ts.URL, "AdminInitiateAuth", map[string]interface{}{
		"UserPoolId": "real-pool",
		"ClientId":   "no-such-client",
		"AuthFlow":   "ADMIN_NO_SRP_AUTH",
		"AuthParameters": map[string]string{
			"USERNAME": "anyone@example.com",
			"PASSWORD": "anything",
		},
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "ResourceNotFoundException", body["__type"])
}
