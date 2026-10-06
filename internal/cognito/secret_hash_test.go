package cognito

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
)

// seedClientPoolUserWithSecret is the secret-bearing analogue of
// seedClientPoolUser — registers a pool, a client WITH a non-empty
// secret, and a user. Returned `sub` is for any post-checks.
func seedClientPoolUserWithSecret(t *testing.T, store *CognitoStore, poolID, clientID, secret, email, password string) string {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, store.UpsertPool(ctx, poolID, "us-east-1"))
	require.NoError(t, store.UpsertClient(ctx, clientID, poolID, secret))
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	require.NoError(t, err)
	sub, err := store.UpsertUser(ctx, poolID, email, string(hash), false)
	require.NoError(t, err)
	require.NoError(t, store.SetUserAttribute(ctx, sub, "email", email))
	return sub
}

func TestInitiateAuth_UserPasswordAuth_SecretHashRequired_HappyPath(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "p-secret-happy"
		clientID = "c-secret-happy"
		secret   = "shh-its-a-secret"
		email    = "alice@example.com"
		password = "TempPass1!"
	)
	seedClientPoolUserWithSecret(t, store, poolID, clientID, secret, email, password)

	expectedHash := computeSecretHash(secret, email, clientID)

	status, body := postCognito(t, ts.URL, "InitiateAuth", map[string]interface{}{
		"AuthFlow": "USER_PASSWORD_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]string{
			"USERNAME":    email,
			"PASSWORD":    password,
			"SECRET_HASH": expectedHash,
		},
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	auth, ok := body["AuthenticationResult"].(map[string]interface{})
	require.True(t, ok)
	assert.NotEmpty(t, auth["AccessToken"])
}

func TestInitiateAuth_UserPasswordAuth_SecretHashMismatch(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "p-secret-mismatch"
		clientID = "c-secret-mismatch"
		secret   = "shh-its-a-secret"
		email    = "alice@example.com"
		password = "TempPass1!"
	)
	seedClientPoolUserWithSecret(t, store, poolID, clientID, secret, email, password)

	status, body := postCognito(t, ts.URL, "InitiateAuth", map[string]interface{}{
		"AuthFlow": "USER_PASSWORD_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]string{
			"USERNAME":    email,
			"PASSWORD":    password,
			"SECRET_HASH": "totally-bogus",
		},
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NotAuthorizedException", body["__type"])
}

func TestInitiateAuth_UserPasswordAuth_SecretHashMissingWhenRequired(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "p-secret-missing"
		clientID = "c-secret-missing"
		secret   = "shh-its-a-secret"
		email    = "alice@example.com"
		password = "TempPass1!"
	)
	seedClientPoolUserWithSecret(t, store, poolID, clientID, secret, email, password)

	status, body := postCognito(t, ts.URL, "InitiateAuth", map[string]interface{}{
		"AuthFlow": "USER_PASSWORD_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]string{
			"USERNAME": email,
			"PASSWORD": password,
			// SECRET_HASH absent — client has secret, must be rejected.
		},
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NotAuthorizedException", body["__type"])
}

func TestInitiateAuth_UserPasswordAuth_SecretHashIgnoredWhenClientHasNoSecret(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "local-pool-1"
		clientID = "local-client-1"
		email    = "alice@example.com"
		password = "TempPass1!"
	)
	// No secret on the client — passing SECRET_HASH must be silently ignored.
	seedClientPoolUser(t, store, poolID, clientID, email, password)

	status, body := postCognito(t, ts.URL, "InitiateAuth", map[string]interface{}{
		"AuthFlow": "USER_PASSWORD_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]string{
			"USERNAME":    email,
			"PASSWORD":    password,
			"SECRET_HASH": "anything-here",
		},
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
}

func TestInitiateAuth_RefreshTokenAuth_SecretHashRequired_HappyPath(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "p-refresh-secret"
		clientID = "c-refresh-secret"
		secret   = "refresh-secret"
		email    = "alice@example.com"
		password = "TempPass1!"
	)
	seedClientPoolUserWithSecret(t, store, poolID, clientID, secret, email, password)

	// 1. USER_PASSWORD_AUTH to get a refresh token (with the right hash).
	loginStatus, loginBody := postCognito(t, ts.URL, "InitiateAuth", map[string]interface{}{
		"AuthFlow": "USER_PASSWORD_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]string{
			"USERNAME":    email,
			"PASSWORD":    password,
			"SECRET_HASH": computeSecretHash(secret, email, clientID),
		},
	})
	require.Equal(t, http.StatusOK, loginStatus, "body=%v", loginBody)
	refresh := readAuthResult(t, loginBody)["RefreshToken"].(string)

	// Username is a sign-in attribute in this pool, so refresh hashes use
	// the canonical username claim, which this legacy fixture sets to email.
	status, body := postCognito(t, ts.URL, "InitiateAuth", map[string]interface{}{
		"AuthFlow": "REFRESH_TOKEN_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]string{
			"REFRESH_TOKEN": refresh,
			"SECRET_HASH":   computeSecretHash(secret, email, clientID),
		},
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	auth, ok := body["AuthenticationResult"].(map[string]interface{})
	require.True(t, ok)
	assert.NotEmpty(t, auth["AccessToken"])
}

func TestInitiateAuth_RefreshTokenAuth_SecretHashMismatch(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "p-refresh-mismatch"
		clientID = "c-refresh-mismatch"
		secret   = "refresh-secret"
		email    = "alice@example.com"
		password = "TempPass1!"
	)
	seedClientPoolUserWithSecret(t, store, poolID, clientID, secret, email, password)

	// Get a refresh token via the password flow.
	loginStatus, loginBody := postCognito(t, ts.URL, "InitiateAuth", map[string]interface{}{
		"AuthFlow": "USER_PASSWORD_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]string{
			"USERNAME":    email,
			"PASSWORD":    password,
			"SECRET_HASH": computeSecretHash(secret, email, clientID),
		},
	})
	require.Equal(t, http.StatusOK, loginStatus)
	refresh := readAuthResult(t, loginBody)["RefreshToken"].(string)

	status, body := postCognito(t, ts.URL, "InitiateAuth", map[string]interface{}{
		"AuthFlow": "REFRESH_TOKEN_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]string{
			"REFRESH_TOKEN": refresh,
			"SECRET_HASH":   "totally-bogus",
		},
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NotAuthorizedException", body["__type"])
}

func TestAdminInitiateAuth_SecretHashRequired_HappyPath(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "p-admin-secret"
		clientID = "c-admin-secret"
		secret   = "admin-secret"
		email    = "alice@example.com"
		password = "TempPass1!"
	)
	seedClientPoolUserWithSecret(t, store, poolID, clientID, secret, email, password)

	status, body := postCognito(t, ts.URL, "AdminInitiateAuth", map[string]interface{}{
		"UserPoolId": poolID,
		"ClientId":   clientID,
		"AuthFlow":   "ADMIN_NO_SRP_AUTH",
		"AuthParameters": map[string]string{
			"USERNAME":    email,
			"PASSWORD":    password,
			"SECRET_HASH": computeSecretHash(secret, email, clientID),
		},
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
}

func TestAdminInitiateAuth_SecretHashMismatch(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "p-admin-mismatch"
		clientID = "c-admin-mismatch"
		secret   = "admin-secret"
		email    = "alice@example.com"
		password = "TempPass1!"
	)
	seedClientPoolUserWithSecret(t, store, poolID, clientID, secret, email, password)

	status, body := postCognito(t, ts.URL, "AdminInitiateAuth", map[string]interface{}{
		"UserPoolId": poolID,
		"ClientId":   clientID,
		"AuthFlow":   "ADMIN_NO_SRP_AUTH",
		"AuthParameters": map[string]string{
			"USERNAME":    email,
			"PASSWORD":    password,
			"SECRET_HASH": "wrong",
		},
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NotAuthorizedException", body["__type"])
}

func TestRespondToAuthChallenge_SecretHashEnforcedOnChallengeResponses(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "p-chal-secret"
		clientID = "c-chal-secret"
		secret   = "chal-secret"
		email    = "mfa@example.com"
		password = "TempPass1!"
	)
	ctx := context.Background()
	require.NoError(t, store.UpsertPool(ctx, poolID, "us-east-1"))
	require.NoError(t, store.UpsertClient(ctx, clientID, poolID, secret))
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	require.NoError(t, err)
	_, err = store.UpsertUser(ctx, poolID, email, string(hash), true) // mfa_enabled
	require.NoError(t, err)

	// 1. Login → gets challenge.
	loginStatus, loginBody := postCognito(t, ts.URL, "InitiateAuth", map[string]interface{}{
		"AuthFlow": "USER_PASSWORD_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]string{
			"USERNAME":    email,
			"PASSWORD":    password,
			"SECRET_HASH": computeSecretHash(secret, email, clientID),
		},
	})
	require.Equal(t, http.StatusOK, loginStatus, "body=%v", loginBody)
	session := loginBody["Session"].(string)

	// 2. RespondToAuthChallenge with WRONG SECRET_HASH → NotAuthorizedException.
	status, body := postCognito(t, ts.URL, "RespondToAuthChallenge", map[string]interface{}{
		"ChallengeName": "SOFTWARE_TOKEN_MFA",
		"ClientId":      clientID,
		"Session":       session,
		"ChallengeResponses": map[string]string{
			"USERNAME":                email,
			"SOFTWARE_TOKEN_MFA_CODE": "123456",
			"SECRET_HASH":             "wrong",
		},
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NotAuthorizedException", body["__type"])

	// 3. Same again with correct SECRET_HASH → success.
	status2, body2 := postCognito(t, ts.URL, "RespondToAuthChallenge", map[string]interface{}{
		"ChallengeName": "SOFTWARE_TOKEN_MFA",
		"ClientId":      clientID,
		"Session":       session,
		"ChallengeResponses": map[string]string{
			"USERNAME":                email,
			"SOFTWARE_TOKEN_MFA_CODE": "123456",
			"SECRET_HASH":             computeSecretHash(secret, email, clientID),
		},
	})
	require.Equal(t, http.StatusOK, status2, "body=%v", body2)
}
