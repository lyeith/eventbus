// Tests for Tier-C polish (GO-COGNITO-6):
//
//   - SECRET_HASH validation across InitiateAuth (USER_PASSWORD_AUTH and
//     REFRESH_TOKEN_AUTH), RespondToAuthChallenge, AdminInitiateAuth.
//   - Deterministic TOTP for SOFTWARE_TOKEN_MFA, with fall-back to
//     "any 6 digits" for users without a totp_secret.
//   - Configurable per-pool password policy on AdminCreateUser and
//     RespondToAuthChallenge NEW_PASSWORD_REQUIRED.
//   - Pool/client management ops (CreateUserPool, CreateUserPoolClient,
//     DeleteUserPool, DeleteUserPoolClient).
package main

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/pquerna/otp/totp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
)

// --- SECRET_HASH --------------------------------------------------------

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
	sub := seedClientPoolUserWithSecret(t, store, poolID, clientID, secret, email, password)

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

	// 2. REFRESH_TOKEN_AUTH — SECRET_HASH computed from the user's sub.
	status, body := postCognito(t, ts.URL, "InitiateAuth", map[string]interface{}{
		"AuthFlow": "REFRESH_TOKEN_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]string{
			"REFRESH_TOKEN": refresh,
			"SECRET_HASH":   computeSecretHash(secret, sub, clientID),
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

// --- Deterministic TOTP --------------------------------------------------

// seedMFAUserWithTOTPSecret registers an MFA-enabled user with the given
// base32 TOTP secret. Returns the user's sub.
func seedMFAUserWithTOTPSecret(t *testing.T, store *CognitoStore, poolID, clientID, email, password, totpSecret string) string {
	t.Helper()
	sub := seedMFAUser(t, store, poolID, clientID, email, password)
	require.NoError(t, store.SetUserTOTPSecret(context.Background(), sub, totpSecret))
	return sub
}

const testTOTPSecret = "JBSWY3DPEHPK3PXP" // base32 of "Hello!\xde"

func TestRespondToAuthChallenge_SoftwareTokenMFA_TOTP_HappyPath(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "p-totp-happy"
		clientID = "c-totp-happy"
		email    = "totp@example.com"
		password = "TempPass1!"
	)
	seedMFAUserWithTOTPSecret(t, store, poolID, clientID, email, password, testTOTPSecret)
	session := initiateMFAChallenge(t, ts.URL, clientID, email, password)

	code, err := totp.GenerateCode(testTOTPSecret, time.Now())
	require.NoError(t, err)

	status, body := postCognito(t, ts.URL, "RespondToAuthChallenge", map[string]interface{}{
		"ChallengeName": "SOFTWARE_TOKEN_MFA",
		"ClientId":      clientID,
		"Session":       session,
		"ChallengeResponses": map[string]string{
			"USERNAME":                email,
			"SOFTWARE_TOKEN_MFA_CODE": code,
		},
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	auth, ok := body["AuthenticationResult"].(map[string]interface{})
	require.True(t, ok)
	assert.NotEmpty(t, auth["AccessToken"])
}

func TestRespondToAuthChallenge_SoftwareTokenMFA_TOTP_WrongCode(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "p-totp-wrong"
		clientID = "c-totp-wrong"
		email    = "totp@example.com"
		password = "TempPass1!"
	)
	seedMFAUserWithTOTPSecret(t, store, poolID, clientID, email, password, testTOTPSecret)
	session := initiateMFAChallenge(t, ts.URL, clientID, email, password)

	// "000000" is overwhelmingly unlikely to match a real TOTP window.
	status, body := postCognito(t, ts.URL, "RespondToAuthChallenge", map[string]interface{}{
		"ChallengeName": "SOFTWARE_TOKEN_MFA",
		"ClientId":      clientID,
		"Session":       session,
		"ChallengeResponses": map[string]string{
			"USERNAME":                email,
			"SOFTWARE_TOKEN_MFA_CODE": "000000",
		},
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "CodeMismatchException", body["__type"])
}

func TestRespondToAuthChallenge_SoftwareTokenMFA_TOTP_PrevWindowAccepted(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "p-totp-skew"
		clientID = "c-totp-skew"
		email    = "totp@example.com"
		password = "TempPass1!"
	)
	seedMFAUserWithTOTPSecret(t, store, poolID, clientID, email, password, testTOTPSecret)
	session := initiateMFAChallenge(t, ts.URL, clientID, email, password)

	// Code generated for the previous 30s window should still be accepted
	// thanks to skew=1 in validateTOTPCode.
	code, err := totp.GenerateCode(testTOTPSecret, time.Now().Add(-30*time.Second))
	require.NoError(t, err)

	status, body := postCognito(t, ts.URL, "RespondToAuthChallenge", map[string]interface{}{
		"ChallengeName": "SOFTWARE_TOKEN_MFA",
		"ClientId":      clientID,
		"Session":       session,
		"ChallengeResponses": map[string]string{
			"USERNAME":                email,
			"SOFTWARE_TOKEN_MFA_CODE": code,
		},
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
}

func TestRespondToAuthChallenge_SoftwareTokenMFA_FallbackTo6Digits_WhenNoTotpSecret(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "p-totp-fallback"
		clientID = "c-totp-fallback"
		email    = "mfa@example.com"
		password = "TempPass1!"
	)
	// No totp_secret seeded — should accept any 6 digits.
	seedMFAUser(t, store, poolID, clientID, email, password)
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
}

// --- Password policy ----------------------------------------------------

// seedPoolWithPolicy sets up a pool with the given password policy applied.
func seedPoolWithPolicy(t *testing.T, store *CognitoStore, poolID string, policy *PasswordPolicy) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, store.UpsertPool(ctx, poolID, "us-east-1"))
	if policy != nil {
		raw, err := json.Marshal(policy)
		require.NoError(t, err)
		require.NoError(t, store.SetPoolPasswordPolicy(ctx, poolID, string(raw)))
	}
}

func TestAdminCreateUser_PolicyViolation_TooShort(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const poolID = "p-policy-short"
	seedPoolWithPolicy(t, store, poolID, &PasswordPolicy{MinLength: 12})

	status, body := postCognito(t, ts.URL, "AdminCreateUser", map[string]interface{}{
		"UserPoolId":        poolID,
		"Username":          "alice@example.com",
		"TemporaryPassword": "Short1!",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidPasswordException", body["__type"])
	assert.Contains(t, body["message"], "12 characters")
}

func TestAdminCreateUser_PolicyViolation_NoDigit(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const poolID = "p-policy-nodigit"
	seedPoolWithPolicy(t, store, poolID, &PasswordPolicy{
		MinLength:     8,
		RequireDigits: true,
	})

	status, body := postCognito(t, ts.URL, "AdminCreateUser", map[string]interface{}{
		"UserPoolId":        poolID,
		"Username":          "alice@example.com",
		"TemporaryPassword": "NoDigitsHere!",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidPasswordException", body["__type"])
	assert.Contains(t, body["message"], "digit")
}

func TestAdminCreateUser_PolicyViolation_NoUppercase(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const poolID = "p-policy-noupper"
	seedPoolWithPolicy(t, store, poolID, &PasswordPolicy{
		MinLength:        8,
		RequireUppercase: true,
	})

	status, body := postCognito(t, ts.URL, "AdminCreateUser", map[string]interface{}{
		"UserPoolId":        poolID,
		"Username":          "alice@example.com",
		"TemporaryPassword": "no-upper-1!",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidPasswordException", body["__type"])
	assert.Contains(t, body["message"], "uppercase")
}

func TestAdminCreateUser_PolicyHonored_HappyPath(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const poolID = "p-policy-ok"
	seedPoolWithPolicy(t, store, poolID, &PasswordPolicy{
		MinLength:        8,
		RequireUppercase: true,
		RequireLowercase: true,
		RequireDigits:    true,
		RequireSymbols:   true,
	})

	status, body := postCognito(t, ts.URL, "AdminCreateUser", map[string]interface{}{
		"UserPoolId":        poolID,
		"Username":          "alice@example.com",
		"TemporaryPassword": "Strong1Pass!",
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
}

func TestRespondToAuthChallenge_NewPasswordRequired_PolicyViolation(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "p-newpw-policy"
		clientID = "c-newpw-policy"
		email    = "newpw@example.com"
	)
	seedPoolWithPolicy(t, store, poolID, &PasswordPolicy{
		MinLength:     12,
		RequireDigits: true,
	})
	ctx := context.Background()
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
			"NEW_PASSWORD": "weak",
		},
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidPasswordException", body["__type"])
	// Should mention BOTH violations (length + digit) in one message.
	msg, _ := body["message"].(string)
	assert.Contains(t, msg, "12 characters")
	assert.Contains(t, msg, "digit")
}

func TestPasswordPolicy_NoPolicyMeansAccept(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const poolID = "p-policy-none"
	require.NoError(t, store.UpsertPool(context.Background(), poolID, "us-east-1"))

	// Even a single-character password should be accepted when no
	// policy is configured. (Empty password is still rejected, matching
	// PasswordPolicy.Validate's "non-nil-policy" baseline.)
	status, body := postCognito(t, ts.URL, "AdminCreateUser", map[string]interface{}{
		"UserPoolId":        poolID,
		"Username":          "alice@example.com",
		"TemporaryPassword": "x",
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
}

// PasswordPolicy.Validate unit tests catch corner cases the integration
// tests miss (e.g. multiple-failure messages, nil receiver, empty input).

func TestPasswordPolicy_Validate_NilReceiver(t *testing.T) {
	var p *PasswordPolicy
	assert.NoError(t, p.Validate("anything"))
	assert.Error(t, p.Validate(""), "empty password is rejected even with nil policy")
}

func TestPasswordPolicy_Validate_MultipleFailures(t *testing.T) {
	p := &PasswordPolicy{
		MinLength:        12,
		RequireUppercase: true,
		RequireDigits:    true,
		RequireSymbols:   true,
	}
	// "lower" is 5 chars (under 12), all lowercase, no digit, no symbol —
	// fails every rule, so the message should list each.
	err := p.Validate("lower")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "12 characters")
	assert.Contains(t, err.Error(), "uppercase")
	assert.Contains(t, err.Error(), "digit")
	assert.Contains(t, err.Error(), "symbol")
}

// --- Pool/client management ---------------------------------------------

func TestCreateUserPool_HappyPath(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)

	status, body := postCognito(t, ts.URL, "CreateUserPool", map[string]interface{}{
		"PoolName": "my-test-pool",
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)

	pool, ok := body["UserPool"].(map[string]interface{})
	require.True(t, ok)
	id, _ := pool["Id"].(string)
	require.NotEmpty(t, id)
	assert.Equal(t, "my-test-pool", pool["Name"])
	assert.Equal(t, "Enabled", pool["Status"])

	// DB state: pool persisted and retrievable.
	exists, err := store.PoolExists(context.Background(), id)
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestCreateUserPool_WithExplicitPoolID(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)

	status, body := postCognito(t, ts.URL, "CreateUserPool", map[string]interface{}{
		"PoolName": "explicit-pool",
		"PoolId":   "deterministic-pool-1",
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	pool, _ := body["UserPool"].(map[string]interface{})
	assert.Equal(t, "deterministic-pool-1", pool["Id"])

	exists, err := store.PoolExists(context.Background(), "deterministic-pool-1")
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestCreateUserPool_PersistsPasswordPolicy(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)

	status, body := postCognito(t, ts.URL, "CreateUserPool", map[string]interface{}{
		"PoolName": "policy-pool",
		"PoolId":   "policy-pool-1",
		"Policies": map[string]interface{}{
			"PasswordPolicy": map[string]interface{}{
				"min_length":        10,
				"require_uppercase": true,
				"require_digits":    true,
			},
		},
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)

	policy, err := loadPoolPasswordPolicy(context.Background(), store, "policy-pool-1")
	require.NoError(t, err)
	require.NotNil(t, policy)
	assert.Equal(t, 10, policy.MinLength)
	assert.True(t, policy.RequireUppercase)
	assert.True(t, policy.RequireDigits)
}

func TestCreateUserPool_MissingPoolName(t *testing.T) {
	_, ts, _ := newCognitoTestServer(t)
	status, body := postCognito(t, ts.URL, "CreateUserPool", map[string]interface{}{})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterException", body["__type"])
}

func TestCreateUserPoolClient_WithSecret(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const poolID = "p-create-client-secret"
	require.NoError(t, store.UpsertPool(context.Background(), poolID, "us-east-1"))

	status, body := postCognito(t, ts.URL, "CreateUserPoolClient", map[string]interface{}{
		"UserPoolId":     poolID,
		"ClientName":     "my-client",
		"GenerateSecret": true,
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	client, ok := body["UserPoolClient"].(map[string]interface{})
	require.True(t, ok)
	clientID, _ := client["ClientId"].(string)
	require.NotEmpty(t, clientID)
	secret, _ := client["ClientSecret"].(string)
	require.NotEmpty(t, secret)
	assert.Equal(t, "my-client", client["ClientName"])

	got, err := store.LookupClient(context.Background(), clientID)
	require.NoError(t, err)
	assert.Equal(t, secret, got.Secret)
}

func TestCreateUserPoolClient_WithoutSecret(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const poolID = "p-create-client-nosecret"
	require.NoError(t, store.UpsertPool(context.Background(), poolID, "us-east-1"))

	status, body := postCognito(t, ts.URL, "CreateUserPoolClient", map[string]interface{}{
		"UserPoolId": poolID,
		"ClientName": "no-secret-client",
		// GenerateSecret defaults false.
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	client, _ := body["UserPoolClient"].(map[string]interface{})
	clientID, _ := client["ClientId"].(string)
	require.NotEmpty(t, clientID)
	_, hasSecret := client["ClientSecret"]
	assert.False(t, hasSecret, "ClientSecret must be absent when GenerateSecret=false")

	got, err := store.LookupClient(context.Background(), clientID)
	require.NoError(t, err)
	assert.Empty(t, got.Secret)
}

func TestCreateUserPoolClient_MissingPool(t *testing.T) {
	_, ts, _ := newCognitoTestServer(t)
	status, body := postCognito(t, ts.URL, "CreateUserPoolClient", map[string]interface{}{
		"UserPoolId": "no-such-pool",
		"ClientName": "x",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "ResourceNotFoundException", body["__type"])
}

func TestDeleteUserPool_CascadesAllChildren(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "p-delete-cascade"
		clientID = "c-delete-cascade"
		email    = "alice@example.com"
	)
	ctx := context.Background()

	// Pool + client + user + signing key + session.
	require.NoError(t, store.UpsertPool(ctx, poolID, "us-east-1"))
	require.NoError(t, store.UpsertClient(ctx, clientID, poolID, ""))
	sub, err := store.UpsertUser(ctx, poolID, email, "x", false)
	require.NoError(t, err)
	require.NoError(t, store.SetUserAttribute(ctx, sub, "email", email))
	_, err = store.EnsureSigningKey(ctx, poolID)
	require.NoError(t, err)
	require.NoError(t, store.CreateChallengeSession(ctx, "sess-1", sub, poolID, clientID, "SOFTWARE_TOKEN_MFA", time.Minute))

	status, _ := postCognito(t, ts.URL, "DeleteUserPool", map[string]interface{}{
		"UserPoolId": poolID,
	})
	require.Equal(t, http.StatusOK, status)

	// Pool gone.
	exists, err := store.PoolExists(ctx, poolID)
	require.NoError(t, err)
	assert.False(t, exists, "pool should be deleted")

	// User gone (FK cascade).
	_, err = store.LookupUserBySub(ctx, sub)
	assert.Error(t, err, "user should cascade-delete with pool")

	// Client gone (FK cascade).
	_, err = store.LookupClient(ctx, clientID)
	assert.Error(t, err, "client should cascade-delete with pool")

	// Signing key gone (explicit delete inside DeletePool).
	_, err = store.LoadSigningKey(ctx, poolID)
	assert.Error(t, err, "signing key should be deleted")

	// Challenge session gone (explicit delete).
	_, err = store.LookupChallengeSession(ctx, "sess-1")
	assert.Error(t, err, "challenge session should be deleted")
}

func TestDeleteUserPool_Idempotent(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const poolID = "p-delete-idempotent"
	require.NoError(t, store.UpsertPool(context.Background(), poolID, "us-east-1"))

	first, _ := postCognito(t, ts.URL, "DeleteUserPool", map[string]interface{}{
		"UserPoolId": poolID,
	})
	require.Equal(t, http.StatusOK, first)

	// Second delete: must succeed, even though the pool is already gone.
	second, _ := postCognito(t, ts.URL, "DeleteUserPool", map[string]interface{}{
		"UserPoolId": poolID,
	})
	assert.Equal(t, http.StatusOK, second)
}

func TestDeleteUserPoolClient_Idempotent(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "p-delete-client-idem"
		clientID = "c-delete-client-idem"
	)
	ctx := context.Background()
	require.NoError(t, store.UpsertPool(ctx, poolID, "us-east-1"))
	require.NoError(t, store.UpsertClient(ctx, clientID, poolID, ""))

	first, _ := postCognito(t, ts.URL, "DeleteUserPoolClient", map[string]interface{}{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	require.Equal(t, http.StatusOK, first)

	// Second delete on already-gone client: success.
	second, _ := postCognito(t, ts.URL, "DeleteUserPoolClient", map[string]interface{}{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	assert.Equal(t, http.StatusOK, second)
}
