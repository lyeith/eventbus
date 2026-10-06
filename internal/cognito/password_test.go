// Tests for ChangePassword (cognito_password.go), driven through the
// JSON-1.1 wire like the platform's Cognito provider calls it.
package cognito

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const changedPass = "Changed2!pass"

func changePassword(t *testing.T, baseURL, access string, previous *string, proposed string) (int, map[string]interface{}) {
	t.Helper()
	payload := map[string]interface{}{"AccessToken": access, "ProposedPassword": proposed}
	if previous != nil {
		payload["PreviousPassword"] = *previous
	}
	return postCognito(t, baseURL, "ChangePassword", payload)
}

func signInWith(t *testing.T, baseURL, password string) (int, map[string]interface{}) {
	t.Helper()
	return postCognito(t, baseURL, "InitiateAuth", map[string]interface{}{
		"AuthFlow":       "USER_PASSWORD_AUTH",
		"ClientId":       factorClient,
		"AuthParameters": map[string]string{"USERNAME": factorEmail, "PASSWORD": password},
	})
}

func ptr(value string) *string { return &value }

// A change replaces the password and revokes nothing: the tokens from before
// it still work, as in Cognito.
func TestChangePassword_ReplacesThePasswordAndKeepsSessions(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	seedClientPoolUser(t, store, factorPool, factorClient, factorEmail, factorPass)
	session, _ := passwordSignIn(t, ts.URL)

	status, body := changePassword(t, ts.URL, session.access, ptr(factorPass), changedPass)
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	assert.Empty(t, body)

	status, body = signInWith(t, ts.URL, factorPass)
	require.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NotAuthorizedException", body["__type"], "the old password no longer signs in")
	status, body = signInWith(t, ts.URL, changedPass)
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	readAuthResult(t, body)

	status, _ = postCognito(t, ts.URL, "GetUser", map[string]interface{}{"AccessToken": session.access})
	assert.Equal(t, http.StatusOK, status, "the access token from before the change still works")
	status, body = postCognito(t, ts.URL, "InitiateAuth", map[string]interface{}{
		"AuthFlow": "REFRESH_TOKEN_AUTH", "ClientId": factorClient,
		"AuthParameters": map[string]string{"REFRESH_TOKEN": session.refresh},
	})
	assert.Equal(t, http.StatusOK, status, "the refresh token from before the change still works: body=%v", body)
	user, err := store.LookupUserByEmail(t.Context(), factorPool, factorEmail)
	require.NoError(t, err)
	assert.Zero(t, user.AuthVersion, "self-service changes preserve grants")
	assert.NotEmpty(t, user.SRPSalt)
	assert.NotEmpty(t, user.SRPVerifier, "self-service updates password and SRP credentials together")
}

func TestChangePassword_LongPasswordUpdatesSRPAndPreservesGrants(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	user := createAdminAuthUser(t, ts.URL, store, PoolSignInConfig{CaseSensitive: true}, "", true)
	status, body := adminPasswordAuth(t, ts.URL, authFlowAdminUserPassword, user.Username, "", adminAuthPassword)
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	result := readAuthResult(t, body)
	proposed := strings.Repeat("Long2!", 40) // AWS accepts passwords above bcrypt's 72-byte limit.
	status, body = changePassword(t, ts.URL, result["AccessToken"].(string), ptr(adminAuthPassword), proposed)
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	updated, err := store.LookupUserBySub(t.Context(), user.Sub)
	require.NoError(t, err)
	assert.Equal(t, user.AuthVersion, updated.AuthVersion)
	assert.NotEqual(t, user.SRPVerifier, updated.SRPVerifier)
	assert.NotEmpty(t, updated.SRPSalt)
	require.NoError(t, compareUserPasswordHash(updated.PasswordHash, proposed))
	assert.Error(t, compareUserPasswordHash(updated.PasswordHash, adminAuthPassword))
	status, body = adminPasswordAuth(t, ts.URL, authFlowAdminUserPassword, user.Username, "", proposed)
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	status, body = adminRefreshAuth(t, ts.URL, "AdminInitiateAuth", authFlowRefreshToken, result["RefreshToken"].(string), "")
	require.Equal(t, http.StatusOK, status, "body=%v", body)
}

func TestChangePassword_UnicodeLengthUsesCharacters(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	user := createAdminAuthUser(t, ts.URL, store, PoolSignInConfig{CaseSensitive: true}, "", true)
	status, body := adminPasswordAuth(t, ts.URL, authFlowAdminUserPassword, user.Username, "", adminAuthPassword)
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	access := readAuthResult(t, body)["AccessToken"].(string)
	proposed := strings.Repeat("界", 256)
	status, body = changePassword(t, ts.URL, access, ptr(adminAuthPassword), proposed)
	require.Equal(t, http.StatusOK, status, "256 characters remain valid above 256 bytes: %v", body)
	status, body = adminPasswordAuth(t, ts.URL, authFlowAdminUserPassword, user.Username, "", proposed)
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	status, body = changePassword(t, ts.URL, access, ptr(proposed), proposed+"界")
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterException", body["__type"])
	assert.Contains(t, body["message"], "length less than or equal to 256")
}

func TestChangePassword_WrongOrMissingPreviousPassword(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	seedClientPoolUser(t, store, factorPool, factorClient, factorEmail, factorPass)
	session, _ := passwordSignIn(t, ts.URL)

	for name, previous := range map[string]*string{"wrong": ptr("Wrong1!pass"), "omitted": nil} {
		status, body := changePassword(t, ts.URL, session.access, previous, changedPass)
		require.Equal(t, http.StatusBadRequest, status, name)
		assert.Equal(t, "NotAuthorizedException", body["__type"], name)
		assert.Equal(t, "Incorrect username or password.", body["message"], name)
	}
	status, body := signInWith(t, ts.URL, factorPass)
	require.Equal(t, http.StatusOK, status, "a refused change leaves the password: body=%v", body)
}

func TestChangePassword_PoolPolicyInCognitosWords(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	seedClientPoolUser(t, store, factorPool, factorClient, factorEmail, factorPass)
	seedPoolWithPolicy(t, store, factorPool, &PasswordPolicy{
		MinLength: 10, RequireUppercase: true, RequireLowercase: true, RequireDigits: true, RequireSymbols: true,
	})
	session, _ := passwordSignIn(t, ts.URL)

	for proposed, rule := range map[string]string{
		"Sh0rt!":      "Password not long enough",
		"lower1!case": "Password must have uppercase characters",
		"UPPER1!CASE": "Password must have lowercase characters",
		"NoDigits!ok": "Password must have numeric characters",
		"NoSymbol1ok": "Password must have symbol characters",
	} {
		status, body := changePassword(t, ts.URL, session.access, ptr(factorPass), proposed)
		require.Equal(t, http.StatusBadRequest, status, proposed)
		assert.Equal(t, "InvalidPasswordException", body["__type"], proposed)
		assert.Equal(t, "Password does not conform to policy: "+rule, body["message"], proposed)
	}
	status, body := changePassword(t, ts.URL, session.access, ptr(factorPass), "Long3nough!pw")
	require.Equal(t, http.StatusOK, status, "body=%v", body)
}

func TestChangePassword_RequestValidationInCognitosWords(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	seedClientPoolUser(t, store, factorPool, factorClient, factorEmail, factorPass)
	session, _ := passwordSignIn(t, ts.URL)

	status, body := changePassword(t, ts.URL, session.access, ptr(factorPass), " Leading1!space")
	require.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterException", body["__type"])
	assert.Equal(t, "1 validation error detected: Value at 'proposedPassword' failed to satisfy constraint: "+
		"Member must satisfy regular expression pattern: ^[\\S]+.*[\\S]+$", body["message"])

	status, body = changePassword(t, ts.URL, session.access, ptr(""), strings.Repeat("a", 257))
	require.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterException", body["__type"])
	message, _ := body["message"].(string)
	assert.True(t, strings.HasPrefix(message, "2 validation errors detected: Value at 'previousPassword'"), message)
	assert.Contains(t, message, "Member must have length less than or equal to 256")

	// Malformed requests are refused before the token or the password is
	// looked at, so they do not count toward the attempt limit.
	user, err := store.LookupUserByEmail(context.Background(), factorPool, factorEmail)
	require.NoError(t, err)
	assert.Zero(t, user.PasswordFailures)
}

// Five wrong previous passwords lock ChangePassword, whatever is sent next,
// until the lockout passes; a change then clears the count.
func TestChangePassword_AttemptLimit(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	seedClientPoolUser(t, store, factorPool, factorClient, factorEmail, factorPass)
	session, _ := passwordSignIn(t, ts.URL)

	for attempt := 1; attempt <= changePasswordAttemptLimit; attempt++ {
		status, body := changePassword(t, ts.URL, session.access, ptr("Wrong1!pass"), changedPass)
		require.Equal(t, http.StatusBadRequest, status, "attempt %d", attempt)
		assert.Equal(t, "NotAuthorizedException", body["__type"], "attempt %d", attempt)
	}
	status, body := changePassword(t, ts.URL, session.access, ptr(factorPass), changedPass)
	require.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "LimitExceededException", body["__type"])
	assert.Equal(t, "Attempt limit exceeded, please try after some time.", body["message"])

	ctx := context.Background()
	user, err := store.LookupUserByEmail(ctx, factorPool, factorEmail)
	require.NoError(t, err)
	remaining := time.Until(time.Unix(user.PasswordLockedUntil, 0))
	assert.InDelta(t, changePasswordLockout.Seconds(), remaining.Seconds(), 5)

	// The lockout passes.
	_, err = store.DB().ExecContext(ctx, `UPDATE users SET password_locked_until = ? WHERE sub = ?`,
		time.Now().Add(-time.Second).Unix(), user.Sub)
	require.NoError(t, err)
	status, body = changePassword(t, ts.URL, session.access, ptr("Wrong1!pass"), changedPass)
	require.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NotAuthorizedException", body["__type"], "a passed lockout counts from one again")
	status, body = changePassword(t, ts.URL, session.access, ptr(factorPass), changedPass)
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	user, err = store.LookupUserByEmail(ctx, factorPool, factorEmail)
	require.NoError(t, err)
	assert.Zero(t, user.PasswordFailures)
	assert.Zero(t, user.PasswordLockedUntil)
}

// The access token is checked as every AccessToken action checks it: a
// global sign-out revokes it for ChangePassword too.
func TestChangePassword_RevokedAccessTokenIsRefused(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	seedClientPoolUser(t, store, factorPool, factorClient, factorEmail, factorPass)
	session, _ := passwordSignIn(t, ts.URL)
	status, body := postCognito(t, ts.URL, "GlobalSignOut", map[string]interface{}{"AccessToken": session.access})
	require.Equal(t, http.StatusOK, status, "body=%v", body)

	status, body = changePassword(t, ts.URL, session.access, ptr(factorPass), changedPass)
	require.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NotAuthorizedException", body["__type"])
	assert.Equal(t, "Access Token has been revoked", body["message"])
}

// With two-factor on, the access token from a completed challenge is all
// ChangePassword needs; the next sign-in is still challenged.
func TestChangePassword_WithTwoFactorNeedsOnlyTheAccessToken(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	seedClientPoolUser(t, store, factorPool, factorClient, factorEmail, factorPass)
	session, _ := passwordSignIn(t, ts.URL)
	enrol(t, ts.URL, session.access)

	status, body := changePassword(t, ts.URL, session.access, ptr(factorPass), changedPass)
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	status, body = signInWith(t, ts.URL, changedPass)
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	assert.Equal(t, "SOFTWARE_TOKEN_MFA", body["ChallengeName"])
}

func TestPasswordPolicy_CognitoViolationReportsOneRule(t *testing.T) {
	var none *PasswordPolicy
	assert.Empty(t, none.CognitoViolation("x"), "a pool without a policy")
	policy := &PasswordPolicy{MinLength: 12, RequireUppercase: true, RequireDigits: true, RequireSymbols: true}
	assert.Equal(t, "Password not long enough", policy.CognitoViolation("lower"), "length first")
	assert.Equal(t, "Password must have uppercase characters", policy.CognitoViolation("lowercase-only-long"))
	assert.Equal(t, "Password must have numeric characters", policy.CognitoViolation("Uppercase-no-digits"))
	assert.Equal(t, "Password must have symbol characters", policy.CognitoViolation("Uppercase1nosymbol"))
	assert.Equal(t, "Password must have symbol characters", (&PasswordPolicy{RequireSymbols: true}).CognitoViolation("abc"))
	assert.Empty(t, (&PasswordPolicy{RequireSymbols: true}).CognitoViolation("with space"), "a space is a symbol to Cognito")
	assert.Empty(t, policy.CognitoViolation("Uppercase1-symbol"))
}
