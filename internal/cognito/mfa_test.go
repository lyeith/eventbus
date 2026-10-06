// Enrollment and token-revocation wire contracts.
package cognito

import (
	"net/http"
	"testing"
	"time"

	"github.com/pquerna/otp/totp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	factorPool   = "local-pool-1"
	factorClient = "local-client-1"
	factorEmail  = "person@example.com"
	factorPass   = "TempPass1!"
)

type factorSession struct {
	access  string
	refresh string
}

func passwordSignIn(t *testing.T, baseURL string) (factorSession, map[string]interface{}) {
	t.Helper()
	status, body := postCognito(t, baseURL, "InitiateAuth", map[string]interface{}{
		"AuthFlow":       "USER_PASSWORD_AUTH",
		"ClientId":       factorClient,
		"AuthParameters": map[string]string{"USERNAME": factorEmail, "PASSWORD": factorPass},
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	if _, challenged := body["ChallengeName"]; challenged {
		return factorSession{}, body
	}
	result := readAuthResult(t, body)
	return factorSession{
		access:  result["AccessToken"].(string),
		refresh: result["RefreshToken"].(string),
	}, body
}

func totpCode(t *testing.T, secret string) string {
	t.Helper()
	code, err := totp.GenerateCode(secret, time.Now())
	require.NoError(t, err)
	return code
}

// enrol runs Associate → Verify → SetUserMFAPreference and returns the secret.
func enrol(t *testing.T, baseURL, access string) string {
	t.Helper()
	status, body := postCognito(t, baseURL, "AssociateSoftwareToken", map[string]interface{}{"AccessToken": access})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	secret, _ := body["SecretCode"].(string)
	require.NotEmpty(t, secret)

	status, body = postCognito(t, baseURL, "VerifySoftwareToken", map[string]interface{}{
		"AccessToken": access, "UserCode": totpCode(t, secret), "FriendlyDeviceName": "phone",
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	assert.Equal(t, "SUCCESS", body["Status"])

	status, body = postCognito(t, baseURL, "SetUserMFAPreference", map[string]interface{}{
		"AccessToken":              access,
		"SoftwareTokenMfaSettings": map[string]bool{"Enabled": true, "PreferredMfa": true},
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	return secret
}

func TestFactors_EnrolmentThenEverySignInIsChallenged(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	seedClientPoolUser(t, store, factorPool, factorClient, factorEmail, factorPass)

	session, _ := passwordSignIn(t, ts.URL)
	require.NotEmpty(t, session.access, "no factor yet: password alone signs in")

	status, body := postCognito(t, ts.URL, "GetUser", map[string]interface{}{"AccessToken": session.access})
	require.Equal(t, http.StatusOK, status)
	_, listed := body["UserMFASettingList"]
	assert.False(t, listed, "no MFA method before enrolment")

	secret := enrol(t, ts.URL, session.access)

	status, body = postCognito(t, ts.URL, "GetUser", map[string]interface{}{"AccessToken": session.access})
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, []interface{}{"SOFTWARE_TOKEN_MFA"}, body["UserMFASettingList"])
	assert.Equal(t, "SOFTWARE_TOKEN_MFA", body["PreferredMfaSetting"])

	// The next sign-in is challenged, and only the enrolled secret answers.
	_, challenge := passwordSignIn(t, ts.URL)
	require.Equal(t, "SOFTWARE_TOKEN_MFA", challenge["ChallengeName"])
	sessionID := challenge["Session"].(string)
	status, body = postCognito(t, ts.URL, "RespondToAuthChallenge", map[string]interface{}{
		"ClientId": factorClient, "ChallengeName": "SOFTWARE_TOKEN_MFA", "Session": sessionID,
		"ChallengeResponses": map[string]string{"USERNAME": factorEmail, "SOFTWARE_TOKEN_MFA_CODE": "000000"},
	})
	require.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "CodeMismatchException", body["__type"])
	status, body = postCognito(t, ts.URL, "RespondToAuthChallenge", map[string]interface{}{
		"ClientId": factorClient, "ChallengeName": "SOFTWARE_TOKEN_MFA", "Session": sessionID,
		"ChallengeResponses": map[string]string{"USERNAME": factorEmail, "SOFTWARE_TOKEN_MFA_CODE": totpCode(t, secret)},
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	readAuthResult(t, body)

	// Admin sign-in is challenged too: no entry point skips the factor.
	status, body = postCognito(t, ts.URL, "AdminInitiateAuth", map[string]interface{}{
		"UserPoolId": factorPool, "ClientId": factorClient, "AuthFlow": "ADMIN_NO_SRP_AUTH",
		"AuthParameters": map[string]string{"USERNAME": factorEmail, "PASSWORD": factorPass},
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	assert.Equal(t, "SOFTWARE_TOKEN_MFA", body["ChallengeName"])
}

func TestFactors_VerifyRejectsWrongCodeAndEnableNeedsVerifiedToken(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	seedClientPoolUser(t, store, factorPool, factorClient, factorEmail, factorPass)
	session, _ := passwordSignIn(t, ts.URL)

	status, body := postCognito(t, ts.URL, "VerifySoftwareToken", map[string]interface{}{
		"AccessToken": session.access, "UserCode": "123456",
	})
	require.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "SoftwareTokenMFANotFoundException", body["__type"], "nothing associated yet")

	status, body = postCognito(t, ts.URL, "SetUserMFAPreference", map[string]interface{}{
		"AccessToken":              session.access,
		"SoftwareTokenMfaSettings": map[string]bool{"Enabled": true, "PreferredMfa": true},
	})
	require.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterException", body["__type"], "enable needs a verified token")

	status, _ = postCognito(t, ts.URL, "AssociateSoftwareToken", map[string]interface{}{"AccessToken": session.access})
	require.Equal(t, http.StatusOK, status)
	status, body = postCognito(t, ts.URL, "VerifySoftwareToken", map[string]interface{}{
		"AccessToken": session.access, "UserCode": "12345x",
	})
	require.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "EnableSoftwareTokenMFAException", body["__type"])

	// Still no challenge: a failed verification enabled nothing.
	next, _ := passwordSignIn(t, ts.URL)
	assert.NotEmpty(t, next.access)
}

func TestFactors_AdminResetTurnsTheFactorOff(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	sub := seedClientPoolUser(t, store, factorPool, factorClient, factorEmail, factorPass)
	session, _ := passwordSignIn(t, ts.URL)
	enrol(t, ts.URL, session.access)

	// Admin APIs take the sub (or the username).
	status, body := postCognito(t, ts.URL, "AdminSetUserMFAPreference", map[string]interface{}{
		"UserPoolId": factorPool, "Username": sub,
		"SoftwareTokenMfaSettings": map[string]bool{"Enabled": false, "PreferredMfa": false},
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)

	next, _ := passwordSignIn(t, ts.URL)
	assert.NotEmpty(t, next.access, "after the reset the password alone signs in again")

	status, body = postCognito(t, ts.URL, "AdminSetUserMFAPreference", map[string]interface{}{
		"UserPoolId": factorPool, "Username": "nobody@example.com",
		"SoftwareTokenMfaSettings": map[string]bool{"Enabled": false},
	})
	require.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "UserNotFoundException", body["__type"])
}

func refreshStatus(t *testing.T, baseURL, refresh string) (int, map[string]interface{}) {
	t.Helper()
	return postCognito(t, baseURL, "InitiateAuth", map[string]interface{}{
		"AuthFlow":       "REFRESH_TOKEN_AUTH",
		"ClientId":       factorClient,
		"AuthParameters": map[string]string{"REFRESH_TOKEN": refresh},
	})
}

func TestFactors_GlobalSignOutRevokesEverySession(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	seedClientPoolUser(t, store, factorPool, factorClient, factorEmail, factorPass)
	first, _ := passwordSignIn(t, ts.URL)
	second, _ := passwordSignIn(t, ts.URL)

	status, body := postCognito(t, ts.URL, "GlobalSignOut", map[string]interface{}{"AccessToken": first.access})
	require.Equal(t, http.StatusOK, status, "body=%v", body)

	for _, session := range []factorSession{first, second} {
		status, body = refreshStatus(t, ts.URL, session.refresh)
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "NotAuthorizedException", body["__type"])
		status, body = postCognito(t, ts.URL, "GetUser", map[string]interface{}{"AccessToken": session.access})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "NotAuthorizedException", body["__type"])
	}

	// A new login is valid immediately, even in the same second as sign-out.
	later, _ := passwordSignIn(t, ts.URL)
	status, body = refreshStatus(t, ts.URL, later.refresh)
	require.Equal(t, http.StatusOK, status, "body=%v", body)
}

func TestFactors_AdminUserGlobalSignOutByUsername(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	seedClientPoolUser(t, store, factorPool, factorClient, factorEmail, factorPass)
	session, _ := passwordSignIn(t, ts.URL)

	status, body := postCognito(t, ts.URL, "AdminUserGlobalSignOut", map[string]interface{}{
		"UserPoolId": factorPool, "Username": factorEmail,
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	status, body = refreshStatus(t, ts.URL, session.refresh)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NotAuthorizedException", body["__type"])
	status, body = postCognito(t, ts.URL, "GetUser", map[string]interface{}{"AccessToken": session.access})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NotAuthorizedException", body["__type"])

	later, _ := passwordSignIn(t, ts.URL)
	status, body = refreshStatus(t, ts.URL, later.refresh)
	require.Equal(t, http.StatusOK, status, "a new login must survive immediate administrative sign-out: body=%v", body)
}

func TestFactors_RevokeTokenEndsOneSessionOnly(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	seedClientPoolUser(t, store, factorPool, factorClient, factorEmail, factorPass)
	thisDevice, _ := passwordSignIn(t, ts.URL)
	otherDevice, _ := passwordSignIn(t, ts.URL)

	status, body := postCognito(t, ts.URL, "RevokeToken", map[string]interface{}{
		"Token": thisDevice.refresh, "ClientId": factorClient,
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	// Revoking again is not an error.
	status, _ = postCognito(t, ts.URL, "RevokeToken", map[string]interface{}{
		"Token": thisDevice.refresh, "ClientId": factorClient,
	})
	require.Equal(t, http.StatusOK, status)

	status, body = refreshStatus(t, ts.URL, thisDevice.refresh)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NotAuthorizedException", body["__type"])
	status, _ = postCognito(t, ts.URL, "GetUser", map[string]interface{}{"AccessToken": thisDevice.access})
	assert.Equal(t, http.StatusBadRequest, status, "the revoked refresh token's access tokens are revoked too")

	status, _ = refreshStatus(t, ts.URL, otherDevice.refresh)
	assert.Equal(t, http.StatusOK, status, "another device's session is untouched")

	status, body = postCognito(t, ts.URL, "RevokeToken", map[string]interface{}{
		"Token": otherDevice.access, "ClientId": factorClient,
	})
	require.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "UnsupportedTokenTypeException", body["__type"])
}

func TestFactors_RevokeTokenRequiresClientSecret(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const secret = "ClientSecretForRevoke123456"
	seedClientPoolUserWithSecret(t, store, factorPool, factorClient, secret, factorEmail, factorPass)
	status, body := postCognito(t, ts.URL, "InitiateAuth", map[string]interface{}{
		"AuthFlow": "USER_PASSWORD_AUTH", "ClientId": factorClient,
		"AuthParameters": map[string]string{
			"USERNAME": factorEmail, "PASSWORD": factorPass,
			"SECRET_HASH": computeSecretHash(secret, factorEmail, factorClient),
		},
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	result := readAuthResult(t, body)
	refresh := result["RefreshToken"].(string)
	access := result["AccessToken"].(string)
	grant, err := grantOf(parseClaimsUnverified(t, refresh))
	require.NoError(t, err)
	latestAccess := access

	for _, tc := range []struct {
		name, provided string
		omit           bool
	}{
		{"missing", "", true},
		{"empty", "", false},
		{"wrong", "WrongClientSecret1234567890", false},
		{"secret hash instead of raw secret", computeSecretHash(secret, factorEmail, factorClient), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := map[string]interface{}{"Token": refresh, "ClientId": factorClient}
			if !tc.omit {
				request["ClientSecret"] = tc.provided
			}
			status, body := postCognito(t, ts.URL, "RevokeToken", request)
			require.Equal(t, http.StatusBadRequest, status, "body=%v", body)
			require.Equal(t, "UnauthorizedException", body["__type"])
			require.NotEmpty(t, body["message"])
			require.NotContains(t, body, "Message", "Cognito errors retain the JSON-1.1 envelope")
			revoked, err := store.RefreshTokenRevoked(t.Context(), grant.OriginJTI)
			require.NoError(t, err)
			require.False(t, revoked, "failed client authentication must not revoke the grant")

			status, body = postCognito(t, ts.URL, "InitiateAuth", map[string]interface{}{
				"AuthFlow": "REFRESH_TOKEN_AUTH", "ClientId": factorClient,
				"AuthParameters": map[string]string{
					"REFRESH_TOKEN": refresh, "SECRET_HASH": computeSecretHash(secret, factorEmail, factorClient),
				},
			})
			require.Equal(t, http.StatusOK, status, "body=%v", body)
			latestAccess = readAuthResult(t, body)["AccessToken"].(string)
		})
	}

	for range 2 {
		status, body = postCognito(t, ts.URL, "RevokeToken", map[string]interface{}{
			"Token": refresh, "ClientId": factorClient, "ClientSecret": secret,
		})
		require.Equal(t, http.StatusOK, status, "correct raw client secret authorizes idempotent revocation: body=%v", body)
	}
	status, body = postCognito(t, ts.URL, "InitiateAuth", map[string]interface{}{
		"AuthFlow": "REFRESH_TOKEN_AUTH", "ClientId": factorClient,
		"AuthParameters": map[string]string{
			"REFRESH_TOKEN": refresh, "SECRET_HASH": computeSecretHash(secret, factorEmail, factorClient),
		},
	})
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "NotAuthorizedException", body["__type"])
	for _, token := range []string{access, latestAccess} {
		status, body = postCognito(t, ts.URL, "GetUser", map[string]interface{}{"AccessToken": token})
		require.Equal(t, http.StatusBadRequest, status)
		require.Equal(t, "NotAuthorizedException", body["__type"])
	}
}

func TestFactors_RevokeTokenRejectsUnknownAndOtherClients(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	seedClientPoolUser(t, store, factorPool, factorClient, factorEmail, factorPass)
	const otherSecret = "OtherClientSecret1234567890"
	require.NoError(t, store.UpsertClient(t.Context(), "other-secret-client", factorPool, otherSecret))
	require.NoError(t, store.UpsertClient(t.Context(), "other-public-client", factorPool, ""))
	session, _ := passwordSignIn(t, ts.URL)
	grant, err := grantOf(parseClaimsUnverified(t, session.refresh))
	require.NoError(t, err)

	for _, tc := range []struct {
		name, client, secret, code string
	}{
		{"unknown client", "missing-client", "", "UnauthorizedException"},
		{"unknown client with secret", "missing-client", otherSecret, "UnauthorizedException"},
		{"other client without secret", "other-secret-client", "", "UnauthorizedException"},
		{"other client with correct secret", "other-secret-client", otherSecret, "NotAuthorizedException"},
		{"other public client", "other-public-client", "", "NotAuthorizedException"},
		{"unexpected public client secret", factorClient, otherSecret, "UnauthorizedException"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, body := postCognito(t, ts.URL, "RevokeToken", map[string]interface{}{
				"Token": session.refresh, "ClientId": tc.client, "ClientSecret": tc.secret,
			})
			require.Equal(t, http.StatusBadRequest, status, "body=%v", body)
			require.Equal(t, tc.code, body["__type"])
			revoked, err := store.RefreshTokenRevoked(t.Context(), grant.OriginJTI)
			require.NoError(t, err)
			require.False(t, revoked)
			status, body = refreshStatus(t, ts.URL, session.refresh)
			require.Equal(t, http.StatusOK, status, "another client's revocation attempt leaves the original session usable: body=%v", body)
		})
	}

	deleted, err := store.DeleteClient(t.Context(), factorPool, factorClient)
	require.NoError(t, err)
	require.True(t, deleted)
	status, body := postCognito(t, ts.URL, "RevokeToken", map[string]interface{}{
		"Token": session.refresh, "ClientId": factorClient,
	})
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "UnauthorizedException", body["__type"])
	revoked, err := store.RefreshTokenRevoked(t.Context(), grant.OriginJTI)
	require.NoError(t, err)
	require.False(t, revoked, "a deleted client cannot authorize revocation of its old grant")
}

func TestFactors_RevokeTokenClientLookupFailure(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	require.NoError(t, store.Close())
	status, body := postCognito(t, ts.URL, "RevokeToken", map[string]interface{}{
		"Token": "token", "ClientId": factorClient,
	})
	require.Equal(t, http.StatusInternalServerError, status)
	require.Equal(t, "InternalErrorException", body["__type"], "storage errors must not become client-authentication failures")
}
