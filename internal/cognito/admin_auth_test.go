package cognito

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	adminAuthPool     = "admin-auth-pool"
	adminAuthClient   = "admin-auth-client"
	adminAuthUsername = "integration-user-001"
	adminAuthEmail    = "invitee@example.test"
	adminAuthPassword = "Permanent1!Pass"
)

// Administrative provisioning exercises the identity and temporary-password
// contracts rather than bypassing them with the older confirmed seed helper.
func createAdminAuthUser(t *testing.T, baseURL string, store *CognitoStore, config PoolSignInConfig, secret string, permanent bool) *CognitoUser {
	t.Helper()
	require.NoError(t, store.UpsertPool(t.Context(), adminAuthPool, "us-east-1"))
	require.NoError(t, store.SetPoolSignInConfig(t.Context(), adminAuthPool, config))
	require.NoError(t, store.UpsertClient(t.Context(), adminAuthClient, adminAuthPool, secret))
	username := adminAuthUsername
	if config.EmailAsUsername {
		username = adminAuthEmail
	}
	status, body := postCognito(t, baseURL, "AdminCreateUser", map[string]interface{}{
		"UserPoolId": adminAuthPool, "Username": username,
		"TemporaryPassword": adminAuthPassword, "MessageAction": "SUPPRESS",
		"UserAttributes": []map[string]string{
			{"Name": "email", "Value": adminAuthEmail},
			{"Name": "email_verified", "Value": "true"},
			{"Name": "custom:role", "Value": "developer"},
		},
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	if permanent {
		status, body = postCognito(t, baseURL, "AdminSetUserPassword", map[string]interface{}{
			"UserPoolId": adminAuthPool, "Username": username,
			"Password": adminAuthPassword, "Permanent": true,
		})
		require.Equal(t, http.StatusOK, status, "body=%v", body)
	}
	user, err := store.LookupPoolUser(t.Context(), adminAuthPool, username)
	require.NoError(t, err)
	return user
}

func adminPasswordAuth(t *testing.T, baseURL, flow, login, secret, password string) (int, map[string]interface{}) {
	t.Helper()
	parameters := map[string]string{"USERNAME": login, "PASSWORD": password}
	if secret != "" {
		parameters["SECRET_HASH"] = computeSecretHash(secret, login, adminAuthClient)
	}
	return postCognito(t, baseURL, "AdminInitiateAuth", map[string]interface{}{
		"UserPoolId": adminAuthPool, "ClientId": adminAuthClient,
		"AuthFlow": flow, "AuthParameters": parameters,
	})
}

func adminRefreshAuth(t *testing.T, baseURL, operation, flow, refresh, hash string) (int, map[string]interface{}) {
	t.Helper()
	request := map[string]interface{}{
		"AuthFlow": flow, "ClientId": adminAuthClient,
		"AuthParameters": map[string]string{"REFRESH_TOKEN": refresh, "SECRET_HASH": hash},
	}
	if operation == "AdminInitiateAuth" {
		request["UserPoolId"] = adminAuthPool
	}
	return postCognito(t, baseURL, operation, request)
}

func TestAdminPasswordAuth_IdentityClaimsAndEntryPoints(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	user := createAdminAuthUser(t, ts.URL, store, PoolSignInConfig{EmailAlias: true, CaseSensitive: true}, "client-secret", true)
	for _, flow := range []string{authFlowAdminUserPassword, authFlowAdminNoSRP} {
		for _, login := range []string{adminAuthUsername, adminAuthEmail} {
			status, body := adminPasswordAuth(t, ts.URL, flow, login, "client-secret", adminAuthPassword)
			require.Equal(t, http.StatusOK, status, "flow=%s login=%s body=%v", flow, login, body)
			result := readAuthResult(t, body)
			access := parseClaimsUnverified(t, result["AccessToken"].(string))
			assert.Equal(t, user.Sub, access["sub"])
			assert.Equal(t, adminAuthUsername, access["username"])
			assert.Equal(t, adminAuthEmail, access["email"])
			assert.Equal(t, float64(user.AuthVersion), access[authVersionClaim])
			id := result["IdToken"].(string)
			idClaims := parseClaimsUnverified(t, id)
			assert.Equal(t, user.Sub, idClaims["sub"])
			assert.Equal(t, adminAuthUsername, idClaims["cognito:username"])
			assert.Equal(t, adminAuthEmail, idClaims["email"])
			assert.Equal(t, true, idClaims["email_verified"])
			assert.Equal(t, "developer", idClaims["custom:role"])
			assert.Equal(t, "id", idClaims["token_use"])
			assert.Equal(t, adminAuthClient, idClaims["aud"])
			assert.Equal(t, access["auth_time"], idClaims["auth_time"])
			assert.Equal(t, access["origin_jti"], idClaims["origin_jti"])
			signing, err := store.LoadSigningKey(t.Context(), adminAuthPool)
			require.NoError(t, err)
			_, err = jwt.Parse(id, func(token *jwt.Token) (interface{}, error) { return signing.Public, nil },
				jwt.WithIssuer("http://localhost:4100/"+adminAuthPool), jwt.WithAudience(adminAuthClient), jwt.WithValidMethods([]string{"RS256"}))
			require.NoError(t, err)
			status, body = postCognito(t, ts.URL, "GetUser", map[string]interface{}{"AccessToken": id})
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "NotAuthorizedException", body["__type"])
		}
	}
	for _, flow := range []string{authFlowAdminUserPassword, authFlowAdminNoSRP} {
		status, body := postCognito(t, ts.URL, "InitiateAuth", map[string]interface{}{
			"ClientId": adminAuthClient, "AuthFlow": flow,
			"AuthParameters": map[string]string{"USERNAME": adminAuthUsername, "PASSWORD": adminAuthPassword},
		})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidParameterException", body["__type"])
	}
}

func TestAdminRefreshAuth_AliasesAndPoolDependentSecretHash(t *testing.T) {
	for _, config := range []PoolSignInConfig{{EmailAlias: true, CaseSensitive: true}, {EmailAsUsername: true, CaseSensitive: true}} {
		t.Run(map[bool]string{true: "email-sign-in", false: "username-with-email-alias"}[config.EmailAsUsername], func(t *testing.T) {
			_, ts, store := newCognitoTestServer(t)
			user := createAdminAuthUser(t, ts.URL, store, config, "client-secret", true)
			status, body := adminPasswordAuth(t, ts.URL, authFlowAdminUserPassword, adminAuthEmail, "client-secret", adminAuthPassword)
			require.Equal(t, http.StatusOK, status, "body=%v", body)
			initial := readAuthResult(t, body)
			refresh := initial["RefreshToken"].(string)
			original := parseClaimsUnverified(t, initial["AccessToken"].(string))
			hashUsername := user.Username
			if config.EmailAsUsername {
				hashUsername = user.Sub
			}
			for _, operation := range []string{"AdminInitiateAuth", "InitiateAuth"} {
				for _, flow := range []string{authFlowRefreshToken, authFlowRefreshTokenAlias} {
					status, body = adminRefreshAuth(t, ts.URL, operation, flow, refresh, computeSecretHash("client-secret", hashUsername, adminAuthClient))
					require.Equal(t, http.StatusOK, status, "operation=%s flow=%s body=%v", operation, flow, body)
					result := readAuthResult(t, body)
					assert.NotContains(t, result, "RefreshToken")
					for _, name := range []string{"AccessToken", "IdToken"} {
						claims := parseClaimsUnverified(t, result[name].(string))
						assert.Equal(t, original["auth_time"], claims["auth_time"])
						assert.Equal(t, original["origin_jti"], claims["origin_jti"])
						assert.Equal(t, original[authVersionClaim], claims[authVersionClaim])
					}
				}
			}
			wrongIdentities := []string{adminAuthEmail}
			if !config.EmailAsUsername {
				wrongIdentities = append(wrongIdentities, user.Sub)
			}
			for _, identity := range wrongIdentities {
				status, body = adminRefreshAuth(t, ts.URL, "AdminInitiateAuth", authFlowRefreshToken, refresh, computeSecretHash("client-secret", identity, adminAuthClient))
				assert.Equal(t, http.StatusBadRequest, status, "identity=%s body=%v", identity, body)
				assert.Equal(t, "NotAuthorizedException", body["__type"])
			}
		})
	}
}

func TestAdminPasswordAuth_TemporaryPasswordBeforeMFAAndExpiry(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	user := createAdminAuthUser(t, ts.URL, store, PoolSignInConfig{CaseSensitive: true}, "", false)
	_, err := store.DB().ExecContext(t.Context(), `UPDATE users SET mfa_enabled=1 WHERE sub=?`, user.Sub)
	require.NoError(t, err)
	status, body := adminPasswordAuth(t, ts.URL, authFlowAdminUserPassword, user.Username, "", adminAuthPassword)
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	assert.Equal(t, "NEW_PASSWORD_REQUIRED", body["ChallengeName"])
	assert.NotContains(t, body, "AuthenticationResult")
	assert.NotEmpty(t, body["Session"])
	require.NoError(t, store.SetPoolPasswordPolicy(t.Context(), adminAuthPool, `{"TemporaryPasswordValidityDays":2}`))
	_, err = store.DB().ExecContext(t.Context(), `UPDATE users SET password_changed_at=? WHERE sub=?`, time.Now().Add(-3*24*time.Hour).Unix(), user.Sub)
	require.NoError(t, err)
	status, body = adminPasswordAuth(t, ts.URL, authFlowAdminUserPassword, user.Username, "", adminAuthPassword)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NotAuthorizedException", body["__type"])
	assert.NotContains(t, body, "Session")
	assert.Contains(t, body["message"], "expired")
}

func TestAdminAuth_DisableAndAdministrativeResetInvalidateGrants(t *testing.T) {
	for _, mutation := range []string{"disable-enable", "permanent-reset", "temporary-reset"} {
		t.Run(mutation, func(t *testing.T) {
			_, ts, store := newCognitoTestServer(t)
			user := createAdminAuthUser(t, ts.URL, store, PoolSignInConfig{CaseSensitive: true}, "", true)
			status, body := adminPasswordAuth(t, ts.URL, authFlowAdminUserPassword, user.Username, "", adminAuthPassword)
			require.Equal(t, http.StatusOK, status, "body=%v", body)
			result := readAuthResult(t, body)
			access, refresh := result["AccessToken"].(string), result["RefreshToken"].(string)
			if mutation == "disable-enable" {
				status, body = postCognito(t, ts.URL, "AdminDisableUser", map[string]interface{}{"UserPoolId": adminAuthPool, "Username": user.Username})
				require.Equal(t, http.StatusOK, status, "body=%v", body)
				status, body = adminPasswordAuth(t, ts.URL, authFlowAdminUserPassword, user.Username, "", adminAuthPassword)
				assert.Equal(t, http.StatusBadRequest, status)
				assert.Equal(t, "NotAuthorizedException", body["__type"])
				status, body = adminRefreshAuth(t, ts.URL, "AdminInitiateAuth", authFlowRefreshToken, refresh, "")
				assert.Equal(t, http.StatusBadRequest, status)
				assert.Equal(t, "NotAuthorizedException", body["__type"])
				status, body = postCognito(t, ts.URL, "AdminEnableUser", map[string]interface{}{"UserPoolId": adminAuthPool, "Username": user.Username})
			} else {
				status, body = postCognito(t, ts.URL, "AdminSetUserPassword", map[string]interface{}{
					"UserPoolId": adminAuthPool, "Username": user.Username, "Password": adminAuthPassword, "Permanent": mutation == "permanent-reset",
				})
			}
			require.Equal(t, http.StatusOK, status, "body=%v", body)
			status, body = postCognito(t, ts.URL, "GetUser", map[string]interface{}{"AccessToken": access})
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "NotAuthorizedException", body["__type"])
			status, body = adminRefreshAuth(t, ts.URL, "AdminInitiateAuth", authFlowRefreshToken, refresh, "")
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "NotAuthorizedException", body["__type"])
			_, err := VerifyAccessToken(t.Context(), store, "http://localhost:4100", access)
			require.NoError(t, err, "offline signature verification is independent of account lifecycle")
			status, body = adminPasswordAuth(t, ts.URL, authFlowAdminUserPassword, user.Username, "", adminAuthPassword)
			require.Equal(t, http.StatusOK, status, "fresh same-second login must work: body=%v", body)
			if mutation == "temporary-reset" {
				assert.Equal(t, "NEW_PASSWORD_REQUIRED", body["ChallengeName"])
			} else {
				claims := parseClaimsUnverified(t, readAuthResult(t, body)["AccessToken"].(string))
				assert.Greater(t, claims[authVersionClaim].(float64), float64(user.AuthVersion))
			}
		})
	}
}

func TestAdminRefreshAuth_InvalidExpiredRevokedAndForeignPool(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	user := createAdminAuthUser(t, ts.URL, store, PoolSignInConfig{CaseSensitive: true}, "", true)
	status, body := adminPasswordAuth(t, ts.URL, authFlowAdminUserPassword, user.Username, "", adminAuthPassword)
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	refresh := readAuthResult(t, body)["RefreshToken"].(string)
	grant := newTokenGrant()
	grant.AuthVersion = user.AuthVersion
	expired, err := SignRefreshToken(t.Context(), store, "http://localhost:4100", adminAuthPool, adminAuthClient, user.Sub, grant, -time.Hour)
	require.NoError(t, err)
	foreign, err := SignRefreshToken(t.Context(), store, "http://localhost:4100", "foreign-pool", adminAuthClient, user.Sub, grant, time.Hour)
	require.NoError(t, err)
	for name, token := range map[string]string{"malformed": "not-a-token", "tampered": refresh[:len(refresh)-10] + strings.Repeat("A", 10), "expired": expired, "foreign-pool": foreign} {
		status, body = adminRefreshAuth(t, ts.URL, "AdminInitiateAuth", authFlowRefreshToken, token, "")
		assert.Equal(t, http.StatusBadRequest, status, "name=%s body=%v", name, body)
		assert.Equal(t, "NotAuthorizedException", body["__type"])
	}
	// Audience alone cannot authorize a token carrying another client identity.
	claims := parseClaimsUnverified(t, refresh)
	claims["client_id"] = "foreign-client"
	signing, err := store.LoadSigningKey(t.Context(), adminAuthPool)
	require.NoError(t, err)
	wrongClient := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	wrongClient.Header["kid"] = signing.Kid
	wrongClientToken, err := wrongClient.SignedString(signing.Private)
	require.NoError(t, err)
	status, body = adminRefreshAuth(t, ts.URL, "AdminInitiateAuth", authFlowRefreshToken, wrongClientToken, "")
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NotAuthorizedException", body["__type"])
	status, body = postCognito(t, ts.URL, "RevokeToken", map[string]interface{}{"ClientId": adminAuthClient, "Token": refresh})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	status, body = adminRefreshAuth(t, ts.URL, "AdminInitiateAuth", authFlowRefreshToken, refresh, "")
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NotAuthorizedException", body["__type"])
}

func TestWriteAuthenticated_RejectsStaleAccountSnapshot(t *testing.T) {
	for _, mutate := range []string{"disable", "reset", "delete"} {
		t.Run(mutate, func(t *testing.T) {
			handler, ts, store := newCognitoTestServer(t)
			user := createAdminAuthUser(t, ts.URL, store, PoolSignInConfig{CaseSensitive: true}, "", true)
			switch mutate {
			case "disable":
				require.NoError(t, store.SetUserEnabled(t.Context(), user.Sub, false))
			case "reset":
				require.NoError(t, store.SetUserPassword(t.Context(), user.Sub, "Changed2!Pass", "CONFIRMED"))
			case "delete":
				_, err := store.DB().ExecContext(t.Context(), `DELETE FROM users WHERE sub=?`, user.Sub)
				require.NoError(t, err)
			}
			response := httptest.NewRecorder()
			handler.writeAuthenticated(response, httptest.NewRequest(http.MethodPost, "/", nil), adminAuthPool, adminAuthClient, user, "test")
			assert.Equal(t, http.StatusBadRequest, response.Code)
			assert.Contains(t, response.Body.String(), "NotAuthorizedException")
			assert.NotContains(t, response.Body.String(), "AuthenticationResult")
		})
	}
}

func TestAdminPasswordAuth_ExplicitClientFlowConfiguration(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	user := createAdminAuthUser(t, ts.URL, store, PoolSignInConfig{CaseSensitive: true}, "", true)
	require.NoError(t, store.SetClientAuthConfig(t.Context(), adminAuthClient, []string{"ALLOW_ADMIN_USER_PASSWORD_AUTH"}, 3))
	for _, flow := range []string{authFlowAdminUserPassword, authFlowAdminNoSRP} {
		status, body := adminPasswordAuth(t, ts.URL, flow, user.Username, "", adminAuthPassword)
		require.Equal(t, http.StatusOK, status, "body=%v", body)
		status, body = adminRefreshAuth(t, ts.URL, "AdminInitiateAuth", authFlowRefreshToken, readAuthResult(t, body)["RefreshToken"].(string), "")
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidParameterException", body["__type"])
	}
}

func TestClientAllowsAuthFlow_LegacyExplicitFlowNames(t *testing.T) {
	for _, test := range []struct{ enabled, request string }{
		{"CUSTOM_AUTH_FLOW_ONLY", "CUSTOM_AUTH"},
		{"ADMIN_NO_SRP_AUTH", "ADMIN_USER_PASSWORD_AUTH"},
		{"ADMIN_NO_SRP_AUTH", "ADMIN_NO_SRP_AUTH"},
		{"USER_PASSWORD_AUTH", "USER_PASSWORD_AUTH"},
	} {
		t.Run(test.enabled+"/"+test.request, func(t *testing.T) {
			client := &CognitoClient{ExplicitAuthFlows: []string{test.enabled}}
			assert.True(t, clientAllowsAuthFlow(client, test.request))
			assert.False(t, clientAllowsAuthFlow(client, "REFRESH_TOKEN_AUTH"))
		})
	}
}
