package cognito

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
)

func TestNativeUnsupportedSettingsFailBeforeMutation(t *testing.T) {
	env := newWorkflowEnvironment(t, nil, nil)
	poolCases := []map[string]any{
		{"MfaConfiguration": "ON"},
		{"LambdaConfig": map[string]any{"PreSignUp": "arn:aws:lambda:eu-west-1:123456789012:function:pre"}},
		{"UserPoolAddOns": map[string]any{"AdvancedSecurityMode": "ENFORCED"}},
		{"AdminCreateUserConfig": map[string]any{"InviteMessageTemplate": map[string]any{"EmailMessage": "Hello {####}"}}},
		{"Policies": map[string]any{"PasswordPolicy": map[string]any{"PasswordHistorySize": 2}}},
		{"Policies": map[string]any{"PasswordPolicy": map[string]any{"min_length": 10}}},
		{"VerificationMessageTemplate": map[string]any{"DefaultEmailOption": "CONFIRM_WITH_LINK"}},
	}
	for _, settings := range poolCases {
		request := map[string]any{"PoolName": "unsupported"}
		for k, v := range settings {
			request[k] = v
		}
		status, body := postCognito(t, env.server.URL, "CreateUserPool", request)
		require.Equal(t, http.StatusBadRequest, status, "%v: %v", settings, body)
		require.Equal(t, "InvalidParameterException", body["__type"])
	}
	var pools int
	require.NoError(t, env.store.db.QueryRow(`SELECT COUNT(*) FROM pools`).Scan(&pools))
	require.Equal(t, 1, pools)
	clientCases := []map[string]any{
		{"EnableTokenRevocation": false}, {"AllowedOAuthFlowsUserPoolClient": true},
		{"PreventUserExistenceErrors": "ENABLED"},
		{"RefreshTokenRotation": map[string]any{"Feature": "ENABLED"}},
		{"ExplicitAuthFlows": []string{"ALLOW_USER_AUTH"}},
		{"ReadAttributes": []string{"custom:undeclared"}},
		{"WriteAttributes": []string{"email_verified"}},
	}
	for _, settings := range clientCases {
		request := map[string]any{"UserPoolId": env.pool, "ClientName": "unsupported"}
		for k, v := range settings {
			request[k] = v
		}
		status, body := postCognito(t, env.server.URL, "CreateUserPoolClient", request)
		require.Equal(t, http.StatusBadRequest, status, "%v: %v", settings, body)
		require.Equal(t, "InvalidParameterException", body["__type"])
	}
	var clients int
	require.NoError(t, env.store.db.QueryRow(`SELECT COUNT(*) FROM clients`).Scan(&clients))
	require.Equal(t, 1, clients)
}

func TestNativeRequiredAttributesCompleteTemporaryPasswordAtomically(t *testing.T) {
	env := newWorkflowEnvironment(t, map[string]any{"Schema": []map[string]any{{"Name": "email", "Required": true, "Mutable": true}, {"Name": "tenant", "AttributeDataType": "String", "Mutable": true}}}, map[string]any{"ReadAttributes": []string{"email"}, "WriteAttributes": []string{"custom:tenant"}})
	status, body := postCognito(t, env.server.URL, "AdminCreateUser", map[string]any{"UserPoolId": env.pool, "Username": "invited", "TemporaryPassword": "InitialPass1!", "MessageAction": "SUPPRESS"})
	require.Equal(t, 200, status, "%v", body)
	status, body = postCognito(t, env.server.URL, "InitiateAuth", map[string]any{"ClientId": env.client, "AuthFlow": "USER_PASSWORD_AUTH", "AuthParameters": map[string]string{"USERNAME": "invited", "PASSWORD": "InitialPass1!"}})
	require.Equal(t, 200, status, "%v", body)
	require.Equal(t, "NEW_PASSWORD_REQUIRED", body["ChallengeName"])
	parameters := body["ChallengeParameters"].(map[string]any)
	var missing []string
	require.NoError(t, json.Unmarshal([]byte(parameters["requiredAttributes"].(string)), &missing))
	require.Equal(t, []string{"userAttributes.email"}, missing)
	session := body["Session"].(string)
	response := map[string]any{"ClientId": env.client, "Session": session, "ChallengeName": "NEW_PASSWORD_REQUIRED", "ChallengeResponses": map[string]string{"USERNAME": "invited", "NEW_PASSWORD": "PermanentPass2!"}}
	status, body = postCognito(t, env.server.URL, "RespondToAuthChallenge", response)
	require.Equal(t, 400, status, "%v", body)
	require.Equal(t, "InvalidParameterException", body["__type"])
	user, err := env.store.LookupPoolUser(t.Context(), env.pool, "invited")
	require.NoError(t, err)
	require.Equal(t, "FORCE_CHANGE_PASSWORD", user.Status)
	var pending int
	require.NoError(t, env.store.db.QueryRow(`SELECT COUNT(*) FROM challenge_sessions WHERE sub=? AND used=0`, user.Sub).Scan(&pending))
	require.Equal(t, 1, pending)
	responses := response["ChallengeResponses"].(map[string]string)
	responses["userAttributes.email"] = "invited@example.test"
	responses["userAttributes.custom:tenant"] = "tenant-A"
	status, body = postCognito(t, env.server.URL, "RespondToAuthChallenge", response)
	require.Equal(t, 200, status, "%v", body)
	require.NotEmpty(t, body["AuthenticationResult"])
	updated, err := env.store.LookupUserBySub(t.Context(), user.Sub)
	require.NoError(t, err)
	require.Equal(t, "CONFIRMED", updated.Status)
	require.Equal(t, user.AuthVersion+1, updated.AuthVersion)
	attrs, err := env.store.LoadUserAttributes(t.Context(), user.Sub)
	require.NoError(t, err)
	require.Equal(t, "invited@example.test", attrs["email"])
	require.Equal(t, "tenant-A", attrs["custom:tenant"])
	status, body = postCognito(t, env.server.URL, "RespondToAuthChallenge", response)
	require.Equal(t, 400, status, "%v", body)
	require.Equal(t, "NotAuthorizedException", body["__type"])
}

func TestNativeInvitationAdmissionAndResendPreserveAccountOnRefusal(t *testing.T) {
	env := newWorkflowEnvironment(t, nil, nil)
	request := map[string]any{"UserPoolId": env.pool, "Username": "invited", "TemporaryPassword": "InitialPass1!", "UserAttributes": []map[string]string{{"Name": "email", "Value": "invite@example.test"}}}
	// The AWS default medium is SMS, so an email alone cannot admit default delivery.
	status, body := postCognito(t, env.server.URL, "AdminCreateUser", request)
	require.Equal(t, 400, status, "%v", body)
	require.Equal(t, "InvalidParameterException", body["__type"])
	_, err := env.store.LookupPoolUser(t.Context(), env.pool, "invited")
	require.ErrorIs(t, err, sql.ErrNoRows)
	require.Zero(t, env.capture.count())
	request["MessageAction"] = "SUPPRESS"
	request["DesiredDeliveryMediums"] = []string{"INVALID"}
	status, body = postCognito(t, env.server.URL, "AdminCreateUser", request)
	require.Equal(t, 400, status, "%v", body)
	_, err = env.store.LookupPoolUser(t.Context(), env.pool, "invited")
	require.ErrorIs(t, err, sql.ErrNoRows)
	delete(request, "MessageAction")
	request["DesiredDeliveryMediums"] = []string{"EMAIL"}
	status, body = postCognito(t, env.server.URL, "AdminCreateUser", request)
	require.Equal(t, 200, status, "%v", body)
	require.Equal(t, "InitialPass1!", env.capture.last(t).TemporaryPassword)
	user, err := env.store.LookupPoolUser(t.Context(), env.pool, "invited")
	require.NoError(t, err)
	request["MessageAction"] = "RESEND"
	request["TemporaryPassword"] = "ReplacementPass2!"
	request["DesiredDeliveryMediums"] = []string{"EMAIL", "INVALID"}
	status, body = postCognito(t, env.server.URL, "AdminCreateUser", request)
	require.Equal(t, 400, status, "%v", body)
	require.Equal(t, "InvalidParameterException", body["__type"])
	unchanged, err := env.store.LookupUserBySub(t.Context(), user.Sub)
	require.NoError(t, err)
	require.Equal(t, user.PasswordHash, unchanged.PasswordHash)
	require.Equal(t, user.AuthVersion, unchanged.AuthVersion)
	require.Equal(t, 1, env.capture.count())
}

func TestNativeAccessTokenAttributeScopeAndClaimShape(t *testing.T) {
	env := newWorkflowEnvironment(t, nil, nil)
	note := env.signup(t, "native-access", "native@example.test")
	env.confirm(t, "native-access", note.Code)
	status, body := postCognito(t, env.server.URL, "InitiateAuth", map[string]any{"ClientId": env.client, "AuthFlow": "USER_PASSWORD_AUTH", "AuthParameters": map[string]string{"USERNAME": "native-access", "PASSWORD": "InitialPass1!"}})
	require.Equal(t, 200, status, "%v", body)
	token := body["AuthenticationResult"].(map[string]any)["AccessToken"].(string)
	parsed, _, err := jwt.NewParser().ParseUnverified(token, jwt.MapClaims{})
	require.NoError(t, err)
	claims := parsed.Claims.(jwt.MapClaims)
	require.Equal(t, "aws.cognito.signin.user.admin", claims["scope"])
	require.NotContains(t, claims, "email")
	require.NotContains(t, claims, "aud")
	delete(claims, "scope")
	signing, err := env.store.LoadSigningKey(t.Context(), env.pool)
	require.NoError(t, err)
	forged := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	forged.Header["kid"] = signing.Kid
	withoutScope, err := forged.SignedString(signing.Private)
	require.NoError(t, err)
	status, body = postCognito(t, env.server.URL, "GetUser", map[string]any{"AccessToken": withoutScope})
	require.Equal(t, 400, status, "%v", body)
	require.Equal(t, "NotAuthorizedException", body["__type"])
}

func TestLegacyFixtureProfileCannotBypassNativeMFA(t *testing.T) {
	handler, server, store := newCognitoTestServer(t)
	require.Equal(t, DevProfileLegacyFixtures, handler.devProfile)
	status, body := postCognito(t, server.URL, "CreateUserPool", map[string]any{"PoolName": "native"})
	require.Equal(t, 200, status, "%v", body)
	pool := body["UserPool"].(map[string]any)["Id"].(string)
	status, body = postCognito(t, server.URL, "CreateUserPoolClient", map[string]any{"UserPoolId": pool, "ClientName": "native", "ExplicitAuthFlows": []string{"ALLOW_USER_PASSWORD_AUTH"}})
	require.Equal(t, 200, status, "%v", body)
	client := body["UserPoolClient"].(map[string]any)["ClientId"].(string)
	sub, err := store.UpsertSeedUser(t.Context(), pool, "native-mfa", "mfa@example.test", "InitialPass1!", true)
	require.NoError(t, err)
	status, body = postCognito(t, server.URL, "InitiateAuth", map[string]any{"ClientId": client, "AuthFlow": "USER_PASSWORD_AUTH", "AuthParameters": map[string]string{"USERNAME": "native-mfa", "PASSWORD": "InitialPass1!"}})
	require.Equal(t, 200, status, "%v", body)
	require.Equal(t, "SOFTWARE_TOKEN_MFA", body["ChallengeName"])
	session := body["Session"].(string)
	status, body = postCognito(t, server.URL, "RespondToAuthChallenge", map[string]any{"ClientId": client, "Session": session, "ChallengeName": "SOFTWARE_TOKEN_MFA", "ChallengeResponses": map[string]string{"USERNAME": "native-mfa", "SOFTWARE_TOKEN_MFA_CODE": "123456"}})
	require.Equal(t, 400, status, "%v", body)
	require.Equal(t, "CodeMismatchException", body["__type"])
	user, err := store.LookupUserBySub(t.Context(), sub)
	require.NoError(t, err)
	require.True(t, user.MFAEnabled)
	// Strict services reject deterministic AWS request extension fields before persistence.
	strict := NewHandler(store, Options{})
	require.False(t, strict.allowLegacyFixtureAuth(&CognitoClient{}))
}
