package cognito

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func inviteForCompletion(t *testing.T, env *workflowEnvironment, attributes []map[string]string) (*CognitoUser, map[string]any) {
	t.Helper()
	status, body := postCognito(t, env.server.URL, "AdminCreateUser", map[string]any{"UserPoolId": env.pool, "Username": "invited", "TemporaryPassword": "InitialPass1!", "MessageAction": "SUPPRESS", "UserAttributes": attributes})
	require.Equal(t, 200, status, "%v", body)
	status, body = postCognito(t, env.server.URL, "InitiateAuth", map[string]any{"ClientId": env.client, "AuthFlow": "USER_PASSWORD_AUTH", "AuthParameters": map[string]string{"USERNAME": "invited", "PASSWORD": "InitialPass1!"}})
	require.Equal(t, 200, status, "%v", body)
	require.Equal(t, "NEW_PASSWORD_REQUIRED", body["ChallengeName"])
	user, err := env.store.LookupPoolUser(t.Context(), env.pool, "invited")
	require.NoError(t, err)
	return user, map[string]any{"ClientId": env.client, "Session": body["Session"], "ChallengeName": "NEW_PASSWORD_REQUIRED", "ChallengeResponses": map[string]string{"USERNAME": "invited", "NEW_PASSWORD": "PermanentPass2!"}}
}

func TestNewPasswordInitialAssignmentOfRequiredImmutableStandardAttribute(t *testing.T) {
	env := newWorkflowEnvironment(t, map[string]any{"Schema": []map[string]any{{"Name": "name", "Required": true, "Mutable": false}, {"Name": "locked", "Mutable": false}}}, map[string]any{"ReadAttributes": []string{"name", "email"}, "WriteAttributes": []string{"custom:locked"}})
	user, response := inviteForCompletion(t, env, []map[string]string{{"Name": "email", "Value": "invite@example.test"}})
	values := response["ChallengeResponses"].(map[string]string)
	values["userAttributes.name"] = "Initial identity"
	values["userAttributes.custom:locked"] = "not creation"
	status, body := postCognito(t, env.server.URL, "RespondToAuthChallenge", response)
	require.Equal(t, 400, status, "%v", body)
	require.Equal(t, "InvalidParameterException", body["__type"])
	unchanged, err := env.store.LookupUserBySub(t.Context(), user.Sub)
	require.NoError(t, err)
	require.Equal(t, user.PasswordHash, unchanged.PasswordHash)
	require.Equal(t, user.AuthVersion, unchanged.AuthVersion)
	delete(values, "userAttributes.custom:locked")
	status, body = postCognito(t, env.server.URL, "RespondToAuthChallenge", response)
	require.Equal(t, 200, status, "%v", body)
	attrs, err := env.store.LoadUserAttributes(t.Context(), user.Sub)
	require.NoError(t, err)
	require.Equal(t, "Initial identity", attrs["name"])
	idToken := body["AuthenticationResult"].(map[string]any)["IdToken"].(string)
	require.Equal(t, "Initial identity", parseClaimsUnverified(t, idToken)["name"])
	status, body = postCognito(t, env.server.URL, "AdminUpdateUserAttributes", map[string]any{"UserPoolId": env.pool, "Username": "invited", "UserAttributes": []map[string]string{{"Name": "name", "Value": "Replacement"}}})
	require.Equal(t, 400, status, "%v", body)
	require.Equal(t, "InvalidParameterException", body["__type"])
	attrs, err = env.store.LoadUserAttributes(t.Context(), user.Sub)
	require.NoError(t, err)
	require.Equal(t, "Initial identity", attrs["name"])
}

func TestNewPasswordContactReplacementInvalidatesOldVerification(t *testing.T) {
	env := newWorkflowEnvironment(t, nil, map[string]any{"ReadAttributes": []string{"email", "email_verified", "phone_number", "phone_number_verified"}})
	user, response := inviteForCompletion(t, env, []map[string]string{{"Name": "email", "Value": "old@example.test"}, {"Name": "email_verified", "Value": "true"}, {"Name": "phone_number", "Value": "+15551234567"}, {"Name": "phone_number_verified", "Value": "true"}})
	require.NoError(t, env.store.putVerification(t.Context(), user, verificationSpec{"attribute:email", "email", "old@example.test", "123456", env.store.now().Add(time.Hour)}))
	values := response["ChallengeResponses"].(map[string]string)
	values["userAttributes.email"] = "new@example.test"
	values["userAttributes.phone_number"] = "+15557654321"
	status, body := postCognito(t, env.server.URL, "RespondToAuthChallenge", response)
	require.Equal(t, 200, status, "%v", body)
	attrs, err := env.store.LoadUserAttributes(t.Context(), user.Sub)
	require.NoError(t, err)
	require.Equal(t, "new@example.test", attrs["email"])
	require.Equal(t, "false", attrs["email_verified"])
	require.Equal(t, "+15557654321", attrs["phone_number"])
	require.Equal(t, "false", attrs["phone_number_verified"])
	claims := parseClaimsUnverified(t, body["AuthenticationResult"].(map[string]any)["IdToken"].(string))
	require.Equal(t, false, claims["email_verified"])
	require.Equal(t, false, claims["phone_number_verified"])
	var codes int
	require.NoError(t, env.store.db.QueryRow(`SELECT COUNT(*) FROM verification_codes WHERE sub=?`, user.Sub).Scan(&codes))
	require.Zero(t, codes)
}

func TestNewPasswordRevalidatesCurrentRequiredAttributesInsideTransaction(t *testing.T) {
	env := newWorkflowEnvironment(t, map[string]any{"Schema": []map[string]any{{"Name": "name", "Required": true, "Mutable": true}}}, nil)
	user, _ := inviteForCompletion(t, env, []map[string]string{{"Name": "email", "Value": "invite@example.test"}})
	require.NoError(t, env.store.CreateChallengeSession(t.Context(), "snapshot-test", user.Sub, env.pool, env.client, "NEW_PASSWORD_REQUIRED", time.Minute))
	row, err := env.store.LookupChallengeSession(t.Context(), "snapshot-test")
	require.NoError(t, err)
	status, body := postCognito(t, env.server.URL, "AdminUpdateUserAttributes", map[string]any{"UserPoolId": env.pool, "Username": "invited", "UserAttributes": []map[string]string{{"Name": "name", "Value": "Admin supplied"}}})
	require.Equal(t, 200, status, "%v", body)
	pool, err := env.store.LookupPool(t.Context(), env.pool)
	require.NoError(t, err)
	client, err := env.store.LookupClient(t.Context(), env.client)
	require.NoError(t, err)
	_, err = env.store.completeNewPassword(t.Context(), user, row, "PermanentPass2!", map[string]string{"name": "Stale overwrite"}, pool, client)
	require.Error(t, err)
	require.Contains(t, err.Error(), "already has a value")
	unchanged, err := env.store.LookupUserBySub(t.Context(), user.Sub)
	require.NoError(t, err)
	require.Equal(t, user.PasswordHash, unchanged.PasswordHash)
	require.Equal(t, user.AuthVersion, unchanged.AuthVersion)
	require.Equal(t, "FORCE_CHANGE_PASSWORD", unchanged.Status)
	row, err = env.store.LookupChallengeSession(t.Context(), "snapshot-test")
	require.NoError(t, err)
	require.False(t, row.Used)
	attrs, err := env.store.LoadUserAttributes(t.Context(), user.Sub)
	require.NoError(t, err)
	require.Equal(t, "Admin supplied", attrs["name"])
	// A retry that leaves the newly supplied required field alone can complete.
	_, err = env.store.completeNewPassword(t.Context(), user, row, "PermanentPass2!", nil, pool, client)
	require.NoError(t, err)
}
