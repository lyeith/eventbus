package cognito

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func workflowLogin(t *testing.T, env *workflowEnvironment, username, password string) (string, map[string]interface{}) {
	t.Helper()
	parameters := map[string]string{"USERNAME": username, "PASSWORD": password}
	if env.secret != "" {
		parameters["SECRET_HASH"] = computeSecretHash(env.secret, username, env.client)
	}
	status, body := postCognito(t, env.server.URL, "InitiateAuth", map[string]interface{}{"ClientId": env.client, "AuthFlow": "USER_PASSWORD_AUTH", "AuthParameters": parameters})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	auth := readAuthResult(t, body)
	return auth["AccessToken"].(string), parseClaimsUnverified(t, auth["IdToken"].(string))
}

func TestWorkflowAttributePermissionsAndAtomicUpdates(t *testing.T) {
	env := newWorkflowEnvironment(t, map[string]interface{}{"Schema": []map[string]interface{}{{"Name": "tenant", "AttributeDataType": "String", "Mutable": false}}}, map[string]interface{}{"ReadAttributes": []string{"email"}, "WriteAttributes": []string{"email", "name", "custom:tenant"}})
	request := env.request("attribute-user")
	request["Password"] = "InitialPass1!"
	request["UserAttributes"] = []map[string]string{{"Name": "email", "Value": "original@example.test"}, {"Name": "name", "Value": "Initial name"}, {"Name": "custom:tenant", "Value": "tenant-one"}}
	status, body := postCognito(t, env.server.URL, "SignUp", request)
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	env.confirm(t, "attribute-user", env.capture.last(t).Code)
	token, claims := workflowLogin(t, env, "attribute-user", "InitialPass1!")
	require.Equal(t, "original@example.test", claims["email"])
	require.NotContains(t, claims, "name")
	require.NotContains(t, claims, "custom:tenant")
	status, body = postCognito(t, env.server.URL, "GetUser", map[string]string{"AccessToken": token})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	for _, raw := range body["UserAttributes"].([]interface{}) {
		attribute := raw.(map[string]interface{})
		require.Contains(t, []string{"email", "sub"}, attribute["Name"])
	}
	user, err := env.store.LookupPoolUser(t.Context(), env.pool, "attribute-user")
	require.NoError(t, err)
	status, body = postCognito(t, env.server.URL, "UpdateUserAttributes", map[string]interface{}{"AccessToken": token, "UserAttributes": []map[string]string{{"Name": "name", "Value": "Must not persist"}, {"Name": "custom:tenant", "Value": "tenant-two"}}})
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "InvalidParameterException", body["__type"])
	attrs, err := env.store.LoadUserAttributes(t.Context(), user.Sub)
	require.NoError(t, err)
	require.Equal(t, "Initial name", attrs["name"])
	require.Equal(t, "tenant-one", attrs["custom:tenant"])
	status, body = postCognito(t, env.server.URL, "UpdateUserAttributes", map[string]interface{}{"AccessToken": token, "UserAttributes": []map[string]string{{"Name": "email_verified", "Value": "true"}}})
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "NotAuthorizedException", body["__type"])
	env.clock.Add(10)
	status, body = postCognito(t, env.server.URL, "UpdateUserAttributes", map[string]interface{}{"AccessToken": token, "UserAttributes": []map[string]string{{"Name": "name", "Value": "Updated name"}, {"Name": "email", "Value": "changed@example.test"}}})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	require.Len(t, body["CodeDeliveryDetailsList"], 1)
	notification := env.capture.last(t)
	require.Equal(t, "attribute:email", notification.Purpose)
	require.Equal(t, "changed@example.test", notification.Destination)
	attrs, err = env.store.LoadUserAttributes(t.Context(), user.Sub)
	require.NoError(t, err)
	require.Equal(t, "false", attrs["email_verified"])
	require.Equal(t, "Updated name", attrs["name"])
	updated, err := env.store.LookupUserBySub(t.Context(), user.Sub)
	require.NoError(t, err)
	require.Equal(t, "changed@example.test", updated.Email)
	require.Equal(t, user.Username, updated.Username)
	require.Greater(t, updated.UpdatedAt, user.UpdatedAt)
	status, body = postCognito(t, env.server.URL, "ForgotPassword", env.request(user.Username))
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "InvalidParameterException", body["__type"])
	status, body = postCognito(t, env.server.URL, "VerifyUserAttribute", map[string]string{"AccessToken": token, "AttributeName": "email", "Code": notification.Code})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	attrs, err = env.store.LoadUserAttributes(t.Context(), user.Sub)
	require.NoError(t, err)
	require.Equal(t, "true", attrs["email_verified"])
	status, body = postCognito(t, env.server.URL, "VerifyUserAttribute", map[string]string{"AccessToken": token, "AttributeName": "email", "Code": notification.Code})
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "ExpiredCodeException", body["__type"])
}

func TestWorkflowAttributeCodesBindDestinationAndPurpose(t *testing.T) {
	env := newWorkflowEnvironment(t, nil, nil)
	capture := env.signup(t, "verify-user", "verify@example.test")
	env.confirm(t, "verify-user", capture.Code)
	token, _ := workflowLogin(t, env, "verify-user", "InitialPass1!")
	status, body := postCognito(t, env.server.URL, "GetUserAttributeVerificationCode", map[string]string{"AccessToken": token, "AttributeName": "email"})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	prior := env.capture.last(t)
	status, body = postCognito(t, env.server.URL, "AdminUpdateUserAttributes", map[string]interface{}{"UserPoolId": env.pool, "Username": "verify-user", "UserAttributes": []map[string]string{{"Name": "email", "Value": "admin-changed@example.test"}, {"Name": "email_verified", "Value": "true"}}})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	status, body = postCognito(t, env.server.URL, "VerifyUserAttribute", map[string]string{"AccessToken": token, "AttributeName": "email", "Code": prior.Code})
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "ExpiredCodeException", body["__type"])
	status, body = postCognito(t, env.server.URL, "GetUserAttributeVerificationCode", map[string]string{"AccessToken": token, "AttributeName": "email"})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	current := env.capture.last(t)
	require.Equal(t, "admin-changed@example.test", current.Destination)
	status, body = postCognito(t, env.server.URL, "VerifyUserAttribute", map[string]string{"AccessToken": token, "AttributeName": "phone_number", "Code": current.Code})
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "ExpiredCodeException", body["__type"])
	status, body = postCognito(t, env.server.URL, "VerifyUserAttribute", map[string]string{"AccessToken": token, "AttributeName": "email", "Code": current.Code})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
}

func TestWorkflowBlankAttributeDeletionClearsVerificationAndRespectsRequired(t *testing.T) {
	env := newWorkflowEnvironment(t, map[string]interface{}{"Schema": []map[string]interface{}{{"Name": "score", "AttributeDataType": "Number", "Mutable": true}}}, map[string]interface{}{"WriteAttributes": []string{"email", "custom:score"}})
	request := env.request("delete-attributes")
	request["Password"] = "InitialPass1!"
	request["UserAttributes"] = []map[string]string{{"Name": "email", "Value": "delete@example.test"}, {"Name": "custom:score", "Value": "12"}}
	status, body := postCognito(t, env.server.URL, "SignUp", request)
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	env.confirm(t, "delete-attributes", env.capture.last(t).Code)
	token, _ := workflowLogin(t, env, "delete-attributes", "InitialPass1!")
	before := env.capture.count()
	status, body = postCognito(t, env.server.URL, "UpdateUserAttributes", map[string]interface{}{"AccessToken": token, "UserAttributes": []map[string]string{{"Name": "email", "Value": ""}, {"Name": "custom:score", "Value": ""}}})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	require.Equal(t, before, env.capture.count())
	user, err := env.store.LookupPoolUser(t.Context(), env.pool, "delete-attributes")
	require.NoError(t, err)
	require.Empty(t, user.Email)
	attrs, err := env.store.LoadUserAttributes(t.Context(), user.Sub)
	require.NoError(t, err)
	require.NotContains(t, attrs, "email")
	require.NotContains(t, attrs, "email_verified")
	require.NotContains(t, attrs, "custom:score")
	required := newWorkflowEnvironment(t, map[string]interface{}{"Schema": []map[string]interface{}{{"Name": "email", "AttributeDataType": "String", "Required": true}}}, nil)
	capture := required.signup(t, "keep-required", "keep@example.test")
	required.confirm(t, "keep-required", capture.Code)
	requiredToken, _ := workflowLogin(t, required, "keep-required", "InitialPass1!")
	status, body = postCognito(t, required.server.URL, "UpdateUserAttributes", map[string]interface{}{"AccessToken": requiredToken, "UserAttributes": []map[string]string{{"Name": "email", "Value": ""}}})
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "InvalidParameterException", body["__type"])
	user, err = required.store.LookupPoolUser(t.Context(), required.pool, "keep-required")
	require.NoError(t, err)
	require.Equal(t, "keep@example.test", user.Email)
}

func TestWorkflowAdminUpdatesPreserveAllowedRequiredOmissions(t *testing.T) {
	env := newWorkflowEnvironment(t, map[string]interface{}{"Schema": []map[string]interface{}{{"Name": "email", "AttributeDataType": "String", "Required": true}, {"Name": "name", "AttributeDataType": "String", "Required": true}}}, nil)
	status, body := postCognito(t, env.server.URL, "AdminCreateUser", map[string]interface{}{"UserPoolId": env.pool, "Username": "admin-provisioned", "TemporaryPassword": "InitialPass1!", "MessageAction": "SUPPRESS", "UserAttributes": []map[string]string{{"Name": "email", "Value": "invited@example.test"}}})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	status, body = postCognito(t, env.server.URL, "AdminUpdateUserAttributes", map[string]interface{}{"UserPoolId": env.pool, "Username": "admin-provisioned", "UserAttributes": []map[string]string{{"Name": "email_verified", "Value": "true"}}})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	user, err := env.store.LookupPoolUser(t.Context(), env.pool, "admin-provisioned")
	require.NoError(t, err)
	attrs, err := env.store.LoadUserAttributes(t.Context(), user.Sub)
	require.NoError(t, err)
	require.Equal(t, "true", attrs["email_verified"])
	require.NotContains(t, attrs, "name")
	require.Equal(t, "FORCE_CHANGE_PASSWORD", user.Status)
	status, body = postCognito(t, env.server.URL, "AdminUpdateUserAttributes", map[string]interface{}{"UserPoolId": env.pool, "Username": "admin-provisioned", "UserAttributes": []map[string]string{{"Name": "email", "Value": ""}}})
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "InvalidParameterException", body["__type"])
	attrs, err = env.store.LoadUserAttributes(t.Context(), user.Sub)
	require.NoError(t, err)
	require.Equal(t, "invited@example.test", attrs["email"])
}
