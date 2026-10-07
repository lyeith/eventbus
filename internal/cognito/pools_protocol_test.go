package cognito

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreateUserPool_AWSDefaultsAndPolicyEnforcement(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	status, body := postCognito(t, ts.URL, "CreateUserPool", map[string]interface{}{"PoolName": "default-settings"})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	pool := body["UserPool"].(map[string]interface{})
	poolID := pool["Id"].(string)
	require.Regexp(t, "^us-east-1_[a-f0-9]+$", poolID)
	require.Equal(t, map[string]interface{}{"CaseSensitive": true}, pool["UsernameConfiguration"])
	require.NotContains(t, pool, "UsernameAttributes")
	require.NotContains(t, pool, "AliasAttributes")
	policyOut := pool["Policies"].(map[string]interface{})["PasswordPolicy"]
	require.Equal(t, map[string]interface{}{
		"MinimumLength": float64(8), "RequireUppercase": true, "RequireLowercase": true,
		"RequireNumbers": true, "RequireSymbols": true, "TemporaryPasswordValidityDays": float64(7),
	}, policyOut)
	policy, err := loadPoolPasswordPolicy(t.Context(), store, poolID)
	require.NoError(t, err)
	require.Equal(t, &PasswordPolicy{
		MinLength: 8, RequireUppercase: true, RequireLowercase: true,
		RequireDigits: true, RequireSymbols: true, TemporaryPasswordValidityDays: 7,
	}, policy)
	config, err := store.GetPoolSignInConfig(t.Context(), poolID)
	require.NoError(t, err)
	require.Equal(t, PoolSignInConfig{CaseSensitive: true}, config)

	status, body = postCognito(t, ts.URL, "AdminCreateUser", map[string]interface{}{
		"UserPoolId": poolID, "Username": "weak-password-user",
		"TemporaryPassword": "weak", "MessageAction": "SUPPRESS",
	})
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "InvalidPasswordException", body["__type"])
}

func TestCreateUserPool_PasswordPolicyDefaultsAndCanonicalMembers(t *testing.T) {
	for _, test := range []struct {
		name   string
		fields map[string]interface{}
		want   PasswordPolicy
	}{
		{
			name: "partial AWS policy",
			fields: map[string]interface{}{"Policies": map[string]interface{}{"PasswordPolicy": map[string]interface{}{
				"MinimumLength": 12, "RequireUppercase": false, "RequireNumbers": false, "TemporaryPasswordValidityDays": 0,
			}}},
			want: PasswordPolicy{MinLength: 12, RequireLowercase: true, RequireSymbols: true, TemporaryPasswordValidityDays: 7},
		},
		{
			name: "explicit false and upper duration boundary",
			fields: map[string]interface{}{"Policies": map[string]interface{}{"PasswordPolicy": map[string]interface{}{
				"MinimumLength": 6, "RequireUppercase": false, "RequireLowercase": false,
				"RequireNumbers": false, "RequireSymbols": false, "TemporaryPasswordValidityDays": 365,
			}}},
			want: PasswordPolicy{MinLength: 6, TemporaryPasswordValidityDays: 365},
		},
		{
			name: "legacy flat alias",
			// Snake-case aliases belong to an explicit development fixture pool.
			fields: map[string]interface{}{"PoolId": "fixture-policy-flat", "PasswordPolicy": map[string]interface{}{
				"min_length": 9, "require_uppercase": false, "require_digits": false, "require_symbols": false,
			}},
			want: PasswordPolicy{MinLength: 9, RequireLowercase: true, TemporaryPasswordValidityDays: 7},
		},
		{
			name: "AWS members take precedence",
			// Precedence tests mix fixture aliases with AWS spellings deliberately.
			fields: map[string]interface{}{"PoolId": "fixture-policy-precedence", "Policies": map[string]interface{}{"PasswordPolicy": map[string]interface{}{
				"min_length": 6, "MinimumLength": 13,
				"require_digits": true, "RequireDigits": true, "RequireNumbers": false,
			}}},
			want: PasswordPolicy{MinLength: 13, RequireUppercase: true, RequireLowercase: true, RequireSymbols: true, TemporaryPasswordValidityDays: 7},
		},
		{
			name: "maximum minimum length",
			fields: map[string]interface{}{"Policies": map[string]interface{}{"PasswordPolicy": map[string]interface{}{
				"MinimumLength": 99, "TemporaryPasswordValidityDays": 1,
			}}},
			want: PasswordPolicy{MinLength: 99, RequireUppercase: true, RequireLowercase: true, RequireDigits: true, RequireSymbols: true, TemporaryPasswordValidityDays: 1},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, ts, store := newCognitoTestServer(t)
			request := map[string]interface{}{"PoolName": test.name}
			for key, value := range test.fields {
				request[key] = value
			}
			status, body := postCognito(t, ts.URL, "CreateUserPool", request)
			require.Equal(t, http.StatusOK, status, "body=%v", body)
			pool := body["UserPool"].(map[string]interface{})
			policy, err := loadPoolPasswordPolicy(t.Context(), store, pool["Id"].(string))
			require.NoError(t, err)
			require.Equal(t, test.want, *policy)
			out := pool["Policies"].(map[string]interface{})["PasswordPolicy"].(map[string]interface{})
			require.Equal(t, float64(test.want.MinLength), out["MinimumLength"])
			require.Equal(t, test.want.RequireDigits, out["RequireNumbers"])
			require.Equal(t, float64(test.want.TemporaryPasswordValidityDays), out["TemporaryPasswordValidityDays"])
			require.NotContains(t, out, "RequireDigits")
			require.NotContains(t, out, "min_length")
			require.NotContains(t, out, "require_digits")
		})
	}
}

func TestCreateUserPool_SignInConfiguration(t *testing.T) {
	for _, test := range []struct {
		name   string
		fields map[string]interface{}
		want   PoolSignInConfig
	}{
		{"username default", nil, PoolSignInConfig{CaseSensitive: true}},
		{"verified email aliases", map[string]interface{}{"AliasAttributes": []string{"email"}}, PoolSignInConfig{EmailAlias: true, CaseSensitive: true}},
		{"email usernames", map[string]interface{}{"UsernameAttributes": []string{"email"}}, PoolSignInConfig{EmailAsUsername: true, CaseSensitive: true}},
		{"case insensitive email", map[string]interface{}{
			"UsernameAttributes": []string{"email"}, "UsernameConfiguration": map[string]interface{}{"CaseSensitive": false},
		}, PoolSignInConfig{EmailAsUsername: true}},
		{"empty attributes", map[string]interface{}{"UsernameAttributes": []string{}, "AliasAttributes": []string{}}, PoolSignInConfig{CaseSensitive: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, ts, store := newCognitoTestServer(t)
			request := map[string]interface{}{"PoolName": test.name}
			for key, value := range test.fields {
				request[key] = value
			}
			status, body := postCognito(t, ts.URL, "CreateUserPool", request)
			require.Equal(t, http.StatusOK, status, "body=%v", body)
			pool := body["UserPool"].(map[string]interface{})
			config, err := store.GetPoolSignInConfig(t.Context(), pool["Id"].(string))
			require.NoError(t, err)
			require.Equal(t, test.want, config)
			require.Equal(t, map[string]interface{}{"CaseSensitive": test.want.CaseSensitive}, pool["UsernameConfiguration"])
			if test.want.EmailAsUsername {
				require.Equal(t, []interface{}{"email"}, pool["UsernameAttributes"])
				require.NotContains(t, pool, "AliasAttributes")
			}
			if test.want.EmailAlias {
				require.Equal(t, []interface{}{"email"}, pool["AliasAttributes"])
				require.NotContains(t, pool, "UsernameAttributes")
			}
		})
	}
}

func TestCreateUserPool_ConfiguredEmailAliasResolvesSignIn(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	status, body := postCognito(t, ts.URL, "CreateUserPool", map[string]interface{}{
		"PoolName": "email-alias", "AliasAttributes": []string{"email"},
		"UsernameConfiguration": map[string]interface{}{"CaseSensitive": false},
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	poolID := body["UserPool"].(map[string]interface{})["Id"].(string)
	status, body = postCognito(t, ts.URL, "AdminCreateUser", map[string]interface{}{
		"UserPoolId": poolID, "Username": "Owner-42", "TemporaryPassword": "TemporaryPass1!",
		"MessageAction": "SUPPRESS", "UserAttributes": []map[string]string{
			{"Name": "email", "Value": "Owner@Example.test"}, {"Name": "email_verified", "Value": "true"},
		},
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	user, err := store.ResolveSignInUser(t.Context(), poolID, "OWNER@EXAMPLE.TEST")
	require.NoError(t, err)
	require.Equal(t, "owner-42", user.Username)
}

func TestCreateUserPool_InvalidConfigurationDoesNotCreatePool(t *testing.T) {
	for _, test := range []struct {
		name   string
		fields map[string]interface{}
	}{
		{"mutually exclusive", map[string]interface{}{"UsernameAttributes": []string{"email"}, "AliasAttributes": []string{"email"}}},
		{"phone username unsupported", map[string]interface{}{"UsernameAttributes": []string{"phone_number"}}},
		{"preferred username unsupported", map[string]interface{}{"AliasAttributes": []string{"preferred_username"}}},
		{"duplicate aliases", map[string]interface{}{"AliasAttributes": []string{"email", "email"}}},
		{"missing case sensitivity", map[string]interface{}{"UsernameConfiguration": map[string]interface{}{}}},
		{"minimum below range", map[string]interface{}{"PasswordPolicy": map[string]interface{}{"MinimumLength": 5}}},
		{"explicit minimum zero", map[string]interface{}{"PasswordPolicy": map[string]interface{}{"MinimumLength": 0}}},
		{"minimum above range", map[string]interface{}{"PasswordPolicy": map[string]interface{}{"MinimumLength": 100}}},
		{"negative temporary duration", map[string]interface{}{"PasswordPolicy": map[string]interface{}{"TemporaryPasswordValidityDays": -1}}},
		{"temporary duration above range", map[string]interface{}{"PasswordPolicy": map[string]interface{}{"TemporaryPasswordValidityDays": 366}}},
		{"wrong policy member type", map[string]interface{}{"PasswordPolicy": map[string]interface{}{"MinimumLength": "8"}}},
		{"policy array", map[string]interface{}{"PasswordPolicy": []interface{}{}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, ts, store := newCognitoTestServer(t)
			request := map[string]interface{}{"PoolName": test.name, "PoolId": "invalid-pool"}
			for key, value := range test.fields {
				request[key] = value
			}
			status, body := postCognito(t, ts.URL, "CreateUserPool", request)
			require.Equal(t, http.StatusBadRequest, status, "body=%v", body)
			require.Equal(t, "InvalidParameterException", body["__type"])
			exists, err := store.PoolExists(t.Context(), "invalid-pool")
			require.NoError(t, err)
			require.False(t, exists)
		})
	}
}

func TestCreateUserPool_DevEnsurePreservesPopulatedLegacyPool(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const poolID = "existing-local-fixture"
	require.NoError(t, store.UpsertPool(t.Context(), poolID, "us-east-1"))
	config := PoolSignInConfig{EmailAlias: true, CaseSensitive: false}
	require.NoError(t, store.SetPoolSignInConfig(t.Context(), poolID, config))
	_, err := store.CreateUserIdentity(t.Context(), poolID, "canonical-user", "owner@example.test", "Password1!", "CONFIRMED", map[string]string{
		"email": "owner@example.test", "email_verified": "true",
	})
	require.NoError(t, err)
	for _, fields := range []map[string]interface{}{
		{}, {"AliasAttributes": []string{"email"}},
		{"UsernameConfiguration": map[string]interface{}{"CaseSensitive": false}},
	} {
		request := map[string]interface{}{"PoolName": "ensure", "PoolId": poolID}
		for key, value := range fields {
			request[key] = value
		}
		status, body := postCognito(t, ts.URL, "CreateUserPool", request)
		require.Equal(t, http.StatusOK, status, "body=%v", body)
		pool := body["UserPool"].(map[string]interface{})
		require.Equal(t, poolID, pool["Id"])
		require.NotContains(t, pool, "Policies", "legacy nil policy remains permissive")
		require.Equal(t, []interface{}{"email"}, pool["AliasAttributes"])
		require.Equal(t, map[string]interface{}{"CaseSensitive": false}, pool["UsernameConfiguration"])
	}
	status, body := postCognito(t, ts.URL, "CreateUserPool", map[string]interface{}{
		"PoolName": "change immutable config", "PoolId": poolID,
		"UsernameConfiguration": map[string]interface{}{"CaseSensitive": true},
		"PasswordPolicy":        map[string]interface{}{"MinimumLength": 12},
	})
	require.Equal(t, http.StatusBadRequest, status, "body=%v", body)
	require.Equal(t, "InvalidParameterException", body["__type"])
	unchanged, err := store.GetPoolSignInConfig(t.Context(), poolID)
	require.NoError(t, err)
	require.Equal(t, config, unchanged)
	policy, err := loadPoolPasswordPolicy(t.Context(), store, poolID)
	require.NoError(t, err)
	require.Nil(t, policy, "rejected identity configuration must not replace the policy")
}

func TestCreateUserPoolClient_AuthDefaultsAndLegacyFixtures(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const poolID = "client-auth-defaults"
	require.NoError(t, store.UpsertPool(t.Context(), poolID, "us-east-1"))
	status, body := postCognito(t, ts.URL, "CreateUserPoolClient", map[string]interface{}{
		"UserPoolId": poolID, "ClientName": "defaults",
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	out := body["UserPoolClient"].(map[string]interface{})
	clientID := out["ClientId"].(string)
	require.Regexp(t, "^[a-z0-9]+$", clientID)
	require.Equal(t, float64(3), out["AuthSessionValidity"])
	require.Equal(t, []interface{}{"ALLOW_REFRESH_TOKEN_AUTH", "ALLOW_USER_SRP_AUTH", "ALLOW_CUSTOM_AUTH"}, out["ExplicitAuthFlows"])
	client, err := store.LookupClient(t.Context(), clientID)
	require.NoError(t, err)
	require.Equal(t, 3, client.AuthSessionValidity)
	require.Equal(t, []string{"ALLOW_REFRESH_TOKEN_AUTH", "ALLOW_USER_SRP_AUTH", "ALLOW_CUSTOM_AUTH"}, client.ExplicitAuthFlows)
	require.True(t, clientAllowsAuthFlow(client, authFlowRefreshToken))
	require.True(t, clientAllowsAuthFlow(client, authFlowCustom))
	require.False(t, clientAllowsAuthFlow(client, authFlowUserPassword))
	require.NoError(t, store.UpsertClient(t.Context(), "legacy-direct-client", poolID, ""))
	legacy, err := store.LookupClient(t.Context(), "legacy-direct-client")
	require.NoError(t, err)
	require.Nil(t, legacy.ExplicitAuthFlows)
	require.True(t, clientAllowsAuthFlow(legacy, authFlowUserPassword))
}

func TestCreateUserPoolClient_ExplicitConfiguration(t *testing.T) {
	for _, test := range []struct {
		name           string
		flows          []string
		sessionMinutes int
	}{
		{"modern flows", []string{"ALLOW_ADMIN_USER_PASSWORD_AUTH", "ALLOW_USER_PASSWORD_AUTH", "ALLOW_CUSTOM_AUTH", "ALLOW_USER_SRP_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"}, 15},
		{"legacy flows", []string{"ADMIN_NO_SRP_AUTH", "CUSTOM_AUTH_FLOW_ONLY", "USER_PASSWORD_AUTH"}, 3},
		{"explicit empty flows", []string{}, 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, ts, store := newCognitoTestServer(t)
			const poolID = "explicit-auth-settings"
			require.NoError(t, store.UpsertPool(t.Context(), poolID, "us-east-1"))
			status, body := postCognito(t, ts.URL, "CreateUserPoolClient", map[string]interface{}{
				"UserPoolId": poolID, "ClientName": test.name, "ClientId": "deterministic-client",
				"ExplicitAuthFlows": test.flows, "AuthSessionValidity": test.sessionMinutes,
			})
			require.Equal(t, http.StatusOK, status, "body=%v", body)
			out := body["UserPoolClient"].(map[string]interface{})
			require.Equal(t, "deterministic-client", out["ClientId"])
			require.ElementsMatch(t, test.flows, out["ExplicitAuthFlows"])
			require.Equal(t, float64(test.sessionMinutes), out["AuthSessionValidity"])
			client, err := store.LookupClient(t.Context(), "deterministic-client")
			require.NoError(t, err)
			require.NotNil(t, client.ExplicitAuthFlows)
			require.Equal(t, test.flows, client.ExplicitAuthFlows)
			require.Equal(t, test.sessionMinutes, client.AuthSessionValidity)
			if len(test.flows) == 0 {
				require.False(t, clientAllowsAuthFlow(client, authFlowUserPassword))
				require.Equal(t, []interface{}{}, out["ExplicitAuthFlows"])
			}
		})
	}
}

func TestCreateUserPoolClient_InvalidConfigurationDoesNotCreateClient(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const poolID = "invalid-client-config"
	require.NoError(t, store.UpsertPool(t.Context(), poolID, "us-east-1"))
	for i, test := range []map[string]interface{}{
		{"AuthSessionValidity": 0}, {"AuthSessionValidity": 2}, {"AuthSessionValidity": 16},
		{"AuthSessionValidity": "3"}, {"ExplicitAuthFlows": []string{"UNKNOWN_AUTH"}},
		{"ExplicitAuthFlows": []string{"USER_SRP_AUTH"}},
		{"ExplicitAuthFlows": []string{"ADMIN_NO_SRP_AUTH", "ALLOW_CUSTOM_AUTH"}},
	} {
		t.Run(fmt.Sprintf("invalid-%d", i), func(t *testing.T) {
			clientID := fmt.Sprintf("invalid-client-%d", i)
			request := map[string]interface{}{"UserPoolId": poolID, "ClientName": "invalid", "ClientId": clientID}
			for key, value := range test {
				request[key] = value
			}
			status, body := postCognito(t, ts.URL, "CreateUserPoolClient", request)
			require.Equal(t, http.StatusBadRequest, status, "body=%v", body)
			require.Equal(t, "InvalidParameterException", body["__type"])
			_, err := store.LookupClient(t.Context(), clientID)
			require.ErrorIs(t, err, sql.ErrNoRows)
		})
	}
}

func TestCreateUserPoolClient_DevOverrideCannotMoveClientBetweenPools(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	require.NoError(t, store.UpsertPool(context.Background(), "first-pool", "us-east-1"))
	require.NoError(t, store.UpsertPool(context.Background(), "second-pool", "us-east-1"))
	require.NoError(t, store.UpsertClient(t.Context(), "existing-client", "first-pool", "retained-secret"))
	require.NoError(t, store.SetClientAuthConfig(t.Context(), "existing-client", []string{"ALLOW_ADMIN_USER_PASSWORD_AUTH"}, 3))
	status, body := postCognito(t, ts.URL, "CreateUserPoolClient", map[string]interface{}{
		"UserPoolId": "second-pool", "ClientName": "collision", "ClientId": "existing-client", "GenerateSecret": true,
		"ExplicitAuthFlows": []string{"ALLOW_CUSTOM_AUTH"}, "AuthSessionValidity": 15,
	})
	require.Equal(t, http.StatusBadRequest, status, "body=%v", body)
	require.Equal(t, "InvalidParameterException", body["__type"])
	client, err := store.LookupClient(t.Context(), "existing-client")
	require.NoError(t, err)
	require.Equal(t, "first-pool", client.PoolID)
	require.Equal(t, "retained-secret", client.Secret)
	require.Equal(t, []string{"ALLOW_ADMIN_USER_PASSWORD_AUTH"}, client.ExplicitAuthFlows)
	require.Equal(t, 3, client.AuthSessionValidity)
}
