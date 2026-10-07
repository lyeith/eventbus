package cognito

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createNativePasswordUser(t *testing.T, env *nativeProvisioningEnvironment, poolID string) string {
	t.Helper()
	const username = "token-lifetime-user"
	status, body := postCognito(t, env.server.URL, "AdminCreateUser", map[string]any{
		"UserPoolId": poolID, "Username": username, "TemporaryPassword": "InitialPass1!", "MessageAction": "SUPPRESS",
		"UserAttributes": []map[string]string{{"Name": "email", "Value": "lifetimes@example.com"}, {"Name": "email_verified", "Value": "true"}},
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	status, body = postCognito(t, env.server.URL, "AdminSetUserPassword", map[string]any{
		"UserPoolId": poolID, "Username": username, "Password": "PermanentPass2!", "Permanent": true,
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	return username
}

func authenticateNativePassword(t *testing.T, env *nativeProvisioningEnvironment, clientID, username string) map[string]any {
	t.Helper()
	status, body := postCognito(t, env.server.URL, "InitiateAuth", map[string]any{
		"ClientId": clientID, "AuthFlow": "USER_PASSWORD_AUTH",
		"AuthParameters": map[string]string{"USERNAME": username, "PASSWORD": "PermanentPass2!"},
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	return readAuthResult(t, body)
}

func assertNativeTokenLifetime(t *testing.T, result map[string]any, name string, issuedAt int64, lifetime time.Duration) {
	t.Helper()
	token, ok := result[name].(string)
	require.True(t, ok, "%s missing from %v", name, result)
	require.NotEmpty(t, token)
	claims := parseClaimsUnverified(t, token)
	assert.Equal(t, float64(issuedAt), claims["iat"], name)
	assert.Equal(t, float64(issuedAt)+lifetime.Seconds(), claims["exp"], name)
	issued, ok := claims["iat"].(float64)
	require.True(t, ok)
	expires, ok := claims["exp"].(float64)
	require.True(t, ok)
	assert.Equal(t, lifetime.Seconds(), expires-issued, "%s exp-iat", name)
}

func TestNativePerClientTokenValidityIndependentAndPersistent(t *testing.T) {
	// Process TTLs are deliberately distinct from every native client value.
	// They are legacy fixture defaults, never overrides for native resources.
	env := newNativeProvisioningEnvironment(t, Options{AccessTokenTTL: 11 * time.Minute, RefreshTokenTTL: 4 * time.Hour})
	poolID := env.createPool(t, nil)["Id"].(string)
	flows := []string{"ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"}
	custom := env.createClient(t, poolID, map[string]any{
		"ClientName": "mixed units", "ExplicitAuthFlows": flows,
		"AccessTokenValidity": 300, "IdTokenValidity": 10, "RefreshTokenValidity": 1,
		"TokenValidityUnits": map[string]string{"AccessToken": "seconds", "IdToken": "minutes", "RefreshToken": "hours"},
	})
	defaults := env.createClient(t, poolID, map[string]any{"ClientName": "native defaults", "ExplicitAuthFlows": flows})
	customID, defaultID := custom["ClientId"].(string), defaults["ClientId"].(string)
	assert.NotEqual(t, customID, defaultID)
	assert.Equal(t, float64(300), custom["AccessTokenValidity"])
	assert.Equal(t, float64(10), custom["IdTokenValidity"])
	assert.Equal(t, float64(1), custom["RefreshTokenValidity"])
	assert.Equal(t, map[string]any{"AccessToken": "seconds", "IdToken": "minutes", "RefreshToken": "hours"}, custom["TokenValidityUnits"])
	assert.Equal(t, float64(1), defaults["AccessTokenValidity"])
	assert.Equal(t, float64(1), defaults["IdTokenValidity"])
	assert.Equal(t, float64(30), defaults["RefreshTokenValidity"])
	assert.Equal(t, map[string]any{"AccessToken": "hours", "IdToken": "hours", "RefreshToken": "days"}, defaults["TokenValidityUnits"])
	assert.NotContains(t, defaults, "ReadAttributes")
	assert.NotContains(t, defaults, "WriteAttributes")
	username := createNativePasswordUser(t, env, poolID)

	customAuth := authenticateNativePassword(t, env, customID, username)
	assert.Equal(t, float64(300), customAuth["ExpiresIn"])
	assertNativeTokenLifetime(t, customAuth, "AccessToken", env.clock.Load(), 5*time.Minute)
	assertNativeTokenLifetime(t, customAuth, "IdToken", env.clock.Load(), 10*time.Minute)
	assertNativeTokenLifetime(t, customAuth, "RefreshToken", env.clock.Load(), time.Hour)
	defaultAuth := authenticateNativePassword(t, env, defaultID, username)
	assert.Equal(t, float64(3600), defaultAuth["ExpiresIn"])
	assertNativeTokenLifetime(t, defaultAuth, "AccessToken", env.clock.Load(), time.Hour)
	assertNativeTokenLifetime(t, defaultAuth, "IdToken", env.clock.Load(), time.Hour)
	assertNativeTokenLifetime(t, defaultAuth, "RefreshToken", env.clock.Load(), 30*24*time.Hour)

	env.clock.Add(123)
	env.options.AccessTokenTTL = 17 * time.Minute
	env.options.RefreshTokenTTL = 8 * time.Hour
	env.restart(t)
	for _, expected := range []map[string]any{custom, defaults} {
		status, body := postCognito(t, env.server.URL, "DescribeUserPoolClient", map[string]any{"UserPoolId": poolID, "ClientId": expected["ClientId"]})
		require.Equal(t, http.StatusOK, status, "body=%v", body)
		assert.Equal(t, expected, body["UserPoolClient"])
	}
	customAuth = authenticateNativePassword(t, env, customID, username)
	assert.Equal(t, float64(300), customAuth["ExpiresIn"])
	assertNativeTokenLifetime(t, customAuth, "AccessToken", env.clock.Load(), 5*time.Minute)
	assertNativeTokenLifetime(t, customAuth, "IdToken", env.clock.Load(), 10*time.Minute)
	assertNativeTokenLifetime(t, customAuth, "RefreshToken", env.clock.Load(), time.Hour)
	defaultAuth = authenticateNativePassword(t, env, defaultID, username)
	assertNativeTokenLifetime(t, defaultAuth, "AccessToken", env.clock.Load(), time.Hour)
	assertNativeTokenLifetime(t, defaultAuth, "IdToken", env.clock.Load(), time.Hour)
	assertNativeTokenLifetime(t, defaultAuth, "RefreshToken", env.clock.Load(), 30*24*time.Hour)
}

func TestNativeRefreshRetainsOriginalGrantAndExpiryBoundary(t *testing.T) {
	env := newNativeProvisioningEnvironment(t, Options{})
	poolID := env.createPool(t, nil)["Id"].(string)
	clientID := env.createClient(t, poolID, map[string]any{
		"ExplicitAuthFlows":   []string{"ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"},
		"AccessTokenValidity": 300, "IdTokenValidity": 10, "RefreshTokenValidity": 1,
		"TokenValidityUnits": map[string]string{"AccessToken": "seconds", "IdToken": "minutes", "RefreshToken": "hours"},
	})["ClientId"].(string)
	username := createNativePasswordUser(t, env, poolID)
	issuedAt := env.clock.Load()
	original := authenticateNativePassword(t, env, clientID, username)
	refresh := original["RefreshToken"].(string)
	originalRefreshClaims := parseClaimsUnverified(t, refresh)
	request := map[string]any{
		"ClientId": clientID, "AuthFlow": "REFRESH_TOKEN_AUTH", "AuthParameters": map[string]string{"REFRESH_TOKEN": refresh},
	}

	var lastResult map[string]any
	for _, elapsed := range []int64{100, 3599} {
		env.clock.Store(issuedAt + elapsed)
		if elapsed == 100 {
			env.restart(t) // Original token and persisted signing key survive restart.
		}
		status, body := postCognito(t, env.server.URL, "InitiateAuth", request)
		require.Equal(t, http.StatusOK, status, "body=%v", body)
		lastResult = readAuthResult(t, body)
		assert.NotContains(t, lastResult, "RefreshToken", "refresh rotation is disabled")
		assert.Equal(t, float64(300), lastResult["ExpiresIn"])
		assertNativeTokenLifetime(t, lastResult, "AccessToken", issuedAt+elapsed, 5*time.Minute)
		assertNativeTokenLifetime(t, lastResult, "IdToken", issuedAt+elapsed, 10*time.Minute)
		for _, name := range []string{"AccessToken", "IdToken"} {
			claims := parseClaimsUnverified(t, lastResult[name].(string))
			assert.Equal(t, float64(issuedAt), claims["auth_time"], "refresh must retain original authentication time")
			assert.Equal(t, originalRefreshClaims["jti"], claims["origin_jti"], "refresh must retain original grant identity")
		}
	}
	env.clock.Store(issuedAt + 3600)
	status, body := postCognito(t, env.server.URL, "InitiateAuth", request)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NotAuthorizedException", body["__type"], "refresh expires exactly at its original exp")
	// Refresh expiry doesn't revoke a recently renewed access token.
	status, body = postCognito(t, env.server.URL, "GetUser", map[string]any{"AccessToken": lastResult["AccessToken"]})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	assert.Equal(t, username, body["Username"])
	assert.Equal(t, float64(issuedAt+3600), parseClaimsUnverified(t, refresh)["exp"], "refresh requests cannot extend the original token")
}

func TestNativeInvalidTokenValidityRefusedBeforeClientMutation(t *testing.T) {
	env := newNativeProvisioningEnvironment(t, Options{})
	poolID := env.createPool(t, nil)["Id"].(string)
	cases := []struct {
		name   string
		fields map[string]any
	}{
		{"access below minimum", map[string]any{"AccessTokenValidity": 299, "TokenValidityUnits": map[string]string{"AccessToken": "seconds"}}},
		{"access above maximum", map[string]any{"AccessTokenValidity": 86401, "TokenValidityUnits": map[string]string{"AccessToken": "seconds"}}},
		{"access zero", map[string]any{"AccessTokenValidity": 0}},
		{"access negative", map[string]any{"AccessTokenValidity": -1}},
		{"id below minimum", map[string]any{"IdTokenValidity": 4, "TokenValidityUnits": map[string]string{"IdToken": "minutes"}}},
		{"id above maximum", map[string]any{"IdTokenValidity": 25, "TokenValidityUnits": map[string]string{"IdToken": "hours"}}},
		{"refresh below minimum", map[string]any{"RefreshTokenValidity": 3599, "TokenValidityUnits": map[string]string{"RefreshToken": "seconds"}}},
		{"refresh above maximum", map[string]any{"RefreshTokenValidity": 315360001, "TokenValidityUnits": map[string]string{"RefreshToken": "seconds"}}},
		{"refresh negative", map[string]any{"RefreshTokenValidity": -1}},
		{"overflow magnitude", map[string]any{"AccessTokenValidity": int64(1 << 62)}},
		{"invalid unit", map[string]any{"TokenValidityUnits": map[string]string{"AccessToken": "weeks"}}},
		{"unit case", map[string]any{"TokenValidityUnits": map[string]string{"IdToken": "HOURS"}}},
		{"explicit empty unit", map[string]any{"TokenValidityUnits": map[string]string{"RefreshToken": ""}}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			request := map[string]any{"UserPoolId": poolID, "ClientName": test.name}
			for name, value := range test.fields {
				request[name] = value
			}
			status, body := postCognito(t, env.server.URL, "CreateUserPoolClient", request)
			require.Equal(t, http.StatusBadRequest, status, "body=%v", body)
			assert.Equal(t, "InvalidParameterException", body["__type"])
			var count int
			require.NoError(t, env.store.DB().QueryRow(`SELECT COUNT(*) FROM clients`).Scan(&count))
			assert.Zero(t, count, "invalid configuration must not persist a client")
		})
	}
	// Zero refresh validity is the documented native exception: reset to
	// thirty days. Partial units preserve omitted token durations independently.
	client := env.createClient(t, poolID, map[string]any{
		"RefreshTokenValidity": 0, "TokenValidityUnits": map[string]string{"AccessToken": "seconds", "RefreshToken": "hours"},
	})
	assert.Equal(t, float64(3600), client["AccessTokenValidity"])
	assert.Equal(t, float64(1), client["IdTokenValidity"])
	assert.Equal(t, float64(720), client["RefreshTokenValidity"])
	assert.Equal(t, map[string]any{"AccessToken": "seconds", "IdToken": "hours", "RefreshToken": "hours"}, client["TokenValidityUnits"])
}

func TestLegacyClientGlobalDefaultsYieldToExplicitNativeValidity(t *testing.T) {
	env := newNativeProvisioningEnvironment(t, Options{
		DevProfile: DevProfileLegacyFixtures, AccessTokenTTL: 11 * time.Minute, RefreshTokenTTL: 4 * time.Hour,
	})
	poolID := env.createPool(t, nil)["Id"].(string)
	flows := []string{"ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"}
	legacy := env.createClient(t, poolID, map[string]any{"ClientId": "legacy-defaults-client", "ExplicitAuthFlows": flows})
	configured := env.createClient(t, poolID, map[string]any{
		"ClientId": "legacy-configured-client", "ExplicitAuthFlows": flows,
		"AccessTokenValidity": 300, "IdTokenValidity": 7, "RefreshTokenValidity": 1,
		"TokenValidityUnits": map[string]string{"AccessToken": "seconds", "IdToken": "minutes", "RefreshToken": "hours"},
	})
	username := createNativePasswordUser(t, env, poolID)
	assert.NotContains(t, legacy, "TokenValidityUnits", "nil persisted validity remains a fixture default")
	stored, err := env.store.LookupClient(t.Context(), legacy["ClientId"].(string))
	require.NoError(t, err)
	assert.Nil(t, stored.TokenValidity)
	legacyResult := authenticateNativePassword(t, env, legacy["ClientId"].(string), username)
	assert.Equal(t, float64(660), legacyResult["ExpiresIn"])
	assertNativeTokenLifetime(t, legacyResult, "AccessToken", env.clock.Load(), 11*time.Minute)
	assertNativeTokenLifetime(t, legacyResult, "IdToken", env.clock.Load(), 11*time.Minute)
	assertNativeTokenLifetime(t, legacyResult, "RefreshToken", env.clock.Load(), 4*time.Hour)
	configuredResult := authenticateNativePassword(t, env, configured["ClientId"].(string), username)
	assert.Equal(t, float64(300), configuredResult["ExpiresIn"])
	assertNativeTokenLifetime(t, configuredResult, "AccessToken", env.clock.Load(), 5*time.Minute)
	assertNativeTokenLifetime(t, configuredResult, "IdToken", env.clock.Load(), 7*time.Minute)
	assertNativeTokenLifetime(t, configuredResult, "RefreshToken", env.clock.Load(), time.Hour)

	// New native clients retain native defaults even in a process that also
	// permits legacy fixture requests and has custom process TTLs.
	native := env.createClient(t, poolID, map[string]any{"ExplicitAuthFlows": flows})
	nativeResult := authenticateNativePassword(t, env, native["ClientId"].(string), username)
	assertNativeTokenLifetime(t, nativeResult, "AccessToken", env.clock.Load(), time.Hour)
	assertNativeTokenLifetime(t, nativeResult, "IdToken", env.clock.Load(), time.Hour)
	assertNativeTokenLifetime(t, nativeResult, "RefreshToken", env.clock.Load(), 30*24*time.Hour)
}
