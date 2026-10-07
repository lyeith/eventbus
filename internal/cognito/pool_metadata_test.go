package cognito

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Each native contract test owns its HTTP listener, SQLite file and clock.
// Provisioning goes through AWS-shaped requests, without fixture ID overrides.
type nativeProvisioningEnvironment struct {
	path    string
	store   *CognitoStore
	server  *httptest.Server
	options Options
	clock   atomic.Int64
}

func newNativeProvisioningEnvironment(t *testing.T, options Options) *nativeProvisioningEnvironment {
	t.Helper()
	if options.Region == "" {
		options.Region = "eu-west-1"
	}
	if options.AccountID == "" {
		options.AccountID = "123456789012"
	}
	if options.IssuerBase == "" {
		options.IssuerBase = "http://localhost:4100"
	}
	env := &nativeProvisioningEnvironment{path: filepath.Join(t.TempDir(), "cognito.db"), options: options}
	env.clock.Store(time.Date(2026, time.October, 7, 10, 0, 0, 0, time.UTC).Unix())
	env.start(t)
	t.Cleanup(func() {
		env.server.Close()
		require.NoError(t, env.store.Close())
	})
	return env
}

func (e *nativeProvisioningEnvironment) start(t *testing.T) {
	t.Helper()
	var err error
	e.store, err = OpenCognitoStore(e.path)
	require.NoError(t, err)
	e.options.Clock = func() time.Time { return time.Unix(e.clock.Load(), 0) }
	handler := NewHandler(e.store, e.options)
	e.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.ServeAction(w, r, Action(r.Header.Get("X-Amz-Target")))
	}))
}

func (e *nativeProvisioningEnvironment) restart(t *testing.T) {
	t.Helper()
	e.server.Close()
	require.NoError(t, e.store.Close())
	e.start(t)
}

func (e *nativeProvisioningEnvironment) createPool(t *testing.T, settings map[string]any) map[string]any {
	t.Helper()
	request := map[string]any{"PoolName": "native-contract-pool"}
	for key, value := range settings {
		request[key] = value
	}
	status, body := postCognito(t, e.server.URL, "CreateUserPool", request)
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	pool, ok := body["UserPool"].(map[string]any)
	require.True(t, ok)
	require.NotEmpty(t, pool["Id"])
	return pool
}

func (e *nativeProvisioningEnvironment) createClient(t *testing.T, poolID string, settings map[string]any) map[string]any {
	t.Helper()
	request := map[string]any{"UserPoolId": poolID, "ClientName": "native-contract-client"}
	for key, value := range settings {
		request[key] = value
	}
	status, body := postCognito(t, e.server.URL, "CreateUserPoolClient", request)
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	client, ok := body["UserPoolClient"].(map[string]any)
	require.True(t, ok)
	require.NotEmpty(t, client["ClientId"])
	return client
}

func TestNativePoolAndClientMetadataSurviveRestart(t *testing.T) {
	env := newNativeProvisioningEnvironment(t, Options{})
	createdAt := env.clock.Load()
	pool := env.createPool(t, map[string]any{
		"PoolName":              "Ireland contract pool",
		"AliasAttributes":       []string{"email"},
		"UsernameConfiguration": map[string]any{"CaseSensitive": false},
		"Policies": map[string]any{"PasswordPolicy": map[string]any{
			"MinimumLength": 12, "RequireUppercase": false, "RequireLowercase": true,
			"RequireNumbers": false, "RequireSymbols": false, "TemporaryPasswordValidityDays": 10,
		}},
		"Schema": []map[string]any{
			{"Name": "email", "Required": true},
			{"Name": "tier", "AttributeDataType": "String", "Mutable": true, "StringAttributeConstraints": map[string]string{"MinLength": "2", "MaxLength": "20"}},
		},
		"AutoVerifiedAttributes": []string{"email"},
		"AccountRecoverySetting": map[string]any{"RecoveryMechanisms": []map[string]any{{"Name": "verified_email", "Priority": 1}}},
		"AdminCreateUserConfig":  map[string]any{"AllowAdminCreateUserOnly": true},
	})
	poolID := pool["Id"].(string)
	assert.True(t, strings.HasPrefix(poolID, "eu-west-1_"), "pool identity must use the configured region")
	assert.Equal(t, "arn:aws:cognito-idp:eu-west-1:123456789012:userpool/"+poolID, pool["Arn"])
	assert.Equal(t, "Ireland contract pool", pool["Name"])
	assert.Equal(t, float64(createdAt), pool["CreationDate"])
	assert.Equal(t, float64(createdAt), pool["LastModifiedDate"])
	assert.Equal(t, false, pool["UsernameConfiguration"].(map[string]any)["CaseSensitive"])
	passwordPolicy := pool["Policies"].(map[string]any)["PasswordPolicy"].(map[string]any)
	assert.Equal(t, float64(12), passwordPolicy["MinimumLength"])
	assert.Equal(t, false, passwordPolicy["RequireUppercase"])
	assert.Equal(t, float64(10), passwordPolicy["TemporaryPasswordValidityDays"])

	env.clock.Add(7)
	client := env.createClient(t, poolID, map[string]any{
		"ClientName": "generated secret contract", "GenerateSecret": true,
		"ExplicitAuthFlows":   []string{"ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH", "ALLOW_USER_SRP_AUTH"},
		"AuthSessionValidity": 7, "ReadAttributes": []string{"email", "custom:tier"}, "WriteAttributes": []string{"email", "custom:tier"},
		"AccessTokenValidity": 15, "IdTokenValidity": 20, "RefreshTokenValidity": 24,
		"TokenValidityUnits": map[string]string{"AccessToken": "minutes", "IdToken": "minutes", "RefreshToken": "hours"},
	})
	clientID := client["ClientId"].(string)
	assert.Equal(t, poolID, client["UserPoolId"])
	assert.Equal(t, "generated secret contract", client["ClientName"])
	assert.Equal(t, float64(createdAt+7), client["CreationDate"])
	assert.Equal(t, float64(createdAt+7), client["LastModifiedDate"])
	assert.NotEmpty(t, client["ClientSecret"])
	assert.Equal(t, float64(7), client["AuthSessionValidity"])
	assert.Equal(t, []any{"email", "custom:tier"}, client["ReadAttributes"])
	assert.Equal(t, []any{"email", "custom:tier"}, client["WriteAttributes"])

	// Reads use persisted metadata. Advancing time or restarting with another
	// process region/account must not change an existing resource's identity.
	env.clock.Add(86400)
	env.options.Region = "ap-southeast-1"
	env.options.AccountID = "999999999999"
	env.restart(t)
	status, body := postCognito(t, env.server.URL, "DescribeUserPool", map[string]any{"UserPoolId": poolID})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	assert.Equal(t, pool, body["UserPool"])
	status, body = postCognito(t, env.server.URL, "DescribeUserPoolClient", map[string]any{"UserPoolId": poolID, "ClientId": clientID})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	assert.Equal(t, client, body["UserPoolClient"])
	storedClient, err := env.store.LookupClient(t.Context(), clientID)
	require.NoError(t, err)
	assert.Equal(t, "generated secret contract", storedClient.Name)
	assert.Equal(t, client["ClientSecret"], storedClient.Secret)
	assert.Equal(t, int64(createdAt+7), storedClient.CreatedAt)

	status, body = postCognito(t, env.server.URL, "DeleteUserPoolClient", map[string]any{"UserPoolId": poolID, "ClientId": clientID})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	status, body = postCognito(t, env.server.URL, "DescribeUserPoolClient", map[string]any{"UserPoolId": poolID, "ClientId": clientID})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "ResourceNotFoundException", body["__type"])
	status, body = postCognito(t, env.server.URL, "DeleteUserPool", map[string]any{"UserPoolId": poolID})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	status, body = postCognito(t, env.server.URL, "DescribeUserPool", map[string]any{"UserPoolId": poolID})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "ResourceNotFoundException", body["__type"])
}

func TestNativeClientPoolBoundaryAndIndependentInstances(t *testing.T) {
	first := newNativeProvisioningEnvironment(t, Options{})
	second := newNativeProvisioningEnvironment(t, Options{})
	pool := first.createPool(t, nil)
	poolID := pool["Id"].(string)
	otherPool := first.createPool(t, map[string]any{"PoolName": "different parent"})
	otherID := otherPool["Id"].(string)
	client := first.createClient(t, poolID, nil)
	clientID := client["ClientId"].(string)
	for _, action := range []string{"DescribeUserPoolClient", "DeleteUserPoolClient"} {
		status, body := postCognito(t, first.server.URL, action, map[string]any{"UserPoolId": otherID, "ClientId": clientID})
		assert.Equal(t, http.StatusBadRequest, status, "action=%s body=%v", action, body)
		assert.Equal(t, "ResourceNotFoundException", body["__type"])
	}
	status, body := postCognito(t, first.server.URL, "DescribeUserPoolClient", map[string]any{"UserPoolId": poolID, "ClientId": clientID})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	assert.Equal(t, client, body["UserPoolClient"], "wrong-pool operations must leave the owner's client intact")

	for _, request := range []struct {
		action string
		body   map[string]any
	}{
		{"CreateUserPoolClient", map[string]any{"UserPoolId": "eu-west-1_missing", "ClientName": "orphan"}},
		{"DescribeUserPoolClient", map[string]any{"UserPoolId": "eu-west-1_missing", "ClientId": clientID}},
		{"DeleteUserPoolClient", map[string]any{"UserPoolId": "eu-west-1_missing", "ClientId": clientID}},
		{"DescribeUserPool", map[string]any{"UserPoolId": "eu-west-1_missing"}},
		{"DeleteUserPool", map[string]any{"UserPoolId": "eu-west-1_missing"}},
	} {
		status, body := postCognito(t, first.server.URL, request.action, request.body)
		assert.Equal(t, http.StatusBadRequest, status, "action=%s body=%v", request.action, body)
		assert.Equal(t, "ResourceNotFoundException", body["__type"])
	}
	var clients int
	require.NoError(t, first.store.DB().QueryRow(`SELECT COUNT(*) FROM clients`).Scan(&clients))
	assert.Equal(t, 1, clients, "refused orphan provisioning must not leave a client")
	for _, request := range []struct {
		action string
		body   map[string]any
	}{
		{"DescribeUserPool", map[string]any{"UserPoolId": poolID}},
		{"CreateUserPoolClient", map[string]any{"UserPoolId": poolID, "ClientName": "foreign-store"}},
		{"DescribeUserPoolClient", map[string]any{"UserPoolId": poolID, "ClientId": clientID}},
	} {
		status, body := postCognito(t, second.server.URL, request.action, request.body)
		assert.Equal(t, http.StatusBadRequest, status, "action=%s body=%v", request.action, body)
		assert.Equal(t, "ResourceNotFoundException", body["__type"])
	}
	var pools int
	require.NoError(t, second.store.DB().QueryRow(`SELECT COUNT(*) FROM pools`).Scan(&pools))
	require.NoError(t, second.store.DB().QueryRow(`SELECT COUNT(*) FROM clients`).Scan(&clients))
	assert.Zero(t, pools)
	assert.Zero(t, clients)

	// Removing one parent cascades its app client without affecting siblings.
	status, body = postCognito(t, first.server.URL, "DeleteUserPool", map[string]any{"UserPoolId": poolID})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	status, body = postCognito(t, first.server.URL, "DescribeUserPoolClient", map[string]any{"UserPoolId": poolID, "ClientId": clientID})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "ResourceNotFoundException", body["__type"])
	status, body = postCognito(t, first.server.URL, "DescribeUserPool", map[string]any{"UserPoolId": otherID})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	assert.Equal(t, otherPool, body["UserPool"])
}
