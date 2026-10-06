package cognito

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
