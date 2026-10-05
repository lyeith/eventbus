package main

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	smtypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupSecretsClient(t *testing.T) (*httptest.Server, *secretsmanager.Client) {
	t.Helper()

	broker := NewBroker("us-east-1", "000000000000", 0)
	fm := NewFirehoseManager("us-east-1", "000000000000", "http://localhost:9000", "test", "test")
	ssmStore := NewSSMStore()
	secrets := NewSecretsStore("us-east-1", "000000000000")
	server := NewServer(broker, fm, ssmStore, secrets)
	ts := httptest.NewServer(server)
	t.Cleanup(ts.Close)

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := secretsmanager.NewFromConfig(cfg, func(o *secretsmanager.Options) {
		o.BaseEndpoint = aws.String(ts.URL)
	})
	return ts, client
}

func TestSecretsCreateSecret(t *testing.T) {
	_, client := setupSecretsClient(t)
	ctx := context.Background()

	result, err := client.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:         aws.String("connections/org-1/conn-1"),
		SecretString: aws.String(`{"api_key":"sk-test-123"}`),
		Tags: []smtypes.Tag{
			{Key: aws.String("org_id"), Value: aws.String("org-1")},
			{Key: aws.String("managed_by"), Value: aws.String("connections-service")},
		},
	})
	require.NoError(t, err)
	assert.Contains(t, *result.ARN, "connections/org-1/conn-1")
	assert.Equal(t, "connections/org-1/conn-1", *result.Name)
	assert.NotEmpty(t, *result.VersionId)
}
func TestSecretsPreservesClientRequestTokensAndExactVersions(t *testing.T) {
	_, client := setupSecretsClient(t)
	ctx := context.Background()
	const (
		secretName = "connections/org-versioned/conn-versioned"
		versionOne = "11111111111111111111111111111111"
		versionTwo = "22222222222222222222222222222222"
	)

	created, err := client.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:               aws.String(secretName),
		SecretString:       aws.String(`{"api_key":"first"}`),
		ClientRequestToken: aws.String(versionOne),
	})
	require.NoError(t, err)
	require.Equal(t, versionOne, aws.ToString(created.VersionId))

	updated, err := client.PutSecretValue(ctx, &secretsmanager.PutSecretValueInput{
		SecretId:           aws.String(secretName),
		SecretString:       aws.String(`{"api_key":"second"}`),
		ClientRequestToken: aws.String(versionTwo),
	})
	require.NoError(t, err)
	require.Equal(t, versionTwo, aws.ToString(updated.VersionId))

	first, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId:  aws.String(secretName),
		VersionId: aws.String(versionOne),
	})
	require.NoError(t, err)
	assert.Equal(t, versionOne, aws.ToString(first.VersionId))
	assert.Equal(t, `{"api_key":"first"}`, aws.ToString(first.SecretString))

	latest, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(secretName),
	})
	require.NoError(t, err)
	assert.Equal(t, versionTwo, aws.ToString(latest.VersionId))
	assert.Equal(t, `{"api_key":"second"}`, aws.ToString(latest.SecretString))

	replayed, err := client.PutSecretValue(ctx, &secretsmanager.PutSecretValueInput{
		SecretId:           aws.String(secretName),
		SecretString:       aws.String(`{"api_key":"second"}`),
		ClientRequestToken: aws.String(versionTwo),
	})
	require.NoError(t, err)
	assert.Equal(t, versionTwo, aws.ToString(replayed.VersionId))

	_, err = client.PutSecretValue(ctx, &secretsmanager.PutSecretValueInput{
		SecretId:           aws.String(secretName),
		SecretString:       aws.String(`{"api_key":"conflict"}`),
		ClientRequestToken: aws.String(versionTwo),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ResourceExistsException")
}

func TestSecretsCreateSecretDuplicate(t *testing.T) {
	_, client := setupSecretsClient(t)
	ctx := context.Background()

	_, err := client.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:         aws.String("dup-secret"),
		SecretString: aws.String("value1"),
	})
	require.NoError(t, err)

	_, err = client.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:         aws.String("dup-secret"),
		SecretString: aws.String("value2"),
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ResourceExistsException")
}

func TestSecretsGetSecretValue(t *testing.T) {
	_, client := setupSecretsClient(t)
	ctx := context.Background()

	_, err := client.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:         aws.String("connections/org-2/conn-2"),
		SecretString: aws.String(`{"token":"xero-token-abc"}`),
	})
	require.NoError(t, err)

	result, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: aws.String("connections/org-2/conn-2"),
	})
	require.NoError(t, err)
	assert.Equal(t, "connections/org-2/conn-2", *result.Name)
	assert.Equal(t, `{"token":"xero-token-abc"}`, *result.SecretString)
	assert.NotEmpty(t, *result.VersionId)
}

func TestSecretsGetSecretValueNotFound(t *testing.T) {
	_, client := setupSecretsClient(t)
	ctx := context.Background()

	_, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: aws.String("nonexistent"),
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ResourceNotFoundException")
}

func TestSecretsUpdateSecret(t *testing.T) {
	_, client := setupSecretsClient(t)
	ctx := context.Background()

	_, err := client.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:         aws.String("update-me"),
		SecretString: aws.String("old-value"),
	})
	require.NoError(t, err)

	_, err = client.UpdateSecret(ctx, &secretsmanager.UpdateSecretInput{
		SecretId:     aws.String("update-me"),
		SecretString: aws.String("new-value"),
	})
	require.NoError(t, err)

	result, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: aws.String("update-me"),
	})
	require.NoError(t, err)
	assert.Equal(t, "new-value", *result.SecretString)
}

func TestSecretsUpdateNotFound(t *testing.T) {
	_, client := setupSecretsClient(t)
	ctx := context.Background()

	_, err := client.UpdateSecret(ctx, &secretsmanager.UpdateSecretInput{
		SecretId:     aws.String("nonexistent"),
		SecretString: aws.String("value"),
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ResourceNotFoundException")
}

func TestSecretsDeleteSecret(t *testing.T) {
	_, client := setupSecretsClient(t)
	ctx := context.Background()

	_, err := client.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:         aws.String("delete-me"),
		SecretString: aws.String("sensitive"),
	})
	require.NoError(t, err)

	_, err = client.DeleteSecret(ctx, &secretsmanager.DeleteSecretInput{
		SecretId:                   aws.String("delete-me"),
		ForceDeleteWithoutRecovery: aws.Bool(true),
	})
	require.NoError(t, err)

	_, err = client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: aws.String("delete-me"),
	})
	assert.Error(t, err)
}

func TestSecretsDeleteNotFound(t *testing.T) {
	_, client := setupSecretsClient(t)
	ctx := context.Background()

	_, err := client.DeleteSecret(ctx, &secretsmanager.DeleteSecretInput{
		SecretId: aws.String("nonexistent"),
	})
	assert.Error(t, err)
}

// Unit tests for SecretsStore
func TestSecretsStoreUnit(t *testing.T) {
	t.Run("create and get", func(t *testing.T) {
		store := NewSecretsStore("us-east-1", "000000000000")
		secret, err := store.CreateSecret("test", "value", "", nil, "us-east-1", "000000000000")
		require.NoError(t, err)
		assert.Equal(t, "test", secret.Name)
		assert.Contains(t, secret.ARN, "test")

		got, err := store.GetSecretValue("test", "")
		require.NoError(t, err)
		assert.Equal(t, "value", got.SecretString)
	})

	t.Run("update changes version", func(t *testing.T) {
		store := NewSecretsStore("us-east-1", "000000000000")
		s1, _ := store.CreateSecret("ver", "v1", "", nil, "us-east-1", "000000000000")
		v1 := s1.VersionID

		s2, _ := store.UpdateSecret("ver", "v2", "")
		assert.NotEqual(t, v1, s2.VersionID)
		assert.Equal(t, "v2", s2.SecretString)
	})

	t.Run("delete removes", func(t *testing.T) {
		store := NewSecretsStore("us-east-1", "000000000000")
		_, err := store.CreateSecret("del", "val", "", nil, "us-east-1", "000000000000")
		require.NoError(t, err)
		err = store.DeleteSecret("del", true)
		require.NoError(t, err)

		_, err = store.GetSecretValue("del", "")
		assert.Error(t, err)
	})
}
