package secrets

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Unit tests for SecretsStore
func TestSecretsStoreUnit(t *testing.T) {
	t.Run("create and get", func(t *testing.T) {
		store := NewSecretsStore("us-east-1", "000000000000")
		secret, err := store.CreateSecret("test", "value", "", nil)
		require.NoError(t, err)
		assert.Equal(t, "test", secret.Name)
		assert.Contains(t, secret.ARN, "test")

		got, err := store.GetSecretValue("test", "")
		require.NoError(t, err)
		assert.Equal(t, "value", got.SecretString)
	})

	t.Run("update changes version", func(t *testing.T) {
		store := NewSecretsStore("us-east-1", "000000000000")
		s1, _ := store.CreateSecret("ver", "v1", "", nil)
		v1 := s1.VersionID

		s2, _ := store.UpdateSecret("ver", "v2", "")
		assert.NotEqual(t, v1, s2.VersionID)
		assert.Equal(t, "v2", s2.SecretString)
	})

	t.Run("delete removes", func(t *testing.T) {
		store := NewSecretsStore("us-east-1", "000000000000")
		_, err := store.CreateSecret("del", "val", "", nil)
		require.NoError(t, err)
		err = store.DeleteSecret("del", true)
		require.NoError(t, err)

		_, err = store.GetSecretValue("del", "")
		assert.Error(t, err)
	})
}

func TestSecretsStoreOwnsARNIdentity(t *testing.T) {
	store := NewSecretsStore("ap-southeast-1", "123456789012")
	secret, err := store.CreateSecret("standalone", "value", "", nil)
	require.NoError(t, err)
	require.Contains(t, secret.ARN, "arn:aws:secretsmanager:ap-southeast-1:123456789012:secret:standalone-")
}
