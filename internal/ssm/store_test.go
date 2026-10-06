package ssm

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Unit tests for SSMStore
func TestSSMStoreUnit(t *testing.T) {
	t.Run("put and get", func(t *testing.T) {
		store := NewSSMStore()
		err := store.PutParameter("/test", "value", "String", false)
		require.NoError(t, err)

		param, err := store.GetParameter("/test")
		require.NoError(t, err)
		assert.Equal(t, "value", param.Value)
	})

	t.Run("duplicate without overwrite", func(t *testing.T) {
		store := NewSSMStore()
		require.NoError(t, store.PutParameter("/test", "v1", "String", false))
		err := store.PutParameter("/test", "v2", "String", false)
		assert.Error(t, err)
	})

	t.Run("overwrite", func(t *testing.T) {
		store := NewSSMStore()
		require.NoError(t, store.PutParameter("/test", "v1", "String", false))
		err := store.PutParameter("/test", "v2", "String", true)
		require.NoError(t, err)
		param, _ := store.GetParameter("/test")
		assert.Equal(t, "v2", param.Value)
	})

	t.Run("get by path prefix", func(t *testing.T) {
		store := NewSSMStore()
		require.NoError(t, store.PutParameter("/a/b/c", "1", "String", false))
		require.NoError(t, store.PutParameter("/a/b/d", "2", "String", false))
		require.NoError(t, store.PutParameter("/a/x", "3", "String", false))

		params := store.GetParametersByPath("/a/b/")
		assert.Len(t, params, 2)
	})
}
