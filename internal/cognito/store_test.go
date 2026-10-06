package cognito

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCognitoStore_PoolUserRoundTrip(t *testing.T) {
	store, _ := newCognitoTestStore(t)
	ctx := t.Context()

	require.NoError(t, store.UpsertPool(ctx, "p1", "us-east-1"))
	exists, err := store.PoolExists(ctx, "p1")
	require.NoError(t, err)
	assert.True(t, exists)

	require.NoError(t, store.UpsertClient(ctx, "c1", "p1", ""))

	sub, err := store.UpsertUser(ctx, "p1", "alice@example.com", "hash1", false)
	require.NoError(t, err)
	assert.NotEmpty(t, sub)

	require.NoError(t, store.SetUserAttribute(ctx, sub, "email", "alice@example.com"))
	require.NoError(t, store.SetUserAttribute(ctx, sub, "email_verified", "true"))
	attrs, err := store.LoadUserAttributes(ctx, sub)
	require.NoError(t, err)
	assert.Equal(t, "alice@example.com", attrs["email"])
	assert.Equal(t, "true", attrs["email_verified"])

	// Idempotency: re-upsert with new password keeps the same sub.
	sub2, err := store.UpsertUser(ctx, "p1", "alice@example.com", "hash2", true)
	require.NoError(t, err)
	assert.Equal(t, sub, sub2)
}
