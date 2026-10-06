package cognito

import (
	"fmt"
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

func TestConcurrentClientIDCreationPreservesWinningPool(t *testing.T) {
	store, _ := newCognitoTestStore(t)
	ctx := t.Context()
	require.NoError(t, store.UpsertPool(ctx, "client-pool-one", "us-east-1"))
	require.NoError(t, store.UpsertPool(ctx, "client-pool-two", "us-east-1"))
	type result struct {
		pool, secret string
		err          error
	}
	for iteration := 0; iteration < 8; iteration++ {
		clientID := fmt.Sprintf("shared-client-%d", iteration)
		start := make(chan struct{})
		results := make(chan result, 2)
		for _, pool := range []string{"client-pool-one", "client-pool-two"} {
			go func(pool string) {
				<-start
				secret := pool + "-secret"
				results <- result{pool, secret, store.UpsertClient(ctx, clientID, pool, secret)}
			}(pool)
		}
		close(start)
		first, second := <-results, <-results
		winner, loser := first, second
		if winner.err != nil {
			winner, loser = loser, winner
		}
		require.NoError(t, winner.err)
		require.ErrorIs(t, loser.err, errClientPoolConflict)
		client, err := store.LookupClient(ctx, clientID)
		require.NoError(t, err)
		require.Equal(t, winner.pool, client.PoolID)
		require.Equal(t, winner.secret, client.Secret)
		require.NoError(t, store.SetClientAuthConfig(ctx, clientID, []string{"ALLOW_USER_SRP_AUTH"}, 7))
		before, err := store.LookupClient(ctx, clientID)
		require.NoError(t, err)
		require.ErrorIs(t, store.UpsertClient(ctx, clientID, loser.pool, "must-not-replace-secret"), errClientPoolConflict)
		after, err := store.LookupClient(ctx, clientID)
		require.NoError(t, err)
		require.Equal(t, before, after, "losing pool cannot change the secret or auth configuration")
		require.NoError(t, store.UpsertClient(ctx, clientID, winner.pool, "owner-updated-secret"))
		after, err = store.LookupClient(ctx, clientID)
		require.NoError(t, err)
		require.Equal(t, "owner-updated-secret", after.Secret)
		require.Equal(t, before.ExplicitAuthFlows, after.ExplicitAuthFlows)
		require.Equal(t, before.AuthSessionValidity, after.AuthSessionValidity)
	}
}
