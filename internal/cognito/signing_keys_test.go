package cognito

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSigningKeyCacheReusesDecodedKeysWithoutSQL(t *testing.T) {
	store, _ := newCognitoTestStore(t)
	require.NoError(t, store.UpsertPool(t.Context(), "cached-pool", "us-east-1"))
	first, err := store.ensureSigningKey(t.Context(), "cached-pool")
	require.NoError(t, err)

	// Hold the only SQL connection. Native key/JWKS paths must use the decoded
	// key rather than reread PEM; no extra connection is provisioned.
	connection, err := store.db.Conn(t.Context())
	require.NoError(t, err)
	defer connection.Close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	loaded, err := store.loadSigningKey(ctx, "cached-pool")
	require.NoError(t, err)
	require.Same(t, first, loaded)
	ensured, err := store.ensureSigningKey(ctx, "cached-pool")
	require.NoError(t, err)
	require.Same(t, first, ensured)
	body, err := store.BuildJWKS(ctx, "cached-pool")
	require.NoError(t, err)
	require.NotEmpty(t, body)
	require.Equal(t, 1, store.db.Stats().MaxOpenConnections)
}

func TestSigningKeySnapshotsCannotMutateCachedMaterial(t *testing.T) {
	store, _ := newCognitoTestStore(t)
	require.NoError(t, store.UpsertPool(t.Context(), "snapshot-pool", "us-east-1"))
	key, err := store.ensureSigningKey(t.Context(), "snapshot-pool")
	require.NoError(t, err)
	body, err := store.BuildJWKS(t.Context(), "snapshot-pool")
	require.NoError(t, err)
	for _, accessor := range []func(context.Context, string) (*SigningKey, error){store.EnsureSigningKey, store.LoadSigningKey} {
		snapshot, err := accessor(t.Context(), "snapshot-pool")
		require.NoError(t, err)
		require.NotSame(t, key, snapshot)
		require.NotSame(t, key.Private, snapshot.Private)
		require.NoError(t, snapshot.Private.Validate())
		snapshot.Kid = "changed"
		snapshot.PoolID = "changed"
		snapshot.CreatedAt = -1
		snapshot.Public.N.SetInt64(1)
		snapshot.Public.E = 1
		snapshot.Private.N.SetInt64(1)
		snapshot.Private.E = 1
		snapshot.Private.D.SetInt64(1)
		for _, prime := range snapshot.Private.Primes {
			prime.SetInt64(1)
		}
		snapshot.Private.Precomputed.Dp.SetInt64(1)
		snapshot.Private.Precomputed.Dq.SetInt64(1)
		snapshot.Private.Precomputed.Qinv.SetInt64(1)
		again, err := store.loadSigningKey(t.Context(), "snapshot-pool")
		require.NoError(t, err)
		require.Same(t, key, again)
		require.NoError(t, key.Private.Validate())
		gotBody, err := store.BuildJWKS(t.Context(), "snapshot-pool")
		require.NoError(t, err)
		require.Equal(t, body, gotBody)
		// Cached material still signs and verifies with real RSA after mutation
		// of every exported mutable component on the caller's snapshot.
		digest := sha256.Sum256([]byte("snapshot isolation"))
		signed, err := rsa.SignPKCS1v15(nil, key.Private, crypto.SHA256, digest[:])
		require.NoError(t, err)
		require.NoError(t, rsa.VerifyPKCS1v15(key.Public, crypto.SHA256, digest[:], signed))
	}
}

func TestSigningKeyDeleteRollbackRetainsCacheAndGeneration(t *testing.T) {
	store, _ := newCognitoTestStore(t)
	require.NoError(t, store.UpsertPool(t.Context(), "cached-pool", "us-east-1"))
	require.NoError(t, store.UpsertPool(t.Context(), "pending-pool", "us-east-1"))
	cached, err := store.ensureSigningKey(t.Context(), "cached-pool")
	require.NoError(t, err)
	pending := &signingKeyGeneration{done: make(chan struct{})}
	store.mu.Lock()
	store.signingKeyGenerations["pending-pool"] = pending
	store.mu.Unlock()
	_, err = store.db.ExecContext(t.Context(), "CREATE TRIGGER refuse_pool_delete BEFORE DELETE ON pools BEGIN SELECT RAISE(ABORT,'delete refused'); END")
	require.NoError(t, err)
	for _, pool := range []string{"cached-pool", "pending-pool"} {
		deleted, err := store.DeletePool(t.Context(), pool)
		require.Error(t, err)
		require.False(t, deleted)
	}
	loaded, err := store.loadSigningKey(t.Context(), "cached-pool")
	require.NoError(t, err)
	require.Same(t, cached, loaded)
	// The explicit key delete preceded the failed parent delete, so assert the
	// transaction rolled back the durable row as well as retaining its cache.
	var kid string
	require.NoError(t, store.db.QueryRowContext(t.Context(), "SELECT kid FROM signing_keys WHERE pool_id=?", "cached-pool").Scan(&kid))
	require.Equal(t, cached.Kid, kid)
	store.mu.Lock()
	retained := store.signingKeyGenerations["pending-pool"]
	store.mu.Unlock()
	require.Same(t, pending, retained)
	key, privatePEM, publicPEM, err := generateSigningKey("pending-pool")
	require.NoError(t, err)
	store.mu.Lock()
	store.completeSigningKeyGenerationLocked(t.Context(), "pending-pool", pending, key, privatePEM, publicPEM, nil)
	store.mu.Unlock()
	require.NoError(t, pending.err)
	require.Same(t, key, pending.key)
}

func TestSigningKeyGenerationInvalidation(t *testing.T) {
	for _, state := range []string{"deleted", "reseeded", "replacement-generation", "closed", "cancelled"} {
		t.Run(state, func(t *testing.T) {
			store, _ := newCognitoTestStore(t)
			const pool = "generation-pool"
			require.NoError(t, store.UpsertPool(t.Context(), pool, "us-east-1"))
			pending := &signingKeyGeneration{done: make(chan struct{})}
			store.mu.Lock()
			store.signingKeyGenerations[pool] = pending
			store.mu.Unlock()
			key, privatePEM, publicPEM, err := generateSigningKey(pool)
			require.NoError(t, err)
			ctx := t.Context()
			wantErr := sql.ErrNoRows
			var replacement *signingKeyGeneration
			switch state {
			case "closed":
				require.NoError(t, store.Close())
				wantErr = sql.ErrConnDone
			case "cancelled":
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = cancelled
				wantErr = context.Canceled
			default:
				deleted, err := store.DeletePool(ctx, pool)
				require.NoError(t, err)
				require.True(t, deleted)
				if state != "deleted" {
					require.NoError(t, store.UpsertPool(ctx, pool, "us-east-1"))
				}
				if state == "replacement-generation" {
					replacement = &signingKeyGeneration{done: make(chan struct{})}
					store.mu.Lock()
					store.signingKeyGenerations[pool] = replacement
					store.mu.Unlock()
				}
			}
			store.mu.Lock()
			store.completeSigningKeyGenerationLocked(ctx, pool, pending, key, privatePEM, publicPEM, nil)
			cached := store.signingKeys[pool]
			retained := store.signingKeyGenerations[pool]
			if replacement != nil {
				delete(store.signingKeyGenerations, pool)
			}
			store.mu.Unlock()
			require.Nil(t, cached)
			if replacement != nil {
				require.Same(t, replacement, retained, "old completion must not clear a new generation")
			}
			require.ErrorIs(t, pending.err, wantErr)
			require.Nil(t, pending.key)
			select {
			case <-pending.done:
			default:
				t.Fatal("generation waiters were not released")
			}
			if state != "closed" {
				var count int
				require.NoError(t, store.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM signing_keys WHERE pool_id=?", pool).Scan(&count))
				require.Zero(t, count, "invalidated/cancelled generation must not persist")
				if state != "deleted" {
					fresh, err := store.ensureSigningKey(t.Context(), pool)
					require.NoError(t, err)
					require.NotEqual(t, key.Kid, fresh.Kid)
				}
			}
		})
	}
}

func TestSigningKeyConcurrentFirstUseAndCancelledWaiter(t *testing.T) {
	store, _ := newCognitoTestStore(t)
	require.NoError(t, store.UpsertPool(t.Context(), "concurrent-pool", "us-east-1"))
	const callers = 16
	keys := make([]*SigningKey, callers)
	failures := make([]error, callers)
	start := make(chan struct{})
	var joined sync.WaitGroup
	for index := range callers {
		joined.Add(1)
		go func() {
			defer joined.Done()
			<-start
			keys[index], failures[index] = store.ensureSigningKey(t.Context(), "concurrent-pool")
		}()
	}
	close(start)
	joined.Wait()
	for index := range callers {
		require.NoError(t, failures[index])
		require.Same(t, keys[0], keys[index])
	}
	var count int
	require.NoError(t, store.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM signing_keys WHERE pool_id=?", "concurrent-pool").Scan(&count))
	require.Equal(t, 1, count)

	require.NoError(t, store.UpsertPool(t.Context(), "waiting-pool", "us-east-1"))
	pending := &signingKeyGeneration{done: make(chan struct{})}
	store.mu.Lock()
	store.signingKeyGenerations["waiting-pool"] = pending
	store.mu.Unlock()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	key, err := store.ensureSigningKey(ctx, "waiting-pool")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, key)
	store.mu.Lock()
	retained := store.signingKeyGenerations["waiting-pool"]
	store.completeSigningKeyGenerationLocked(t.Context(), "waiting-pool", pending, nil, "", "", errors.New("fixture generation finished"))
	store.mu.Unlock()
	require.Same(t, pending, retained)
}

// Observe a waiter's first context check while it holds the store lock. This
// makes origin-cancellation and Close tests independent of RSA timing.
type signingKeyObservedContext struct {
	context.Context
	checked chan struct{}
	once    sync.Once
}

func (ctx *signingKeyObservedContext) Err() error {
	ctx.once.Do(func() { close(ctx.checked) })
	return ctx.Context.Err()
}

func TestSigningKeyLiveWaiterRetriesCancelledOrigin(t *testing.T) {
	store, _ := newCognitoTestStore(t)
	const pool = "cancelled-origin-pool"
	require.NoError(t, store.UpsertPool(t.Context(), pool, "us-east-1"))
	pending := &signingKeyGeneration{done: make(chan struct{})}
	store.mu.Lock()
	store.signingKeyGenerations[pool] = pending
	store.mu.Unlock()
	ctx := &signingKeyObservedContext{Context: t.Context(), checked: make(chan struct{})}
	result := make(chan *SigningKey, 1)
	failure := make(chan error, 1)
	go func() {
		key, err := store.ensureSigningKey(ctx, pool)
		result <- key
		failure <- err
	}()
	<-ctx.checked
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	store.mu.Lock()
	store.completeSigningKeyGenerationLocked(cancelled, pool, pending, nil, "", "", nil)
	store.mu.Unlock()
	require.NoError(t, <-failure)
	key := <-result
	require.NotNil(t, key)
	require.ErrorIs(t, pending.err, context.Canceled)
	persisted, err := store.LoadSigningKey(t.Context(), pool)
	require.NoError(t, err)
	require.Equal(t, key.Kid, persisted.Kid)
}

func TestSigningKeyCloseJoinsGenerationAndRejectsItsResult(t *testing.T) {
	store, _ := newCognitoTestStore(t)
	const pool = "closing-pool"
	require.NoError(t, store.UpsertPool(t.Context(), pool, "us-east-1"))
	pending := &signingKeyGeneration{done: make(chan struct{})}
	store.mu.Lock()
	store.signingKeyGenerations[pool] = pending
	store.signingKeyTasks.Add(1)
	store.mu.Unlock()
	closed := make(chan error, 1)
	go func() { closed <- store.Close() }()

	// Close marks the store closed before waiting for RSA ownership. Observe
	// that guarded state with a bounded wait rather than guessing RSA duration.
	deadline := time.Now().Add(5 * time.Second)
	for {
		store.mu.Lock()
		closing := store.closed
		store.mu.Unlock()
		if closing {
			break
		}
		if time.Now().After(deadline) {
			store.signingKeyTasks.Done()
			t.Fatal("Close did not invalidate pending generation")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-closed:
		store.signingKeyTasks.Done()
		t.Fatalf("Close returned before generation joined: %v", err)
	default:
	}
	store.mu.Lock()
	store.completeSigningKeyGenerationLocked(t.Context(), pool, pending, nil, "", "", nil)
	store.mu.Unlock()
	store.signingKeyTasks.Done()
	require.NoError(t, <-closed)
	require.ErrorIs(t, pending.err, sql.ErrConnDone)
	require.Nil(t, pending.key)
}
