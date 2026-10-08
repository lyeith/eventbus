package cognito

import (
	"bytes"
	"context"
	"database/sql"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func requireSeedPasswordCredentials(t *testing.T, user *CognitoUser, password string) {
	t.Helper()
	require.NoError(t, compareUserPasswordHash(user.PasswordHash, password))
	salt, valid := new(big.Int).SetString(user.SRPSalt, 16)
	require.True(t, valid)
	saltBytes := salt.FillBytes(make([]byte, srpSaltBytes))
	expectedSalt, expectedVerifier, err := makeSRPCredentialsWithEntropy(user.PoolID, user.Username, password, bytes.NewReader(saltBytes))
	require.NoError(t, err)
	require.Equal(t, expectedSalt, user.SRPSalt)
	require.Equal(t, expectedVerifier, user.SRPVerifier, "persisted verifier must use the canonical username and original plaintext")
}

func TestUpsertSeedUserFreshUnchangedAndChangedCredentials(t *testing.T) {
	store, _ := newCognitoTestStore(t)
	ctx := t.Context()
	now := time.Date(2026, 10, 8, 10, 0, 0, 0, time.UTC)
	NewHandler(store, Options{Clock: func() time.Time { return now }})
	const poolID = "us-east-1_seed"
	require.NoError(t, store.UpsertPool(ctx, poolID, "us-east-1"))
	// A long password also guards the shared SHA256+bcrypt representation:
	// creation must retain the original plaintext only in the SRP computation.
	password := strings.Repeat("A", 90) + "a1!"
	sub, err := store.UpsertSeedUser(ctx, poolID, "canonical-seed-user", "seed@example.test", password, false)
	require.NoError(t, err)
	user, err := store.LookupUserBySub(ctx, sub)
	require.NoError(t, err)
	require.Equal(t, "canonical-seed-user", user.Username)
	require.Equal(t, "CONFIRMED", user.Status)
	require.True(t, user.Enabled)
	require.Zero(t, user.AuthVersion)
	require.Equal(t, now.Unix(), user.CreatedAt)
	require.Equal(t, now.Unix(), user.PasswordChangedAt)
	requireSeedPasswordCredentials(t, user, password)
	require.NoError(t, store.SetUserAttribute(ctx, sub, "custom:owner", "preserved"))

	now = now.Add(time.Minute)
	againSub, err := store.UpsertSeedUser(ctx, poolID, user.Username, user.Email, password, false)
	require.NoError(t, err)
	require.Equal(t, sub, againSub)
	again, err := store.LookupUserBySub(ctx, sub)
	require.NoError(t, err)
	require.Equal(t, user, again, "same password must not renew timestamps, regenerate credentials or change grant revision")
	require.NoError(t, store.CreateChallengeSession(ctx, "before-fixture-password-change", sub, poolID, "fixture-client", "NEW_PASSWORD_REQUIRED", time.Hour))

	now = now.Add(time.Minute)
	replacement := "ChangedSeedPass1!"
	againSub, err = store.UpsertSeedUser(ctx, poolID, user.Username, user.Email, replacement, false)
	require.NoError(t, err)
	require.Equal(t, sub, againSub)
	changed, err := store.LookupUserBySub(ctx, sub)
	require.NoError(t, err)
	require.Equal(t, user.Sub, changed.Sub)
	require.Equal(t, user.Username, changed.Username)
	require.Equal(t, user.CreatedAt, changed.CreatedAt)
	require.Equal(t, user.AuthVersion+1, changed.AuthVersion)
	require.Equal(t, now.Unix(), changed.PasswordChangedAt)
	require.NotEqual(t, user.PasswordHash, changed.PasswordHash)
	require.NotEqual(t, user.SRPSalt, changed.SRPSalt)
	requireSeedPasswordCredentials(t, changed, replacement)
	require.Error(t, compareUserPasswordHash(changed.PasswordHash, password))
	attributes, err := store.LoadUserAttributes(ctx, sub)
	require.NoError(t, err)
	require.Equal(t, "preserved", attributes["custom:owner"])
	_, err = store.LookupChallengeSession(ctx, "before-fixture-password-change")
	require.ErrorIs(t, err, sql.ErrNoRows)
}

func TestUpsertSeedUserErrorsDoNotAdmitInvalidCredentials(t *testing.T) {
	store, _ := newCognitoTestStore(t)
	ctx := t.Context()
	require.NoError(t, store.UpsertPool(ctx, "seed-errors", "us-east-1"))
	sub, err := store.UpsertSeedUser(ctx, "seed-errors", "blank-new", "", "", false)
	require.Error(t, err)
	require.Empty(t, sub)
	_, err = store.LookupPoolUser(ctx, "seed-errors", "blank-new")
	require.ErrorIs(t, err, sql.ErrNoRows)

	sub, err = store.UpsertSeedUser(ctx, "seed-errors", "existing", "", "SeedPass1!", false)
	require.NoError(t, err)
	before, err := store.LookupUserBySub(ctx, sub)
	require.NoError(t, err)
	_, err = store.UpsertSeedUser(ctx, "seed-errors", "existing", "", "", false)
	require.Error(t, err)
	after, err := store.LookupUserBySub(ctx, sub)
	require.NoError(t, err)
	require.Equal(t, before, after)

	// SRP failure occurs after bcrypt computation but before the atomic insert.
	require.NoError(t, store.UpsertPool(ctx, "_invalid-srp-pool", "us-east-1"))
	_, err = store.UpsertSeedUser(ctx, "_invalid-srp-pool", "srp-invalid", "", "SeedPass1!", false)
	require.ErrorContains(t, err, "invalid SRP user pool identifier")
	_, err = store.LookupPoolUser(ctx, "_invalid-srp-pool", "srp-invalid")
	require.ErrorIs(t, err, sql.ErrNoRows)

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = store.UpsertSeedUser(canceled, "seed-errors", "canceled", "", "SeedPass1!", false)
	require.ErrorIs(t, err, context.Canceled)
	_, err = store.LookupPoolUser(ctx, "seed-errors", "canceled")
	require.ErrorIs(t, err, sql.ErrNoRows)
}

func TestUpsertSeedUserConcurrentCreatePreservesOneIdentity(t *testing.T) {
	store, _ := newCognitoTestStore(t)
	ctx := t.Context()
	require.NoError(t, store.UpsertPool(ctx, "us-east-1_concurrentseed", "us-east-1"))
	type result struct {
		sub string
		err error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	for range 2 {
		go func() {
			<-start
			sub, err := store.UpsertSeedUser(ctx, "us-east-1_concurrentseed", "same-user", "same@example.test", "ConcurrentSeedPass1!", false)
			results <- result{sub, err}
		}()
	}
	close(start)
	first, second := <-results, <-results // Both callers join before assertions.
	require.NoError(t, first.err)
	require.NoError(t, second.err)
	require.Equal(t, first.sub, second.sub)
	var count int
	require.NoError(t, store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM users WHERE pool_id=?`, "us-east-1_concurrentseed").Scan(&count))
	require.Equal(t, 1, count)
	user, err := store.LookupUserBySub(ctx, first.sub)
	require.NoError(t, err)
	require.Zero(t, user.AuthVersion, "losing concurrent create must compare the persisted credential rather than reset it")
	requireSeedPasswordCredentials(t, user, "ConcurrentSeedPass1!")
}
