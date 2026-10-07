package cognito

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
)

const sampleSeedYAML = `
pools:
  - id: local-pool-1
    region: us-east-1
    clients:
      - id: local-client-1
        secret: ""
    users:
      - email: admin@platform.local
        password: dev
        mfa_enabled: false
        attributes:
          custom:role: platform_admin
      - email: mfa@platform.local
        password: dev
        mfa_enabled: true
`

// writeSeedFile drops `body` into a temp YAML file and returns its path.
func writeSeedFile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cognito_pools.yaml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

func TestLoadCognitoSeed_HappyPath(t *testing.T) {
	path := writeSeedFile(t, sampleSeedYAML)
	seed, err := LoadCognitoSeed(path)
	require.NoError(t, err)
	require.Len(t, seed.Pools, 1)
	pool := seed.Pools[0]
	assert.Equal(t, "local-pool-1", pool.ID)
	assert.Equal(t, "us-east-1", pool.Region)
	require.Len(t, pool.Clients, 1)
	require.Len(t, pool.Users, 2)
	assert.Equal(t, "admin@platform.local", pool.Users[0].Email)
	assert.Equal(t, "platform_admin", pool.Users[0].Attributes["custom:role"])
	assert.True(t, pool.Users[1].MFAEnabled)
}

func TestLoadCognitoSeed_MissingFile(t *testing.T) {
	_, err := LoadCognitoSeed("/no/such/path.yaml")
	assert.Error(t, err)
}

func TestLoadCognitoSeed_RejectsMissingEmail(t *testing.T) {
	bad := `
pools:
  - id: pool-x
    users:
      - password: secret
`
	_, err := LoadCognitoSeed(writeSeedFile(t, bad))
	assert.ErrorContains(t, err, "email is required")
}

// TestApplyCognitoSeed_Idempotent is the central design contract from §5f /
// the BACKLOG entry: re-running the seed must not error and must not
// duplicate rows. We assert pool/client/user counts stay constant on the
// second apply.
func TestApplyCognitoSeed_Idempotent(t *testing.T) {
	store, _ := newCognitoTestStore(t)
	ctx := context.Background()

	seed, err := LoadCognitoSeed(writeSeedFile(t, sampleSeedYAML))
	require.NoError(t, err)

	require.NoError(t, ApplyCognitoSeed(ctx, store, seed))
	require.NoError(t, ApplyCognitoSeed(ctx, store, seed))

	// Use raw SQL to count rows — exact counts catch any accidental dup insert.
	count := func(table string) int {
		t.Helper()
		var n int
		require.NoError(t, store.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&n))
		return n
	}

	assert.Equal(t, 1, count("pools"))
	assert.Equal(t, 1, count("clients"))
	assert.Equal(t, 2, count("users"))
	// 3 attributes per user expected: email, email_verified, optional custom:role.
	// User 1 has 3, user 2 has 2 (no custom attrs). Total: 5.
	assert.Equal(t, 5, count("user_attributes"))
}

// TestApplyCognitoSeed_HashesPasswords ensures plaintext never lands in DB
// and bcrypt verifies cleanly.
func TestApplyCognitoSeed_HashesPasswords(t *testing.T) {
	store, _ := newCognitoTestStore(t)
	ctx := context.Background()

	seed, err := LoadCognitoSeed(writeSeedFile(t, sampleSeedYAML))
	require.NoError(t, err)
	require.NoError(t, ApplyCognitoSeed(ctx, store, seed))

	var hash string
	require.NoError(t, store.DB().QueryRowContext(ctx,
		`SELECT password_hash FROM users WHERE email = ?`, "admin@platform.local",
	).Scan(&hash))
	assert.NotEqual(t, "dev", hash, "password must not be stored in cleartext")
	assert.NoError(t, bcrypt.CompareHashAndPassword([]byte(hash), []byte("dev")))
}

// TestApplyCognitoSeed_CreatesSigningKey ensures pools are immediately ready
// to publish JWKS without a separate first-use call.
func TestApplyCognitoSeed_CreatesSigningKey(t *testing.T) {
	store, _ := newCognitoTestStore(t)
	ctx := context.Background()

	seed, err := LoadCognitoSeed(writeSeedFile(t, sampleSeedYAML))
	require.NoError(t, err)
	require.NoError(t, ApplyCognitoSeed(ctx, store, seed))

	key, err := store.LoadSigningKey(ctx, "local-pool-1")
	require.NoError(t, err)
	assert.Len(t, key.Kid, 16)
}

func TestApplyCognitoSeed_BackfillsLegacyVerifierWithoutRenewingLifecycle(t *testing.T) {
	store, _ := newCognitoTestStore(t)
	ctx := t.Context()
	require.NoError(t, store.UpsertPool(ctx, "legacy-seed", "us-east-1"))
	hash, err := hashUserPassword("LegacyPass1!")
	require.NoError(t, err)
	sub, err := store.UpsertUser(ctx, "legacy-seed", "legacy@example.test", hash, false)
	require.NoError(t, err)
	require.NoError(t, store.SetUserAttribute(ctx, sub, "email", "legacy@example.test"))
	require.NoError(t, store.SetUserAttribute(ctx, sub, "email_verified", "true"))
	require.NoError(t, store.SetUserEnabled(ctx, sub, false))
	_, err = store.DB().ExecContext(ctx, `UPDATE users SET status='FORCE_CHANGE_PASSWORD',created_at=100,updated_at=101,password_changed_at=102 WHERE sub=?`, sub)
	require.NoError(t, err)
	before, err := store.LookupUserBySub(ctx, sub)
	require.NoError(t, err)
	seed := &CognitoSeedFile{Pools: []CognitoSeedPool{{ID: "legacy-seed", Region: "us-east-1", Users: []CognitoSeedUser{{Email: "legacy@example.test", Password: "LegacyPass1!"}}}}}
	require.NoError(t, ApplyCognitoSeed(ctx, store, seed))
	after, err := store.LookupUserBySub(ctx, sub)
	require.NoError(t, err)
	require.NotEmpty(t, after.SRPSalt)
	require.NotEmpty(t, after.SRPVerifier)
	before.SRPSalt, before.SRPVerifier = after.SRPSalt, after.SRPVerifier
	require.Equal(t, before, after, "same-password verifier backfill must preserve disabled state, temporary expiry and grant revision")
	require.NoError(t, ApplyCognitoSeed(ctx, store, seed))
	again, err := store.LookupUserBySub(ctx, sub)
	require.NoError(t, err)
	require.Equal(t, after, again, "reapplication must not regenerate credentials or modification timestamps")
}

func TestApplyCognitoSeed_ExplicitUsernameAndAliasConfiguration(t *testing.T) {
	store, _ := newCognitoTestStore(t)
	ctx := t.Context()
	seed, err := LoadCognitoSeed(writeSeedFile(t, `
pools:
  - id: alias-seed
    sign_in:
      email_alias: true
      case_sensitive: false
    users:
      - username: StableUsername
        email: Invitee@Example.test
        password: SeedPass1!
        attributes:
          email_verified: "false"
          custom:role: developer
`))
	require.NoError(t, err)
	require.NoError(t, ApplyCognitoSeed(ctx, store, seed))
	user, err := store.LookupPoolUser(ctx, "alias-seed", "STABLEUSERNAME")
	require.NoError(t, err)
	require.Equal(t, "stableusername", user.Username)
	require.Equal(t, "invitee@example.test", user.Email)
	require.NotEqual(t, user.Username, user.Email)
	require.True(t, user.Enabled)
	require.Equal(t, "CONFIRMED", user.Status)
	attributes, err := store.LoadUserAttributes(ctx, user.Sub)
	require.NoError(t, err)
	require.Equal(t, "false", attributes["email_verified"])
	require.Equal(t, "developer", attributes["custom:role"])
	_, err = store.ResolveSignInUser(ctx, "alias-seed", "Invitee@Example.test")
	require.Error(t, err, "unverified email aliases cannot sign in")
	require.NoError(t, ApplyCognitoSeed(ctx, store, seed))
	again, err := store.LookupUserBySub(ctx, user.Sub)
	require.NoError(t, err)
	require.Equal(t, user, again)
}

func TestApplyCognitoSeed_EmailUsernameKeepsGeneratedIdentity(t *testing.T) {
	store, _ := newCognitoTestStore(t)
	seed := &CognitoSeedFile{Pools: []CognitoSeedPool{{ID: "email-seed", Region: "us-east-1", SignIn: &CognitoSeedSignIn{EmailAsUsername: true}, Users: []CognitoSeedUser{{Email: "email@example.test", Password: "SeedPass1!"}}}}}
	require.NoError(t, ApplyCognitoSeed(t.Context(), store, seed))
	user, err := store.ResolveSignInUser(t.Context(), "email-seed", "email@example.test")
	require.NoError(t, err)
	require.Equal(t, user.Sub, user.Username)
	require.NoError(t, ApplyCognitoSeed(t.Context(), store, seed))
	again, err := store.ResolveSignInUser(t.Context(), "email-seed", "email@example.test")
	require.NoError(t, err)
	require.Equal(t, user, again)
}
