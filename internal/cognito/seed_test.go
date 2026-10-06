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
