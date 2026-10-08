package cognito

import (
	"crypto/rand"
	"crypto/rsa"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// readBootstrapRows snapshots only this test's owned SQLite fixture. Keeping
// the original SQL values also verifies exact persisted key bytes on rollback.
func readBootstrapRows(t *testing.T, db *sql.DB, query string) [][]any {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), query)
	require.NoError(t, err)
	defer rows.Close()
	columns, err := rows.Columns()
	require.NoError(t, err)
	var result [][]any
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		require.NoError(t, rows.Scan(pointers...))
		result = append(result, values)
	}
	require.NoError(t, rows.Err())
	return result
}

func TestCognitoBootstrapRollsBackLegacySchemaAndIdentityOnFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy-rollback.db")
	legacy, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = legacy.Close() })
	for _, statement := range []string{
		`CREATE TABLE pools(id TEXT PRIMARY KEY,region TEXT NOT NULL,created_at INTEGER NOT NULL)`,
		`CREATE TABLE clients(id TEXT PRIMARY KEY,pool_id TEXT NOT NULL,secret TEXT NOT NULL DEFAULT '',created_at INTEGER NOT NULL)`,
		`CREATE TABLE users(sub TEXT PRIMARY KEY,pool_id TEXT NOT NULL,email TEXT NOT NULL,password_hash TEXT NOT NULL DEFAULT '',mfa_enabled INTEGER NOT NULL DEFAULT 0,mfa_secret TEXT NOT NULL DEFAULT '',status TEXT NOT NULL DEFAULT 'CONFIRMED',created_at INTEGER NOT NULL)`,
		`CREATE INDEX idx_users_pool_email ON users(pool_id,email)`,
		`CREATE TABLE user_attributes(sub TEXT NOT NULL,name TEXT NOT NULL,value TEXT NOT NULL DEFAULT '',PRIMARY KEY(sub,name))`,
		`CREATE TABLE signing_keys(pool_id TEXT PRIMARY KEY,kid TEXT NOT NULL,private_pem TEXT NOT NULL,public_pem TEXT NOT NULL,created_at INTEGER NOT NULL)`,
		`INSERT INTO pools VALUES('legacy-pool','us-east-1',1600000000)`,
		`INSERT INTO clients VALUES('legacy-client','legacy-pool','durable-client-secret',1600000001)`,
		`INSERT INTO users VALUES('durable-sub-one','legacy-pool','duplicate@example.test','durable-hash-one',1,'durable-mfa-secret','FORCE_CHANGE_PASSWORD',1600000002)`,
		`INSERT INTO users VALUES('durable-sub-two','legacy-pool','duplicate@example.test','durable-hash-two',0,'','CONFIRMED',1600000003)`,
		`INSERT INTO user_attributes VALUES('durable-sub-one','custom:owner','preserved')`,
	} {
		_, err = legacy.ExecContext(t.Context(), statement)
		require.NoError(t, err)
	}
	private, err := rsa.GenerateKey(rand.Reader, rsaKeySize)
	require.NoError(t, err)
	privatePEM, err := encodePrivateKeyPEM(private)
	require.NoError(t, err)
	publicPEM, err := encodePublicKeyPEM(&private.PublicKey)
	require.NoError(t, err)
	_, err = legacy.ExecContext(t.Context(), `INSERT INTO signing_keys VALUES('legacy-pool','durable-kid',?,?,1600000004)`, privatePEM, publicPEM)
	require.NoError(t, err)

	const schemaQuery = `SELECT type,name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name`
	dataQueries := []string{
		`SELECT id,region,created_at FROM pools ORDER BY id`,
		`SELECT id,pool_id,secret,created_at FROM clients ORDER BY id`,
		`SELECT sub,pool_id,email,password_hash,mfa_enabled,mfa_secret,status,created_at FROM users ORDER BY sub`,
		`SELECT sub,name,value FROM user_attributes ORDER BY sub,name`,
		`SELECT pool_id,kid,private_pem,public_pem,created_at FROM signing_keys ORDER BY pool_id`,
	}
	schemaBefore := readBootstrapRows(t, legacy, schemaQuery)
	dataBefore := make([][][]any, len(dataQueries))
	for i, query := range dataQueries {
		dataBefore[i] = readBootstrapRows(t, legacy, query)
	}
	require.NoError(t, legacy.Close())

	// The legacy rows conflict only after username backfill. This fails at the
	// final identity index, after CREATE, ALTER and all identity repair UPDATEs.
	store, err := OpenCognitoStore(path)
	require.ErrorContains(t, err, "migrate user identity")
	require.Nil(t, store, "a partially bootstrapped store must never be published")
	failed, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = failed.Close() })
	require.Equal(t, schemaBefore, readBootstrapRows(t, failed, schemaQuery), "failure must roll back new tables, indexes and columns")
	for i, query := range dataQueries {
		require.Equal(t, dataBefore[i], readBootstrapRows(t, failed, query), "failure must preserve original rows: %s", query)
	}

	// Repair only the duplicate in this owned fixture. The failed open must
	// leave neither a lock nor partially repaired state that prevents retry.
	_, err = failed.ExecContext(t.Context(), `UPDATE users SET email='second@example.test' WHERE sub='durable-sub-two'`)
	require.NoError(t, err)
	require.NoError(t, failed.Close())
	var poolBefore *CognitoPool
	var clientBefore *CognitoClient
	var usersBefore []*CognitoUser
	var migratedSchema [][]any
	for attempt := 0; attempt < 2; attempt++ {
		store, err = OpenCognitoStore(path)
		require.NoError(t, err)
		// Register close before assertions; a failed assertion must still release
		// this test's SQLite connection before its temporary directory is removed.
		opened := store
		t.Cleanup(func() { _ = opened.Close() })
		pool, err := store.LookupPool(t.Context(), "legacy-pool")
		require.NoError(t, err)
		require.Equal(t, "us-east-1", pool.Region)
		require.Equal(t, int64(1600000000), pool.CreatedAt)
		client, err := store.LookupClient(t.Context(), "legacy-client")
		require.NoError(t, err)
		require.Equal(t, "durable-client-secret", client.Secret)
		var users []*CognitoUser
		for i, email := range []string{"duplicate@example.test", "second@example.test"} {
			user, err := store.LookupPoolUser(t.Context(), "legacy-pool", email)
			require.NoError(t, err)
			require.Equal(t, email, user.Username)
			require.True(t, user.Enabled)
			require.Zero(t, user.AuthVersion)
			require.Equal(t, int64(1600000002+i), user.CreatedAt)
			require.Equal(t, user.CreatedAt, user.UpdatedAt)
			require.Equal(t, user.CreatedAt, user.PasswordChangedAt)
			users = append(users, user)
		}
		require.Equal(t, "durable-sub-one", users[0].Sub)
		require.Equal(t, "durable-sub-two", users[1].Sub)
		require.Equal(t, "durable-hash-one", users[0].PasswordHash)
		require.Equal(t, "FORCE_CHANGE_PASSWORD", users[0].Status)
		require.True(t, users[0].MFAEnabled)
		attributes, err := store.LoadUserAttributes(t.Context(), users[0].Sub)
		require.NoError(t, err)
		require.Equal(t, "preserved", attributes["custom:owner"])
		key, err := store.LoadSigningKey(t.Context(), "legacy-pool")
		require.NoError(t, err)
		require.Equal(t, "durable-kid", key.Kid)
		require.Equal(t, private.N, key.Private.N)
		require.Equal(t, dataBefore[4], readBootstrapRows(t, store.db, dataQueries[4]), "migration and reopen must preserve exact signing-key bytes")
		jwks, err := store.BuildJWKS(t.Context(), "legacy-pool")
		require.NoError(t, err)
		require.Contains(t, string(jwks), "durable-kid")
		var verificationTable string
		require.NoError(t, store.db.QueryRowContext(t.Context(), `SELECT name FROM sqlite_master WHERE type='table' AND name='verification_codes'`).Scan(&verificationTable))
		if attempt == 0 {
			poolBefore, clientBefore, usersBefore = pool, client, users
			migratedSchema = readBootstrapRows(t, store.db, schemaQuery)
		} else {
			require.Equal(t, poolBefore, pool)
			require.Equal(t, clientBefore, client)
			require.Equal(t, usersBefore, users, "idempotent reopen must preserve migrated lifecycle state")
			require.Equal(t, migratedSchema, readBootstrapRows(t, store.db, schemaQuery))
		}
		require.NoError(t, store.Close())
	}
}
