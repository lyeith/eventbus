// SQLite owns all pools, app clients, users, attributes, signing keys and challenges.
// The pure-Go driver keeps release builds CGO-free; WAL and a signing-key mutex
// coordinate requests within the single emulator process.
package cognito

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const (
	// DefaultCognitoDBPath is the default on-disk location for the dev Cognito DB.
	DefaultCognitoDBPath = "/tmp/cognito-dev.db"
)

// CognitoStore is the SQLite-backed store for the local Cognito dev service.
type CognitoStore struct {
	db *sql.DB
	// mu serialises read-modify-write paths (e.g. ensure-signing-key).
	// Plain reads/writes go through the WAL-enabled connection pool directly.
	mu sync.Mutex
}

// OpenCognitoStore opens (or creates) the SQLite database at path and
// bootstraps the schema. Caller must Close the store to release the file lock.
func OpenCognitoStore(path string) (*CognitoStore, error) {
	if path == "" {
		path = DefaultCognitoDBPath
	}
	// modernc.org/sqlite uses driver name "sqlite". Enable WAL + foreign keys
	// + busy timeout so concurrent goroutines don't trip "database is locked".
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open cognito sqlite at %q: %w", path, err)
	}
	// Single writer is enough; SQLite serialises writes anyway.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	store := &CognitoStore{db: db}
	if err := store.bootstrap(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("bootstrap cognito schema: %w", err)
	}
	return store, nil
}

// Close releases the underlying SQL handle.
func (s *CognitoStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// DB exposes the underlying handle for tests and lower-level callers.
func (s *CognitoStore) DB() *sql.DB {
	return s.db
}

// bootstrap creates the current schema and adds columns missing from existing stores.
//
// challenge_sessions backs the InitiateAuth → RespondToAuthChallenge round-
// trip. Each row is short-lived (5min default TTL) and is
// either marked `used=1` after a successful response (replay rejection) or
// reaped by the application's owned cleanup worker.
func (s *CognitoStore) bootstrap() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS pools (
			id TEXT PRIMARY KEY,
			region TEXT NOT NULL,
			password_policy TEXT,
			created_at INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS clients (
			id TEXT PRIMARY KEY,
			pool_id TEXT NOT NULL,
			secret TEXT NOT NULL DEFAULT '',
			created_at INTEGER NOT NULL,
			FOREIGN KEY (pool_id) REFERENCES pools(id) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS users (
			sub TEXT PRIMARY KEY,
			pool_id TEXT NOT NULL,
			email TEXT NOT NULL,
			password_hash TEXT NOT NULL DEFAULT '',
			mfa_enabled INTEGER NOT NULL DEFAULT 0,
			mfa_secret TEXT NOT NULL DEFAULT '',
			totp_secret TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL DEFAULT 'CONFIRMED',
			created_at INTEGER NOT NULL,
			FOREIGN KEY (pool_id) REFERENCES pools(id) ON DELETE CASCADE
		)`,
		`CREATE INDEX IF NOT EXISTS idx_users_pool_email ON users(pool_id, email)`,
		`CREATE TABLE IF NOT EXISTS user_attributes (
			sub TEXT NOT NULL,
			name TEXT NOT NULL,
			value TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (sub, name),
			FOREIGN KEY (sub) REFERENCES users(sub) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS signing_keys (
			pool_id TEXT PRIMARY KEY,
			kid TEXT NOT NULL,
			private_pem TEXT NOT NULL,
			public_pem TEXT NOT NULL,
			created_at INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS challenge_sessions (
			session TEXT PRIMARY KEY,
			sub TEXT NOT NULL,
			pool_id TEXT NOT NULL,
			client_id TEXT NOT NULL,
			challenge_name TEXT NOT NULL,
			used INTEGER NOT NULL DEFAULT 0,
			created_at INTEGER NOT NULL,
			expires_at INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_challenge_sessions_expires_at
			ON challenge_sessions(expires_at)`,
		// RevokeToken: one refresh token (by jti) and every access token
		// minted from it (their origin_jti) stop working, as in Cognito.
		`CREATE TABLE IF NOT EXISTS revoked_refresh_tokens (
			jti TEXT PRIMARY KEY,
			revoked_at INTEGER NOT NULL
		)`,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exec %q: %w", firstLine(stmt), err)
		}
	}

	// Existing fixture stores survive restarts; CREATE TABLE IF NOT EXISTS above
	// is a no-op on pre-existing schemas, so any new column must be added
	// via ALTER TABLE. We swallow "duplicate column" errors so the migration
	// is idempotent when re-run on a fresh-from-CREATE-TABLE schema.
	migrations := []string{
		`ALTER TABLE pools ADD COLUMN password_policy TEXT`,
		`ALTER TABLE users ADD COLUMN totp_secret TEXT NOT NULL DEFAULT ''`,
		// Software-token enrolment (AssociateSoftwareToken → VerifySoftwareToken
		// → SetUserMFAPreference) and global sign-out.
		`ALTER TABLE users ADD COLUMN totp_pending_secret TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE users ADD COLUMN software_token_verified INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE users ADD COLUMN tokens_revoked_before INTEGER NOT NULL DEFAULT 0`,
		// ChangePassword's attempt limit (cognito_password.go).
		`ALTER TABLE users ADD COLUMN password_failures INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE users ADD COLUMN password_locked_until INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE pools ADD COLUMN sign_in_config TEXT NOT NULL DEFAULT '{}'`,
		`ALTER TABLE clients ADD COLUMN explicit_auth_flows TEXT`,
		`ALTER TABLE clients ADD COLUMN auth_session_validity INTEGER NOT NULL DEFAULT 3`,
		`ALTER TABLE users ADD COLUMN username TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE users ADD COLUMN username_key TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE users ADD COLUMN enabled INTEGER NOT NULL DEFAULT 1`,
		`ALTER TABLE users ADD COLUMN updated_at INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE users ADD COLUMN srp_salt TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE users ADD COLUMN srp_verifier TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE users ADD COLUMN password_changed_at INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE users ADD COLUMN auth_version INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE challenge_sessions ADD COLUMN state_json TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE challenge_sessions ADD COLUMN auth_version INTEGER NOT NULL DEFAULT 0`,
	}
	for _, stmt := range migrations {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			// modernc.org/sqlite reports duplicate columns via the message
			// "duplicate column name". Tolerate that, fail anything else.
			if !strings.Contains(err.Error(), "duplicate column name") {
				return fmt.Errorf("migrate %q: %w", firstLine(stmt), err)
			}
		}
	}

	return s.migrateUserIdentity(ctx)
}

// --- Pool / Client CRUD --------------------------------------------------

// UpsertPool inserts the pool if missing, leaving an existing row alone.
func (s *CognitoStore) UpsertPool(ctx context.Context, id, region string) error {
	if id == "" {
		return errors.New("pool id required")
	}
	if region == "" {
		region = "us-east-1"
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO pools (id, region, created_at)
		VALUES (?, ?, ?)
		ON CONFLICT(id) DO NOTHING
	`, id, region, time.Now().Unix())
	return err
}

// SetPoolPasswordPolicy stores the JSON-encoded password policy for a pool.
// Pass empty string to clear (pool then accepts any non-empty password).
func (s *CognitoStore) SetPoolPasswordPolicy(ctx context.Context, poolID, policyJSON string) error {
	if poolID == "" {
		return errors.New("pool id required")
	}
	var arg interface{}
	if policyJSON == "" {
		arg = nil
	} else {
		arg = policyJSON
	}
	_, err := s.db.ExecContext(ctx, `UPDATE pools SET password_policy = ? WHERE id = ?`, arg, poolID)
	return err
}

// GetPoolPasswordPolicy returns the JSON-encoded policy for poolID, or "" if
// none is set. Returns sql.ErrNoRows if the pool doesn't exist.
func (s *CognitoStore) GetPoolPasswordPolicy(ctx context.Context, poolID string) (string, error) {
	var policy sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT password_policy FROM pools WHERE id = ?`, poolID).Scan(&policy)
	if err != nil {
		return "", err
	}
	if !policy.Valid {
		return "", nil
	}
	return policy.String, nil
}

// DeletePool cascades-deletes the pool and all child rows (clients, users,
// user_attributes, signing_keys, challenge_sessions). Idempotent: missing
// pool returns (false, nil). Wrapped in a transaction so a partial failure
// doesn't leave an inconsistent state.
//
// Note: FK ON DELETE CASCADE handles users → user_attributes and
// pools → clients and pools → users automatically. challenge_sessions and
// signing_keys are NOT FK-bound (intentional — they're maintained
// independently for ergonomics), so this helper deletes them explicitly.
func (s *CognitoStore) DeletePool(ctx context.Context, poolID string) (bool, error) {
	if poolID == "" {
		return false, errors.New("pool id required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	// Delete non-cascading children first.
	if _, err := tx.ExecContext(ctx, `DELETE FROM challenge_sessions WHERE pool_id = ?`, poolID); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM signing_keys WHERE pool_id = ?`, poolID); err != nil {
		return false, err
	}
	res, err := tx.ExecContext(ctx, `DELETE FROM pools WHERE id = ?`, poolID)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	committed = true
	return n > 0, nil
}

// DeleteClient removes the client row keyed on (pool_id, id). Idempotent —
// returns (false, nil) when no row matches. Used by DeleteUserPoolClient.
func (s *CognitoStore) DeleteClient(ctx context.Context, poolID, clientID string) (bool, error) {
	if poolID == "" || clientID == "" {
		return false, errors.New("pool id and client id required")
	}
	res, err := s.db.ExecContext(ctx, `DELETE FROM clients WHERE pool_id = ? AND id = ?`, poolID, clientID)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// PoolExists reports whether a pool with the given id is present.
func (s *CognitoStore) PoolExists(ctx context.Context, id string) (bool, error) {
	var n int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pools WHERE id = ?`, id).Scan(&n); err != nil {
		return false, err
	}
	return n > 0, nil
}

var errClientPoolConflict = errors.New("client ID already belongs to another user pool")

// UpsertClient inserts or updates the secret for a client in its owning pool.
// A conflicting pool never changes the existing client's secret or configuration.
func (s *CognitoStore) UpsertClient(ctx context.Context, id, poolID, secret string) error {
	if id == "" || poolID == "" {
		return errors.New("client id and pool id required")
	}
	result, err := s.db.ExecContext(ctx, `
		INSERT INTO clients (id, pool_id, secret, created_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET secret = excluded.secret
		WHERE clients.pool_id = excluded.pool_id
	`, id, poolID, secret, time.Now().Unix())
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return errClientPoolConflict
	}
	return nil
}

// CognitoClient is a thin row representation of the clients table.
type CognitoClient struct {
	ID                  string
	PoolID              string
	Secret              string
	ExplicitAuthFlows   []string
	AuthSessionValidity int
}

// LookupClient returns the client row keyed on id. Returns sql.ErrNoRows if
// no such client exists. Used by InitiateAuth to resolve `ClientId` → pool.
func (s *CognitoStore) LookupClient(ctx context.Context, id string) (*CognitoClient, error) {
	var c CognitoClient
	var flows sql.NullString
	err := s.db.QueryRowContext(ctx, `
		SELECT id, pool_id, secret,explicit_auth_flows,auth_session_validity FROM clients WHERE id = ?
	`, id).Scan(&c.ID, &c.PoolID, &c.Secret, &flows, &c.AuthSessionValidity)
	if err != nil {
		return nil, err
	}
	if flows.Valid {
		if err := json.Unmarshal([]byte(flows.String), &c.ExplicitAuthFlows); err != nil {
			return nil, err
		}
	}
	return &c, nil
}

// --- User CRUD -----------------------------------------------------------

// CognitoUser is a thin row representation; richer attribute access uses
// LoadUserAttributes.
type CognitoUser struct {
	Sub               string
	PoolID            string
	Username          string
	Email             string
	Enabled           bool
	UpdatedAt         int64
	SRPSalt           string
	SRPVerifier       string
	PasswordChangedAt int64
	AuthVersion       int64
	PasswordHash      string
	MFAEnabled        bool
	TOTPSecret        string
	Status            string
	CreatedAt         int64
	// PendingTOTPSecret is the secret AssociateSoftwareToken issued and
	// VerifySoftwareToken has not yet confirmed.
	PendingTOTPSecret string
	// SoftwareTokenVerified is true once a software token was verified; only
	// then may SetUserMFAPreference turn software-token MFA on.
	SoftwareTokenVerified bool
	// TokensRevokedBefore (unix seconds) is the latest global sign-out: a
	// token whose authentication is at or before it is revoked.
	TokensRevokedBefore int64
	// PasswordFailures counts ChangePassword calls with a wrong previous
	// password since the last change or lockout.
	PasswordFailures int
	// PasswordLockedUntil (unix seconds): ChangePassword is refused with
	// LimitExceededException until then.
	PasswordLockedUntil int64
}

// userColumns is the column list scanUserRow reads, in order.
const userColumns = `sub, pool_id, email, password_hash, mfa_enabled, totp_secret, status, created_at,
	totp_pending_secret, software_token_verified, tokens_revoked_before, password_failures, password_locked_until,
	username, enabled, updated_at, srp_salt, srp_verifier, password_changed_at, auth_version`

// UpsertUser is the legacy hash-only seed/test helper. It retains username=email
// and confirms new users; plaintext callers should use UpsertSeedUser instead.
func (s *CognitoStore) UpsertUser(ctx context.Context, poolID, email, passwordHash string, mfaEnabled bool) (string, error) {
	if poolID == "" || email == "" {
		return "", errors.New("pool id and email required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	config, err := s.GetPoolSignInConfig(ctx, poolID)
	if err != nil {
		return "", err
	}
	username := normalizeSignIn(email, config)
	var sub string
	err = s.db.QueryRowContext(ctx, `SELECT sub FROM users WHERE pool_id = ? AND username_key = ?`, poolID, username).Scan(&sub)
	if errors.Is(err, sql.ErrNoRows) {
		sub = newOpaqueID()
		now := time.Now().Unix()
		_, err = s.db.ExecContext(ctx, `INSERT INTO users
			(sub,pool_id,username,username_key,email,password_hash,mfa_enabled,status,created_at,updated_at,password_changed_at)
			VALUES (?,?,?,?,?,?,?,'CONFIRMED',?,?,?)`, sub, poolID, username, username, email, passwordHash, boolToInt(mfaEnabled), now, now, now)
		return sub, err
	}
	if err != nil {
		return "", err
	}
	_, err = s.db.ExecContext(ctx, `UPDATE users SET password_hash=?,srp_salt='',srp_verifier='',mfa_enabled=?,updated_at=? WHERE sub=?`, passwordHash, boolToInt(mfaEnabled), time.Now().Unix(), sub)
	return sub, err
}

// LookupUserByEmail returns the user row keyed on (pool_id, email).
// Returns sql.ErrNoRows if no such user exists.
func (s *CognitoStore) LookupUserByEmail(ctx context.Context, poolID, email string) (*CognitoUser, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT `+userColumns+`
		FROM users WHERE pool_id = ? AND email = ?
	`, poolID, email)
	return scanUserRow(row)
}

// LookupUserBySub returns the user row keyed on sub.
// Returns sql.ErrNoRows if no such user exists.
func (s *CognitoStore) LookupUserBySub(ctx context.Context, sub string) (*CognitoUser, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT `+userColumns+`
		FROM users WHERE sub = ?
	`, sub)
	return scanUserRow(row)
}

// SetPendingTOTPSecret records the secret AssociateSoftwareToken issued.
func (s *CognitoStore) SetPendingTOTPSecret(ctx context.Context, sub, secret string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE users SET totp_pending_secret = ? WHERE sub = ?`, secret, sub)
	return err
}

// ConfirmPendingTOTPSecret makes the pending secret the user's software
// token, as a successful VerifySoftwareToken does.
func (s *CognitoStore) ConfirmPendingTOTPSecret(ctx context.Context, sub string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE users SET totp_secret = totp_pending_secret, totp_pending_secret = '',
			software_token_verified = 1
		WHERE sub = ? AND totp_pending_secret != ''
	`, sub)
	return err
}

// SetMFAEnabled records the user's software-token MFA preference.
func (s *CognitoStore) SetMFAEnabled(ctx context.Context, sub string, enabled bool) error {
	_, err := s.db.ExecContext(ctx, `UPDATE users SET mfa_enabled = ? WHERE sub = ?`, boolToInt(enabled), sub)
	return err
}

// RevokeUserTokens advances the grant revision atomically with invalidating
// challenges. The seconds cutoff remains monotonic for legacy grants.
func (s *CognitoStore) RevokeUserTokens(ctx context.Context, sub string, at int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `UPDATE users SET auth_version=auth_version+1,
		tokens_revoked_before=MAX(tokens_revoked_before,?) WHERE sub=?`, at, sub); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM challenge_sessions WHERE sub=?`, sub); err != nil {
		return err
	}
	return tx.Commit()
}

// ChangeUserPassword stores a new password hash, as a successful
// ChangePassword does, and clears the attempt limit. It revokes nothing:
// Cognito keeps the user's tokens valid across a password change.
func (s *CognitoStore) ChangeUserPassword(ctx context.Context, sub, passwordHash string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE users SET password_hash = ?, password_failures = 0, password_locked_until = 0 WHERE sub = ?
	`, passwordHash, sub)
	return err
}

// RecordPasswordFailure counts one ChangePassword call with a wrong previous
// password at now. The limit-th failure locks ChangePassword for lockout and
// starts the count again; a lock that has passed starts it again too.
func (s *CognitoStore) RecordPasswordFailure(ctx context.Context, sub string, now time.Time, limit int, lockout time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	user, err := s.LookupUserBySub(ctx, sub)
	if err != nil {
		return err
	}
	failures, lockedUntil := user.PasswordFailures, user.PasswordLockedUntil
	if lockedUntil != 0 && lockedUntil <= now.Unix() {
		failures, lockedUntil = 0, 0
	}
	failures++
	if failures >= limit {
		failures = 0
		lockedUntil = now.Add(lockout).Unix()
	}
	_, err = s.db.ExecContext(ctx, `
		UPDATE users SET password_failures = ?, password_locked_until = ? WHERE sub = ?
	`, failures, lockedUntil, sub)
	return err
}

// RevokeRefreshToken records one revoked refresh token by jti. Idempotent.
func (s *CognitoStore) RevokeRefreshToken(ctx context.Context, jti string) error {
	if jti == "" {
		return errors.New("jti required")
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO revoked_refresh_tokens (jti, revoked_at) VALUES (?, ?)
		ON CONFLICT(jti) DO NOTHING
	`, jti, time.Now().Unix())
	return err
}

// RefreshTokenRevoked reports whether RevokeToken revoked the refresh token
// with this jti.
func (s *CognitoStore) RefreshTokenRevoked(ctx context.Context, jti string) (bool, error) {
	if jti == "" {
		return false, nil
	}
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM revoked_refresh_tokens WHERE jti = ?`, jti).Scan(&n)
	return n > 0, err
}

// SetUserTOTPSecret writes a base32-encoded TOTP secret to the users row.
// Pass empty string to clear (user then falls back to "any 6 digits" path).
func (s *CognitoStore) SetUserTOTPSecret(ctx context.Context, sub, secret string) error {
	if sub == "" {
		return errors.New("sub required")
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE users SET totp_secret = ?, software_token_verified = ? WHERE sub = ?
	`, secret, boolToInt(secret != ""), sub)
	return err
}

// CreateUser is the legacy hash-only test helper with username=email identity.
// New HTTP operations and seeds use plaintext provisioning for SRP credentials.
func (s *CognitoStore) CreateUser(ctx context.Context, sub, poolID, email, passwordHash string, mfaEnabled bool) error {
	if sub == "" || poolID == "" || email == "" {
		return errors.New("sub, pool id, and email required")
	}
	config, err := s.GetPoolSignInConfig(ctx, poolID)
	if err != nil {
		return err
	}
	username := normalizeSignIn(email, config)
	now := time.Now().Unix()
	_, err = s.db.ExecContext(ctx, `INSERT INTO users
		(sub,pool_id,username,username_key,email,password_hash,mfa_enabled,status,created_at,updated_at,password_changed_at)
		VALUES (?,?,?,?,?,?,?,'CONFIRMED',?,?,?)`, sub, poolID, username, username, email, passwordHash, boolToInt(mfaEnabled), now, now, now)
	return err
}

// DeleteUserByEmail removes the user (and via FK cascade, their attributes)
// with the given (pool_id, email). Returns (false, nil) when no row matched —
// AdminDeleteUser is idempotent on missing user (design §5d, cognito.py:240-243,
// cognito.go:691-697).
func (s *CognitoStore) DeleteUserByEmail(ctx context.Context, poolID, email string) (bool, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM users WHERE pool_id = ? AND email = ?`, poolID, email)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// scanUserRow is shared between LookupUserByEmail / LookupUserBySub.
func scanUserRow(row interface{ Scan(...interface{}) error }) (*CognitoUser, error) {
	var u CognitoUser
	var mfa, verified, enabled int
	if err := row.Scan(
		&u.Sub, &u.PoolID, &u.Email, &u.PasswordHash, &mfa, &u.TOTPSecret, &u.Status, &u.CreatedAt,
		&u.PendingTOTPSecret, &verified, &u.TokensRevokedBefore, &u.PasswordFailures, &u.PasswordLockedUntil,
		&u.Username, &enabled, &u.UpdatedAt, &u.SRPSalt, &u.SRPVerifier, &u.PasswordChangedAt, &u.AuthVersion,
	); err != nil {
		return nil, err
	}
	u.Enabled = enabled != 0
	u.MFAEnabled = mfa != 0
	u.SoftwareTokenVerified = verified != 0
	return &u, nil
}

// SetUserAttribute keeps the profile and searchable email projection together.
// Reapplying an unchanged fixture attribute preserves modification timestamps.
func (s *CognitoStore) SetUserAttribute(ctx context.Context, sub, name, value string) error {
	if name == "" {
		return errors.New("attribute name required")
	}
	if name == "sub" {
		if sub != value {
			return errors.New("sub is immutable")
		}
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	user, err := s.LookupUserBySub(ctx, sub)
	if err != nil {
		return err
	}
	config, err := s.GetPoolSignInConfig(ctx, user.PoolID)
	if err != nil {
		return err
	}
	if name == "email" {
		value = normalizeSignIn(value, config)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if name == "email" || name == "email_verified" {
		email := user.Email
		if name == "email" {
			email = value
		}
		var verified string
		err = tx.QueryRowContext(ctx, `SELECT value FROM user_attributes WHERE sub=? AND name='email_verified'`, sub).Scan(&verified)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if name == "email_verified" {
			verified = value
		}
		if email != "" && (config.EmailAsUsername || config.EmailAlias && verified == "true") {
			var count int
			query := `SELECT COUNT(*) FROM users WHERE pool_id=? AND sub!=? AND email=? COLLATE `
			if config.CaseSensitive {
				query += "BINARY"
			} else {
				query += "NOCASE"
			}
			if config.EmailAlias {
				query += ` AND EXISTS(SELECT 1 FROM user_attributes a WHERE a.sub=users.sub AND a.name='email_verified' AND a.value='true')`
			}
			if err = tx.QueryRowContext(ctx, query, user.PoolID, sub, email).Scan(&count); err != nil {
				return err
			}
			if count != 0 {
				return errEmailAliasExists
			}
		}
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO user_attributes(sub,name,value) VALUES(?,?,?) ON CONFLICT(sub,name)
		DO UPDATE SET value=excluded.value WHERE user_attributes.value!=excluded.value`, sub, name, value)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if name == "email" {
		if _, err = tx.ExecContext(ctx, `UPDATE users SET email=? WHERE sub=?`, value, sub); err != nil {
			return err
		}
	}
	if changed != 0 {
		if _, err = tx.ExecContext(ctx, `UPDATE users SET updated_at=MAX(updated_at,?) WHERE sub=?`, time.Now().Unix(), sub); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// LoadUserAttributes returns the attribute map for a user.
func (s *CognitoStore) LoadUserAttributes(ctx context.Context, sub string) (map[string]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT name, value FROM user_attributes WHERE sub = ? AND name != 'sub'
		UNION ALL SELECT 'sub', sub FROM users WHERE sub = ?`, sub, sub)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		out[k] = v
	}
	return out, rows.Err()
}

// --- Challenge sessions  -----------------------------------

// CognitoChallengeSession is a thin row representation of the
// challenge_sessions table.
type CognitoChallengeSession struct {
	Session       string
	Sub           string
	PoolID        string
	ClientID      string
	ChallengeName string
	Used          bool
	CreatedAt     int64
	ExpiresAt     int64
	StateJSON     string
	AuthVersion   int64
}

// CreateChallengeSession inserts a new challenge_sessions row. The opaque
// session id is generated by the caller (see EncodeChallengeSession in
// cognito_session.go) so the HMAC stamp is bound to the pool's signing key.
//
// `ttl` is the wall-clock validity window; rows are reaped by the 60s
// cleanup goroutine after expires_at < now.
func (s *CognitoStore) CreateChallengeSession(ctx context.Context, sessionID, sub, poolID, clientID, challengeName string, ttl time.Duration) error {
	return s.CreateChallengeSessionWithState(ctx, sessionID, sub, poolID, clientID, challengeName, ttl, "")
}

// CreateChallengeSessionWithState atomically binds trigger history and the
// enabled user's current grant revision to the opaque challenge session.
func (s *CognitoStore) CreateChallengeSessionWithState(ctx context.Context, sessionID, sub, poolID, clientID, challengeName string, ttl time.Duration, stateJSON string) error {
	if sessionID == "" || sub == "" || poolID == "" || clientID == "" || challengeName == "" {
		return errors.New("session,sub,pool,client and challenge required")
	}
	if stateJSON != "" && !json.Valid([]byte(stateJSON)) {
		return errors.New("session state must be JSON")
	}
	now := time.Now().Unix()
	result, err := s.db.ExecContext(ctx, `INSERT INTO challenge_sessions
		(session,sub,pool_id,client_id,challenge_name,used,created_at,expires_at,state_json,auth_version)
		SELECT ?,sub,pool_id,?,?,0,?,?,?,auth_version FROM users WHERE sub=? AND pool_id=? AND enabled=1`, sessionID, clientID, challengeName, now, now+int64(ttl.Seconds()), stateJSON, sub, poolID)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		user, err := s.LookupUserBySub(ctx, sub)
		if err != nil {
			return err
		}
		if user.PoolID != poolID {
			return sql.ErrNoRows
		}
		return errUserDisabled
	}
	return nil
}

// LookupChallengeSession returns the row for sessionID. Returns sql.ErrNoRows
// if no such row exists.
func (s *CognitoStore) LookupChallengeSession(ctx context.Context, sessionID string) (*CognitoChallengeSession, error) {
	var (
		row  CognitoChallengeSession
		used int
	)
	err := s.db.QueryRowContext(ctx, `
		SELECT session, sub, pool_id, client_id, challenge_name, used, created_at, expires_at, state_json,auth_version
		FROM challenge_sessions WHERE session = ?
	`, sessionID).Scan(
		&row.Session, &row.Sub, &row.PoolID, &row.ClientID,
		&row.ChallengeName, &used, &row.CreatedAt, &row.ExpiresAt, &row.StateJSON, &row.AuthVersion,
	)
	if err != nil {
		return nil, err
	}
	row.Used = used != 0
	return &row, nil
}

// MarkChallengeSessionUsed flips `used=1`. Idempotent: re-marking an already-
// used row is a no-op (UPDATE with no rows affected returns nil).
func (s *CognitoStore) MarkChallengeSessionUsed(ctx context.Context, sessionID string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE challenge_sessions SET used = 1 WHERE session = ?`, sessionID)
	return err
}

// DeleteExpiredChallengeSessions reaps rows older than `now`. Called every
// 60s by the cleanup goroutine in main.go. Returns the deletion count.
func (s *CognitoStore) DeleteExpiredChallengeSessions(ctx context.Context, now int64) (int, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM challenge_sessions WHERE expires_at < ?`, now)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int(n), nil
}

// --- Helpers -------------------------------------------------------------

// newOpaqueID returns 32 hex chars from crypto/rand. Cognito sub is normally
// a UUID; keeping it 32 hex chars stays comfortably within consumer parsers
// that tolerate any opaque string.
func newOpaqueID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func firstLine(s string) string {
	for i, r := range s {
		if r == '\n' {
			return s[:i]
		}
	}
	return s
}
