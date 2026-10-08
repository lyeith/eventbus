package cognito

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	errUsernameExists            = errors.New("username already exists")
	errEmailAliasExists          = errors.New("email alias already exists")
	errUserDisabled              = errors.New("user is disabled")
	errPoolSignInConfigImmutable = errors.New("sign-in configuration cannot change after user creation")
)

// PoolSignInConfig selects username sign-in, verified-email aliases, or email
// sign-in with generated canonical usernames. Email modes are mutually exclusive.
type PoolSignInConfig struct {
	EmailAsUsername bool `json:"email_as_username"`
	EmailAlias      bool `json:"email_alias"`
	CaseSensitive   bool `json:"case_sensitive"`
}

func normalizeSignIn(value string, config PoolSignInConfig) string {
	if !config.CaseSensitive {
		return strings.ToLower(value)
	}
	return value
}

func (s *CognitoStore) GetPoolSignInConfig(ctx context.Context, poolID string) (PoolSignInConfig, error) {
	config := PoolSignInConfig{CaseSensitive: true}
	var raw string
	if err := s.db.QueryRowContext(ctx, `SELECT sign_in_config FROM pools WHERE id=?`, poolID).Scan(&raw); err != nil {
		return config, err
	}
	err := json.Unmarshal([]byte(raw), &config)
	return config, err
}

// SetPoolSignInConfig cannot change populated pools' identity rules. AWS fixes
// these rules at pool creation; fixture reapplication of the same rules is safe.
func (s *CognitoStore) SetPoolSignInConfig(ctx context.Context, poolID string, config PoolSignInConfig) error {
	if config.EmailAsUsername && config.EmailAlias {
		return errors.New("email username and email alias modes are mutually exclusive")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	previous, err := s.GetPoolSignInConfig(ctx, poolID)
	if err != nil {
		return err
	}
	if previous == config {
		return nil
	}
	var count int
	if err = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM users WHERE pool_id=?`, poolID).Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return errPoolSignInConfigImmutable
	}
	raw, err := json.Marshal(config)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `UPDATE pools SET sign_in_config=? WHERE id=?`, string(raw), poolID)
	return err
}

// migrateUserIdentity participates in bootstrap's transaction. Startup owns
// commit and rollback so identity repair cannot leave a partial schema behind.
func (s *CognitoStore) migrateUserIdentity(ctx context.Context, tx *sql.Tx) error {
	for _, statement := range []string{
		`UPDATE users SET username=email WHERE username=''`,
		`UPDATE users SET username_key=username WHERE username_key=''`,
		`UPDATE users SET updated_at=created_at WHERE updated_at=0`,
		`UPDATE users SET password_changed_at=created_at WHERE password_changed_at=0 AND password_hash!=''`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_users_pool_username ON users(pool_id,username_key)`,
	} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("migrate user identity: %w", err)
		}
	}
	return nil
}

func (s *CognitoStore) lookupUserByUsername(ctx context.Context, poolID, username string, config PoolSignInConfig) (*CognitoUser, error) {
	return scanUserRow(s.db.QueryRowContext(ctx, `SELECT `+userColumns+` FROM users WHERE pool_id=? AND username_key=?`, poolID, normalizeSignIn(username, config)))
}

func (s *CognitoStore) lookupSignInEmail(ctx context.Context, poolID, email string, config PoolSignInConfig) (*CognitoUser, error) {
	collation := "BINARY"
	if !config.CaseSensitive {
		collation = "NOCASE"
	}
	query := `SELECT ` + userColumns + ` FROM users WHERE pool_id=? AND email=? COLLATE ` + collation
	if config.EmailAlias {
		query += ` AND EXISTS(SELECT 1 FROM user_attributes a WHERE a.sub=users.sub AND a.name='email_verified' AND a.value='true')`
	}
	return scanUserRow(s.db.QueryRowContext(ctx, query, poolID, email))
}

// LookupPoolUser resolves admin identifiers only within the requested pool.
func (s *CognitoStore) LookupPoolUser(ctx context.Context, poolID, identifier string) (*CognitoUser, error) {
	config, err := s.GetPoolSignInConfig(ctx, poolID)
	if err != nil {
		return nil, err
	}
	user, err := scanUserRow(s.db.QueryRowContext(ctx, `SELECT `+userColumns+` FROM users WHERE pool_id=? AND sub=?`, poolID, identifier))
	if err == nil {
		return user, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	user, err = s.lookupUserByUsername(ctx, poolID, identifier, config)
	if err == nil || !errors.Is(err, sql.ErrNoRows) {
		return user, err
	}
	if config.EmailAlias || config.EmailAsUsername {
		return s.lookupSignInEmail(ctx, poolID, identifier, config)
	}
	return nil, sql.ErrNoRows
}

// ResolveSignInUser accepts exactly the configured sign-in attributes; a sub
// is accepted here only when it is itself the canonical username.
func (s *CognitoStore) ResolveSignInUser(ctx context.Context, poolID, login string) (*CognitoUser, error) {
	config, err := s.GetPoolSignInConfig(ctx, poolID)
	if err != nil {
		return nil, err
	}
	if config.EmailAsUsername {
		return s.lookupSignInEmail(ctx, poolID, login, config)
	}
	user, err := s.lookupUserByUsername(ctx, poolID, login, config)
	if err == nil || !errors.Is(err, sql.ErrNoRows) {
		return user, err
	}
	if config.EmailAlias {
		return s.lookupSignInEmail(ctx, poolID, login, config)
	}
	return nil, sql.ErrNoRows
}

// CreateUserIdentity stores the immutable username/sub and all initial
// attributes together with bcrypt and SRP credentials in one transaction.
func (s *CognitoStore) CreateUserIdentity(ctx context.Context, poolID, username, email, plaintext, status string, attributes map[string]string) (*CognitoUser, error) {
	if poolID == "" || username == "" || plaintext == "" {
		return nil, errors.New("pool, username and password required")
	}
	if status != "CONFIRMED" && status != "FORCE_CHANGE_PASSWORD" && status != "UNCONFIRMED" {
		return nil, errors.New("unsupported user status")
	}
	config, err := s.GetPoolSignInConfig(ctx, poolID)
	if err != nil {
		return nil, err
	}
	username = normalizeSignIn(username, config)
	email = normalizeSignIn(email, config)
	sub := newULIDSub()
	if config.EmailAsUsername {
		username = sub
	}
	hash, err := hashUserPassword(plaintext)
	if err != nil {
		return nil, err
	}
	salt, verifier, err := makeSRPCredentials(poolID, username, plaintext)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if config.EmailAsUsername || config.EmailAlias && attributes["email_verified"] == "true" {
		var count int
		query := `SELECT COUNT(*) FROM users WHERE pool_id=? AND email=? COLLATE `
		if config.CaseSensitive {
			query += "BINARY"
		} else {
			query += "NOCASE"
		}
		if config.EmailAlias {
			query += ` AND EXISTS(SELECT 1 FROM user_attributes a WHERE a.sub=users.sub AND a.name='email_verified' AND a.value='true')`
		}
		if err = tx.QueryRowContext(ctx, query, poolID, email).Scan(&count); err != nil {
			return nil, err
		}
		if count != 0 {
			if config.EmailAsUsername {
				return nil, errUsernameExists
			}
			return nil, errEmailAliasExists
		}
	}
	now := s.now().Unix()
	_, err = tx.ExecContext(ctx, `INSERT INTO users(sub,pool_id,username,username_key,email,password_hash,status,created_at,updated_at,password_changed_at,srp_salt,srp_verifier)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, sub, poolID, username, username, email, hash, status, now, now, now, salt, verifier)
	if err != nil {
		if isUsersConstraintViolation(err) {
			return nil, fmt.Errorf("%w: %v", errUsernameExists, err)
		}
		return nil, err
	}
	for name, value := range attributes {
		if name == "sub" {
			continue
		}
		if name == "email" {
			value = email
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO user_attributes(sub,name,value) VALUES(?,?,?)`, sub, name, value); err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return s.LookupUserBySub(ctx, sub)
}

func (s *CognitoStore) replaceUserPassword(ctx context.Context, sub, plaintext, status string, invalidate bool, expectedVersion *int64) error {
	if plaintext == "" {
		return errors.New("password required")
	}
	if status != "CONFIRMED" && status != "FORCE_CHANGE_PASSWORD" {
		return errors.New("unsupported user status")
	}
	user, err := s.LookupUserBySub(ctx, sub)
	if err != nil {
		return err
	}
	hash, err := hashUserPassword(plaintext)
	if err != nil {
		return err
	}
	salt, verifier, err := makeSRPCredentials(user.PoolID, user.Username, plaintext)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	revision := 0
	if invalidate {
		revision = 1
	}
	query := `UPDATE users SET password_hash=?,srp_salt=?,srp_verifier=?,status=?,updated_at=MAX(updated_at,?),password_changed_at=?,
	password_failures=0,password_locked_until=0,auth_version=auth_version+? WHERE sub=?`
	arguments := []interface{}{hash, salt, verifier, status, s.now().Unix(), s.now().Unix(), revision, sub}
	if expectedVersion != nil {
		query += ` AND enabled=1 AND auth_version=?`
		arguments = append(arguments, *expectedVersion)
	}
	result, err := tx.ExecContext(ctx, query, arguments...)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		if expectedVersion != nil {
			return errTokenRevoked
		}
		return sql.ErrNoRows
	}

	if invalidate {
		if _, err = tx.ExecContext(ctx, `DELETE FROM challenge_sessions WHERE sub=?`, sub); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// SetUserPassword is an administrative credential replacement that invalidates
// outstanding challenge sessions and previously issued emulator grants.
func (s *CognitoStore) SetUserPassword(ctx context.Context, sub, plaintext, status string) error {
	return s.replaceUserPassword(ctx, sub, plaintext, status, true, nil)
}

// ChangeUserPasswordPlaintext preserves grants during self-service changes.
func (s *CognitoStore) ChangeUserPasswordPlaintext(ctx context.Context, sub, plaintext string) error {
	user, err := s.LookupUserBySub(ctx, sub)
	if err != nil {
		return err
	}
	return s.replaceUserPassword(ctx, sub, plaintext, user.Status, false, nil)
}

func (s *CognitoStore) SetUserEnabled(ctx context.Context, sub string, enabled bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var current int
	if err = tx.QueryRowContext(ctx, `SELECT enabled FROM users WHERE sub=?`, sub).Scan(&current); err != nil {
		return err
	}
	if current == boolToInt(enabled) {
		return nil
	}
	revision := 0
	if !enabled {
		revision = 1
	}
	if _, err = tx.ExecContext(ctx, `UPDATE users SET enabled=?,updated_at=MAX(updated_at,?),auth_version=auth_version+? WHERE sub=?`, boolToInt(enabled), s.now().Unix(), revision, sub); err != nil {
		return err
	}
	if !enabled {
		if _, err = tx.ExecContext(ctx, `DELETE FROM challenge_sessions WHERE sub=?`, sub); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *CognitoStore) DeletePoolUser(ctx context.Context, poolID, identifier string) (bool, error) {
	user, err := s.LookupPoolUser(ctx, poolID, identifier)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `DELETE FROM challenge_sessions WHERE sub=?`, user.Sub); err != nil {
		return false, err
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM users WHERE sub=? AND pool_id=?`, user.Sub, poolID)
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	return count != 0, nil
}

// A cursor is bound to one query and expires after the AWS-documented hour.
// Pool-scoped keyset pagination never repeats a returned identity.
type userListCursor struct {
	Version    int    `json:"v"`
	PoolID     string `json:"pool"`
	Filter     string `json:"filter"`
	Attributes string `json:"attributes"`
	Limit      int    `json:"limit"`
	After      string `json:"after"`
	Expires    int64  `json:"expires"`
}

type UserListOptions struct {
	Filter          string
	Limit           int
	PaginationToken string
	AttributesToGet []string
}

func (s *CognitoStore) ListPoolUsers(ctx context.Context, poolID string, options UserListOptions) ([]*CognitoUser, string, error) {
	limit := options.Limit
	if limit < 0 || limit > 60 {
		return nil, "", &invalidUserListParameter{"Limit must be between 0 and 60"}
	}
	cursor := userListCursor{Version: 1, PoolID: poolID, Filter: options.Filter, Attributes: strings.Join(options.AttributesToGet, "\x00"), Limit: limit, Expires: s.now().Add(time.Hour).Unix()}
	if options.PaginationToken != "" {
		data, err := base64.RawURLEncoding.DecodeString(options.PaginationToken)
		if err != nil {
			return nil, "", &invalidUserListParameter{"invalid pagination token"}
		}
		if err = json.Unmarshal(data, &cursor); err != nil || cursor.Version != 1 || cursor.PoolID != poolID || cursor.Filter != options.Filter || cursor.Attributes != strings.Join(options.AttributesToGet, "\x00") || cursor.Limit != limit || cursor.Expires < s.now().Unix() {
			return nil, "", &invalidUserListParameter{"invalid pagination token"}
		}
	}
	if limit == 0 {
		return []*CognitoUser{}, "", nil
	}
	query := `SELECT ` + userColumns + ` FROM users WHERE pool_id=? AND sub>?`
	args := []interface{}{poolID, cursor.After}
	if options.Filter != "" {
		operator, value, err := parseEmailFilter(options.Filter)
		if err != nil {
			return nil, "", err
		}
		if operator == "=" {
			query += ` AND lower(email)=lower(?)`
			args = append(args, value)
		} else {
			query += ` AND substr(lower(email),1,length(?))=lower(?)`
			args = append(args, value, value)
		}
	}
	query += ` ORDER BY sub LIMIT ?`
	args = append(args, limit+1)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	users := make([]*CognitoUser, 0)
	for rows.Next() {
		user, err := scanUserRow(rows)
		if err != nil {
			return nil, "", err
		}
		users = append(users, user)
	}
	if err = rows.Err(); err != nil {
		return nil, "", err
	}
	if len(users) <= limit {
		return users, "", nil
	}
	users = users[:limit]
	cursor.After = users[len(users)-1].Sub
	data, err := json.Marshal(cursor)
	if err != nil {
		return nil, "", err
	}
	return users, base64.RawURLEncoding.EncodeToString(data), nil
}

// ConsumeChallengeSession rejects expired, replayed, disabled or superseded
// grants atomically; successful concurrent responses can consume only once.
func (s *CognitoStore) ConsumeChallengeSession(ctx context.Context, sessionID string) (bool, error) {
	result, err := s.db.ExecContext(ctx, `UPDATE challenge_sessions SET used=1 WHERE session=? AND used=0 AND expires_at>?
		AND EXISTS(SELECT 1 FROM users WHERE users.sub=challenge_sessions.sub AND users.pool_id=challenge_sessions.pool_id
			AND users.enabled=1 AND users.auth_version=challenge_sessions.auth_version)`, sessionID, s.now().Unix())
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	return count == 1, err
}

func (s *CognitoStore) SetClientAuthConfig(ctx context.Context, clientID string, flows []string, sessionMinutes int) error {
	if sessionMinutes < 3 || sessionMinutes > 15 {
		return errors.New("AuthSessionValidity must be between 3 and 15")
	}
	var raw interface{}
	if flows != nil {
		encoded, err := json.Marshal(flows)
		if err != nil {
			return err
		}
		raw = string(encoded)
	}
	result, err := s.db.ExecContext(ctx, `UPDATE clients SET explicit_auth_flows=?,auth_session_validity=? WHERE id=?`, raw, sessionMinutes, clientID)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (s *CognitoStore) GetPoolRegion(ctx context.Context, poolID string) (string, error) {
	var region string
	err := s.db.QueryRowContext(ctx, `SELECT region FROM pools WHERE id=?`, poolID).Scan(&region)
	return region, err
}

// ChangeUserPasswordAtVersion rejects a proof made stale by a concurrent
// administrator reset or disable, while preserving valid current grants.
func (s *CognitoStore) ChangeUserPasswordAtVersion(ctx context.Context, sub, plaintext string, expectedVersion int64) error {
	user, err := s.LookupUserBySub(ctx, sub)
	if err != nil {
		return err
	}
	return s.replaceUserPassword(ctx, sub, plaintext, user.Status, false, &expectedVersion)
}

// SetUserPasswordAtVersion completes a temporary-password challenge only while
// its authenticated user revision remains current and enabled.
func (s *CognitoStore) SetUserPasswordAtVersion(ctx context.Context, sub, plaintext, status string, expectedVersion int64) error {
	return s.replaceUserPassword(ctx, sub, plaintext, status, true, &expectedVersion)
}
