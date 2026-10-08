package cognito

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"time"
)

// Workflow codes are persisted independently of sign-in challenge sessions.
// Each replaces its predecessor for one user/purpose and binds the identity,
// destination and grant revision. Only a digest is stored in the identity DB.
// bootstrapVerification shares startup's atomic schema transaction.
func (s *CognitoStore) bootstrapVerification(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS verification_codes (
  sub TEXT NOT NULL,purpose TEXT NOT NULL,attribute_name TEXT NOT NULL,destination TEXT NOT NULL,
  code_salt TEXT NOT NULL,code_hash TEXT NOT NULL,expires_at INTEGER NOT NULL,
  auth_version INTEGER NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(sub,purpose),FOREIGN KEY(sub) REFERENCES users(sub) ON DELETE CASCADE)`)
	return err
}

type workflowError struct{ Code, Message string }

func (err *workflowError) Error() string           { return err.Message }
func workflowCodeError(code, message string) error { return &workflowError{code, message} }

type verificationSpec struct {
	Purpose, Attribute, Destination, Code string
	ExpiresAt                             time.Time
}

func generateVerificationCode() (string, error) {
	value, err := rand.Int(rand.Reader, big.NewInt(1000000))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%06d", value.Int64()), nil
}
func verificationDigest(salt, code string) string {
	sum := sha256.Sum256([]byte(salt + ":" + code))
	return hex.EncodeToString(sum[:])
}

func (s *CognitoStore) putVerification(ctx context.Context, user *CognitoUser, spec verificationSpec) error {
	var saltBytes [16]byte
	if _, err := rand.Read(saltBytes[:]); err != nil {
		return err
	}
	salt := hex.EncodeToString(saltBytes[:])
	s.mu.Lock()
	defer s.mu.Unlock()
	result, err := s.db.ExecContext(ctx, `INSERT INTO verification_codes(sub,purpose,attribute_name,destination,code_salt,code_hash,expires_at,auth_version,attempts)
  SELECT sub,?,?,?,?,?,?,auth_version,0 FROM users WHERE sub=? AND pool_id=? AND enabled=1 AND auth_version=? AND status=?
		AND EXISTS(SELECT 1 FROM user_attributes a WHERE a.sub=users.sub AND a.name=? AND a.value=?)
  ON CONFLICT(sub,purpose) DO UPDATE SET attribute_name=excluded.attribute_name,destination=excluded.destination,
  code_salt=excluded.code_salt,code_hash=excluded.code_hash,expires_at=excluded.expires_at,auth_version=excluded.auth_version,attempts=0`,
		spec.Purpose, spec.Attribute, spec.Destination, salt, verificationDigest(salt, spec.Code), spec.ExpiresAt.Unix(), user.Sub, user.PoolID, user.AuthVersion, user.Status, spec.Attribute, spec.Destination)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return errTokenRevoked
	}
	return nil
}

type verificationRecord struct {
	Attribute, Destination, Salt, Hash string
	ExpiresAt, AuthVersion             int64
	Attempts                           int
}

func (s *CognitoStore) admitVerification(ctx context.Context, tx *sql.Tx, user *CognitoUser, purpose, code string) (verificationRecord, error) {
	var record verificationRecord
	err := tx.QueryRowContext(ctx, `SELECT attribute_name,destination,code_salt,code_hash,expires_at,auth_version,attempts FROM verification_codes WHERE sub=? AND purpose=?`, user.Sub, purpose).Scan(&record.Attribute, &record.Destination, &record.Salt, &record.Hash, &record.ExpiresAt, &record.AuthVersion, &record.Attempts)
	if errors.Is(err, sql.ErrNoRows) {
		return record, workflowCodeError("ExpiredCodeException", "No active verification code; request a new code")
	}
	if err != nil {
		return record, err
	}
	if record.AuthVersion != user.AuthVersion || record.ExpiresAt <= s.now().Unix() {
		return record, workflowCodeError("ExpiredCodeException", "Verification code has expired; request a new code")
	}
	if record.Attempts >= 5 {
		return record, workflowCodeError("LimitExceededException", "Too many incorrect verification attempts; request a new code")
	}
	if subtle.ConstantTimeCompare([]byte(record.Hash), []byte(verificationDigest(record.Salt, code))) != 1 {
		if _, err = tx.ExecContext(ctx, `UPDATE verification_codes SET attempts=attempts+1 WHERE sub=? AND purpose=?`, user.Sub, purpose); err != nil {
			return record, err
		}
		if err = tx.Commit(); err != nil {
			return record, err
		}
		return record, workflowCodeError("CodeMismatchException", "Invalid verification code provided, please try again")
	}
	var current string
	if err = tx.QueryRowContext(ctx, `SELECT value FROM user_attributes WHERE sub=? AND name=?`, user.Sub, record.Attribute).Scan(&current); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return record, workflowCodeError("ExpiredCodeException", "Verification destination has changed; request a new code")
		}
		return record, err
	}
	if current != record.Destination {
		return record, workflowCodeError("ExpiredCodeException", "Verification destination has changed; request a new code")
	}
	return record, nil
}

func currentWorkflowUser(ctx context.Context, tx *sql.Tx, expected *CognitoUser, requireEnabled bool) (*CognitoUser, error) {
	user, err := scanUserRow(tx.QueryRowContext(ctx, `SELECT `+userColumns+` FROM users WHERE sub=? AND pool_id=?`, expected.Sub, expected.PoolID))
	if err != nil {
		return nil, err
	}
	if requireEnabled && (!user.Enabled || user.AuthVersion != expected.AuthVersion) {
		return nil, errTokenRevoked
	}
	return user, nil
}

func checkWorkflowAlias(ctx context.Context, tx *sql.Tx, user *CognitoUser, config PoolSignInConfig, attributes map[string]string) error {
	email := attributes["email"]
	if email == "" || !(config.EmailAsUsername || config.EmailAlias && attributes["email_verified"] == "true") {
		return nil
	}
	query := `SELECT COUNT(*) FROM users WHERE pool_id=? AND sub!=? AND email=? COLLATE `
	if config.CaseSensitive {
		query += "BINARY"
	} else {
		query += "NOCASE"
	}
	if config.EmailAlias {
		query += ` AND EXISTS(SELECT 1 FROM user_attributes a WHERE a.sub=users.sub AND a.name='email_verified' AND a.value='true')`
	}
	var count int
	if err := tx.QueryRowContext(ctx, query, user.PoolID, user.Sub, email).Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return errEmailAliasExists
	}
	return nil
}
func transactionAttributes(ctx context.Context, tx *sql.Tx, sub string) (map[string]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT name,value FROM user_attributes WHERE sub=?`, sub)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	attrs := map[string]string{"sub": sub}
	for rows.Next() {
		var name, value string
		if err = rows.Scan(&name, &value); err != nil {
			return nil, err
		}
		attrs[name] = value
	}
	return attrs, rows.Err()
}

func (s *CognitoStore) confirmVerification(ctx context.Context, expected *CognitoUser, purpose, code string, signup bool) error {
	config, err := s.GetPoolSignInConfig(ctx, expected.PoolID)
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
	user, err := currentWorkflowUser(ctx, tx, expected, true)
	if err != nil {
		return err
	}
	if signup && user.Status != "UNCONFIRMED" {
		return workflowCodeError("NotAuthorizedException", "User cannot be confirmed. Current status is "+user.Status)
	}
	record, err := s.admitVerification(ctx, tx, user, purpose, code)
	if err != nil {
		return err
	}
	attributes, err := transactionAttributes(ctx, tx, user.Sub)
	if err != nil {
		return err
	}
	attributes[record.Attribute+"_verified"] = "true"
	if err = checkWorkflowAlias(ctx, tx, user, config, attributes); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO user_attributes(sub,name,value) VALUES(?,?,'true') ON CONFLICT(sub,name) DO UPDATE SET value='true'`, user.Sub, record.Attribute+"_verified"); err != nil {
		return err
	}
	status := user.Status
	if signup {
		status = "CONFIRMED"
	}
	if _, err = tx.ExecContext(ctx, `UPDATE users SET status=?,updated_at=MAX(updated_at,?) WHERE sub=?`, status, s.now().Unix(), user.Sub); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM verification_codes WHERE sub=? AND purpose=?`, user.Sub, purpose); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *CognitoStore) confirmPasswordRecovery(ctx context.Context, expected *CognitoUser, code, password string) error {
	hash, err := hashUserPassword(password)
	if err != nil {
		return err
	}
	salt, verifier, err := makeSRPCredentials(expected.PoolID, expected.Username, password)
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
	user, err := currentWorkflowUser(ctx, tx, expected, true)
	if err != nil {
		return err
	}
	if user.Status == "UNCONFIRMED" {
		return workflowCodeError("InvalidParameterException", "Cannot reset password for an unconfirmed user")
	}
	if _, err = s.admitVerification(ctx, tx, user, "recovery", code); err != nil {
		return err
	}
	now := s.now().Unix()
	if _, err = tx.ExecContext(ctx, `UPDATE users SET password_hash=?,srp_salt=?,srp_verifier=?,status='CONFIRMED',password_changed_at=?,updated_at=MAX(updated_at,?),password_failures=0,password_locked_until=0,auth_version=auth_version+1 WHERE sub=?`, hash, salt, verifier, now, now, user.Sub); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM challenge_sessions WHERE sub=?`, user.Sub); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM verification_codes WHERE sub=?`, user.Sub); err != nil {
		return err
	}
	return tx.Commit()
}

// updateWorkflowAttributes validates and applies a batch together, including
// immutable/required constraints and email alias ownership. Self-service changes
// are guarded against concurrent account revocation.
func (s *CognitoStore) updateWorkflowAttributes(ctx context.Context, expected *CognitoUser, schema []SchemaAttribute, updates map[string]string, administrator bool) error {
	config, err := s.GetPoolSignInConfig(ctx, expected.PoolID)
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
	user, err := currentWorkflowUser(ctx, tx, expected, !administrator)
	if err != nil {
		return err
	}
	current, err := transactionAttributes(ctx, tx, user.Sub)
	if err != nil {
		return err
	}
	normalized := normalizeAttributeUpdates(updates, config)
	validationCurrent := attributeValidationState(current, normalized)
	if err = ValidateUserAttributes(schema, validationCurrent, normalized, !administrator, false, administrator); err != nil {
		return err
	}
	merged := mergeAttributeUpdates(current, normalized, administrator)
	if err = checkWorkflowAlias(ctx, tx, user, config, merged); err != nil {
		return err
	}
	changed, err := s.persistAttributeChanges(ctx, tx, user.Sub, current, merged)
	if err != nil {
		return err
	}
	if changed {
		if _, err = tx.ExecContext(ctx, `UPDATE users SET email=?,updated_at=MAX(updated_at,?) WHERE sub=?`, merged["email"], s.now().Unix(), user.Sub); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *CognitoStore) adminConfirmSignUp(ctx context.Context, expected *CognitoUser) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	user, err := currentWorkflowUser(ctx, tx, expected, false)
	if err != nil {
		return err
	}
	if user.Status != "UNCONFIRMED" {
		return workflowCodeError("NotAuthorizedException", "User cannot be confirmed. Current status is "+user.Status)
	}
	if _, err = tx.ExecContext(ctx, `UPDATE users SET status='CONFIRMED',updated_at=MAX(updated_at,?) WHERE sub=?`, s.now().Unix(), user.Sub); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM verification_codes WHERE sub=? AND purpose='signup'`, user.Sub); err != nil {
		return err
	}
	return tx.Commit()
}
