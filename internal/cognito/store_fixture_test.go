// Same-package store fixtures construct deliberately incomplete legacy rows
// and session state. These helpers are excluded from the emulator executable;
// production callers use guarded identity/password/session operations.
package cognito

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"time"
)

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

// MarkChallengeSessionUsed flips `used=1`. Idempotent: re-marking an already-
// used row is a no-op (UPDATE with no rows affected returns nil).
func (s *CognitoStore) MarkChallengeSessionUsed(ctx context.Context, sessionID string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE challenge_sessions SET used = 1 WHERE session = ?`, sessionID)
	return err
}

// newOpaqueID returns 32 hex chars from crypto/rand. Cognito sub is normally
// a UUID; keeping it 32 hex chars stays comfortably within consumer parsers
// that tolerate any opaque string.
func newOpaqueID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
