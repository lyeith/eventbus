// Native software-token transitions own admission and promotion of verified
// account snapshots. Fixture provisioning does not use these authorization paths.
package cognito

import (
	"context"
	"database/sql"
	"errors"
)

var (
	errTOTPEnrollmentChanged    = errors.New("software token association changed")
	errSoftwareTokenNotVerified = errors.New("software token has not been verified")
)

// mfaAuthorization carries the existing access-token authorization result into
// the write. Admin preferences bind the account revision without a user grant.
type mfaAuthorization struct {
	AuthVersion int64
	OriginJTI   string
	SelfService bool
}

const mfaAuthorizationPredicate = ` WHERE sub=:sub AND auth_version=:auth_version
	AND (:self_service=0 OR (enabled=1 AND NOT EXISTS (
		SELECT 1 FROM revoked_refresh_tokens WHERE jti=:origin_jti)))`

// Association leaves the existing verified authenticator active until a new
// code verifies its replacement. It cannot publish a secret for a stale grant.
func (s *CognitoStore) setPendingTOTPSecret(ctx context.Context, sub, secret string, authorization mfaAuthorization) error {
	if secret == "" {
		return errors.New("software token secret required")
	}
	authorization.SelfService = true
	return s.applyMFATransition(ctx, sub, authorization,
		`UPDATE users SET totp_pending_secret=:secret`+mfaAuthorizationPredicate,
		errTokenRevoked, sql.Named("secret", secret))
}

// Promotion requires the exact pending secret whose code was checked, together
// with a current, enabled account and unrevoked self-service grant. A replaced
// or already consumed association cannot silently promote a different secret.
func (s *CognitoStore) confirmPendingTOTPSecret(ctx context.Context, sub, expectedSecret string, authorization mfaAuthorization) error {
	if expectedSecret == "" {
		return errTOTPEnrollmentChanged
	}
	authorization.SelfService = true
	return s.applyMFATransition(ctx, sub, authorization,
		`UPDATE users SET totp_secret=totp_pending_secret,totp_pending_secret='',software_token_verified=1`+
			mfaAuthorizationPredicate+` AND totp_pending_secret=:secret`,
		errTOTPEnrollmentChanged, sql.Named("secret", expectedSecret))
}

// Enabling checks persisted verification at the same write that changes the
// preference; an earlier user snapshot cannot enable a cleared authenticator.
func (s *CognitoStore) setMFAPreference(ctx context.Context, sub string, enabled bool, authorization mfaAuthorization) error {
	return s.applyMFATransition(ctx, sub, authorization,
		`UPDATE users SET mfa_enabled=:enabled`+mfaAuthorizationPredicate+
			` AND (:enabled=0 OR (software_token_verified=1 AND totp_secret!=''))`,
		errSoftwareTokenNotVerified, sql.Named("enabled", boolToInt(enabled)))
}

// The SQL predicate is the admission decision. On refusal, the same transaction
// classifies the unchanged account while retaining SQLite's writer lock, so a
// second mutation cannot turn a revocation into an enrollment mismatch response.
func (s *CognitoStore) applyMFATransition(ctx context.Context, sub string, authorization mfaAuthorization, statement string, refusal error, arguments ...any) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	arguments = append(arguments,
		sql.Named("sub", sub), sql.Named("auth_version", authorization.AuthVersion),
		sql.Named("self_service", boolToInt(authorization.SelfService)), sql.Named("origin_jti", authorization.OriginJTI))
	result, err := tx.ExecContext(ctx, statement, arguments...)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		var enabled, revoked int
		var version int64
		err = tx.QueryRowContext(ctx, `SELECT enabled,auth_version,
			EXISTS(SELECT 1 FROM revoked_refresh_tokens WHERE jti=?) FROM users WHERE sub=?`,
			authorization.OriginJTI, sub).Scan(&enabled, &version, &revoked)
		if errors.Is(err, sql.ErrNoRows) && authorization.SelfService {
			return errTokenRevoked
		}
		if err != nil {
			return err
		}
		if version != authorization.AuthVersion || authorization.SelfService && (enabled == 0 || revoked != 0) {
			return errTokenRevoked
		}
		return refusal
	}
	return tx.Commit()
}
