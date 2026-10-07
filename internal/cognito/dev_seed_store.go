// Development harness seed persistence preserves deterministic identities and
// fixture credentials through the shared Cognito identity/password operations.
package cognito

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// UpsertSeedUser preserves identity and lifecycle state when reapplying the
// same credential. Plaintext exists only during hashing and SRP provisioning.
func (s *CognitoStore) UpsertSeedUser(ctx context.Context, poolID, username, email, plaintext string, mfaEnabled bool) (string, error) {
	if username == "" {
		username = email
	}
	config, err := s.GetPoolSignInConfig(ctx, poolID)
	if err != nil {
		return "", err
	}
	user, err := s.lookupUserByUsername(ctx, poolID, username, config)
	if config.EmailAsUsername && errors.Is(err, sql.ErrNoRows) {
		user, err = s.lookupSignInEmail(ctx, poolID, email, config)
	}
	if errors.Is(err, sql.ErrNoRows) {
		user, err = s.CreateUserIdentity(ctx, poolID, username, email, plaintext, "CONFIRMED", nil)
		if errors.Is(err, errUsernameExists) {
			return s.UpsertSeedUser(ctx, poolID, username, email, plaintext, mfaEnabled)
		}
		if err != nil {
			return "", err
		}
	} else if err != nil {
		return "", err
	}
	samePassword := compareUserPasswordHash(user.PasswordHash, plaintext) == nil
	if !samePassword {
		if err = s.SetUserPassword(ctx, user.Sub, plaintext, "CONFIRMED"); err != nil {
			return "", err
		}
	} else if user.SRPSalt == "" || user.SRPVerifier == "" {
		// Adding a verifier for an unchanged legacy password must not renew its
		// temporary lifetime, change profile timestamps or revoke existing grants.
		salt, verifier, err := makeSRPCredentials(user.PoolID, user.Username, plaintext)
		if err != nil {
			return "", err
		}
		result, err := s.db.ExecContext(ctx, `UPDATE users SET srp_salt=?,srp_verifier=? WHERE sub=? AND password_hash=? AND auth_version=?`, salt, verifier, user.Sub, user.PasswordHash, user.AuthVersion)
		if err != nil {
			return "", err
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return "", err
		}
		if changed == 0 {
			return "", errTokenRevoked
		}
	}
	if email != "" && normalizeSignIn(email, config) != user.Email {
		if err = s.SetUserAttribute(ctx, user.Sub, "email", email); err != nil {
			return "", err
		}
	}
	_, err = s.db.ExecContext(ctx, `UPDATE users SET mfa_enabled=?,updated_at=MAX(updated_at,?) WHERE sub=? AND mfa_enabled!=?`, boolToInt(mfaEnabled), time.Now().Unix(), user.Sub, boolToInt(mfaEnabled))

	return user.Sub, err
}
