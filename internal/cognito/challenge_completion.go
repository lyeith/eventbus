// Temporary-password completion owns the atomic credential/attribute/session
// transition; no other challenge can consume an older account revision.
package cognito

import (
	"context"
	"database/sql"
	"errors"
)

func (s *CognitoStore) completeNewPassword(ctx context.Context, expected *CognitoUser, row *CognitoChallengeSession, password string, updates map[string]string, pool *CognitoPool, client *CognitoClient) (*CognitoUser, error) {
	hash, err := hashUserPassword(password)
	if err != nil {
		return nil, err
	}
	salt, verifier, err := makeSRPCredentials(expected.PoolID, expected.Username, password)
	if err != nil {
		return nil, err
	}
	config, err := s.GetPoolSignInConfig(ctx, expected.PoolID)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	defer func() { _ = tx.Rollback(); s.mu.Unlock() }()
	user, err := currentWorkflowUser(ctx, tx, expected, true)
	if err != nil {
		return nil, err
	}
	if user.Status != "FORCE_CHANGE_PASSWORD" {
		return nil, errTokenRevoked
	}
	attributes, err := transactionAttributes(ctx, tx, user.Sub)
	if err != nil {
		return nil, err
	}
	normalized := normalizeAttributeUpdates(updates, config)
	if pool.SchemaAttributes != nil {
		if err = ValidateClientWriteAttributes(pool.SchemaAttributes, client.WriteAttributes, normalized); err != nil {
			return nil, err
		}
		if err = ValidateNewPasswordAttributes(pool.SchemaAttributes, attributes, normalized); err != nil {
			return nil, err
		}
	}
	merged := mergeAttributeUpdates(attributes, normalized, false)
	if err = checkWorkflowAlias(ctx, tx, user, config, merged); err != nil {
		return nil, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE challenge_sessions SET used=1 WHERE session=? AND sub=? AND pool_id=? AND client_id=? AND used=0 AND expires_at>? AND auth_version=?`, row.Session, user.Sub, user.PoolID, row.ClientID, s.now().Unix(), expected.AuthVersion)
	if err != nil {
		return nil, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}
	if count != 1 {
		return nil, errTokenRevoked
	}
	now := s.now().Unix()
	email := merged["email"]
	result, err = tx.ExecContext(ctx, `UPDATE users SET password_hash=?,srp_salt=?,srp_verifier=?,status='CONFIRMED',password_changed_at=?,updated_at=MAX(updated_at,?),auth_version=auth_version+1,email=? WHERE sub=? AND pool_id=? AND enabled=1 AND auth_version=?`, hash, salt, verifier, now, now, email, user.Sub, user.PoolID, expected.AuthVersion)
	if err != nil {
		return nil, err
	}
	count, err = result.RowsAffected()
	if err != nil {
		return nil, err
	}
	if count != 1 {
		return nil, errTokenRevoked
	}
	if _, err = s.persistAttributeChanges(ctx, tx, user.Sub, attributes, merged); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	current, err := s.LookupUserBySub(ctx, user.Sub)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errTokenRevoked
	}
	return current, err
}
