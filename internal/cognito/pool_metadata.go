// Persisted native pool/client configuration and AWS metadata readback.
package cognito

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

type CognitoPool struct {
	ID                     string
	Name                   string
	Region                 string
	AccountID              string
	CreatedAt              int64
	ModifiedAt             int64
	Native                 bool
	SignIn                 PoolSignInConfig
	PasswordPolicy         *PasswordPolicy
	SchemaAttributes       []SchemaAttribute
	AutoVerifiedAttributes []string
	AccountRecoverySetting *AccountRecoverySetting
	AdminCreateUserConfig  *AdminCreateUserConfig
}

func (s *CognitoStore) LookupPool(ctx context.Context, id string) (*CognitoPool, error) {
	var p CognitoPool
	var metadata, policy, signIn sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT id,region,created_at,last_modified_at,metadata,password_policy,sign_in_config FROM pools WHERE id=?`, id).Scan(&p.ID, &p.Region, &p.CreatedAt, &p.ModifiedAt, &metadata, &policy, &signIn)
	if err != nil {
		return nil, err
	}
	if metadata.Valid {
		var config CognitoPool
		if err = json.Unmarshal([]byte(metadata.String), &config); err != nil {
			return nil, err
		}
		config.ID, config.Region, config.CreatedAt, config.ModifiedAt = p.ID, p.Region, p.CreatedAt, p.ModifiedAt
		p = config
	}
	if p.ModifiedAt == 0 {
		p.ModifiedAt = p.CreatedAt
	}
	p.SignIn = PoolSignInConfig{CaseSensitive: true}
	if signIn.Valid {
		if err = json.Unmarshal([]byte(signIn.String), &p.SignIn); err != nil {
			return nil, err
		}
	}
	if policy.Valid && policy.String != "" {
		if err = json.Unmarshal([]byte(policy.String), &p.PasswordPolicy); err != nil {
			return nil, err
		}
	}
	return &p, nil
}

func (s *CognitoStore) SavePool(ctx context.Context, p *CognitoPool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var oldConfig string
	var createdAt int64
	err = tx.QueryRowContext(ctx, `SELECT sign_in_config,created_at FROM pools WHERE id=?`, p.ID).Scan(&oldConfig, &createdAt)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err == nil {
		previous := PoolSignInConfig{CaseSensitive: true}
		if e := json.Unmarshal([]byte(oldConfig), &previous); e != nil {
			return e
		}
		if previous != p.SignIn {
			var count int
			if e := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM users WHERE pool_id=?`, p.ID).Scan(&count); e != nil {
				return e
			}
			if count > 0 {
				return errPoolSignInConfigImmutable
			}
		}
		p.CreatedAt = createdAt
	} else {
		p.CreatedAt = s.now().Unix()
	}
	p.ModifiedAt = s.now().Unix()
	metadata, err := json.Marshal(p)
	if err != nil {
		return err
	}
	signIn, err := json.Marshal(p.SignIn)
	if err != nil {
		return err
	}
	var policy any
	if p.PasswordPolicy != nil {
		raw, e := json.Marshal(p.PasswordPolicy)
		if e != nil {
			return e
		}
		policy = string(raw)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO pools(id,region,password_policy,sign_in_config,created_at,last_modified_at,metadata)VALUES(?,?,?,?,?,?,?) ON CONFLICT(id)DO UPDATE SET password_policy=excluded.password_policy,sign_in_config=excluded.sign_in_config,last_modified_at=excluded.last_modified_at,metadata=excluded.metadata`, p.ID, p.Region, policy, string(signIn), p.CreatedAt, p.ModifiedAt, string(metadata))
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *CognitoStore) SaveClient(ctx context.Context, c *CognitoClient) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var poolID string
	var createdAt int64
	err = tx.QueryRowContext(ctx, `SELECT pool_id,created_at FROM clients WHERE id=?`, c.ID).Scan(&poolID, &createdAt)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err == nil {
		if poolID != c.PoolID {
			return errClientPoolConflict
		}
		c.CreatedAt = createdAt
	} else {
		c.CreatedAt = s.now().Unix()
	}
	var parent int
	if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM pools WHERE id=?`, c.PoolID).Scan(&parent); err != nil {
		return err
	}
	if parent == 0 {
		return sql.ErrNoRows
	}
	c.ModifiedAt = s.now().Unix()
	metadata, err := json.Marshal(c)
	if err != nil {
		return err
	}
	var flows any
	if c.ExplicitAuthFlows != nil {
		raw, e := json.Marshal(c.ExplicitAuthFlows)
		if e != nil {
			return e
		}
		flows = string(raw)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO clients(id,pool_id,secret,created_at,explicit_auth_flows,auth_session_validity,last_modified_at,metadata) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(id)DO UPDATE SET secret=excluded.secret,explicit_auth_flows=excluded.explicit_auth_flows,auth_session_validity=excluded.auth_session_validity,last_modified_at=excluded.last_modified_at,metadata=excluded.metadata WHERE clients.pool_id=excluded.pool_id`, c.ID, c.PoolID, c.Secret, c.CreatedAt, flows, c.AuthSessionValidity, c.ModifiedAt, string(metadata))
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Handler) poolResponse(p *CognitoPool) map[string]any {
	out := map[string]any{"Id": p.ID, "CreationDate": float64(p.CreatedAt), "LastModifiedDate": float64(p.ModifiedAt), "Status": "Enabled", "UsernameConfiguration": map[string]any{"CaseSensitive": p.SignIn.CaseSensitive}}
	if p.Name != "" {
		out["Name"] = p.Name
	}
	account := p.AccountID
	if account == "" {
		account = s.accountID
	}
	partition := "aws"
	if strings.HasPrefix(p.Region, "cn-") {
		partition = "aws-cn"
	}
	if strings.HasPrefix(p.Region, "us-gov-") {
		partition = "aws-us-gov"
	}
	out["Arn"] = fmt.Sprintf("arn:%s:cognito-idp:%s:%s:userpool/%s", partition, p.Region, account, p.ID)
	if p.SignIn.EmailAsUsername {
		out["UsernameAttributes"] = []string{"email"}
	}
	if p.SignIn.EmailAlias {
		out["AliasAttributes"] = []string{"email"}
	}
	if p.PasswordPolicy != nil {
		out["Policies"] = map[string]any{"PasswordPolicy": poolPasswordPolicyResponse(p.PasswordPolicy)}
	}
	if p.SchemaAttributes != nil {
		out["SchemaAttributes"] = p.SchemaAttributes
	}
	if p.AutoVerifiedAttributes != nil {
		out["AutoVerifiedAttributes"] = p.AutoVerifiedAttributes
	}
	if p.AccountRecoverySetting != nil {
		out["AccountRecoverySetting"] = p.AccountRecoverySetting
	}
	if p.AdminCreateUserConfig != nil {
		out["AdminCreateUserConfig"] = p.AdminCreateUserConfig
	}
	return out
}

func clientResponse(c *CognitoClient) map[string]any {
	out := map[string]any{"UserPoolId": c.PoolID, "ClientId": c.ID, "CreationDate": float64(c.CreatedAt), "LastModifiedDate": float64(c.ModifiedAt), "AuthSessionValidity": c.AuthSessionValidity}
	if c.Name != "" {
		out["ClientName"] = c.Name
	}
	if c.Secret != "" {
		out["ClientSecret"] = c.Secret
	}
	if c.ExplicitAuthFlows != nil {
		out["ExplicitAuthFlows"] = c.ExplicitAuthFlows
	}
	if c.TokenValidity != nil {
		out["AccessTokenValidity"] = c.TokenValidity.AccessTokenValidity
		out["IdTokenValidity"] = c.TokenValidity.IdTokenValidity
		out["RefreshTokenValidity"] = c.TokenValidity.RefreshTokenValidity
		out["TokenValidityUnits"] = c.TokenValidity.TokenValidityUnits
	}
	if c.ReadAttributes != nil {
		out["ReadAttributes"] = c.ReadAttributes
	}
	if c.WriteAttributes != nil {
		out["WriteAttributes"] = c.WriteAttributes
	}
	return out
}

func (s *Handler) handleDescribeUserPool(w http.ResponseWriter, r *http.Request) {
	var req deleteUserPoolRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	if req.UserPoolID == "" {
		cognitoJSONError(w, 400, "InvalidParameterException", "UserPoolId is required")
		return
	}
	p, err := s.cognito.LookupPool(r.Context(), req.UserPoolID)
	if errors.Is(err, sql.ErrNoRows) {
		cognitoJSONError(w, 400, "ResourceNotFoundException", "User pool does not exist")
		return
	}
	if err != nil {
		authInternalError(w, err, "DescribeUserPool")
		return
	}
	cognitoJSONResponse(w, 200, map[string]any{"UserPool": s.poolResponse(p)})
}
func (s *Handler) handleDescribeUserPoolClient(w http.ResponseWriter, r *http.Request) {
	var req deleteUserPoolClientRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	if req.UserPoolID == "" || req.ClientID == "" {
		cognitoJSONError(w, 400, "InvalidParameterException", "UserPoolId and ClientId are required")
		return
	}
	c, ok := s.authClient(w, r, req.ClientID, req.UserPoolID)
	if !ok {
		return
	}
	cognitoJSONResponse(w, 200, map[string]any{"UserPoolClient": clientResponse(c)})
}
