// Pool and app-client AWS operations. Legacy local request extensions are
// declared separately in dev_provisioning.go; they are not Cognito API fields.
package cognito

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

type createUserPoolRequest struct {
	devCreateUserPoolFields
	PoolName              string                           `json:"PoolName"`
	Policies              *createPoolPoliciesEnv           `json:"Policies"`
	UsernameAttributes    []string                         `json:"UsernameAttributes"`
	AliasAttributes       []string                         `json:"AliasAttributes"`
	UsernameConfiguration *createPoolUsernameConfiguration `json:"UsernameConfiguration"`
}

type createPoolPoliciesEnv struct {
	PasswordPolicy json.RawMessage `json:"PasswordPolicy"`
}

type createPoolUsernameConfiguration struct {
	CaseSensitive *bool `json:"CaseSensitive"`
}

// handleCreateUserPool creates an AWS-shaped pool, or reapplies an explicit
// local fixture ID. Omitted configuration on a fixture reapplication preserves
// its existing policy and identity rules.
func (s *Handler) handleCreateUserPool(w http.ResponseWriter, r *http.Request) {
	var req createUserPoolRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	if req.PoolName == "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "PoolName is required")
		return
	}
	signIn, err := createPoolSignInConfig(req)
	if err != nil {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", err.Error())
		return
	}
	rawPolicy := req.PasswordPolicy
	if !hasJSONValue(rawPolicy) && req.Policies != nil {
		rawPolicy = req.Policies.PasswordPolicy
	}
	policy, err := createPoolPasswordPolicy(rawPolicy)
	if err != nil {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", err.Error())
		return
	}
	poolID := req.PoolID
	if poolID == "" {
		poolID = newPoolID()
	}
	ctx := r.Context()
	exists, err := s.cognito.PoolExists(ctx, poolID)
	if err != nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}
	if exists {
		previous, err := s.cognito.GetPoolSignInConfig(ctx, poolID)
		if err != nil {
			cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
			return
		}
		if req.UsernameAttributes == nil && req.AliasAttributes == nil {
			signIn.EmailAsUsername, signIn.EmailAlias = previous.EmailAsUsername, previous.EmailAlias
		}
		if req.UsernameConfiguration == nil {
			signIn.CaseSensitive = previous.CaseSensitive
		}
		if !hasJSONValue(rawPolicy) {
			policy, err = loadPoolPasswordPolicy(ctx, s.cognito, poolID)
			if err != nil {
				cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
				return
			}
		}
	}
	if err := s.cognito.UpsertPool(ctx, poolID, "us-east-1"); err != nil {
		log.Error().Err(err).Msg("UpsertPool failed in CreateUserPool")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}
	if err := s.cognito.SetPoolSignInConfig(ctx, poolID, signIn); err != nil {
		if errors.Is(err, errPoolSignInConfigImmutable) {
			cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", err.Error())
		} else {
			cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		}
		return
	}
	if policy != nil {
		raw, err := json.Marshal(policy)
		if err != nil {
			cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
			return
		}
		if err := s.cognito.SetPoolPasswordPolicy(ctx, poolID, string(raw)); err != nil {
			log.Error().Err(err).Msg("SetPoolPasswordPolicy failed in CreateUserPool")
			cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
			return
		}
	}
	now := float64(time.Now().Unix())
	out := map[string]interface{}{
		"Id": poolID, "Name": req.PoolName, "CreationDate": now,
		"LastModifiedDate": now, "Status": "Enabled",
		"UsernameConfiguration": map[string]interface{}{"CaseSensitive": signIn.CaseSensitive},
	}
	if signIn.EmailAsUsername {
		out["UsernameAttributes"] = []string{"email"}
	}
	if signIn.EmailAlias {
		out["AliasAttributes"] = []string{"email"}
	}
	if policy != nil {
		out["Policies"] = map[string]interface{}{"PasswordPolicy": poolPasswordPolicyResponse(policy)}
	}
	cognitoJSONResponse(w, http.StatusOK, map[string]interface{}{"UserPool": out})
}

func createPoolSignInConfig(req createUserPoolRequest) (PoolSignInConfig, error) {
	config := PoolSignInConfig{CaseSensitive: true}
	if len(req.UsernameAttributes) > 0 && len(req.AliasAttributes) > 0 {
		return config, errors.New("UsernameAttributes and AliasAttributes are mutually exclusive")
	}
	for _, attributes := range [][]string{req.UsernameAttributes, req.AliasAttributes} {
		if len(attributes) > 1 || len(attributes) == 1 && attributes[0] != "email" {
			return config, errors.New("the local Cognito service supports only the email sign-in attribute")
		}
	}
	config.EmailAsUsername = len(req.UsernameAttributes) == 1
	config.EmailAlias = len(req.AliasAttributes) == 1
	if req.UsernameConfiguration != nil {
		if req.UsernameConfiguration.CaseSensitive == nil {
			return config, errors.New("UsernameConfiguration.CaseSensitive is required")
		}
		config.CaseSensitive = *req.UsernameConfiguration.CaseSensitive
	}
	return config, nil
}

// The shared PasswordPolicy decoder owns AWS/fixture spelling and precedence.
// Request-member presence is retained here so omission uses AWS defaults while
// explicit false complexity flags and invalid numeric zero remain distinguishable.
func createPoolPasswordPolicy(raw json.RawMessage) (*PasswordPolicy, error) {
	policy := &PasswordPolicy{
		MinLength: 8, RequireUppercase: true, RequireLowercase: true,
		RequireDigits: true, RequireSymbols: true, TemporaryPasswordValidityDays: 7,
	}
	if !hasJSONValue(raw) {
		return policy, nil
	}
	var supplied PasswordPolicy
	if err := json.Unmarshal(raw, &supplied); err != nil {
		return nil, fmt.Errorf("invalid PasswordPolicy: %w", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return nil, errors.New("PasswordPolicy must be an object")
	}
	hasField := func(names ...string) bool {
		for field := range fields {
			for _, name := range names {
				if strings.EqualFold(field, name) {
					return true
				}
			}
		}
		return false
	}
	if hasField("MinimumLength", "min_length") {
		policy.MinLength = supplied.MinLength
	}
	if hasField("RequireUppercase", "require_uppercase") {
		policy.RequireUppercase = supplied.RequireUppercase
	}
	if hasField("RequireLowercase", "require_lowercase") {
		policy.RequireLowercase = supplied.RequireLowercase
	}
	if hasField("RequireNumbers", "RequireDigits", "require_digits") {
		policy.RequireDigits = supplied.RequireDigits
	}
	if hasField("RequireSymbols", "require_symbols") {
		policy.RequireSymbols = supplied.RequireSymbols
	}
	if hasField("TemporaryPasswordValidityDays", "temporary_password_validity_days") {
		policy.TemporaryPasswordValidityDays = supplied.TemporaryPasswordValidityDays
	}
	if policy.MinLength < 6 || policy.MinLength > 99 {
		return nil, errors.New("PasswordPolicy.MinimumLength must be between 6 and 99")
	}
	if policy.TemporaryPasswordValidityDays < 0 || policy.TemporaryPasswordValidityDays > 365 {
		return nil, errors.New("PasswordPolicy.TemporaryPasswordValidityDays must be between 0 and 365")
	}
	if policy.TemporaryPasswordValidityDays == 0 {
		policy.TemporaryPasswordValidityDays = 7
	}
	return policy, nil
}

func hasJSONValue(raw json.RawMessage) bool {
	return len(raw) != 0 && strings.TrimSpace(string(raw)) != "null"
}

func poolPasswordPolicyResponse(policy *PasswordPolicy) map[string]interface{} {
	return map[string]interface{}{
		"MinimumLength": policy.MinLength, "RequireUppercase": policy.RequireUppercase,
		"RequireLowercase": policy.RequireLowercase, "RequireNumbers": policy.RequireDigits,
		"RequireSymbols": policy.RequireSymbols, "TemporaryPasswordValidityDays": policy.TemporaryPasswordValidityDays,
	}
}

type createUserPoolClientRequest struct {
	devCreateUserPoolClientFields
	UserPoolID          string   `json:"UserPoolId"`
	ClientName          string   `json:"ClientName"`
	GenerateSecret      bool     `json:"GenerateSecret"`
	ExplicitAuthFlows   []string `json:"ExplicitAuthFlows"`
	AuthSessionValidity *int     `json:"AuthSessionValidity"`
}

func (s *Handler) handleCreateUserPoolClient(w http.ResponseWriter, r *http.Request) {
	var req createUserPoolClientRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	if req.UserPoolID == "" || req.ClientName == "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "UserPoolId and ClientName are required")
		return
	}
	flows, sessionMinutes, err := createClientAuthConfig(req)
	if err != nil {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", err.Error())
		return
	}
	ctx := r.Context()
	exists, err := s.cognito.PoolExists(ctx, req.UserPoolID)
	if err != nil {
		log.Error().Err(err).Msg("PoolExists failed in CreateUserPoolClient")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}
	if !exists {
		cognitoJSONError(w, http.StatusBadRequest, "ResourceNotFoundException", fmt.Sprintf("User pool %s does not exist", req.UserPoolID))
		return
	}
	clientID := req.ClientID
	if clientID == "" {
		clientID = newClientID()
	} else {
		previous, err := s.cognito.LookupClient(ctx, clientID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
			return
		}
		if err == nil && previous.PoolID != req.UserPoolID {
			cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "ClientId already belongs to another user pool")
			return
		}
	}
	secret := ""
	if req.GenerateSecret {
		secret = newClientSecret()
	}
	if err := s.cognito.UpsertClient(ctx, clientID, req.UserPoolID, secret); err != nil {
		if errors.Is(err, errClientPoolConflict) {
			cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "ClientId already belongs to another user pool")
		} else {
			log.Error().Err(err).Msg("UpsertClient failed in CreateUserPoolClient")
			cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		}
		return
	}
	if err := s.cognito.SetClientAuthConfig(ctx, clientID, flows, sessionMinutes); err != nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}
	now := float64(time.Now().Unix())
	out := map[string]interface{}{
		"UserPoolId": req.UserPoolID, "ClientId": clientID, "ClientName": req.ClientName,
		"CreationDate": now, "LastModifiedDate": now,
		"ExplicitAuthFlows": flows, "AuthSessionValidity": sessionMinutes,
	}
	if secret != "" {
		out["ClientSecret"] = secret
	}
	cognitoJSONResponse(w, http.StatusOK, map[string]interface{}{"UserPoolClient": out})
}

func createClientAuthConfig(req createUserPoolClientRequest) ([]string, int, error) {
	flows := req.ExplicitAuthFlows
	if flows == nil {
		flows = []string{"ALLOW_REFRESH_TOKEN_AUTH", "ALLOW_USER_SRP_AUTH", "ALLOW_CUSTOM_AUTH"}
	}
	legacy, modern := false, false
	for _, flow := range flows {
		switch flow {
		case "ADMIN_NO_SRP_AUTH", "CUSTOM_AUTH_FLOW_ONLY", "USER_PASSWORD_AUTH":
			legacy = true
		case "ALLOW_ADMIN_USER_PASSWORD_AUTH", "ALLOW_CUSTOM_AUTH", "ALLOW_USER_PASSWORD_AUTH",
			"ALLOW_USER_SRP_AUTH", "ALLOW_REFRESH_TOKEN_AUTH", "ALLOW_USER_AUTH":
			modern = true
		default:
			return nil, 0, fmt.Errorf("invalid ExplicitAuthFlows value %q", flow)
		}
	}
	if legacy && modern {
		return nil, 0, errors.New("legacy ExplicitAuthFlows values cannot be combined with ALLOW_ values")
	}
	sessionMinutes := 3
	if req.AuthSessionValidity != nil {
		sessionMinutes = *req.AuthSessionValidity
	}
	if sessionMinutes < 3 || sessionMinutes > 15 {
		return nil, 0, errors.New("AuthSessionValidity must be between 3 and 15 minutes")
	}
	return flows, sessionMinutes, nil
}

// --- DeleteUserPool ----------------------------------------------------

// deleteUserPoolRequest mirrors the AWS wire request.
type deleteUserPoolRequest struct {
	UserPoolID string `json:"UserPoolId"`
}

// handleDeleteUserPool implements DeleteUserPool. Cascades to children
// (clients, users, user_attributes via FK; signing_keys and
// challenge_sessions explicitly inside DeletePool's transaction).
// Idempotent — missing pool returns success.
func (s *Handler) handleDeleteUserPool(w http.ResponseWriter, r *http.Request) {
	var req deleteUserPoolRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	if req.UserPoolID == "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "UserPoolId is required")
		return
	}
	ctx := r.Context()
	if _, err := s.cognito.DeletePool(ctx, req.UserPoolID); err != nil {
		log.Error().Err(err).Msg("DeletePool failed")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}
	cognitoJSONResponse(w, http.StatusOK, map[string]interface{}{})
}

// --- DeleteUserPoolClient ----------------------------------------------

// deleteUserPoolClientRequest mirrors the AWS wire request.
type deleteUserPoolClientRequest struct {
	UserPoolID string `json:"UserPoolId"`
	ClientID   string `json:"ClientId"`
}

// handleDeleteUserPoolClient implements DeleteUserPoolClient. Idempotent
// on missing client.
func (s *Handler) handleDeleteUserPoolClient(w http.ResponseWriter, r *http.Request) {
	var req deleteUserPoolClientRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	if req.UserPoolID == "" || req.ClientID == "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException",
			"UserPoolId and ClientId are required")
		return
	}
	ctx := r.Context()
	if _, err := s.cognito.DeleteClient(ctx, req.UserPoolID, req.ClientID); err != nil {
		log.Error().Err(err).Msg("DeleteClient failed")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}
	cognitoJSONResponse(w, http.StatusOK, map[string]interface{}{})
}

// Generated IDs fit the AWS model constraints and work with SRP pool parsing.
// Explicit local fixture IDs never pass through these generators.
func newPoolID() string {
	return "us-east-1_" + randomHex(12)
}

func newClientID() string {
	return randomHex(16)
}

func newClientSecret() string {
	return randomHex(24)
}

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Errorf("crypto/rand: %w", err))
	}
	return hex.EncodeToString(b)
}
