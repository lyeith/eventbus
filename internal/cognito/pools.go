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

	"github.com/rs/zerolog/log"
)

type createUserPoolRequest struct {
	devCreateUserPoolFields
	Schema                 []SchemaAttribute                `json:"Schema"`
	AutoVerifiedAttributes []string                         `json:"AutoVerifiedAttributes"`
	AccountRecoverySetting *AccountRecoverySetting          `json:"AccountRecoverySetting"`
	AdminCreateUserConfig  *AdminCreateUserConfig           `json:"AdminCreateUserConfig"`
	PoolName               string                           `json:"PoolName"`
	Policies               *createPoolPoliciesEnv           `json:"Policies"`
	UsernameAttributes     []string                         `json:"UsernameAttributes"`
	AliasAttributes        []string                         `json:"AliasAttributes"`
	UsernameConfiguration  *createPoolUsernameConfiguration `json:"UsernameConfiguration"`
}

type createPoolPoliciesEnv struct {
	raw            json.RawMessage
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
		cognitoJSONError(w, 400, "InvalidParameterException", "PoolName is required")
		return
	}
	if (req.PoolID != "" || hasJSONValue(req.PasswordPolicy)) && s.devProfile != DevProfileLegacyFixtures {
		cognitoJSONError(w, 400, "InvalidParameterException", "PoolId and flat PasswordPolicy require the legacy-fixtures development profile")
		return
	}
	signIn, err := createPoolSignInConfig(req)
	if err != nil {
		cognitoJSONError(w, 400, "InvalidParameterException", err.Error())
		return
	}
	rawPolicy := req.PasswordPolicy
	if !hasJSONValue(rawPolicy) && req.Policies != nil {
		rawPolicy = req.Policies.PasswordPolicy
	}
	if (req.PoolID == "" || s.devProfile != DevProfileLegacyFixtures) && hasJSONValue(rawPolicy) {
		if err := validateNativeFields(rawPolicy, []string{"MinimumLength", "RequireUppercase", "RequireLowercase", "RequireNumbers", "RequireSymbols", "TemporaryPasswordValidityDays"}, nil); err != nil {
			cognitoJSONError(w, 400, "InvalidParameterException", err.Error())
			return
		}
	}
	policy, err := createPoolPasswordPolicy(rawPolicy)
	if err != nil {
		cognitoJSONError(w, 400, "InvalidParameterException", err.Error())
		return
	}
	if err = NormalizePoolWorkflowConfig(req.AutoVerifiedAttributes, req.AccountRecoverySetting, req.AdminCreateUserConfig); err != nil {
		cognitoJSONError(w, 400, "InvalidParameterException", err.Error())
		return
	}
	var schema []SchemaAttribute
	if req.PoolID == "" || req.Schema != nil {
		schema, err = NormalizeSchema(req.Schema)
		if err != nil {
			cognitoJSONError(w, 400, "InvalidParameterException", err.Error())
			return
		}
	}
	poolID := req.PoolID
	if poolID == "" {
		poolID = s.region + "_" + randomHex(12)
	}
	p := &CognitoPool{ID: poolID, Name: req.PoolName, Region: s.region, AccountID: s.accountID, Native: req.PoolID == "", SignIn: signIn, PasswordPolicy: policy, SchemaAttributes: schema, AutoVerifiedAttributes: req.AutoVerifiedAttributes, AccountRecoverySetting: req.AccountRecoverySetting, AdminCreateUserConfig: req.AdminCreateUserConfig}
	previous, err := s.cognito.LookupPool(r.Context(), poolID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		authInternalError(w, err, "CreateUserPool")
		return
	}
	if err == nil {
		p.Region, p.AccountID, p.Native = previous.Region, previous.AccountID, previous.Native
		if req.UsernameAttributes == nil && req.AliasAttributes == nil {
			p.SignIn.EmailAsUsername, p.SignIn.EmailAlias = previous.SignIn.EmailAsUsername, previous.SignIn.EmailAlias
		}
		if req.UsernameConfiguration == nil {
			p.SignIn.CaseSensitive = previous.SignIn.CaseSensitive
		}
		if !hasJSONValue(rawPolicy) {
			p.PasswordPolicy = previous.PasswordPolicy
		}
		if req.Schema == nil {
			p.SchemaAttributes = previous.SchemaAttributes
		}
		if req.AutoVerifiedAttributes == nil {
			p.AutoVerifiedAttributes = previous.AutoVerifiedAttributes
		}
		if req.AccountRecoverySetting == nil {
			p.AccountRecoverySetting = previous.AccountRecoverySetting
		}
		if req.AdminCreateUserConfig == nil {
			p.AdminCreateUserConfig = previous.AdminCreateUserConfig
		}
	}
	if err = s.cognito.SavePool(r.Context(), p); err != nil {
		if errors.Is(err, errPoolSignInConfigImmutable) {
			cognitoJSONError(w, 400, "InvalidParameterException", err.Error())
		} else {
			authInternalError(w, err, "CreateUserPool")
		}
		return
	}
	cognitoJSONResponse(w, 200, map[string]any{"UserPool": s.poolResponse(p)})
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
	if err := validateNativeFields(raw, []string{"MinimumLength", "RequireUppercase", "RequireLowercase", "RequireNumbers", "RequireDigits", "RequireSymbols", "TemporaryPasswordValidityDays", "min_length", "require_uppercase", "require_lowercase", "require_digits", "require_symbols", "temporary_password_validity_days"}, nil); err != nil {
		return nil, err
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
	clientValidityRequest
	ClientSecret        *string  `json:"ClientSecret"`
	ReadAttributes      []string `json:"ReadAttributes"`
	WriteAttributes     []string `json:"WriteAttributes"`
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
		cognitoJSONError(w, 400, "InvalidParameterException", "UserPoolId and ClientName are required")
		return
	}
	if req.ClientID != "" && s.devProfile != DevProfileLegacyFixtures {
		cognitoJSONError(w, 400, "InvalidParameterException", "ClientId requires the legacy-fixtures development profile")
		return
	}
	flows, minutes, err := createClientAuthConfig(req)
	if err != nil {
		cognitoJSONError(w, 400, "InvalidParameterException", err.Error())
		return
	}
	validity, err := normalizeClientValidity(req.clientValidityRequest)
	if err != nil {
		cognitoJSONError(w, 400, "InvalidParameterException", err.Error())
		return
	}
	p, err := s.cognito.LookupPool(r.Context(), req.UserPoolID)
	if errors.Is(err, sql.ErrNoRows) {
		cognitoJSONError(w, 400, "ResourceNotFoundException", "User pool does not exist")
		return
	}
	if err != nil {
		authInternalError(w, err, "CreateUserPoolClient")
		return
	}
	schema := p.SchemaAttributes
	if schema == nil {
		schema, err = NormalizeSchema(nil)
		if err != nil {
			authInternalError(w, err, "CreateUserPoolClient")
			return
		}
	}
	reads, err := NormalizeClientAttributes(schema, req.ReadAttributes, false)
	if err != nil {
		cognitoJSONError(w, 400, "InvalidParameterException", err.Error())
		return
	}
	writes, err := NormalizeClientAttributes(schema, req.WriteAttributes, true)
	if err != nil {
		cognitoJSONError(w, 400, "InvalidParameterException", err.Error())
		return
	}
	id := req.ClientID
	if id == "" {
		id = newClientID()
	}
	secret := ""
	if req.GenerateSecret {
		secret = newClientSecret()
	}
	if req.ClientSecret != nil {
		if req.GenerateSecret || len(*req.ClientSecret) < 24 || len(*req.ClientSecret) > 64 || strings.ContainsFunc(*req.ClientSecret, func(r rune) bool {
			return !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '+')
		}) {
			cognitoJSONError(w, 400, "InvalidParameterException", "ClientSecret must contain 24 to 64 word or + characters and cannot be combined with GenerateSecret")
			return
		}
		secret = *req.ClientSecret
	}
	c := &CognitoClient{ID: id, PoolID: req.UserPoolID, Name: req.ClientName, Secret: secret, ExplicitAuthFlows: flows, AuthSessionValidity: minutes, Native: req.ClientID == "", TokenValidity: validity, ReadAttributes: reads, WriteAttributes: writes}
	if req.ClientID != "" && req.AccessTokenValidity == nil && req.IdTokenValidity == nil && req.RefreshTokenValidity == nil && req.TokenValidityUnits == nil {
		c.TokenValidity = nil
	}
	if err = s.cognito.SaveClient(r.Context(), c); err != nil {
		if errors.Is(err, errClientPoolConflict) {
			cognitoJSONError(w, 400, "InvalidParameterException", "ClientId already belongs to another user pool")
		} else {
			authInternalError(w, err, "CreateUserPoolClient")
		}
		return
	}
	cognitoJSONResponse(w, 200, map[string]any{"UserPoolClient": clientResponse(c)})
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
			"ALLOW_USER_SRP_AUTH", "ALLOW_REFRESH_TOKEN_AUTH":
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
	if _, err := s.cognito.LookupPool(ctx, req.UserPoolID); err != nil {
		if errors.Is(err, sql.ErrNoRows) && s.devProfile == DevProfileLegacyFixtures {
			cognitoJSONResponse(w, 200, map[string]any{})
			return
		}
		if errors.Is(err, sql.ErrNoRows) {
			cognitoJSONError(w, 400, "ResourceNotFoundException", "User pool does not exist")
		} else {
			authInternalError(w, err, "DeleteUserPool")
		}
		return
	}
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
	client, err := s.cognito.LookupClient(ctx, req.ClientID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) && s.devProfile == DevProfileLegacyFixtures {
			cognitoJSONResponse(w, 200, map[string]any{})
			return
		}
		if errors.Is(err, sql.ErrNoRows) {
			cognitoJSONError(w, 400, "ResourceNotFoundException", "App client does not exist")
		} else {
			authInternalError(w, err, "DeleteUserPoolClient")
		}
		return
	}
	if client.PoolID != req.UserPoolID {
		cognitoJSONError(w, 400, "ResourceNotFoundException", "App client does not exist in the user pool")
		return
	}
	if _, err = s.cognito.LookupPool(ctx, req.UserPoolID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			cognitoJSONError(w, 400, "ResourceNotFoundException", "User pool does not exist")
		} else {
			authInternalError(w, err, "DeleteUserPoolClient")
		}
		return
	}
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
