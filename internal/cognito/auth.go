// Password authentication, refresh grants and access-token authorization.
package cognito

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	authFlowUserPassword      = "USER_PASSWORD_AUTH"
	authFlowRefreshToken      = "REFRESH_TOKEN_AUTH"
	authFlowRefreshTokenAlias = "REFRESH_TOKEN"
	authFlowAdminNoSRP        = "ADMIN_NO_SRP_AUTH"
	authFlowAdminUserPassword = "ADMIN_USER_PASSWORD_AUTH"
	authFlowUserSRP           = "USER_SRP_AUTH"
	authFlowCustom            = "CUSTOM_AUTH"
)

// authorizeAccessToken adds current Cognito account and revocation checks to
// cryptographic JWT validation. Offline JWT validators deliberately do not
// consult account state and continue to accept otherwise valid signed tokens.
func (s *Handler) authorizeAccessToken(w http.ResponseWriter, r *http.Request, token, action string) (*CognitoUser, bool) {
	user, _, ok := s.checkedAccessToken(w, r, token, action)
	return user, ok
}

func (s *Handler) checkedAccessToken(w http.ResponseWriter, r *http.Request, token, action string) (*CognitoUser, map[string]interface{}, bool) {
	refuse := func() (*CognitoUser, map[string]interface{}, bool) {
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Access Token has been revoked")
		return nil, nil, false
	}
	claims, err := VerifyAccessToken(r.Context(), s.cognito, s.issuerBase, token)
	if err != nil {
		log.Debug().Err(err).Str("action", action).Msg("access token verification failed")
		return refuse()
	}
	sub, _ := claims["sub"].(string)
	user, err := s.cognito.LookupUserBySub(r.Context(), sub)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			cognitoJSONError(w, http.StatusBadRequest, "UserNotFoundException", "User does not exist")
			return nil, nil, false
		}
		authInternalError(w, err, action)
		return nil, nil, false
	}
	version, err := authVersionOf(claims)
	issuer, _ := claims["iss"].(string)
	if err != nil || !user.Enabled || version != user.AuthVersion || issuer != strings.TrimRight(s.issuerBase, "/")+"/"+user.PoolID {
		return refuse()
	}
	authTime, _ := claims["auth_time"].(float64)
	originJTI, _ := claims["origin_jti"].(string)
	if err := s.cognito.checkNotRevoked(r.Context(), user, int64(authTime), originJTI); err != nil {
		if !errors.Is(err, errTokenRevoked) {
			authInternalError(w, err, action)
			return nil, nil, false
		}
		return refuse()
	}
	return user, claims, true
}

type initiateAuthRequest struct {
	AuthFlow       string            `json:"AuthFlow"`
	ClientID       string            `json:"ClientId"`
	AuthParameters map[string]string `json:"AuthParameters"`
}

func (s *Handler) handleInitiateAuth(w http.ResponseWriter, r *http.Request) {
	var req initiateAuthRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	if !validateAuthInput(w, req) {
		return
	}
	if req.AuthFlow == authFlowAdminNoSRP || req.AuthFlow == authFlowAdminUserPassword {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", req.AuthFlow+" is only valid on AdminInitiateAuth")
		return
	}
	client, ok := s.authClient(w, r, req.ClientID, "")
	if ok {
		s.dispatchAuth(w, r, client, req, "InitiateAuth")
	}
}

// AdminInitiateAuth shares password/refresh execution with InitiateAuth; only
// the allowed entry-point flows and explicit pool binding differ.
func (s *Handler) handleAdminInitiateAuth(w http.ResponseWriter, r *http.Request) {
	var req adminInitiateAuthRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	if req.UserPoolID == "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "UserPoolId is required")
		return
	}
	input := initiateAuthRequest{AuthFlow: req.AuthFlow, ClientID: req.ClientID, AuthParameters: req.AuthParameters}
	if !validateAuthInput(w, input) {
		return
	}
	if req.AuthFlow == authFlowUserPassword {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "USER_PASSWORD_AUTH is only valid on InitiateAuth")
		return
	}
	client, ok := s.authClient(w, r, req.ClientID, req.UserPoolID)
	if ok {
		s.dispatchAuth(w, r, client, input, "AdminInitiateAuth")
	}
}

func validateAuthInput(w http.ResponseWriter, req initiateAuthRequest) bool {
	for _, field := range []struct{ name, value string }{{"ClientId", req.ClientID}, {"AuthFlow", req.AuthFlow}} {
		if field.value == "" {
			cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", field.name+" is required")
			return false
		}
	}
	return true
}

func (s *Handler) authClient(w http.ResponseWriter, r *http.Request, clientID, poolID string) (*CognitoClient, bool) {
	if poolID != "" {
		exists, err := s.cognito.PoolExists(r.Context(), poolID)
		if err != nil {
			authInternalError(w, err, "pool lookup")
			return nil, false
		}
		if !exists {
			cognitoJSONError(w, http.StatusBadRequest, "ResourceNotFoundException", fmt.Sprintf("User pool %s does not exist", poolID))
			return nil, false
		}
	}
	client, err := s.cognito.LookupClient(r.Context(), clientID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			cognitoJSONError(w, http.StatusBadRequest, "ResourceNotFoundException", fmt.Sprintf("App client %s does not exist", clientID))
			return nil, false
		}
		authInternalError(w, err, "client lookup")
		return nil, false
	}
	if poolID != "" && client.PoolID != poolID {
		cognitoJSONError(w, http.StatusBadRequest, "ResourceNotFoundException", fmt.Sprintf("App client %s does not belong to pool %s", clientID, poolID))
		return nil, false
	}
	return client, true
}

func (s *Handler) dispatchAuth(w http.ResponseWriter, r *http.Request, client *CognitoClient, req initiateAuthRequest, action string) {
	if !clientAllowsAuthFlow(client, req.AuthFlow) {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "Auth flow not enabled for this client")
		return
	}
	switch req.AuthFlow {
	case authFlowUserPassword, authFlowAdminNoSRP, authFlowAdminUserPassword:
		s.handlePasswordAuth(w, r, client, req, action)
	case authFlowRefreshToken, authFlowRefreshTokenAlias:
		s.handleInitiateAuthRefreshToken(w, r, client, req)
	case authFlowCustom:
		s.handleCustomInitiateAuth(w, r, client, req.AuthParameters)
	case authFlowUserSRP:
		s.handleSRPInitiateAuth(w, r, client, req.AuthParameters)
	default:
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "Unsupported AuthFlow: "+req.AuthFlow)
	}
}

// A nil configuration preserves legacy fixture clients. Explicit configuration
// admits only the AWS ALLOW_* flows the app client enables.
func clientAllowsAuthFlow(client *CognitoClient, flow string) bool {
	if client.ExplicitAuthFlows == nil {
		return true
	}
	var permission string
	switch flow {
	case authFlowUserPassword:
		permission = "ALLOW_USER_PASSWORD_AUTH"
	case authFlowAdminNoSRP, authFlowAdminUserPassword:
		permission = "ALLOW_ADMIN_USER_PASSWORD_AUTH"
	case authFlowRefreshToken, authFlowRefreshTokenAlias:
		permission = "ALLOW_REFRESH_TOKEN_AUTH"
	case authFlowUserSRP:
		permission = "ALLOW_USER_SRP_AUTH"
	case authFlowCustom:
		permission = "ALLOW_CUSTOM_AUTH"
	default:
		return false
	}
	for _, enabled := range client.ExplicitAuthFlows {
		if enabled == permission || enabled == flow ||
			(permission == "ALLOW_ADMIN_USER_PASSWORD_AUTH" && enabled == authFlowAdminNoSRP) ||
			(permission == "ALLOW_CUSTOM_AUTH" && enabled == "CUSTOM_AUTH_FLOW_ONLY") {
			return true
		}
	}
	return false
}

func (s *Handler) handlePasswordAuth(w http.ResponseWriter, r *http.Request, client *CognitoClient, req initiateAuthRequest, action string) {
	login, password := req.AuthParameters["USERNAME"], req.AuthParameters["PASSWORD"]
	if login == "" || password == "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "USERNAME and PASSWORD are required for "+req.AuthFlow)
		return
	}
	if err := verifySecretHash(client.Secret, login, client.ID, req.AuthParameters["SECRET_HASH"]); err != nil {
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Incorrect username or password.")
		return
	}
	user, err := s.cognito.ResolveSignInUser(r.Context(), client.PoolID, login)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			cognitoJSONError(w, http.StatusBadRequest, "UserNotFoundException", fmt.Sprintf("User %q does not exist", login))
			return
		}
		authInternalError(w, err, action)
		return
	}
	if !user.Enabled {
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "User is disabled.")
		return
	}
	if err := compareUserPasswordHash(user.PasswordHash, password); err != nil {
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Incorrect username or password.")
		return
	}
	switch user.Status {
	case "UNCONFIRMED":
		cognitoJSONError(w, http.StatusBadRequest, "UserNotConfirmedException", "User is not confirmed.")
		return
	case "RESET_REQUIRED":
		cognitoJSONError(w, http.StatusBadRequest, "PasswordResetRequiredException", "Password reset required for the user")
		return
	case "FORCE_CHANGE_PASSWORD":
		expired, err := s.temporaryPasswordExpired(r.Context(), user)
		if err != nil {
			authInternalError(w, err, action)
			return
		}
		if expired {
			cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Temporary password has expired and must be reset by an administrator.")
			return
		}
		s.issueNewPasswordChallenge(w, r, client.PoolID, client.ID, user)
		return
	}
	if user.MFAEnabled {
		s.issueMFAChallenge(w, r, client.PoolID, client.ID, user)
		return
	}
	s.writeAuthenticated(w, r, client.PoolID, client.ID, user, action)
}

// Password and SRP flows use the same temporary-password lifetime after
// checking their password proof. Zero policy validity uses AWS's seven days.
func (s *Handler) temporaryPasswordExpired(ctx context.Context, user *CognitoUser) (bool, error) {
	if user.Status != "FORCE_CHANGE_PASSWORD" || user.PasswordChangedAt <= 0 {
		return false, nil
	}
	policy, err := loadPoolPasswordPolicy(ctx, s.cognito, user.PoolID)
	if err != nil {
		return false, err
	}
	validityDays := 7
	if policy != nil && policy.TemporaryPasswordValidityDays > 0 {
		validityDays = policy.TemporaryPasswordValidityDays
	}
	return !s.cognito.now().Before(time.Unix(user.PasswordChangedAt, 0).Add(time.Duration(validityDays) * 24 * time.Hour)), nil
}

// Every completed password/challenge authentication binds its account version
// before minting. A reset or disable during proof verification cannot mint a
// fresh grant from an older account snapshot.
func (s *Handler) writeAuthenticated(w http.ResponseWriter, r *http.Request, poolID, clientID string, user *CognitoUser, action string) {
	current, ok := s.currentAuthenticationUser(w, r, poolID, user, action)
	if !ok {
		return
	}
	grant := tokenGrant{AuthTime: s.cognito.now(), OriginJTI: newJTI()}
	grant.AuthVersion = current.AuthVersion
	s.writeTokenResult(w, r, clientID, current, grant, true, action)
}

func (s *Handler) currentAuthenticationUser(w http.ResponseWriter, r *http.Request, poolID string, authenticated *CognitoUser, action string) (*CognitoUser, bool) {
	current, err := s.cognito.LookupUserBySub(r.Context(), authenticated.Sub)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		authInternalError(w, err, action)
		return nil, false
	}
	if err != nil || !current.Enabled || current.PoolID != poolID || authenticated.PoolID != poolID || current.AuthVersion != authenticated.AuthVersion {
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Authentication is no longer valid")
		return nil, false
	}
	return current, true
}

// Password sign-in issues all three tokens. Refresh renews access and ID tokens
// using the original grant while leaving its refresh token unrotated.
func (s *Handler) writeTokenResult(w http.ResponseWriter, r *http.Request, clientID string, user *CognitoUser, grant tokenGrant, includeRefresh bool, action string) {
	ctx := r.Context()
	client, err := s.cognito.LookupClient(ctx, clientID)
	if err != nil || client.PoolID != user.PoolID {
		if err == nil {
			err = errors.New("client does not belong to user pool")
		}
		authInternalError(w, err, action)
		return
	}
	accessTTL, idTTL, refreshTTL, err := s.clientTokenDurations(client)
	if err != nil {
		authInternalError(w, err, action)
		return
	}
	access, err := SignAccessToken(ctx, s.cognito, s.issuerBase, user.PoolID, clientID, user.Sub, user.Email, grant, accessTTL)
	if err != nil {
		authInternalError(w, err, action)
		return
	}
	id, err := SignIDToken(ctx, s.cognito, s.issuerBase, user.PoolID, clientID, user, grant, idTTL)
	if err != nil {
		authInternalError(w, err, action)
		return
	}
	result := map[string]interface{}{"AccessToken": access, "IdToken": id, "ExpiresIn": int(accessTTL.Seconds()), "TokenType": "Bearer"}
	if includeRefresh {
		refresh, err := SignRefreshToken(ctx, s.cognito, s.issuerBase, user.PoolID, clientID, user.Sub, grant, refreshTTL)
		if err != nil {
			authInternalError(w, err, action)
			return
		}
		result["RefreshToken"] = refresh
	}
	current, ok := s.currentAuthenticationUser(w, r, user.PoolID, user, action)
	if !ok {
		return
	}
	if err := s.cognito.checkNotRevoked(ctx, current, grant.AuthTime.Unix(), grant.OriginJTI); err != nil {
		if !errors.Is(err, errTokenRevoked) {
			authInternalError(w, err, action)
			return
		}
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Authentication is no longer valid")
		return
	}
	cognitoJSONResponse(w, http.StatusOK, map[string]interface{}{"AuthenticationResult": result})
}

func (s *Handler) handleInitiateAuthRefreshToken(w http.ResponseWriter, r *http.Request, client *CognitoClient, req initiateAuthRequest) {
	refreshToken := req.AuthParameters["REFRESH_TOKEN"]
	if refreshToken == "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "REFRESH_TOKEN is required for "+req.AuthFlow)
		return
	}
	refuse := func() {
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Refresh Token has been revoked")
	}
	ctx := r.Context()
	claims, err := VerifyRefreshToken(ctx, s.cognito, s.issuerBase, client.ID, refreshToken)
	if err != nil {
		log.Debug().Err(err).Msg("Refresh token verification failed")
		refuse()
		return
	}
	issuer, _ := claims["iss"].(string)
	if issuer != strings.TrimRight(s.issuerBase, "/")+"/"+client.PoolID {
		refuse()
		return
	}
	sub, _ := claims["sub"].(string)
	user, err := s.cognito.LookupUserBySub(ctx, sub)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			refuse()
			return
		}
		authInternalError(w, err, "refresh")
		return
	}
	grant, err := grantOf(claims)
	if err != nil || !user.Enabled || user.PoolID != client.PoolID || grant.AuthVersion != user.AuthVersion {
		refuse()
		return
	}
	config, err := s.cognito.GetPoolSignInConfig(ctx, client.PoolID)
	if err != nil {
		authInternalError(w, err, "refresh")
		return
	}
	secretUsername := user.Username
	if config.EmailAsUsername {
		secretUsername = user.Sub
	}
	if err := verifySecretHash(client.Secret, secretUsername, client.ID, req.AuthParameters["SECRET_HASH"]); err != nil {
		refuse()
		return
	}
	if err := s.cognito.checkNotRevoked(ctx, user, grant.AuthTime.Unix(), grant.OriginJTI); err != nil {
		if !errors.Is(err, errTokenRevoked) {
			authInternalError(w, err, "refresh")
			return
		}
		refuse()
		return
	}
	s.writeTokenResult(w, r, client.ID, user, grant, false, "refresh")
}

func authInternalError(w http.ResponseWriter, err error, action string) {
	log.Error().Err(err).Str("action", action).Msg("Cognito authentication failed")
	cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", "Authentication failed due to an internal error")
}

// authorizeClientAccessToken retains the authenticated client for native
// attribute permissions. Administrator operations use their separate lookup.
func (s *Handler) authorizeClientAccessToken(w http.ResponseWriter, r *http.Request, token, action string) (*CognitoUser, *CognitoClient, bool) {
	user, claims, ok := s.checkedAccessToken(w, r, token, action)
	if !ok {
		return nil, nil, false
	}
	clientID, _ := claims["client_id"].(string)
	client, err := s.cognito.LookupClient(r.Context(), clientID)
	if err != nil || client.PoolID != user.PoolID {
		cognitoJSONError(w, 400, "NotAuthorizedException", "App client is no longer valid")
		return nil, nil, false
	}
	if client.Native && !containsScope(claims["scope"], "aws.cognito.signin.user.admin") {
		cognitoJSONError(w, 400, "NotAuthorizedException", "Access Token does not have required scopes")
		return nil, nil, false
	}
	return user, client, true
}

func containsScope(value any, required string) bool {
	scope, _ := value.(string)
	for _, item := range strings.Fields(scope) {
		if item == required {
			return true
		}
	}
	return false
}
