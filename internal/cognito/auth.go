// Password authentication, refresh grants and access-token authorization.
package cognito

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"

	"github.com/rs/zerolog/log"
	"golang.org/x/crypto/bcrypt"
)

const (
	authFlowUserPassword = "USER_PASSWORD_AUTH"
	authFlowRefreshToken = "REFRESH_TOKEN_AUTH"
	authFlowAdminNoSRP   = "ADMIN_NO_SRP_AUTH"
	authFlowUserSRP      = "USER_SRP_AUTH"
)

// authorizeAccessToken is the access-token check every AccessToken-bearing
// action shares: signature, expiry and token_use (VerifyAccessToken), the
// user still exists, and the token was not revoked by a global sign-out or
// RevokeToken. On failure it writes Cognito's response and returns false.
func (s *Handler) authorizeAccessToken(w http.ResponseWriter, r *http.Request, token, action string) (*CognitoUser, bool) {
	if token == "" {
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Access Token has been revoked")
		return nil, false
	}
	ctx := r.Context()
	claims, err := VerifyAccessToken(ctx, s.cognito, s.issuerBase, token)
	if err != nil {
		log.Debug().Err(err).Str("action", action).Msg("access token verification failed")
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Access Token has been revoked")
		return nil, false
	}
	sub, _ := claims["sub"].(string)
	user, err := s.cognito.LookupUserBySub(ctx, sub)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			cognitoJSONError(w, http.StatusBadRequest, "UserNotFoundException", "User does not exist")
			return nil, false
		}
		log.Error().Err(err).Str("action", action).Msg("LookupUserBySub failed")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return nil, false
	}
	authTime, _ := claims["auth_time"].(float64)
	originJTI, _ := claims["origin_jti"].(string)
	if err := s.cognito.checkNotRevoked(ctx, user, int64(authTime), originJTI); err != nil {
		if !errors.Is(err, errTokenRevoked) {
			log.Error().Err(err).Str("action", action).Msg("revocation check failed")
			cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
			return nil, false
		}
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Access Token has been revoked")
		return nil, false
	}
	return user, true
}

// initiateAuthRequest mirrors the AWS password and refresh-authentication input.
// SECRET_HASH is required when the configured app client has a secret.
type initiateAuthRequest struct {
	AuthFlow       string            `json:"AuthFlow"`
	ClientID       string            `json:"ClientId"`
	AuthParameters map[string]string `json:"AuthParameters"`
}

// handleInitiateAuth supports password authentication and non-rotating refresh.
// MFA users receive a challenge before any tokens are issued. Unknown clients
// and users, wrong passwords, and invalid refresh tokens use AWS typed errors.
func (s *Handler) handleInitiateAuth(w http.ResponseWriter, r *http.Request) {
	var req initiateAuthRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	if req.ClientID == "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "ClientId is required")
		return
	}
	if req.AuthFlow == "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "AuthFlow is required")
		return
	}

	ctx := r.Context()

	// Resolve the client → pool. Both flows need this; doing it up-front also
	// gives us the cleanest error path for an unknown ClientId (which AWS
	// surfaces as ResourceNotFoundException, not UserNotFoundException).
	client, err := s.cognito.LookupClient(ctx, req.ClientID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			cognitoJSONError(w, http.StatusBadRequest, "ResourceNotFoundException",
				fmt.Sprintf("App client %s does not exist", req.ClientID))
			return
		}
		log.Error().Err(err).Msg("LookupClient failed in InitiateAuth")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}

	switch req.AuthFlow {
	case authFlowUserPassword:
		s.handleInitiateAuthUserPassword(w, r, client, req)
	case authFlowRefreshToken:
		s.handleInitiateAuthRefreshToken(w, r, client, req)
	case authFlowAdminNoSRP:
		// Admin authentication has its own operation.
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException",
			"ADMIN_NO_SRP_AUTH is only valid on AdminInitiateAuth (GO-COGNITO-5)")
	case authFlowUserSRP:
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException",
			"USER_SRP_AUTH is not supported by the local Cognito dev service (design §3h)")
	default:
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException",
			fmt.Sprintf("Unsupported AuthFlow: %s", req.AuthFlow))
	}
}

// handleInitiateAuthUserPassword owns the USER_PASSWORD_AUTH branch. The
// dispatch happens in handleInitiateAuth; this function assumes the caller
// has already validated AuthFlow and resolved the pool from ClientId.
func (s *Handler) handleInitiateAuthUserPassword(w http.ResponseWriter, r *http.Request, client *CognitoClient, req initiateAuthRequest) {
	username := req.AuthParameters["USERNAME"]
	password := req.AuthParameters["PASSWORD"]
	if username == "" || password == "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException",
			"USERNAME and PASSWORD are required for USER_PASSWORD_AUTH")
		return
	}

	// SECRET_HASH (§3k) — only enforced when the client has a secret.
	// Mismatch / missing-when-required → NotAuthorizedException, matching
	// the typed-error decoder in go/libs/platform-lib/.../cognito.go.
	if err := verifySecretHash(client.Secret, username, client.ID, req.AuthParameters["SECRET_HASH"]); err != nil {
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Incorrect username or password.")
		return
	}

	ctx := r.Context()
	user, err := s.cognito.LookupUserByEmail(ctx, client.PoolID, username)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			cognitoJSONError(w, http.StatusBadRequest, "UserNotFoundException",
				fmt.Sprintf("User %q does not exist", username))
			return
		}
		log.Error().Err(err).Msg("LookupUserByEmail failed in InitiateAuth")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}

	// bcrypt-verify against the persisted hash. CompareHashAndPassword returns
	// nil only on a successful match.
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)); err != nil {
		// Same error code for missing-hash users (e.g. seeded with empty
		// password) — never surface "wrong password" vs. "no password set"
		// distinction to the client.
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Incorrect username or password.")
		return
	}

	// MFA branch . Issue a SOFTWARE_TOKEN_MFA challenge:
	// generate an HMAC-stamped opaque session bound to the pool's signing
	// key, persist a challenge_sessions row with a 5-minute TTL, and return
	// {ChallengeName, Session, ChallengeParameters} WITHOUT
	// AuthenticationResult. The caller proves possession via a 6-digit
	// code through RespondToAuthChallenge.
	if user.MFAEnabled {
		s.issueMFAChallenge(w, r, client.PoolID, client.ID, user)
		return
	}

	// Cognito omits ChallengeName/Session when there is no challenge. The
	// Python provider tests presence (`if "ChallengeName" in response`,
	// cognito.py:285) and the Go provider checks for empty string
	// (cognito.go:892); either way, omitting is the right call.
	s.writeAuthenticated(w, r, client.PoolID, client.ID, user, "InitiateAuth")
}

// writeAuthenticated completes an authentication: it mints an access and a
// refresh token that share one authentication time (now) and writes the
// AuthenticationResult. Every path that authenticates a person ends here, so
// a session's age always starts at its password (and any challenge), and
// REFRESH_TOKEN_AUTH is the only path that mints from an earlier one.
func (s *Handler) writeAuthenticated(
	w http.ResponseWriter,
	r *http.Request,
	poolID, clientID string,
	user *CognitoUser,
	action string,
) {
	ctx := r.Context()
	grant := newTokenGrant()
	access, err := SignAccessToken(ctx, s.cognito, s.issuerBase, poolID, clientID, user.Sub, user.Email, grant, s.accessTokenTTL)
	if err != nil {
		log.Error().Err(err).Str("action", action).Msg("SignAccessToken failed")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", "failed to mint access token")
		return
	}
	refresh, err := SignRefreshToken(ctx, s.cognito, s.issuerBase, poolID, clientID, user.Sub, grant, s.refreshTokenTTL)
	if err != nil {
		log.Error().Err(err).Str("action", action).Msg("SignRefreshToken failed")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", "failed to mint refresh token")
		return
	}
	cognitoJSONResponse(w, http.StatusOK, map[string]interface{}{
		"AuthenticationResult": map[string]interface{}{
			"AccessToken":  access,
			"RefreshToken": refresh,
			"ExpiresIn":    int(s.accessTokenTTL.Seconds()),
			"TokenType":    "Bearer",
		},
	})
}

// handleInitiateAuthRefreshToken verifies the persisted grant and renews only
// the access token. Refresh tokens and authentication age remain unchanged.
func (s *Handler) handleInitiateAuthRefreshToken(w http.ResponseWriter, r *http.Request, client *CognitoClient, req initiateAuthRequest) {
	refreshToken := req.AuthParameters["REFRESH_TOKEN"]
	if refreshToken == "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException",
			"REFRESH_TOKEN is required for REFRESH_TOKEN_AUTH")
		return
	}

	ctx := r.Context()

	// SECRET_HASH on REFRESH_TOKEN_AUTH (§3k). The Go provider derives
	// `username` from the refresh token's `sub` claim before computing the
	// hash (cognito.go:1050), so we do the same: parse-without-verify the
	// `sub` first, then enforce. Failures here collapse to
	// NotAuthorizedException to avoid leaking which mode hit.
	if client.Secret != "" {
		sub, perr := refreshTokenSubUnverified(refreshToken)
		if perr != nil {
			cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Refresh Token has been revoked")
			return
		}
		if err := verifySecretHash(client.Secret, sub, client.ID, req.AuthParameters["SECRET_HASH"]); err != nil {
			cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Refresh Token has been revoked")
			return
		}
	}

	claims, err := VerifyRefreshToken(ctx, s.cognito, s.issuerBase, client.ID, refreshToken)
	if err != nil {
		log.Debug().Err(err).Msg("Refresh token verification failed")
		// Both invalid signature and expired refresh map to the same error:
		// the Go provider squashes them at cognito.go:419-422, and the
		// Python provider does the same at cognito.py:104-105.
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Refresh Token has been revoked")
		return
	}

	sub, _ := claims["sub"].(string)
	if sub == "" {
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Refresh Token has been revoked")
		return
	}

	// Look up the user to fill in `email` on the new access token. If the
	// user has been deleted since the refresh token was issued, fail with
	// NotAuthorizedException (a deleted user must not be able to mint new
	// access tokens via a stale refresh).
	user, err := s.cognito.LookupUserBySub(ctx, sub)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Refresh Token has been revoked")
			return
		}
		log.Error().Err(err).Msg("LookupUserBySub failed in REFRESH_TOKEN_AUTH")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}

	// The new access token belongs to the refresh token's authentication:
	// refresh renews expiry, never authentication age. A refresh token
	// revoked by RevokeToken or a global sign-out mints nothing.
	grant, err := grantOf(claims)
	if err != nil {
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Refresh Token has been revoked")
		return
	}
	if err := s.cognito.checkNotRevoked(ctx, user, grant.AuthTime.Unix(), grant.OriginJTI); err != nil {
		if !errors.Is(err, errTokenRevoked) {
			log.Error().Err(err).Msg("revocation check failed in REFRESH_TOKEN_AUTH")
			cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
			return
		}
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Refresh Token has been revoked")
		return
	}
	access, err := SignAccessToken(ctx, s.cognito, s.issuerBase, user.PoolID, client.ID, user.Sub, user.Email, grant, s.accessTokenTTL)
	if err != nil {
		log.Error().Err(err).Msg("SignAccessToken failed in REFRESH_TOKEN_AUTH")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", "failed to mint access token")
		return
	}

	// Return only the renewed access token; callers retain the original refresh.
	resp := map[string]interface{}{
		"AuthenticationResult": map[string]interface{}{
			"AccessToken": access,
			"ExpiresIn":   int(s.accessTokenTTL.Seconds()),
			"TokenType":   "Bearer",
		},
	}
	cognitoJSONResponse(w, http.StatusOK, resp)
}

// handleAdminInitiateAuth supports only ADMIN_NO_SRP_AUTH.
//
// Behaviour:
//   - Required: UserPoolId, ClientId, AuthParameters.{USERNAME, PASSWORD}.
//   - AuthFlow MUST be ADMIN_NO_SRP_AUTH; any other value →
//     InvalidParameterException.
//   - Pool missing → ResourceNotFoundException.
//   - Client missing → ResourceNotFoundException.
//   - User missing → UserNotFoundException.
//   - Wrong password → NotAuthorizedException.
//   - A user with MFA enabled gets the SOFTWARE_TOKEN_MFA challenge, exactly
//     as InitiateAuth issues it: real Cognito challenges admin flows too, so
//     no entry point here mints tokens past an enabled factor.
//   - Otherwise: mint access+refresh tokens.
func (s *Handler) handleAdminInitiateAuth(w http.ResponseWriter, r *http.Request) {
	var req adminInitiateAuthRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	if req.UserPoolID == "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "UserPoolId is required")
		return
	}
	if req.ClientID == "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "ClientId is required")
		return
	}
	if req.AuthFlow != authFlowAdminNoSRP {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException",
			fmt.Sprintf("AdminInitiateAuth supports only ADMIN_NO_SRP_AUTH; got %q", req.AuthFlow))
		return
	}
	username := req.AuthParameters["USERNAME"]
	password := req.AuthParameters["PASSWORD"]
	if username == "" || password == "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException",
			"USERNAME and PASSWORD are required for ADMIN_NO_SRP_AUTH")
		return
	}

	ctx := r.Context()

	// Pool check before client check — Cognito does the same and the
	// Directory service surfaces a clearer error to the operator.
	exists, err := s.cognito.PoolExists(ctx, req.UserPoolID)
	if err != nil {
		log.Error().Err(err).Msg("PoolExists failed in AdminInitiateAuth")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}
	if !exists {
		cognitoJSONError(w, http.StatusBadRequest, "ResourceNotFoundException",
			fmt.Sprintf("User pool %s does not exist", req.UserPoolID))
		return
	}

	client, err := s.cognito.LookupClient(ctx, req.ClientID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			cognitoJSONError(w, http.StatusBadRequest, "ResourceNotFoundException",
				fmt.Sprintf("App client %s does not exist", req.ClientID))
			return
		}
		log.Error().Err(err).Msg("LookupClient failed in AdminInitiateAuth")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}
	// The client must belong to the supplied pool — otherwise the caller
	// is mixing identifiers across pools and we should fail loud.
	if client.PoolID != req.UserPoolID {
		cognitoJSONError(w, http.StatusBadRequest, "ResourceNotFoundException",
			fmt.Sprintf("App client %s does not belong to pool %s", req.ClientID, req.UserPoolID))
		return
	}

	// SECRET_HASH (§3k) before lookup — same behaviour as InitiateAuth
	// USER_PASSWORD_AUTH. Failure collapses to NotAuthorizedException.
	if err := verifySecretHash(client.Secret, username, client.ID, req.AuthParameters["SECRET_HASH"]); err != nil {
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Incorrect username or password.")
		return
	}

	user, err := s.cognito.LookupUserByEmail(ctx, req.UserPoolID, username)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			cognitoJSONError(w, http.StatusBadRequest, "UserNotFoundException",
				fmt.Sprintf("User %q does not exist", username))
			return
		}
		log.Error().Err(err).Msg("LookupUserByEmail failed in AdminInitiateAuth")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)); err != nil {
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Incorrect username or password.")
		return
	}

	if user.MFAEnabled {
		s.issueMFAChallenge(w, r, req.UserPoolID, client.ID, user)
		return
	}
	s.writeAuthenticated(w, r, req.UserPoolID, client.ID, user, "AdminInitiateAuth")
}
