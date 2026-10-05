// Software-token (TOTP) enrolment, MFA preference and token revocation for
// the local Cognito dev service.
//
// These are the Cognito operations the platform's provider-owned two-factor
// flow uses (organisation backend design V2):
//
//   - AssociateSoftwareToken(AccessToken)            → SecretCode
//   - VerifySoftwareToken(AccessToken, UserCode)     → Status SUCCESS
//   - SetUserMFAPreference(AccessToken, settings)    → enable/disable
//   - AdminSetUserMFAPreference(pool, user, settings) (support reset)
//   - GlobalSignOut(AccessToken), AdminUserGlobalSignOut(pool, user)
//   - RevokeToken(Token, ClientId)                   → one refresh token
//
// Semantics follow Cognito: associating issues a new pending secret;
// verifying confirms it without enabling MFA; only a verified software token
// can be enabled; a global sign-out revokes every token authenticated at or
// before it and RevokeToken revokes one refresh token and the access tokens
// minted from it, for Cognito's own APIs. A resource server that validates
// JWTs offline still accepts a revoked access token until it expires, which
// is exactly why the platform keeps its own session cutoff in Trust.
//
// Only the AccessToken form of Associate/VerifySoftwareToken is emulated; the
// Session form belongs to the MFA_SETUP challenge, which the platform does not
// issue (its pool leaves MFA optional).
package main

import (
	"crypto/rand"
	"database/sql"
	"encoding/base32"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

type associateSoftwareTokenRequest struct {
	AccessToken string `json:"AccessToken"`
	Session     string `json:"Session"`
}

type verifySoftwareTokenRequest struct {
	AccessToken        string `json:"AccessToken"`
	Session            string `json:"Session"`
	UserCode           string `json:"UserCode"`
	FriendlyDeviceName string `json:"FriendlyDeviceName"`
}

type mfaSettings struct {
	Enabled      bool `json:"Enabled"`
	PreferredMfa bool `json:"PreferredMfa"`
}

type setUserMFAPreferenceRequest struct {
	AccessToken              string       `json:"AccessToken"`
	SoftwareTokenMfaSettings *mfaSettings `json:"SoftwareTokenMfaSettings"`
}

type adminSetUserMFAPreferenceRequest struct {
	UserPoolID               string       `json:"UserPoolId"`
	Username                 string       `json:"Username"`
	SoftwareTokenMfaSettings *mfaSettings `json:"SoftwareTokenMfaSettings"`
}

type globalSignOutRequest struct {
	AccessToken string `json:"AccessToken"`
}

type adminUserGlobalSignOutRequest struct {
	UserPoolID string `json:"UserPoolId"`
	Username   string `json:"Username"`
}

type revokeTokenRequest struct {
	Token    string `json:"Token"`
	ClientID string `json:"ClientId"`
}

// newTOTPSecret returns a 160-bit base32 secret, the size authenticator apps
// and Cognito use.
func newTOTPSecret() (string, error) {
	var raw [20]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(raw[:]), nil
}

func (s *Server) handleAssociateSoftwareToken(w http.ResponseWriter, r *http.Request) {
	var req associateSoftwareTokenRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	if req.Session != "" && req.AccessToken == "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException",
			"AssociateSoftwareToken with Session (MFA_SETUP) is not supported by the local Cognito dev service")
		return
	}
	user, ok := s.authorizeAccessToken(w, r, req.AccessToken, "AssociateSoftwareToken")
	if !ok {
		return
	}
	secret, err := newTOTPSecret()
	if err != nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", "failed to generate secret")
		return
	}
	if err := s.cognito.SetPendingTOTPSecret(r.Context(), user.Sub, secret); err != nil {
		log.Error().Err(err).Msg("SetPendingTOTPSecret failed")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}
	cognitoJSONResponse(w, http.StatusOK, map[string]interface{}{"SecretCode": secret})
}

func (s *Server) handleVerifySoftwareToken(w http.ResponseWriter, r *http.Request) {
	var req verifySoftwareTokenRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	if req.Session != "" && req.AccessToken == "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException",
			"VerifySoftwareToken with Session (MFA_SETUP) is not supported by the local Cognito dev service")
		return
	}
	user, ok := s.authorizeAccessToken(w, r, req.AccessToken, "VerifySoftwareToken")
	if !ok {
		return
	}
	if user.PendingTOTPSecret == "" {
		cognitoJSONError(w, http.StatusBadRequest, "SoftwareTokenMFANotFoundException",
			"Software token MFA has not been associated")
		return
	}
	if !isSixDigits(req.UserCode) || validateTOTPCode(user.PendingTOTPSecret, req.UserCode) != nil {
		// Cognito reports a wrong enrolment code as EnableSoftwareTokenMFAException.
		cognitoJSONError(w, http.StatusBadRequest, "EnableSoftwareTokenMFAException", "Code mismatch")
		return
	}
	if err := s.cognito.ConfirmPendingTOTPSecret(r.Context(), user.Sub); err != nil {
		log.Error().Err(err).Msg("ConfirmPendingTOTPSecret failed")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}
	cognitoJSONResponse(w, http.StatusOK, map[string]interface{}{"Status": "SUCCESS"})
}

// applyMFAPreference applies software-token settings to a user. Enabling
// requires a verified software token; nil settings change nothing.
func (s *Server) applyMFAPreference(w http.ResponseWriter, r *http.Request, user *CognitoUser, settings *mfaSettings) bool {
	if settings == nil {
		return true
	}
	if settings.Enabled && !user.SoftwareTokenVerified {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException",
			"User has not verified software token mfa")
		return false
	}
	if err := s.cognito.SetMFAEnabled(r.Context(), user.Sub, settings.Enabled); err != nil {
		log.Error().Err(err).Msg("SetMFAEnabled failed")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return false
	}
	return true
}

func (s *Server) handleSetUserMFAPreference(w http.ResponseWriter, r *http.Request) {
	var req setUserMFAPreferenceRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	user, ok := s.authorizeAccessToken(w, r, req.AccessToken, "SetUserMFAPreference")
	if !ok {
		return
	}
	if !s.applyMFAPreference(w, r, user, req.SoftwareTokenMfaSettings) {
		return
	}
	cognitoJSONResponse(w, http.StatusOK, map[string]interface{}{})
}

// lookupAdminUser resolves an admin action's pool and Username (sub or
// email), writing Cognito's error on failure.
func (s *Server) lookupAdminUser(w http.ResponseWriter, r *http.Request, poolID, username, action string) (*CognitoUser, bool) {
	if poolID == "" || username == "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "UserPoolId and Username are required")
		return nil, false
	}
	exists, err := s.cognito.PoolExists(r.Context(), poolID)
	if err != nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return nil, false
	}
	if !exists {
		cognitoJSONError(w, http.StatusBadRequest, "ResourceNotFoundException", "User pool "+poolID+" does not exist")
		return nil, false
	}
	user, err := s.cognito.LookupPoolUser(r.Context(), poolID, username)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			cognitoJSONError(w, http.StatusBadRequest, "UserNotFoundException", "User does not exist")
			return nil, false
		}
		log.Error().Err(err).Str("action", action).Msg("LookupPoolUser failed")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return nil, false
	}
	return user, true
}

func (s *Server) handleAdminSetUserMFAPreference(w http.ResponseWriter, r *http.Request) {
	var req adminSetUserMFAPreferenceRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	user, ok := s.lookupAdminUser(w, r, req.UserPoolID, req.Username, "AdminSetUserMFAPreference")
	if !ok {
		return
	}
	if !s.applyMFAPreference(w, r, user, req.SoftwareTokenMfaSettings) {
		return
	}
	cognitoJSONResponse(w, http.StatusOK, map[string]interface{}{})
}

func (s *Server) handleGlobalSignOut(w http.ResponseWriter, r *http.Request) {
	var req globalSignOutRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	user, ok := s.authorizeAccessToken(w, r, req.AccessToken, "GlobalSignOut")
	if !ok {
		return
	}
	if err := s.cognito.RevokeUserTokens(r.Context(), user.Sub, time.Now().Unix()); err != nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}
	cognitoJSONResponse(w, http.StatusOK, map[string]interface{}{})
}

func (s *Server) handleAdminUserGlobalSignOut(w http.ResponseWriter, r *http.Request) {
	var req adminUserGlobalSignOutRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	user, ok := s.lookupAdminUser(w, r, req.UserPoolID, req.Username, "AdminUserGlobalSignOut")
	if !ok {
		return
	}
	if err := s.cognito.RevokeUserTokens(r.Context(), user.Sub, time.Now().Unix()); err != nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}
	cognitoJSONResponse(w, http.StatusOK, map[string]interface{}{})
}

// handleRevokeToken revokes one refresh token (and, through origin_jti, the
// access tokens minted from it). Revoking an already revoked token succeeds;
// an access token or a forged token is refused.
func (s *Server) handleRevokeToken(w http.ResponseWriter, r *http.Request) {
	var req revokeTokenRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	if req.Token == "" || req.ClientID == "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "Token and ClientId are required")
		return
	}
	claims, err := VerifyRefreshToken(r.Context(), s.cognito, s.issuerBase, req.ClientID, req.Token)
	if err != nil {
		if strings.Contains(err.Error(), "token_use") {
			cognitoJSONError(w, http.StatusBadRequest, "UnsupportedTokenTypeException", "Only refresh tokens can be revoked")
			return
		}
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Invalid token")
		return
	}
	grant, err := grantOf(claims)
	if err != nil {
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Invalid token")
		return
	}
	if err := s.cognito.RevokeRefreshToken(r.Context(), grant.OriginJTI); err != nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}
	cognitoJSONResponse(w, http.StatusOK, map[string]interface{}{})
}
