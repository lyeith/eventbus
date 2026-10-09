// Software-token enrollment, MFA preference and token revocation operations.
// Pending secrets require verification before enablement. Cognito API calls
// enforce revocation; offline JWT consumers retain their own session policies.
package cognito

import (
	"crypto/hmac"
	"crypto/rand"
	"database/sql"
	"encoding/base32"
	"errors"
	"net/http"
	"strings"

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
	Token        string `json:"Token"`
	ClientID     string `json:"ClientId"`
	ClientSecret string `json:"ClientSecret"`
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

func (s *Handler) handleAssociateSoftwareToken(w http.ResponseWriter, r *http.Request) {
	var req associateSoftwareTokenRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	if req.Session != "" && req.AccessToken == "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException",
			"AssociateSoftwareToken with Session (MFA_SETUP) is not supported by the local Cognito dev service")
		return
	}
	user, claims, ok := s.checkedAccessToken(w, r, req.AccessToken, "AssociateSoftwareToken")
	if !ok {
		return
	}
	secret, err := newTOTPSecret()
	if err != nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", "failed to generate secret")
		return
	}
	if err := s.cognito.setPendingTOTPSecret(r.Context(), user.Sub, secret, mfaGrantAuthorization(user, claims)); err != nil {
		writeMFATransitionError(w, err, "AssociateSoftwareToken", true)
		return
	}
	cognitoJSONResponse(w, http.StatusOK, map[string]interface{}{"SecretCode": secret})
}

func (s *Handler) handleVerifySoftwareToken(w http.ResponseWriter, r *http.Request) {
	var req verifySoftwareTokenRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	if req.Session != "" && req.AccessToken == "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException",
			"VerifySoftwareToken with Session (MFA_SETUP) is not supported by the local Cognito dev service")
		return
	}
	user, claims, ok := s.checkedAccessToken(w, r, req.AccessToken, "VerifySoftwareToken")
	if !ok {
		return
	}
	s.verifySoftwareTokenForUser(w, r, user, mfaGrantAuthorization(user, claims), req.UserCode)
}

// verifySoftwareTokenForUser binds code validation to the authorized account
// snapshot. The store admits only that secret and authorization at promotion.
func (s *Handler) verifySoftwareTokenForUser(w http.ResponseWriter, r *http.Request, user *CognitoUser, authorization mfaAuthorization, code string) {
	if user.PendingTOTPSecret == "" {
		cognitoJSONError(w, http.StatusBadRequest, "SoftwareTokenMFANotFoundException",
			"Software token MFA has not been associated")
		return
	}
	if !isSixDigits(code) || validateTOTPCodeAt(user.PendingTOTPSecret, code, s.cognito.now()) != nil {
		// Cognito reports a wrong enrolment code as EnableSoftwareTokenMFAException.
		cognitoJSONError(w, http.StatusBadRequest, "EnableSoftwareTokenMFAException", "Code mismatch")
		return
	}
	if err := s.cognito.confirmPendingTOTPSecret(r.Context(), user.Sub, user.PendingTOTPSecret, authorization); err != nil {
		writeMFATransitionError(w, err, "VerifySoftwareToken", true)
		return
	}
	cognitoJSONResponse(w, http.StatusOK, map[string]interface{}{"Status": "SUCCESS"})
}

// applyMFAPreference applies software-token settings to a user. Enabling
// requires a verified software token; nil settings change nothing.
func (s *Handler) applyMFAPreference(w http.ResponseWriter, r *http.Request, user *CognitoUser, authorization mfaAuthorization, settings *mfaSettings) bool {
	if settings == nil {
		return true
	}
	if settings.Enabled && !user.SoftwareTokenVerified {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException",
			"User has not verified software token mfa")
		return false
	}
	if err := s.cognito.setMFAPreference(r.Context(), user.Sub, settings.Enabled, authorization); err != nil {
		action := "SetUserMFAPreference"
		if !authorization.SelfService {
			action = "AdminSetUserMFAPreference"
		}
		writeMFATransitionError(w, err, action, authorization.SelfService)
		return false
	}
	return true
}

func (s *Handler) handleSetUserMFAPreference(w http.ResponseWriter, r *http.Request) {
	var req setUserMFAPreferenceRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	user, claims, ok := s.checkedAccessToken(w, r, req.AccessToken, "SetUserMFAPreference")
	if !ok {
		return
	}
	if !s.applyMFAPreference(w, r, user, mfaGrantAuthorization(user, claims), req.SoftwareTokenMfaSettings) {
		return
	}
	cognitoJSONResponse(w, http.StatusOK, map[string]interface{}{})
}

func (s *Handler) handleAdminSetUserMFAPreference(w http.ResponseWriter, r *http.Request) {
	var req adminSetUserMFAPreferenceRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	user, ok := s.lookupAdminUser(w, r, req.UserPoolID, req.Username, "AdminSetUserMFAPreference")
	if !ok {
		return
	}
	if !s.applyMFAPreference(w, r, user, mfaAuthorization{AuthVersion: user.AuthVersion}, req.SoftwareTokenMfaSettings) {
		return
	}
	cognitoJSONResponse(w, http.StatusOK, map[string]interface{}{})
}

func mfaGrantAuthorization(user *CognitoUser, claims map[string]interface{}) mfaAuthorization {
	originJTI, _ := claims["origin_jti"].(string)
	return mfaAuthorization{AuthVersion: user.AuthVersion, OriginJTI: originJTI, SelfService: true}
}

func writeMFATransitionError(w http.ResponseWriter, err error, action string, selfService bool) {
	switch {
	case errors.Is(err, errTokenRevoked), selfService && errors.Is(err, sql.ErrNoRows):
		message := "User authorization state changed"
		if selfService {
			message = "Access Token has been revoked"
		}
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", message)
	case errors.Is(err, sql.ErrNoRows):
		cognitoJSONError(w, http.StatusBadRequest, "UserNotFoundException", "User does not exist")
	case errors.Is(err, errTOTPEnrollmentChanged):
		cognitoJSONError(w, http.StatusBadRequest, "EnableSoftwareTokenMFAException", "Software token association changed; associate and verify again")
	case errors.Is(err, errSoftwareTokenNotVerified):
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "User has not verified software token mfa")
	default:
		log.Error().Err(err).Str("action", action).Msg("MFA state transition failed")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", "Failed to update software token MFA")
	}
}

func (s *Handler) handleGlobalSignOut(w http.ResponseWriter, r *http.Request) {
	var req globalSignOutRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	user, ok := s.authorizeAccessToken(w, r, req.AccessToken, "GlobalSignOut")
	if !ok {
		return
	}
	if err := s.cognito.RevokeUserTokens(r.Context(), user.Sub, s.cognito.now().Unix()); err != nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}
	cognitoJSONResponse(w, http.StatusOK, map[string]interface{}{})
}

func (s *Handler) handleAdminUserGlobalSignOut(w http.ResponseWriter, r *http.Request) {
	var req adminUserGlobalSignOutRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	user, ok := s.lookupAdminUser(w, r, req.UserPoolID, req.Username, "AdminUserGlobalSignOut")
	if !ok {
		return
	}
	if err := s.cognito.RevokeUserTokens(r.Context(), user.Sub, s.cognito.now().Unix()); err != nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}
	cognitoJSONResponse(w, http.StatusOK, map[string]interface{}{})
}

// handleRevokeToken revokes one refresh token (and, through origin_jti, the
// access tokens minted from it). Revoking an already revoked token succeeds;
// an access token or a forged token is refused.
func (s *Handler) handleRevokeToken(w http.ResponseWriter, r *http.Request) {
	var req revokeTokenRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	if req.Token == "" || req.ClientID == "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "Token and ClientId are required")
		return
	}
	client, err := s.cognito.LookupClient(r.Context(), req.ClientID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			cognitoJSONError(w, http.StatusBadRequest, "UnauthorizedException", "Invalid client credentials")
			return
		}
		log.Error().Err(err).Msg("RevokeToken client lookup failed")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", "Failed to authorize token revocation")
		return
	}
	// RevokeToken accepts the app-client secret itself, rather than the
	// username-bound SECRET_HASH used by authentication operations.
	if !hmac.Equal([]byte(client.Secret), []byte(req.ClientSecret)) {
		cognitoJSONError(w, http.StatusBadRequest, "UnauthorizedException", "Invalid client credentials")
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
