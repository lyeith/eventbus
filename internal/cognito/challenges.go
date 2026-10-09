// Cognito MFA, password replacement and persisted authentication challenges.
package cognito

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

var supportedChallenges = map[string]bool{
	"SOFTWARE_TOKEN_MFA": true, "SMS_MFA": true,
	"NEW_PASSWORD_REQUIRED": true, "PASSWORD_VERIFIER": true, "CUSTOM_CHALLENGE": true,
}

type respondToAuthChallengeRequest struct {
	UserPoolID         string            `json:"UserPoolId"`
	ClientID           string            `json:"ClientId"`
	ChallengeName      string            `json:"ChallengeName"`
	Session            string            `json:"Session"`
	ChallengeResponses map[string]string `json:"ChallengeResponses"`
	ClientMetadata     map[string]string `json:"ClientMetadata"`
}

type adminInitiateAuthRequest struct {
	UserPoolID     string            `json:"UserPoolId"`
	ClientID       string            `json:"ClientId"`
	AuthFlow       string            `json:"AuthFlow"`
	AuthParameters map[string]string `json:"AuthParameters"`
	ClientMetadata map[string]string `json:"ClientMetadata"`
}

func (s *Handler) issueMFAChallenge(w http.ResponseWriter, r *http.Request, poolID, clientID string, user *CognitoUser) {
	client, err := s.cognito.LookupClient(r.Context(), clientID)
	if err != nil || client.PoolID != poolID {
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Invalid client")
		return
	}
	s.issueMFAStateChallenge(w, r, client, user, nil)
}

func (s *Handler) issueMFAStateChallenge(w http.ResponseWriter, r *http.Request, client *CognitoClient, user *CognitoUser, state *authChallengeState) {
	s.issueStateChallenge(w, r, client, user, "SOFTWARE_TOKEN_MFA",
		map[string]string{"USER_ID_FOR_SRP": user.Username, "USERNAME": user.Username}, state)
}

func (s *Handler) handleRespondToAuthChallenge(w http.ResponseWriter, r *http.Request) {
	s.respondToAuthChallenge(w, r, false)
}

func (s *Handler) handleAdminRespondToAuthChallenge(w http.ResponseWriter, r *http.Request) {
	s.respondToAuthChallenge(w, r, true)
}

func (s *Handler) respondToAuthChallenge(w http.ResponseWriter, r *http.Request, admin bool) {
	var req respondToAuthChallengeRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	if req.ClientID == "" || req.ChallengeName == "" || req.Session == "" || (admin && req.UserPoolID == "") {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "ClientId, ChallengeName and Session are required; admin operations also require UserPoolId")
		return
	}
	if !supportedChallenges[req.ChallengeName] {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", fmt.Sprintf("Unsupported ChallengeName: %s", req.ChallengeName))
		return
	}
	client, err := s.cognito.LookupClient(r.Context(), req.ClientID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			cognitoJSONError(w, http.StatusBadRequest, "ResourceNotFoundException", "App client does not exist")
		} else {
			authInternalError(w, err, "RespondToAuthChallenge")
		}
		return
	}
	if admin && req.UserPoolID != client.PoolID {
		cognitoJSONError(w, http.StatusBadRequest, "ResourceNotFoundException", "App client does not exist in the user pool")
		return
	}
	signing, err := s.cognito.loadSigningKey(r.Context(), client.PoolID)
	if err != nil {
		invalidChallengeSession(w)
		return
	}
	privatePEM, err := encodePrivateKeyPEM(signing.Private)
	if err != nil {
		authInternalError(w, err, "RespondToAuthChallenge")
		return
	}
	prefix, err := VerifyChallengeSession(req.Session, []byte(privatePEM))
	if err != nil {
		invalidChallengeSession(w)
		return
	}
	row, err := s.cognito.LookupChallengeSession(r.Context(), challengeSessionDBKey(prefix))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			invalidChallengeSession(w)
		} else {
			authInternalError(w, err, "RespondToAuthChallenge")
		}
		return
	}
	if row.Used || row.ClientID != client.ID || row.PoolID != client.PoolID || row.ChallengeName != req.ChallengeName {
		invalidChallengeSession(w)
		return
	}
	if row.ExpiresAt <= s.cognito.now().Unix() {
		if row.StateJSON != "" {
			invalidChallengeSession(w)
		} else {
			cognitoJSONError(w, http.StatusBadRequest, "ExpiredCodeException", "Code has expired")
		}
		return
	}
	user, err := s.cognito.LookupUserBySub(r.Context(), row.Sub)
	if err != nil || !user.Enabled || user.PoolID != row.PoolID || user.AuthVersion != row.AuthVersion {
		invalidChallengeSession(w)
		return
	}
	if req.ChallengeName == "PASSWORD_VERIFIER" || req.ChallengeName == "CUSTOM_CHALLENGE" {
		s.respondStateChallenge(w, r, req, client, user, row)
		return
	}
	var state authChallengeState
	if row.StateJSON != "" {
		if json.Unmarshal([]byte(row.StateJSON), &state) != nil || (state.Mode != "custom" && state.Mode != "srp") || !state.PasswordVerified || state.SRP != nil ||
			req.ChallengeResponses["USERNAME"] != user.Username {
			invalidChallengeSession(w)
			return
		}
		if state.Mode == "custom" && req.ChallengeName == "SOFTWARE_TOKEN_MFA" && user.TOTPSecret == "" {
			invalidChallengeSession(w)
			return
		}
	}
	username := req.ChallengeResponses["USERNAME"]
	// Existing explicit MFA fixtures may omit USERNAME. Echoed identities must
	// always refer to this account, and secret clients hash its canonical name.
	if username != "" && username != user.Username {
		invalidChallengeSession(w)
		return
	}
	if err := verifySecretHash(client.Secret, user.Username, client.ID, req.ChallengeResponses["SECRET_HASH"]); err != nil {
		invalidChallengeSession(w)
		return
	}
	switch req.ChallengeName {
	case "SOFTWARE_TOKEN_MFA":
		code := req.ChallengeResponses["SOFTWARE_TOKEN_MFA_CODE"]
		if err := validateTOTPCodeAt(user.TOTPSecret, code, s.cognito.now()); err != nil && (!errors.Is(err, errFallbackToAnyDigits) || !s.allowLegacyFixtureAuth(client) || !isSixDigits(code)) {
			cognitoJSONError(w, http.StatusBadRequest, "CodeMismatchException", "Invalid code")
			return
		}
	case "SMS_MFA":
		if !s.allowLegacyFixtureAuth(client) || !isSixDigits(req.ChallengeResponses["SMS_MFA_CODE"]) {
			cognitoJSONError(w, http.StatusBadRequest, "CodeMismatchException", "Invalid code")
			return
		}
	case "NEW_PASSWORD_REQUIRED":
		s.respondNewPassword(w, r, req, client, user, row)
		return
	}
	if !s.consumeChallenge(w, r, row) {
		return
	}
	if row.StateJSON != "" && state.Mode == "custom" {
		state.History = append(state.History, challengeResult{Name: req.ChallengeName, Result: true})
		s.advanceCustomAuth(w, r, client, user, state, req.ClientMetadata)
	} else {
		s.writeAuthenticated(w, r, row.PoolID, row.ClientID, user, "RespondToAuthChallenge")
	}
}

func invalidChallengeSession(w http.ResponseWriter) {
	cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Invalid session")
}

func (s *Handler) consumeChallenge(w http.ResponseWriter, r *http.Request, row *CognitoChallengeSession) bool {
	consumed, err := s.cognito.ConsumeChallengeSession(r.Context(), row.Session)
	if err != nil {
		authInternalError(w, err, "RespondToAuthChallenge")
		return false
	}
	if !consumed {
		invalidChallengeSession(w)
		return false
	}
	return true
}

func (s *Handler) respondNewPassword(w http.ResponseWriter, r *http.Request, req respondToAuthChallengeRequest, client *CognitoClient, user *CognitoUser, row *CognitoChallengeSession) {
	password := req.ChallengeResponses["NEW_PASSWORD"]
	if password == "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "NEW_PASSWORD is required for NEW_PASSWORD_REQUIRED")
		return
	}
	policy, err := loadPoolPasswordPolicy(r.Context(), s.cognito, user.PoolID)
	if err != nil {
		authInternalError(w, err, "RespondToAuthChallenge")
		return
	}
	if message := lifecyclePasswordValidation(password, nil); message != "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidPasswordException", message)
		return
	}
	if err := policy.Validate(password); err != nil {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidPasswordException", err.Error())
		return
	}
	expired, err := s.temporaryPasswordExpired(r.Context(), user)
	if err != nil {
		authInternalError(w, err, "RespondToAuthChallenge")
		return
	}
	if expired {
		invalidChallengeSession(w)
		return
	}
	var state authChallengeState
	if row.StateJSON != "" {
		if json.Unmarshal([]byte(row.StateJSON), &state) != nil || (state.Mode != "custom" && state.Mode != "srp") || !state.PasswordVerified {
			invalidChallengeSession(w)
			return
		}
	}
	pool, err := s.cognito.LookupPool(r.Context(), user.PoolID)
	if err != nil {
		authInternalError(w, err, "RespondToAuthChallenge")
		return
	}
	updates := map[string]string{}
	for name, value := range req.ChallengeResponses {
		if strings.HasPrefix(name, "userAttributes.") {
			updates[strings.TrimPrefix(name, "userAttributes.")] = value
		}
	}
	completedVersion := user.AuthVersion + 1
	user, err = s.cognito.completeNewPassword(r.Context(), user, row, password, updates, pool, client)
	if err != nil {
		if errors.Is(err, errTokenRevoked) {
			invalidChallengeSession(w)
		} else {
			writeWorkflowError(w, err)
		}
		return
	}
	if !user.Enabled || user.AuthVersion != completedVersion {
		invalidChallengeSession(w)
		return
	}
	if row.StateJSON != "" {
		state.History = append(state.History, challengeResult{Name: "NEW_PASSWORD_REQUIRED", Result: true})
	}
	if user.MFAEnabled {
		var pending *authChallengeState
		if row.StateJSON != "" {
			pending = &state
		}
		s.issueMFAStateChallenge(w, r, client, user, pending)
	} else if row.StateJSON != "" && state.Mode == "custom" {
		s.advanceCustomAuth(w, r, client, user, state, req.ClientMetadata)
	} else {
		s.writeAuthenticated(w, r, user.PoolID, client.ID, user, "RespondToAuthChallenge")
	}
}

func isSixDigits(s string) bool {
	if len(s) != 6 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
