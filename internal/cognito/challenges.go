// Cognito MFA and new-password challenge operations.
package cognito

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/rs/zerolog/log"
	"golang.org/x/crypto/bcrypt"
)

// challengeSessionTTL is the wall-clock validity for an opaque challenge
// session. 5 minutes is comfortably longer than a human takes to type a
// 6-digit MFA code and shorter than real Cognito's 3-minute default by a
// margin that doesn't matter in dev. Cleanup goroutine reaps after this.
const challengeSessionTTL = 5 * time.Minute

// supportedChallenges enumerates the ChallengeName values the dev service
// accepts on RespondToAuthChallenge. Mirrors what cognito.py:127-170 and
// cognito.go:467-496 send through.
var supportedChallenges = map[string]bool{
	"SOFTWARE_TOKEN_MFA":    true,
	"SMS_MFA":               true,
	"NEW_PASSWORD_REQUIRED": true,
}

// respondToAuthChallengeRequest mirrors the AWS wire request.
//
// `ChallengeResponses` is a free-form string map; the required fields are
// challenge-specific (see issueMFAChallenge / handleRespondToAuthChallenge
// for the per-type validation).
type respondToAuthChallengeRequest struct {
	ClientID           string            `json:"ClientId"`
	ChallengeName      string            `json:"ChallengeName"`
	Session            string            `json:"Session"`
	ChallengeResponses map[string]string `json:"ChallengeResponses"`
}

// adminInitiateAuthRequest mirrors the AWS wire request for AdminInitiateAuth.
// The dev service supports only ADMIN_NO_SRP_AUTH (cognito.go:826-833).
type adminInitiateAuthRequest struct {
	UserPoolID     string            `json:"UserPoolId"`
	ClientID       string            `json:"ClientId"`
	AuthFlow       string            `json:"AuthFlow"`
	AuthParameters map[string]string `json:"AuthParameters"`
}

// issueMFAChallenge generates an HMAC-stamped session, persists a row to
// challenge_sessions with the SOFTWARE_TOKEN_MFA name, and writes the
// challenge response (no AuthenticationResult). Called from
// handleInitiateAuthUserPassword when the user has mfa_enabled=true.
func (s *Handler) issueMFAChallenge(w http.ResponseWriter, r *http.Request, poolID, clientID string, user *CognitoUser) {
	ctx := r.Context()

	signing, err := s.cognito.EnsureSigningKey(ctx, poolID)
	if err != nil {
		log.Error().Err(err).Msg("EnsureSigningKey failed in issueMFAChallenge")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}
	privPEM, err := encodePrivateKeyPEM(signing.Private)
	if err != nil {
		log.Error().Err(err).Msg("encodePrivateKeyPEM failed in issueMFAChallenge")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}

	sessionWire, randomPrefix, err := EncodeChallengeSession([]byte(privPEM))
	if err != nil {
		log.Error().Err(err).Msg("EncodeChallengeSession failed")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}
	dbKey := challengeSessionDBKey(randomPrefix)
	if err := s.cognito.CreateChallengeSession(ctx, dbKey, user.Sub, poolID, clientID, "SOFTWARE_TOKEN_MFA", challengeSessionTTL); err != nil {
		log.Error().Err(err).Msg("CreateChallengeSession failed")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}

	resp := map[string]interface{}{
		"ChallengeName": "SOFTWARE_TOKEN_MFA",
		"Session":       sessionWire,
		"ChallengeParameters": map[string]string{
			"USER_ID_FOR_SRP": user.Email,
			"USERNAME":        user.Email,
		},
	}
	cognitoJSONResponse(w, http.StatusOK, resp)
}

// handleRespondToAuthChallenge validates an owned, unexpired, unused session.
// Software tokens use real TOTP when a secret is enrolled; otherwise software
// and SMS fixtures accept six ASCII digits. New-password challenges enforce
// the pool policy before hashing the replacement password.
func (s *Handler) handleRespondToAuthChallenge(w http.ResponseWriter, r *http.Request) {
	var req respondToAuthChallengeRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	if req.ClientID == "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "ClientId is required")
		return
	}
	if req.ChallengeName == "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "ChallengeName is required")
		return
	}
	if req.Session == "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "Session is required")
		return
	}
	if !supportedChallenges[req.ChallengeName] {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException",
			fmt.Sprintf("Unsupported ChallengeName: %s", req.ChallengeName))
		return
	}

	ctx := r.Context()

	// Resolve client → pool first so we can fetch the right signing key for
	// HMAC verification. Unknown client → ResourceNotFoundException.
	client, err := s.cognito.LookupClient(ctx, req.ClientID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			cognitoJSONError(w, http.StatusBadRequest, "ResourceNotFoundException",
				fmt.Sprintf("App client %s does not exist", req.ClientID))
			return
		}
		log.Error().Err(err).Msg("LookupClient failed in RespondToAuthChallenge")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}

	signing, err := s.cognito.LoadSigningKey(ctx, client.PoolID)
	if err != nil {
		// No signing key → no way the client could have a valid session
		// for this pool. Collapse to NotAuthorizedException.
		log.Debug().Err(err).Str("pool", client.PoolID).Msg("LoadSigningKey failed in RespondToAuthChallenge")
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Invalid session")
		return
	}
	privPEM, err := encodePrivateKeyPEM(signing.Private)
	if err != nil {
		log.Error().Err(err).Msg("encodePrivateKeyPEM failed in RespondToAuthChallenge")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}

	// 1. HMAC verification — runs BEFORE the DB lookup so forged sessions
	//    are rejected without a query.
	prefix, err := VerifyChallengeSession(req.Session, []byte(privPEM))
	if err != nil {
		log.Debug().Err(err).Msg("Session HMAC verification failed")
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Invalid session")
		return
	}
	dbKey := challengeSessionDBKey(prefix)

	// 2. DB lookup.
	row, err := s.cognito.LookupChallengeSession(ctx, dbKey)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Invalid session")
			return
		}
		log.Error().Err(err).Msg("LookupChallengeSession failed")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}

	// 3. Replay rejection.
	if row.Used {
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Invalid session")
		return
	}

	// 4. Expiry. The cleanup goroutine usually reaps these, but we still
	//    check here in case a request lands within the 60s window.
	if row.ExpiresAt < time.Now().Unix() {
		cognitoJSONError(w, http.StatusBadRequest, "ExpiredCodeException", "Code has expired")
		return
	}

	// 5. Challenge type must match the stored type. A client can't ask for
	//    NEW_PASSWORD_REQUIRED against a session that was issued for
	//    SOFTWARE_TOKEN_MFA.
	if row.ChallengeName != req.ChallengeName {
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Challenge type mismatch")
		return
	}

	// SECRET_HASH (§3k) on RespondToAuthChallenge — only enforced when the
	// client has a secret configured. The username field is the
	// challenge-response USERNAME (matches what the Go provider sends at
	// cognito.go:535-540). Mismatch / missing-when-required collapses to
	// NotAuthorizedException for indistinguishability.
	if client.Secret != "" {
		username := req.ChallengeResponses["USERNAME"]
		if username == "" {
			// Fall back to the stored row's email if USERNAME wasn't echoed
			// — provider implementations differ on whether the client
			// re-sends USERNAME, so be tolerant.
			if userRow, lerr := s.cognito.LookupUserBySub(ctx, row.Sub); lerr == nil {
				username = userRow.Email
			}
		}
		if err := verifySecretHash(client.Secret, username, client.ID, req.ChallengeResponses["SECRET_HASH"]); err != nil {
			cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Invalid session")
			return
		}
	}

	// 6. Per-challenge validation.
	switch req.ChallengeName {
	case "SOFTWARE_TOKEN_MFA":
		code := req.ChallengeResponses["SOFTWARE_TOKEN_MFA_CODE"]
		// Look up the user to check whether they have a TOTP secret enrolled.
		// Missing user mid-challenge collapses to NotAuthorizedException.
		userRow, lerr := s.cognito.LookupUserBySub(ctx, row.Sub)
		if lerr != nil {
			cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Invalid session")
			return
		}
		if verr := validateTOTPCode(userRow.TOTPSecret, code); verr != nil {
			if !errors.Is(verr, errFallbackToAnyDigits) {
				cognitoJSONError(w, http.StatusBadRequest, "CodeMismatchException", "Invalid code")
				return
			}
			// No secret enrolled → "any 6 digits" preserves the cheap dev
			// path for fixtures without an enrolled secret.
			if !isSixDigits(code) {
				cognitoJSONError(w, http.StatusBadRequest, "CodeMismatchException", "Invalid code")
				return
			}
		}
	case "SMS_MFA":
		// SMS_MFA stays on "any 6 digits" — there is no enrolment story
		// for SMS in the dev service (no carrier hookup).
		code := req.ChallengeResponses["SMS_MFA_CODE"]
		if !isSixDigits(code) {
			cognitoJSONError(w, http.StatusBadRequest, "CodeMismatchException", "Invalid code")
			return
		}
	case "NEW_PASSWORD_REQUIRED":
		newPassword := req.ChallengeResponses["NEW_PASSWORD"]
		if newPassword == "" {
			cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException",
				"NEW_PASSWORD is required for NEW_PASSWORD_REQUIRED")
			return
		}
		// Password policy (§3g): if the pool has one configured, enforce
		// it before bcrypt. InvalidPasswordException matches the typed
		// error decoder in both providers (cognito.py / cognito.go).
		policy, perr := loadPoolPasswordPolicy(ctx, s.cognito, row.PoolID)
		if perr == nil && policy != nil {
			if vErr := policy.Validate(newPassword); vErr != nil {
				cognitoJSONError(w, http.StatusBadRequest, "InvalidPasswordException", vErr.Error())
				return
			}
		}
		hash, herr := bcrypt.GenerateFromPassword([]byte(newPassword), bcrypt.DefaultCost)
		if herr != nil {
			log.Error().Err(herr).Msg("bcrypt failed in NEW_PASSWORD_REQUIRED")
			cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", "failed to hash password")
			return
		}
		if _, uerr := s.cognito.DB().ExecContext(ctx, `UPDATE users SET password_hash = ? WHERE sub = ?`, string(hash), row.Sub); uerr != nil {
			log.Error().Err(uerr).Msg("UPDATE password failed in NEW_PASSWORD_REQUIRED")
			cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", uerr.Error())
			return
		}
	}

	// 7. Mark session used (replay rejection on subsequent calls).
	if err := s.cognito.MarkChallengeSessionUsed(ctx, dbKey); err != nil {
		log.Error().Err(err).Msg("MarkChallengeSessionUsed failed")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}

	// 8. Look up user → mint tokens. If the user was deleted between
	//    challenge issuance and response, fail loudly.
	user, err := s.cognito.LookupUserBySub(ctx, row.Sub)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Invalid session")
			return
		}
		log.Error().Err(err).Msg("LookupUserBySub failed in RespondToAuthChallenge")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}

	s.writeAuthenticated(w, r, row.PoolID, row.ClientID, user, "RespondToAuthChallenge")
}

// isSixDigits validates the code shape for fixtures without a TOTP secret.
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
