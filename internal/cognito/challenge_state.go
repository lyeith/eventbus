// Shared persisted challenge state and issuance for password, SRP, MFA and
// custom authentication. Custom trigger policy remains in custom_auth.go.
package cognito

import (
	"encoding/json"
	"net/http"
	"time"
)

// Legacy fixture clients retain a five-minute session when no client lifetime
// is configured. API-created clients use Cognito's configured minute lifetime.
const challengeSessionTTL = 5 * time.Minute

type challengeResult struct {
	Name     string `json:"challengeName"`
	Result   bool   `json:"challengeResult"`
	Metadata string `json:"challengeMetadata,omitempty"`
}

// State is private SQLite data. Only the current public challenge parameters
// cross the client boundary; private answers and SRP keys never do.
type authChallengeState struct {
	Mode             string            `json:"mode"`
	SRP              *SRPState         `json:"srp,omitempty"`
	History          []challengeResult `json:"history"`
	Private          map[string]string `json:"private,omitempty"`
	Metadata         string            `json:"metadata,omitempty"`
	PasswordVerified bool              `json:"password_verified"`
}

func (s *Handler) issueStateChallenge(w http.ResponseWriter, r *http.Request, client *CognitoClient, user *CognitoUser, name string, parameters map[string]string, state *authChallengeState) {
	stateJSON := ""
	if state != nil {
		encoded, err := json.Marshal(state)
		if err != nil {
			cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", "Failed to encode challenge state")
			return
		}
		stateJSON = string(encoded)
	}
	signing, err := s.cognito.EnsureSigningKey(r.Context(), client.PoolID)
	if err != nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", "Failed to create challenge session")
		return
	}
	privatePEM, err := encodePrivateKeyPEM(signing.Private)
	if err != nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", "Failed to create challenge session")
		return
	}
	wire, random, err := EncodeChallengeSession([]byte(privatePEM))
	if err != nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", "Failed to create challenge session")
		return
	}
	ttl := time.Duration(client.AuthSessionValidity) * time.Minute
	if ttl <= 0 {
		ttl = challengeSessionTTL
	}
	key := challengeSessionDBKey(random)
	if err := s.cognito.CreateChallengeSessionWithState(r.Context(), key, user.Sub, client.PoolID, client.ID, name, ttl, stateJSON); err != nil {
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "User cannot continue authentication")
		return
	}
	// Prevent a disable/reset racing between password validation and insertion.
	current, err := s.cognito.LookupUserBySub(r.Context(), user.Sub)
	if err != nil || !current.Enabled || current.AuthVersion != user.AuthVersion {
		_, _ = s.cognito.ConsumeChallengeSession(r.Context(), key)
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "User cannot continue authentication")
		return
	}
	cognitoJSONResponse(w, http.StatusOK, map[string]any{"ChallengeName": name, "Session": wire, "ChallengeParameters": parameters})
}

func newPasswordParameters(user *CognitoUser) map[string]string {
	return map[string]string{"USER_ID_FOR_SRP": user.Username, "USERNAME": user.Username, "requiredAttributes": "[]"}
}

func (s *Handler) issueNewPasswordChallenge(w http.ResponseWriter, r *http.Request, poolID, clientID string, user *CognitoUser) {
	client, err := s.cognito.LookupClient(r.Context(), clientID)
	if err != nil || client.PoolID != poolID {
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Invalid client")
		return
	}
	s.issueStateChallenge(w, r, client, user, "NEW_PASSWORD_REQUIRED", newPasswordParameters(user), nil)
}
