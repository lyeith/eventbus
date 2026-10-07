// Custom authentication coordinates AWS challenge events; configured application
// handlers decide custom challenges. Password proof uses actual Cognito SRP.
package cognito

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
)

const (
	defineAuthTrigger = "DefineAuthChallenge"
	createAuthTrigger = "CreateAuthChallenge"
	verifyAuthTrigger = "VerifyAuthChallengeResponse"
)

func (s *Handler) handleCustomInitiateAuth(w http.ResponseWriter, r *http.Request, client *CognitoClient, parameters map[string]string) {
	if s.triggers == nil || !s.triggers.Supports(client.PoolID) {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "Custom authentication triggers are not configured for this user pool")
		return
	}
	if parameters["CHALLENGE_NAME"] != "SRP_A" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "CUSTOM_AUTH requires CHALLENGE_NAME=SRP_A")
		return
	}
	s.beginSRP(w, r, client, parameters, "custom")
}

func (s *Handler) handleSRPInitiateAuth(w http.ResponseWriter, r *http.Request, client *CognitoClient, parameters map[string]string) {
	s.beginSRP(w, r, client, parameters, "srp")
}

func (s *Handler) beginSRP(w http.ResponseWriter, r *http.Request, client *CognitoClient, parameters map[string]string, mode string) {
	username, a := parameters["USERNAME"], parameters["SRP_A"]
	if username == "" || a == "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "USERNAME and SRP_A are required")
		return
	}
	if err := verifySecretHash(client.Secret, username, client.ID, parameters["SECRET_HASH"]); err != nil {
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Incorrect username or password.")
		return
	}
	user, err := s.cognito.ResolveSignInUser(r.Context(), client.PoolID, username)
	if err != nil {
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Incorrect username or password.")
		return
	}
	if !user.Enabled {
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "User is disabled.")
		return
	}
	if user.Status != "CONFIRMED" && user.Status != "FORCE_CHANGE_PASSWORD" {
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "User cannot authenticate in the current state")
		return
	}
	if mode == "custom" && user.MFAEnabled && user.TOTPSecret == "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "Custom authentication with software MFA requires an enrolled TOTP secret")
		return
	}
	if user.SRPSalt == "" || user.SRPVerifier == "" {
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "SRP credentials are unavailable; provision or reset the user's password")
		return
	}
	srp, err := newSRPChallengeWithEntropy(client.PoolID, user.Username, user.SRPSalt, user.SRPVerifier, a, rand.Reader, s.cognito.now().UTC())
	if err != nil {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "Invalid SRP_A")
		return
	}
	state := authChallengeState{Mode: mode, SRP: &srp, History: []challengeResult{{Name: "SRP_A", Result: true}}}
	if mode == "custom" {
		decision, apiErr := s.defineChallenge(r.Context(), client, user, state, nil)
		if apiErr != nil {
			writeAuthFlowError(w, apiErr)
			return
		}
		if decision.fail {
			cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Custom authentication failed")
			return
		}
		if decision.issue || decision.name != "PASSWORD_VERIFIER" {
			cognitoJSONError(w, http.StatusBadRequest, "InvalidLambdaResponseException", "SRP custom authentication must verify PASSWORD_VERIFIER before issuing tokens")
			return
		}
	}
	s.issueStateChallenge(w, r, client, user, "PASSWORD_VERIFIER", srp.ChallengeParameters(), &state)
}

type authFlowError struct{ code, message string }

func writeAuthFlowError(w http.ResponseWriter, failure *authFlowError) {
	cognitoJSONError(w, http.StatusBadRequest, failure.code, failure.message)
}

type triggerFailure interface{ FailureKind() string }

func triggerError(err error) *authFlowError {
	var classified triggerFailure
	if errors.As(err, &classified) && classified.FailureKind() == "invalid_response" {
		return &authFlowError{"InvalidLambdaResponseException", "Custom authentication trigger returned an invalid response"}
	}
	return &authFlowError{"UnexpectedLambdaException", "Custom authentication trigger invocation failed"}
}

type challengeDecision struct {
	name        string
	issue, fail bool
}

func (s *Handler) triggerEvent(ctx context.Context, client *CognitoClient, user *CognitoUser, trigger string, request map[string]any) (map[string]any, error) {
	attributes, err := s.cognito.LoadUserAttributes(ctx, user.Sub)
	if err != nil {
		return nil, err
	}
	attributes["sub"] = user.Sub
	if user.Email != "" {
		attributes["email"] = user.Email
	}
	region, err := s.cognito.GetPoolRegion(ctx, user.PoolID)
	if err != nil {
		return nil, err
	}
	if metadata, _ := request["clientMetadata"].(map[string]string); metadata == nil {
		delete(request, "clientMetadata")
	}
	request["userAttributes"] = attributes
	return map[string]any{
		"version": "1", "region": region, "userPoolId": user.PoolID,
		"userName": user.Username, "triggerSource": trigger,
		"callerContext": map[string]any{"awsSdkVersion": "eventbus", "clientId": client.ID},
		"request":       request, "response": map[string]any{},
	}, nil
}

func historyValue(history []challengeResult) []any {
	out := make([]any, 0, len(history))
	for _, item := range history {
		value := map[string]any{"challengeName": item.Name, "challengeResult": item.Result}
		if item.Metadata != "" {
			value["challengeMetadata"] = item.Metadata
		}
		out = append(out, value)
	}
	return out
}

func (s *Handler) invokeAuthTrigger(ctx context.Context, client *CognitoClient, user *CognitoUser, name, source string, request map[string]any) (map[string]any, *authFlowError) {
	if s.triggers == nil || !s.triggers.Supports(client.PoolID) {
		return nil, &authFlowError{"InvalidParameterException", "Custom authentication triggers are not configured for this user pool"}
	}
	event, err := s.triggerEvent(ctx, client, user, source, request)
	if err != nil {
		return nil, &authFlowError{"UnexpectedLambdaException", "Unable to construct custom authentication trigger event"}
	}
	result, err := s.triggers.Invoke(ctx, client.PoolID, name, event)
	if err != nil {
		return nil, triggerError(err)
	}
	response, ok := result["response"].(map[string]any)
	if !ok {
		return nil, &authFlowError{"InvalidLambdaResponseException", "Custom authentication trigger response is required"}
	}
	return response, nil
}

func (s *Handler) defineChallenge(ctx context.Context, client *CognitoClient, user *CognitoUser, state authChallengeState, metadata map[string]string) (challengeDecision, *authFlowError) {
	response, failure := s.invokeAuthTrigger(ctx, client, user, defineAuthTrigger, "DefineAuthChallenge_Authentication",
		map[string]any{"session": historyValue(state.History), "clientMetadata": metadata, "userNotFound": false})
	if failure != nil {
		return challengeDecision{}, failure
	}
	issue, issueOK := response["issueTokens"].(bool)
	fail, failOK := response["failAuthentication"].(bool)
	name, nameOK := response["challengeName"].(string)
	if !issueOK || !failOK || (issue && fail) || (!issue && !fail && (!nameOK || name == "")) {
		return challengeDecision{}, &authFlowError{"InvalidLambdaResponseException", "Invalid DefineAuthChallenge response"}
	}
	return challengeDecision{name: name, issue: issue, fail: fail}, nil
}

func stringParameters(value any) (map[string]string, bool) {
	if value == nil {
		return map[string]string{}, true
	}
	if values, ok := value.(map[string]string); ok {
		return values, true
	}
	values, ok := value.(map[string]any)
	if !ok {
		return nil, false
	}
	out := make(map[string]string, len(values))
	for name, value := range values {
		text, ok := value.(string)
		if !ok {
			return nil, false
		}
		out[name] = text
	}
	return out, true
}

func (s *Handler) advanceCustomAuth(w http.ResponseWriter, r *http.Request, client *CognitoClient, user *CognitoUser, state authChallengeState, metadata map[string]string) {
	decision, failure := s.defineChallenge(r.Context(), client, user, state, metadata)
	if failure != nil {
		writeAuthFlowError(w, failure)
		return
	}
	if decision.fail || !state.PasswordVerified {
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Custom authentication failed")
		return
	}
	if decision.issue {
		s.writeAuthenticated(w, r, user.PoolID, client.ID, user, "RespondToAuthChallenge")
		return
	}
	if decision.name != "CUSTOM_CHALLENGE" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidLambdaResponseException", "DefineAuthChallenge returned an unsupported challenge")
		return
	}
	response, failure := s.invokeAuthTrigger(r.Context(), client, user, createAuthTrigger, "CreateAuthChallenge_Authentication",
		map[string]any{"session": historyValue(state.History), "challengeName": "CUSTOM_CHALLENGE", "clientMetadata": metadata, "userNotFound": false})
	if failure != nil {
		writeAuthFlowError(w, failure)
		return
	}
	public, publicOK := stringParameters(response["publicChallengeParameters"])
	private, privateOK := stringParameters(response["privateChallengeParameters"])
	challengeMetadata, metadataOK := response["challengeMetadata"].(string)
	if response["challengeMetadata"] == nil {
		metadataOK = true
	}
	if !publicOK || !privateOK || !metadataOK {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidLambdaResponseException", "Invalid CreateAuthChallenge response")
		return
	}
	state.SRP, state.Private, state.Metadata = nil, private, challengeMetadata
	// Username is Cognito's canonical challenge identity, not a mutable email.
	public["USERNAME"] = user.Username
	s.issueStateChallenge(w, r, client, user, "CUSTOM_CHALLENGE", public, &state)
}

func (s *Handler) respondStateChallenge(w http.ResponseWriter, r *http.Request, req respondToAuthChallengeRequest, client *CognitoClient, user *CognitoUser, row *CognitoChallengeSession) {
	var state authChallengeState
	if row.StateJSON == "" || json.Unmarshal([]byte(row.StateJSON), &state) != nil || (state.Mode != "custom" && state.Mode != "srp") {
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Invalid session")
		return
	}
	if req.ChallengeResponses["USERNAME"] != user.Username {
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Invalid session")
		return
	}
	if err := verifySecretHash(client.Secret, user.Username, client.ID, req.ChallengeResponses["SECRET_HASH"]); err != nil {
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Invalid session")
		return
	}
	if req.ChallengeName == "PASSWORD_VERIFIER" {
		suffix, err := srpPoolSuffix(user.PoolID)
		if err != nil || state.PasswordVerified || state.SRP == nil || state.SRP.Username != user.Username || state.SRP.PoolSuffix != suffix ||
			len(state.History) != 1 || state.History[0].Name != "SRP_A" || !state.History[0].Result {
			invalidChallengeSession(w)
			return
		}
	} else if req.ChallengeName == "CUSTOM_CHALLENGE" && (state.Mode != "custom" || !state.PasswordVerified || state.SRP != nil) {
		invalidChallengeSession(w)
		return
	}
	// Claim exactly once before invoking application side effects. Every
	// continuation receives a new opaque session, including an answer retry.
	consumed, err := s.cognito.ConsumeChallengeSession(r.Context(), row.Session)
	if err != nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", "Failed to consume challenge session")
		return
	}
	if !consumed {
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Invalid session")
		return
	}
	switch req.ChallengeName {
	case "PASSWORD_VERIFIER":
		responses := req.ChallengeResponses
		if state.SRP == nil || state.SRP.Verify(responses["PASSWORD_CLAIM_SIGNATURE"], responses["PASSWORD_CLAIM_SECRET_BLOCK"], responses["TIMESTAMP"], s.cognito.now()) != nil {
			if state.Mode == "custom" {
				state.History = append(state.History, challengeResult{Name: "PASSWORD_VERIFIER", Result: false})
				s.advanceCustomAuth(w, r, client, user, state, req.ClientMetadata)
			} else {
				cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Incorrect username or password.")
			}
			return
		}
		state.SRP, state.PasswordVerified = nil, true
		state.History = append(state.History, challengeResult{Name: "PASSWORD_VERIFIER", Result: true})
		if user.Status == "FORCE_CHANGE_PASSWORD" {
			expired, err := s.temporaryPasswordExpired(r.Context(), user)
			if err != nil {
				authInternalError(w, err, "RespondToAuthChallenge")
				return
			}
			if expired {
				cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Temporary password has expired and must be reset by an administrator.")
				return
			}
			if state.Mode == "custom" {
				decision, failure := s.defineChallenge(r.Context(), client, user, state, req.ClientMetadata)
				if failure != nil {
					writeAuthFlowError(w, failure)
					return
				}
				if decision.fail {
					cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Custom authentication failed")
					return
				}
				if decision.issue || decision.name != "NEW_PASSWORD_REQUIRED" {
					cognitoJSONError(w, http.StatusBadRequest, "InvalidLambdaResponseException", "Temporary-password custom authentication requires NEW_PASSWORD_REQUIRED")
					return
				}
			}
			parameters, err := s.newPasswordParameters(r.Context(), user, client)
			if err != nil {
				authInternalError(w, err, "RespondToAuthChallenge")
				return
			}
			s.issueStateChallenge(w, r, client, user, "NEW_PASSWORD_REQUIRED", parameters, &state)
			return
		}
		if user.MFAEnabled {
			s.issueMFAStateChallenge(w, r, client, user, &state)
		} else if state.Mode == "custom" {
			s.advanceCustomAuth(w, r, client, user, state, req.ClientMetadata)
		} else {
			s.writeAuthenticated(w, r, user.PoolID, client.ID, user, "RespondToAuthChallenge")
		}
	case "CUSTOM_CHALLENGE":
		if state.Mode != "custom" || !state.PasswordVerified {
			cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Invalid session")
			return
		}
		answer, present := req.ChallengeResponses["ANSWER"]
		if !present {
			cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "ANSWER is required for CUSTOM_CHALLENGE")
			return
		}
		if state.Private == nil {
			state.Private = map[string]string{}
		}
		response, failure := s.invokeAuthTrigger(r.Context(), client, user, verifyAuthTrigger, "VerifyAuthChallengeResponse_Authentication",
			map[string]any{"privateChallengeParameters": state.Private, "challengeAnswer": answer, "clientMetadata": req.ClientMetadata, "userNotFound": false})
		if failure != nil {
			writeAuthFlowError(w, failure)
			return
		}
		correct, ok := response["answerCorrect"].(bool)
		if !ok {
			cognitoJSONError(w, http.StatusBadRequest, "InvalidLambdaResponseException", "Invalid VerifyAuthChallengeResponse response")
			return
		}
		state.History = append(state.History, challengeResult{Name: "CUSTOM_CHALLENGE", Result: correct, Metadata: state.Metadata})
		state.Private, state.Metadata = nil, ""
		s.advanceCustomAuth(w, r, client, user, state, req.ClientMetadata)
	default:
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Invalid session")
	}
}
