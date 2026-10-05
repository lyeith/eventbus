// Pool/client management handlers (GO-COGNITO-6 Tier C, §6 row 4).
//
// These handlers let tests spin up isolated pools without touching the
// canonical seed YAML. Real Cognito returns rich UserPool/UserPoolClient
// records here; the dev service trims to the fields actually consumed by
// boto3 / aws-sdk-go-v2 callers in the test corpus.
//
// All four ops are idempotent on the wire by design:
//   - Create returns the existing row's id if the caller supplies one
//     that already exists. This keeps "create the test pool" loops safe
//     to re-run without an explicit teardown.
//   - Delete returns success even when the target is missing.
package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/rs/zerolog/log"
)

// --- CreateUserPool ----------------------------------------------------

// createUserPoolRequest mirrors the AWS wire request. We accept far more
// fields in real Cognito; the dev service only reads PoolName + an
// optional explicit PoolId override (handy for deterministic tests).
type createUserPoolRequest struct {
	PoolName       string                 `json:"PoolName"`
	PoolID         string                 `json:"PoolId"` // dev-only override
	Policies       *createPoolPoliciesEnv `json:"Policies"`
	PasswordPolicy *PasswordPolicy        `json:"PasswordPolicy"`
}

// createPoolPoliciesEnv mirrors the wrapped shape AWS uses
// (`{"Policies": {"PasswordPolicy": {...}}}`). The flatter
// `PasswordPolicy` field is the dev-friendly alias.
type createPoolPoliciesEnv struct {
	PasswordPolicy *PasswordPolicy `json:"PasswordPolicy"`
}

// handleCreateUserPool implements CreateUserPool. The dev service generates
// a `local-pool-<random>` id when none is supplied; tests that need a
// specific id pass `PoolId` explicitly (the dev-only override).
//
// Behaviour:
//   - PoolName missing → InvalidParameterException.
//   - Pool with same id already exists → returns the existing row
//     (UpsertPool's ON CONFLICT DO NOTHING). This keeps "ensure pool
//     exists" call loops safe.
//   - Wires password policy from either Policies.PasswordPolicy or the
//     flat PasswordPolicy alias (whichever is non-nil).
func (s *Server) handleCreateUserPool(w http.ResponseWriter, r *http.Request) {
	if s.cognito == nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", "cognito store not configured")
		return
	}
	var req createUserPoolRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	if req.PoolName == "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "PoolName is required")
		return
	}

	poolID := req.PoolID
	if poolID == "" {
		poolID = newPoolID()
	}

	ctx := r.Context()
	if err := s.cognito.UpsertPool(ctx, poolID, "us-east-1"); err != nil {
		log.Error().Err(err).Msg("UpsertPool failed in CreateUserPool")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}

	// Determine which policy shape (if any) was supplied.
	var policy *PasswordPolicy
	switch {
	case req.PasswordPolicy != nil:
		policy = req.PasswordPolicy
	case req.Policies != nil && req.Policies.PasswordPolicy != nil:
		policy = req.Policies.PasswordPolicy
	}
	if policy != nil {
		raw, jerr := json.Marshal(policy)
		if jerr != nil {
			log.Error().Err(jerr).Msg("marshal PasswordPolicy failed")
			cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", jerr.Error())
			return
		}
		if err := s.cognito.SetPoolPasswordPolicy(ctx, poolID, string(raw)); err != nil {
			log.Error().Err(err).Msg("SetPoolPasswordPolicy failed in CreateUserPool")
			cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
			return
		}
	}

	now := float64(time.Now().Unix())
	resp := map[string]interface{}{
		"UserPool": map[string]interface{}{
			"Id":               poolID,
			"Name":             req.PoolName,
			"CreationDate":     now,
			"LastModifiedDate": now,
			"Status":           "Enabled",
		},
	}
	cognitoJSONResponse(w, http.StatusOK, resp)
}

// --- CreateUserPoolClient ----------------------------------------------

// createUserPoolClientRequest mirrors the AWS wire request. Only the
// fields the platform actually reads are surfaced; the rest get stored
// implicitly as defaults.
type createUserPoolClientRequest struct {
	UserPoolID     string `json:"UserPoolId"`
	ClientName     string `json:"ClientName"`
	ClientID       string `json:"ClientId"` // dev-only override
	GenerateSecret bool   `json:"GenerateSecret"`
}

// handleCreateUserPoolClient implements CreateUserPoolClient. Returns the
// generated `ClientId` and `ClientSecret` (when `GenerateSecret=true`).
//
// Behaviour:
//   - UserPoolId or ClientName missing → InvalidParameterException.
//   - Pool not found → ResourceNotFoundException.
//   - Existing client id (when caller supplies one) → updated in place.
func (s *Server) handleCreateUserPoolClient(w http.ResponseWriter, r *http.Request) {
	if s.cognito == nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", "cognito store not configured")
		return
	}
	var req createUserPoolClientRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	if req.UserPoolID == "" || req.ClientName == "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException",
			"UserPoolId and ClientName are required")
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
		cognitoJSONError(w, http.StatusBadRequest, "ResourceNotFoundException",
			fmt.Sprintf("User pool %s does not exist", req.UserPoolID))
		return
	}

	clientID := req.ClientID
	if clientID == "" {
		clientID = newClientID()
	}
	var secret string
	if req.GenerateSecret {
		secret = newClientSecret()
	}

	if err := s.cognito.UpsertClient(ctx, clientID, req.UserPoolID, secret); err != nil {
		log.Error().Err(err).Msg("UpsertClient failed in CreateUserPoolClient")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}

	now := float64(time.Now().Unix())
	out := map[string]interface{}{
		"UserPoolId":       req.UserPoolID,
		"ClientId":         clientID,
		"ClientName":       req.ClientName,
		"CreationDate":     now,
		"LastModifiedDate": now,
	}
	if secret != "" {
		out["ClientSecret"] = secret
	}
	cognitoJSONResponse(w, http.StatusOK, map[string]interface{}{
		"UserPoolClient": out,
	})
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
func (s *Server) handleDeleteUserPool(w http.ResponseWriter, r *http.Request) {
	if s.cognito == nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", "cognito store not configured")
		return
	}
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
func (s *Server) handleDeleteUserPoolClient(w http.ResponseWriter, r *http.Request) {
	if s.cognito == nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", "cognito store not configured")
		return
	}
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

// --- helpers -----------------------------------------------------------

// newPoolID returns a deterministic-prefix opaque pool id for tests.
// Format: `local-pool-<6 hex chars>`. Six bytes of entropy is enough to
// keep collisions astronomically unlikely across a single test run.
func newPoolID() string {
	return "local-pool-" + randomHex(3)
}

// newClientID matches newPoolID's shape, prefixed `local-client-`.
func newClientID() string {
	return "local-client-" + randomHex(3)
}

// newClientSecret returns 24 bytes of crypto-random data hex-encoded
// (48 chars) — enough for tests, far below real Cognito's secret length
// but still resistant to guessing in dev.
func newClientSecret() string {
	return randomHex(24)
}

// randomHex returns 2*n hex chars from crypto/rand. Panics on rand failure
// (catastrophic and we want to know about it; this is not a hot path).
func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand failure is a panic-worthy condition — there's
		// nowhere meaningful to return the error, and the dev service
		// should fail loud.
		panic(fmt.Errorf("crypto/rand: %w", err))
	}
	return hex.EncodeToString(b)
}
