// Cognito IDP JSON-1.1 dispatch.
//
// Cognito IDP uses the AWS JSON 1.1 wire format with X-Amz-Target headers of
// the form `AWSCognitoIdentityProviderService.<Action>`. This file owns the
// detection (`isCognitoTarget`) and routing (`handleCognitoJSON`) for that
// surface.
//
// Phase progression:
//   - GO-COGNITO-1: every action returns InternalErrorException placeholder.
//   - GO-COGNITO-2 (this phase): GetUser / AdminCreateUser / AdminDeleteUser
//     handlers go live. The other three actions stay on the placeholder.
//   - GO-COGNITO-3 / -5: InitiateAuth, RespondToAuthChallenge,
//     AdminInitiateAuth take over the remaining placeholders.
//
// Error envelope shape: AWS JSON 1.1 expects
// `{"__type": "<ServiceCode>", "message": "..."}` with Content-Type
// `application/x-amz-json-1.1`. The SDK's typed-error decoder reads `__type`
// (it strips the optional `<service>#` prefix). `message` (lowercase) is the
// Smithy-canonical field; some old AWS docs say `Message` (capital M). Both
// the v2 SDK and the providers we're matching read either, but lowercase is
// the one Cognito actually emits — match that.
//
// HTTP status convention: Cognito returns HTTP 400 for ALL typed errors —
// the typed-error decoders in cognito.py / cognito.go look at `__type`,
// not status (e.g. cognito.go:1099-1101). We follow that convention so a
// future stricter mapper or sniffer-based decoder still works.
package main

import (
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	mathrand "math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/rs/zerolog/log"
	"golang.org/x/crypto/bcrypt"
)

const cognitoTargetPrefix = "AWSCognitoIdentityProviderService."

// CognitoActions is the canonical ordered list of operations the platform
// invokes (design §2a). Phase-skeleton handlers must exist for every entry.
//
// Note: §2a lists 7 rows but `InitiateAuth` covers two flows
// (USER_PASSWORD_AUTH and REFRESH_TOKEN_AUTH) at the wire layer — that's still
// one action name. So the wire-distinct count is 6.
//
// GO-COGNITO-6 (Tier C) adds CreateUserPool / CreateUserPoolClient /
// DeleteUserPool / DeleteUserPoolClient for tests that need to spin up
// isolated pools without touching the canonical seed YAML.
var CognitoActions = []string{
	"InitiateAuth",
	"RespondToAuthChallenge",
	"GetUser",
	"AdminCreateUser",
	"AdminDeleteUser",
	"AdminInitiateAuth",
	"CreateUserPool",
	"CreateUserPoolClient",
	"DeleteUserPool",
	"DeleteUserPoolClient",
	// Provider-owned two-factor and revocation (organisation backend V2,
	// cognito_factors.go).
	"AssociateSoftwareToken",
	"VerifySoftwareToken",
	"SetUserMFAPreference",
	"AdminSetUserMFAPreference",
	"GlobalSignOut",
	"AdminUserGlobalSignOut",
	"RevokeToken",
	// Self-service password change (cognito_password.go).
	"ChangePassword",
}

// cognitoActionPhase tells operators which migration phase will implement
// each action. Embedded in the InternalErrorException placeholder so logs
// point at the right backlog row.
var cognitoActionPhase = map[string]string{
	"InitiateAuth":           "GO-COGNITO-3",
	"RespondToAuthChallenge": "GO-COGNITO-5",
	"GetUser":                "GO-COGNITO-2",
	"AdminCreateUser":        "GO-COGNITO-2",
	"AdminDeleteUser":        "GO-COGNITO-2",
	"AdminInitiateAuth":      "GO-COGNITO-5",
	"CreateUserPool":         "GO-COGNITO-6",
	"CreateUserPoolClient":   "GO-COGNITO-6",
	"DeleteUserPool":         "GO-COGNITO-6",
	"DeleteUserPoolClient":   "GO-COGNITO-6",
}

// Auth flows the dev service supports on InitiateAuth. SRP is intentionally
// absent (design §3h). The MFA challenge branch is owned by GO-COGNITO-5.
const (
	authFlowUserPassword = "USER_PASSWORD_AUTH"
	authFlowRefreshToken = "REFRESH_TOKEN_AUTH"
	authFlowAdminNoSRP   = "ADMIN_NO_SRP_AUTH" // owned by GO-COGNITO-5 (AdminInitiateAuth)
	authFlowUserSRP      = "USER_SRP_AUTH"     // intentionally unsupported (§3h)
)

// isCognitoTarget reports whether the X-Amz-Target header is a Cognito IDP op.
func isCognitoTarget(target string) bool {
	return strings.HasPrefix(target, cognitoTargetPrefix)
}

// extractCognitoAction strips the service prefix off the X-Amz-Target header.
// Returns "" if the target does not have the expected shape.
func extractCognitoAction(target string) string {
	if !strings.HasPrefix(target, cognitoTargetPrefix) {
		return ""
	}
	return target[len(cognitoTargetPrefix):]
}

// handleCognitoJSON dispatches a Cognito IDP JSON-1.1 request to the right
// per-action handler. Unknown actions get a 400 with `InvalidAction`.
//
// Implemented (GO-COGNITO-2): AdminCreateUser, AdminDeleteUser, GetUser.
// Stubbed (later phases): InitiateAuth, RespondToAuthChallenge,
// AdminInitiateAuth — each still returns the InternalErrorException
// placeholder until the owning phase ships.
func (s *Server) handleCognitoJSON(w http.ResponseWriter, r *http.Request, action string) {
	log.Debug().Str("action", action).Msg("Cognito IDP JSON API request")

	if action == "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidAction", "Missing or malformed X-Amz-Target")
		return
	}

	switch action {
	case "AdminCreateUser":
		s.handleAdminCreateUser(w, r)
		return
	case "AdminDeleteUser":
		s.handleAdminDeleteUser(w, r)
		return
	case "GetUser":
		s.handleGetUser(w, r)
		return
	case "InitiateAuth":
		s.handleInitiateAuth(w, r)
		return
	case "RespondToAuthChallenge":
		s.handleRespondToAuthChallenge(w, r)
		return
	case "AdminInitiateAuth":
		s.handleAdminInitiateAuth(w, r)
		return
	case "CreateUserPool":
		s.handleCreateUserPool(w, r)
		return
	case "CreateUserPoolClient":
		s.handleCreateUserPoolClient(w, r)
		return
	case "DeleteUserPool":
		s.handleDeleteUserPool(w, r)
		return
	case "DeleteUserPoolClient":
		s.handleDeleteUserPoolClient(w, r)
		return
	case "AssociateSoftwareToken":
		s.handleAssociateSoftwareToken(w, r)
		return
	case "VerifySoftwareToken":
		s.handleVerifySoftwareToken(w, r)
		return
	case "SetUserMFAPreference":
		s.handleSetUserMFAPreference(w, r)
		return
	case "AdminSetUserMFAPreference":
		s.handleAdminSetUserMFAPreference(w, r)
		return
	case "GlobalSignOut":
		s.handleGlobalSignOut(w, r)
		return
	case "AdminUserGlobalSignOut":
		s.handleAdminUserGlobalSignOut(w, r)
		return
	case "RevokeToken":
		s.handleRevokeToken(w, r)
		return
	case "ChangePassword":
		s.handleChangePassword(w, r)
		return
	}

	for _, known := range CognitoActions {
		if action == known {
			phase, ok := cognitoActionPhase[action]
			if !ok {
				phase = "later phase"
			}
			cognitoJSONError(
				w,
				http.StatusInternalServerError,
				"InternalErrorException",
				fmt.Sprintf("%s not yet implemented: %s", phase, action),
			)
			return
		}
	}

	cognitoJSONError(w, http.StatusBadRequest, "InvalidAction", fmt.Sprintf("Unknown Cognito IDP action: %s", action))
}

// cognitoJSONError writes an AWS JSON-1.1-shaped error response. Cognito uses
// this exact envelope; the existing Cognito-typed-error mappers in
// libs/platform_lib/.../cognito.py and go/libs/platform-lib/.../cognito.go
// extract `__type` and the message off it.
func cognitoJSONError(w http.ResponseWriter, statusCode int, code, message string) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"__type":  code,
		"message": message,
	})
}

// cognitoJSONResponse writes a successful JSON-1.1 response.
//
// Used by per-action handlers in later phases; kept here so the helper sits
// next to its error twin and so subsequent PRs don't have to add it.
func cognitoJSONResponse(w http.ResponseWriter, statusCode int, body interface{}) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(body)
}

// --- Admin / introspection handlers (GO-COGNITO-2) -----------------------

// cognitoAttribute is the wire-shape `{Name, Value}` element used in
// AdminCreateUser request UserAttributes and in GetUser/AdminCreateUser
// responses' Attributes / UserAttributes arrays.
type cognitoAttribute struct {
	Name  string `json:"Name"`
	Value string `json:"Value"`
}

// adminCreateUserRequest mirrors the AWS wire request for AdminCreateUser.
// We accept only the fields the platform sends today (cognito.py:194-208,
// cognito.go:611-629); unknown fields are tolerated by encoding/json's
// default permissiveness — Cognito does the same.
type adminCreateUserRequest struct {
	UserPoolID        string             `json:"UserPoolId"`
	Username          string             `json:"Username"`
	UserAttributes    []cognitoAttribute `json:"UserAttributes"`
	MessageAction     string             `json:"MessageAction"`     // SUPPRESS or empty (no-op for dev)
	TemporaryPassword string             `json:"TemporaryPassword"` // optional; generated if missing
}

// adminDeleteUserRequest mirrors the AWS wire request for AdminDeleteUser.
type adminDeleteUserRequest struct {
	UserPoolID string `json:"UserPoolId"`
	Username   string `json:"Username"`
}

// getUserRequest mirrors the AWS wire request for GetUser.
type getUserRequest struct {
	AccessToken string `json:"AccessToken"`
}

// readCognitoJSON decodes the request body into `dst`. On malformed JSON or
// read failure it writes an InvalidParameterException response and returns
// false so the caller bails out.
func readCognitoJSON(w http.ResponseWriter, r *http.Request, dst interface{}) bool {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "failed to read request body")
		return false
	}
	defer func() { _ = r.Body.Close() }()
	if len(body) == 0 {
		// Cognito accepts empty body for some ops; let the caller validate
		// required fields against the zero value.
		return true
	}
	if err := json.Unmarshal(body, dst); err != nil {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "malformed JSON request body")
		return false
	}
	return true
}

// handleAdminCreateUser implements the AdminCreateUser action.
// Behaviour (design §5d, §3c, §3i):
//   - Pool missing → ResourceNotFoundException.
//   - User exists in pool → UsernameExistsException.
//   - Generates `sub` as a fresh ULID. bcrypt-hashes the temporary password
//     (or a random one if not supplied) at the same cost the seed loader
//     uses (DefaultCost == 10).
//   - Always sets email_verified=true, matching cognito.py:198-200 and
//     cognito.go:614-625.
//   - Response wire shape: {"User": {Username, UserStatus, Enabled,
//     UserCreateDate, UserLastModifiedDate, Attributes:[...]}}.
func (s *Server) handleAdminCreateUser(w http.ResponseWriter, r *http.Request) {
	if s.cognito == nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", "cognito store not configured")
		return
	}
	var req adminCreateUserRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	if req.UserPoolID == "" || req.Username == "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "UserPoolId and Username are required")
		return
	}

	ctx := r.Context()

	exists, err := s.cognito.PoolExists(ctx, req.UserPoolID)
	if err != nil {
		log.Error().Err(err).Msg("PoolExists failed in AdminCreateUser")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}
	if !exists {
		cognitoJSONError(w, http.StatusBadRequest, "ResourceNotFoundException",
			fmt.Sprintf("User pool %s does not exist", req.UserPoolID))
		return
	}

	if existing, err := s.cognito.LookupUserByEmail(ctx, req.UserPoolID, req.Username); err == nil && existing != nil {
		cognitoJSONError(w, http.StatusBadRequest, "UsernameExistsException",
			fmt.Sprintf("User account already exists for %q", req.Username))
		return
	} else if err != nil && !errors.Is(err, sql.ErrNoRows) {
		log.Error().Err(err).Msg("LookupUserByEmail failed in AdminCreateUser")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}

	password := req.TemporaryPassword
	if password == "" {
		password = generateTemporaryPassword()
	} else {
		// Password policy (§3g): enforce only when the caller actually
		// supplied a TemporaryPassword. Generated passwords are 16 bytes
		// of base64url and would either always satisfy or randomly fail
		// strict policies — neither is useful, so skip the check when the
		// password is server-generated.
		if policy, perr := loadPoolPasswordPolicy(ctx, s.cognito, req.UserPoolID); perr == nil && policy != nil {
			if vErr := policy.Validate(password); vErr != nil {
				cognitoJSONError(w, http.StatusBadRequest, "InvalidPasswordException", vErr.Error())
				return
			}
		}
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		log.Error().Err(err).Msg("bcrypt failed in AdminCreateUser")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", "failed to hash password")
		return
	}

	sub := newULIDSub()
	if err := s.cognito.CreateUser(ctx, sub, req.UserPoolID, req.Username, string(hash), false); err != nil {
		log.Error().Err(err).Msg("CreateUser insert failed in AdminCreateUser")
		if isUsersConstraintViolation(err) {
			// A concurrent AdminCreateUser for the same (pool, username) won
			// the race between the LookupUserByEmail check above and this
			// insert — the only way this INSERT can fail on a genuine
			// collision (see isUsersConstraintViolation).
			cognitoJSONError(w, http.StatusBadRequest, "UsernameExistsException",
				fmt.Sprintf("User account already exists for %q", req.Username))
			return
		}
		// Any other failure (disk I/O, locked database, disk full, ...) is
		// not a duplicate-user conflict and must not be reported as one:
		// doing so sends callers chasing a nonexistent "this email is
		// already registered" bug instead of the real storage failure.
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}

	// Build the final attribute set: caller-supplied attrs, then forced
	// `sub` and `email_verified=true`. We track ordering so the response
	// puts the platform-relevant attributes (`sub`, `email`,
	// `email_verified`) first, then any extras — matching the order
	// MockProvider emits.
	attrSet := map[string]string{}
	for _, a := range req.UserAttributes {
		if a.Name == "" {
			continue
		}
		attrSet[a.Name] = a.Value
	}
	attrSet["sub"] = sub
	attrSet["email"] = req.Username
	attrSet["email_verified"] = "true"

	for name, value := range attrSet {
		if err := s.cognito.SetUserAttribute(ctx, sub, name, value); err != nil {
			log.Error().Err(err).Str("attr", name).Msg("SetUserAttribute failed in AdminCreateUser")
			cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
			return
		}
	}

	now := float64(time.Now().Unix())
	resp := map[string]interface{}{
		"User": map[string]interface{}{
			"Username":             req.Username,
			"UserStatus":           "CONFIRMED",
			"Enabled":              true,
			"UserCreateDate":       now,
			"UserLastModifiedDate": now,
			"Attributes":           orderedAttributes(attrSet),
		},
	}
	cognitoJSONResponse(w, http.StatusOK, resp)
}

// handleAdminDeleteUser implements the AdminDeleteUser action.
// Behaviour (design §5d):
//   - Pool missing → ResourceNotFoundException.
//   - User missing → success ({}, HTTP 200) — both providers swallow
//     UserNotFoundException from this op (cognito.py:240-243,
//     cognito.go:691-697).
func (s *Server) handleAdminDeleteUser(w http.ResponseWriter, r *http.Request) {
	if s.cognito == nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", "cognito store not configured")
		return
	}
	var req adminDeleteUserRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	if req.UserPoolID == "" || req.Username == "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "UserPoolId and Username are required")
		return
	}

	ctx := r.Context()
	exists, err := s.cognito.PoolExists(ctx, req.UserPoolID)
	if err != nil {
		log.Error().Err(err).Msg("PoolExists failed in AdminDeleteUser")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}
	if !exists {
		cognitoJSONError(w, http.StatusBadRequest, "ResourceNotFoundException",
			fmt.Sprintf("User pool %s does not exist", req.UserPoolID))
		return
	}

	if _, err := s.cognito.DeleteUserByEmail(ctx, req.UserPoolID, req.Username); err != nil {
		log.Error().Err(err).Msg("DeleteUserByEmail failed in AdminDeleteUser")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}

	// Idempotent: HTTP 200 with empty body whether the row existed or not.
	cognitoJSONResponse(w, http.StatusOK, map[string]interface{}{})
}

// handleGetUser implements the GetUser action.
// Behaviour (design §3i, §5d):
//   - Token validation failures (malformed, bad signature, expired, wrong
//     token_use) → NotAuthorizedException. We deliberately collapse them
//     all into a single error so attackers can't distinguish modes; AWS
//     does the same (cognito.go:1099-1101 only inspects `__type`).
//   - User absent → UserNotFoundException.
//
// Response field is `UserAttributes`, NOT `Attributes`. AWS uses different
// names for the AdminCreateUser User struct vs. GetUser response — keep
// them separate or boto3's typed parsers misalign.
func (s *Server) handleGetUser(w http.ResponseWriter, r *http.Request) {
	if s.cognito == nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", "cognito store not configured")
		return
	}
	var req getUserRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	user, ok := s.authorizeAccessToken(w, r, req.AccessToken, "GetUser")
	if !ok {
		return
	}

	ctx := r.Context()
	attrs, err := s.cognito.LoadUserAttributes(ctx, user.Sub)
	if err != nil {
		log.Error().Err(err).Msg("LoadUserAttributes failed in GetUser")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}

	resp := map[string]interface{}{
		"Username":       user.Email,
		"UserAttributes": orderedAttributes(attrs),
	}
	// Cognito reports the enabled MFA methods and the preferred one; both
	// are absent when none is enabled.
	if user.MFAEnabled {
		resp["UserMFASettingList"] = []string{"SOFTWARE_TOKEN_MFA"}
		resp["PreferredMfaSetting"] = "SOFTWARE_TOKEN_MFA"
	}
	cognitoJSONResponse(w, http.StatusOK, resp)
}

// authorizeAccessToken is the access-token check every AccessToken-bearing
// action shares: signature, expiry and token_use (VerifyAccessToken), the
// user still exists, and the token was not revoked by a global sign-out or
// RevokeToken. On failure it writes Cognito's response and returns false.
func (s *Server) authorizeAccessToken(w http.ResponseWriter, r *http.Request, token, action string) (*CognitoUser, bool) {
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

// orderedAttributes returns the attribute map as a slice, with the platform-
// relevant attributes (`sub`, `email`, `email_verified`) listed first in that
// order, then the rest. The platform doesn't depend on order, but stable
// ordering keeps test fixtures and golden snapshots quiet.
func orderedAttributes(m map[string]string) []cognitoAttribute {
	pinned := []string{"sub", "email", "email_verified"}
	out := make([]cognitoAttribute, 0, len(m))
	seen := map[string]bool{}
	for _, name := range pinned {
		if v, ok := m[name]; ok {
			out = append(out, cognitoAttribute{Name: name, Value: v})
			seen[name] = true
		}
	}
	// Remaining attributes — sorted lexicographically for determinism.
	rest := make([]string, 0, len(m))
	for k := range m {
		if !seen[k] {
			rest = append(rest, k)
		}
	}
	// Insertion-sort works for short lists and keeps the deps small.
	for i := 1; i < len(rest); i++ {
		j := i
		for j > 0 && rest[j-1] > rest[j] {
			rest[j-1], rest[j] = rest[j], rest[j-1]
			j--
		}
	}
	for _, k := range rest {
		out = append(out, cognitoAttribute{Name: k, Value: m[k]})
	}
	return out
}

// isUsersConstraintViolation reports whether err is a SQLite constraint
// failure (PRIMARY KEY, UNIQUE, NOT NULL, ...) as opposed to an I/O, locking,
// or other storage-layer failure. `users.sub` is the only constrained column
// on the insert path (schema note: idx_users_pool_email, unlike the primary
// key, is a plain non-unique index — see bootstrap()), and sub is a freshly
// generated ULID, so this only ever fires on the astronomically unlikely sub
// collision or a genuine concurrent-insert race. Every other error (notably
// "disk I/O error", which is what a full or memory-starved tmpfs /tmp
// produces) must surface as a storage failure, not a fabricated
// UsernameExistsException — see AdminCreateUser's caller in
// go/services/directory, which treats any "already exists" response as
// proof the email is taken.
func isUsersConstraintViolation(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "constraint failed")
}

// newULIDSub returns a fresh ULID-derived sub. ULID is the project standard
// for sortable IDs (GO_TECHSPEC.md), and 26-char Crockford-base32 stays
// inside any tooling that expects an opaque user id.
func newULIDSub() string {
	// crypto/rand for entropy — never math/rand. We don't depend on a
	// monotonic generator since AdminCreateUser is not in a hot loop.
	return ulid.MustNew(ulid.Timestamp(time.Now()), rand.Reader).String()
}

// generateTemporaryPassword produces a 16-byte URL-safe random string for
// when the caller didn't supply TemporaryPassword. Cognito's real default is
// "AWS will generate one and email it" — for the dev service we just need a
// non-empty input to bcrypt; the password is unreachable until the operator
// resets it via a future SetUserPassword/AdminSetUserPassword op.
func generateTemporaryPassword() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Fallback to math/rand so we still produce *something* on the
		// astronomically unlikely crypto/rand failure path; this is dev-only.
		_, _ = mathrand.New(mathrand.NewSource(time.Now().UnixNano())).Read(b[:])
	}
	return base64.RawURLEncoding.EncodeToString(b[:])
}

// --- InitiateAuth (GO-COGNITO-3) ----------------------------------------

// initiateAuthRequest mirrors the AWS wire request for InitiateAuth.
// `AuthParameters` is a free-form string map; required keys depend on
// `AuthFlow`. `SECRET_HASH` may be present when the configured client has
// a secret — phase 6 (GO-COGNITO-6) adds verification, for now we ignore it.
type initiateAuthRequest struct {
	AuthFlow       string            `json:"AuthFlow"`
	ClientID       string            `json:"ClientId"`
	AuthParameters map[string]string `json:"AuthParameters"`
}

// handleInitiateAuth implements the InitiateAuth action.
//
// Two flows are supported per §2a (rows 1-2):
//
//   - USER_PASSWORD_AUTH: bcrypt-verify the supplied password; mint a fresh
//     access + refresh JWT; return them in AuthenticationResult. If the user
//     has mfa_enabled=true, return NotAuthorizedException with a phase-5
//     pointer message (the proper challenge response is owned by
//     GO-COGNITO-5; we deliberately do NOT silently authenticate MFA users
//     here, otherwise phase-5 work would land into a passing test suite).
//   - REFRESH_TOKEN_AUTH: verify the refresh JWT (signature + iss + aud +
//     exp + token_use=="refresh"); mint a fresh access token only; do NOT
//     issue a new refresh token (design §3f, cognito.py:99, cognito.go:442).
//
// Error mapping (design §3i):
//   - ClientId unknown          → ResourceNotFoundException (HTTP 400)
//   - User unknown              → UserNotFoundException     (HTTP 400)
//   - Wrong password            → NotAuthorizedException    (HTTP 400)
//   - Refresh: any verify error → NotAuthorizedException    (HTTP 400)
func (s *Server) handleInitiateAuth(w http.ResponseWriter, r *http.Request) {
	if s.cognito == nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", "cognito store not configured")
		return
	}
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
		// AdminInitiateAuth ships in GO-COGNITO-5 — but if a caller routes
		// ADMIN_NO_SRP_AUTH through the public InitiateAuth endpoint, fail
		// loud rather than silently misbehave.
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
func (s *Server) handleInitiateAuthUserPassword(w http.ResponseWriter, r *http.Request, client *CognitoClient, req initiateAuthRequest) {
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

	// MFA branch (GO-COGNITO-5). Issue a SOFTWARE_TOKEN_MFA challenge:
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
func (s *Server) writeAuthenticated(
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

// handleInitiateAuthRefreshToken owns the REFRESH_TOKEN_AUTH branch. Per
// design §3f, the refresh token itself is NOT rotated — we mint a new
// access token only and the response intentionally omits RefreshToken
// (matches AWS behavior; the Python provider falls back to the existing
// refresh token at cognito.py:99).
func (s *Server) handleInitiateAuthRefreshToken(w http.ResponseWriter, r *http.Request, client *CognitoClient, req initiateAuthRequest) {
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

	// No RefreshToken field — design §3f. The Python provider already falls
	// back to the original refresh on a missing RefreshToken in the response.
	resp := map[string]interface{}{
		"AuthenticationResult": map[string]interface{}{
			"AccessToken": access,
			"ExpiresIn":   int(s.accessTokenTTL.Seconds()),
			"TokenType":   "Bearer",
		},
	}
	cognitoJSONResponse(w, http.StatusOK, resp)
}

// --- Challenge flows (GO-COGNITO-5) -------------------------------------

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
func (s *Server) issueMFAChallenge(w http.ResponseWriter, r *http.Request, poolID, clientID string, user *CognitoUser) {
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

// handleRespondToAuthChallenge implements RespondToAuthChallenge for the
// three challenge types the platform actually issues:
//
//   - SOFTWARE_TOKEN_MFA / SMS_MFA: validate a 6-digit code (any 6 digits
//     pass for now — deterministic TOTP arrives in GO-COGNITO-6, matching
//     MockProvider mock.py:163-165).
//   - NEW_PASSWORD_REQUIRED: validate the supplied password is non-empty,
//     bcrypt-hash it, and update the user's password. (Password policy
//     lands in GO-COGNITO-6.)
//
// Error mapping (design §3i):
//   - Wrong code            → CodeMismatchException    (HTTP 400)
//   - Expired session       → ExpiredCodeException     (HTTP 400)
//   - Forged HMAC, replay,  →
//     unknown session, type
//     mismatch, missing user → NotAuthorizedException   (HTTP 400)
//
// On success: mark session used=1, mint access+refresh tokens, return
// {AuthenticationResult: {...}}.
func (s *Server) handleRespondToAuthChallenge(w http.ResponseWriter, r *http.Request) {
	if s.cognito == nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", "cognito store not configured")
		return
	}
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
			// path documented in design §3e.
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

// handleAdminInitiateAuth implements AdminInitiateAuth for the
// ADMIN_NO_SRP_AUTH flow only (cognito.go:826-833). The platform no longer
// calls it; it stays for fidelity with the Cognito surface.
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
func (s *Server) handleAdminInitiateAuth(w http.ResponseWriter, r *http.Request) {
	if s.cognito == nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", "cognito store not configured")
		return
	}
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

// isSixDigits reports whether s is exactly 6 ASCII decimal digits. The dev
// service accepts any 6-digit string as a valid MFA code (matches
// MockProvider mock.py:163-165). Phase 6 (GO-COGNITO-6) replaces this with
// deterministic TOTP verification.
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
