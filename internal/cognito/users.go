// Cognito user creation, deletion and introspection.
package cognito

import (
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	mathrand "math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/rs/zerolog/log"
	"golang.org/x/crypto/bcrypt"
)

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

// handleAdminCreateUser implements the AdminCreateUser action.
// Behaviour:
//   - Pool missing → ResourceNotFoundException.
//   - User exists in pool → UsernameExistsException.
//   - Generates `sub` as a fresh ULID. bcrypt-hashes the temporary password
//     (or a random one if not supplied) at the same cost the seed loader
//     uses (DefaultCost == 10).
//   - Always sets email_verified=true, matching cognito.py:198-200 and
//     cognito.go:614-625.
//   - Response wire shape: {"User": {Username, UserStatus, Enabled,
//     UserCreateDate, UserLastModifiedDate, Attributes:[...]}}.
func (s *Handler) handleAdminCreateUser(w http.ResponseWriter, r *http.Request) {
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
// Behaviour :
//   - Pool missing → ResourceNotFoundException.
//   - User missing → success ({}, HTTP 200) — both providers swallow
//     UserNotFoundException from this op (cognito.py:240-243,
//     cognito.go:691-697).
func (s *Handler) handleAdminDeleteUser(w http.ResponseWriter, r *http.Request) {
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
// Behaviour:
//   - Token validation failures (malformed, bad signature, expired, wrong
//     token_use) → NotAuthorizedException. We deliberately collapse them
//     all into a single error so attackers can't distinguish modes; AWS
//     does the same (cognito.go:1099-1101 only inspects `__type`).
//   - User absent → UserNotFoundException.
//
// Response field is `UserAttributes`, NOT `Attributes`. AWS uses different
// names for the AdminCreateUser User struct vs. GetUser response — keep
// them separate or boto3's typed parsers misalign.
func (s *Handler) handleGetUser(w http.ResponseWriter, r *http.Request) {
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
