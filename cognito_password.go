// ChangePassword for the local Cognito dev service: a signed-in user replaces
// their own password with their access token and their previous password
// (the platform's self-service password change).
//
// It follows Cognito, in the order Cognito checks:
//
//   - the request's shape: each password at most 256 characters and matching
//     `^[\S]+.*[\S]+$` (no leading or trailing whitespace, at least two
//     characters), else InvalidParameterException with Cognito's validation
//     message;
//   - the access token, as every AccessToken action checks it (expired,
//     foreign or revoked: NotAuthorizedException);
//   - the attempt limit: after changePasswordAttemptLimit wrong previous
//     passwords, LimitExceededException "Attempt limit exceeded, please try
//     after some time." for changePasswordLockout, whatever is sent;
//   - the previous password: wrong or missing is NotAuthorizedException
//     "Incorrect username or password." and counts toward the limit;
//   - the pool's password policy: InvalidPasswordException in Cognito's words,
//     one rule at a time ("Password does not conform to policy: Password not
//     long enough").
//
// A change revokes no token: Cognito leaves every session of the user valid,
// which is why the platform ends them itself (its session cutoff) and signs
// the user out at the provider.
//
// Cognito documents neither the limit nor the lockout for this call. Five
// matches its sign-in lockout, which starts after five failed attempts; the
// quarter hour is where that lockout tops out.
package main

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/rs/zerolog/log"
	"golang.org/x/crypto/bcrypt"
)

const (
	// changePasswordAttemptLimit wrong previous passwords lock ChangePassword.
	changePasswordAttemptLimit = 5
	// changePasswordLockout is how long ChangePassword stays locked.
	changePasswordLockout = 15 * time.Minute
	// cognitoPasswordMaxLength is the API's length limit on either password.
	cognitoPasswordMaxLength = 256
)

// cognitoPasswordPattern is the API's pattern for PreviousPassword and
// ProposedPassword.
const cognitoPasswordPattern = `^[\S]+.*[\S]+$`

var cognitoPasswordRE = regexp.MustCompile(cognitoPasswordPattern)

// changePasswordPolicyPrefix opens Cognito's InvalidPasswordException message
// for ChangePassword.
const changePasswordPolicyPrefix = "Password does not conform to policy: "

type changePasswordRequest struct {
	AccessToken string `json:"AccessToken"`
	// PreviousPassword is a pointer: Cognito tells an omitted previous
	// password (refused as a wrong one) from an empty one (a malformed
	// request).
	PreviousPassword *string `json:"PreviousPassword"`
	ProposedPassword string  `json:"ProposedPassword"`
}

func (s *Server) handleChangePassword(w http.ResponseWriter, r *http.Request) {
	if s.cognito == nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", "cognito store not configured")
		return
	}
	var req changePasswordRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	if message := changePasswordValidation(req); message != "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", message)
		return
	}
	user, ok := s.authorizeAccessToken(w, r, req.AccessToken, "ChangePassword")
	if !ok {
		return
	}
	ctx := r.Context()
	now := time.Now()
	if user.PasswordLockedUntil > now.Unix() {
		cognitoJSONError(w, http.StatusBadRequest, "LimitExceededException",
			"Attempt limit exceeded, please try after some time.")
		return
	}
	if req.PreviousPassword == nil ||
		bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(*req.PreviousPassword)) != nil {
		if err := s.cognito.RecordPasswordFailure(ctx, user.Sub, now, changePasswordAttemptLimit, changePasswordLockout); err != nil {
			log.Error().Err(err).Msg("RecordPasswordFailure failed in ChangePassword")
			cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
			return
		}
		cognitoJSONError(w, http.StatusBadRequest, "NotAuthorizedException", "Incorrect username or password.")
		return
	}
	policy, err := loadPoolPasswordPolicy(ctx, s.cognito, user.PoolID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		log.Error().Err(err).Msg("loadPoolPasswordPolicy failed in ChangePassword")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}
	if rule := policy.CognitoViolation(req.ProposedPassword); rule != "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidPasswordException", changePasswordPolicyPrefix+rule)
		return
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(req.ProposedPassword), bcrypt.DefaultCost)
	if err != nil {
		log.Error().Err(err).Msg("bcrypt failed in ChangePassword")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", "failed to hash password")
		return
	}
	if err := s.cognito.ChangeUserPassword(ctx, user.Sub, string(hash)); err != nil {
		log.Error().Err(err).Msg("ChangeUserPassword failed in ChangePassword")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}
	cognitoJSONResponse(w, http.StatusOK, map[string]interface{}{})
}

// changePasswordValidation is Cognito's request validation: every field that
// breaks the API's constraints, in its message form, or "" when none does.
func changePasswordValidation(req changePasswordRequest) string {
	var failures []string
	check := func(member string, value string) {
		switch {
		case len(value) > cognitoPasswordMaxLength:
			failures = append(failures, fmt.Sprintf(
				"Value at '%s' failed to satisfy constraint: Member must have length less than or equal to %d",
				member, cognitoPasswordMaxLength))
		case !cognitoPasswordRE.MatchString(value):
			failures = append(failures, fmt.Sprintf(
				"Value at '%s' failed to satisfy constraint: Member must satisfy regular expression pattern: %s",
				member, cognitoPasswordPattern))
		}
	}
	if req.PreviousPassword != nil {
		check("previousPassword", *req.PreviousPassword)
	}
	check("proposedPassword", req.ProposedPassword)
	switch len(failures) {
	case 0:
		return ""
	case 1:
		return "1 validation error detected: " + failures[0]
	default:
		return fmt.Sprintf("%d validation errors detected: %s", len(failures), strings.Join(failures, "; "))
	}
}

// CognitoViolation is the first rule of the policy the password breaks, in
// Cognito's words ("Password not long enough"), or "" when it breaks none.
// Cognito reports one rule at a time. A nil policy is the pool without one.
func (p *PasswordPolicy) CognitoViolation(password string) string {
	if p == nil {
		return ""
	}
	if p.MinLength > 0 && len([]rune(password)) < p.MinLength {
		return "Password not long enough"
	}
	hasUpper, hasLower, hasDigit, hasSymbol := false, false, false, false
	for _, r := range password {
		switch {
		case unicode.IsUpper(r):
			hasUpper = true
		case unicode.IsLower(r):
			hasLower = true
		case unicode.IsDigit(r):
			hasDigit = true
		case unicode.IsPunct(r) || unicode.IsSymbol(r) || r == ' ':
			hasSymbol = true
		}
	}
	switch {
	case p.RequireUppercase && !hasUpper:
		return "Password must have uppercase characters"
	case p.RequireLowercase && !hasLower:
		return "Password must have lowercase characters"
	case p.RequireDigits && !hasDigit:
		return "Password must have numeric characters"
	case p.RequireSymbols && !hasSymbol:
		return "Password must have symbol characters"
	}
	return ""
}
