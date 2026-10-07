package cognito

import (
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/oklog/ulid/v2"
)

type cognitoAttribute struct {
	Name  string `json:"Name"`
	Value string `json:"Value"`
}

type adminCreateUserRequest struct {
	UserPoolID             string             `json:"UserPoolId"`
	Username               string             `json:"Username"`
	UserAttributes         []cognitoAttribute `json:"UserAttributes"`
	MessageAction          string             `json:"MessageAction"`
	TemporaryPassword      string             `json:"TemporaryPassword"`
	DesiredDeliveryMediums []string           `json:"DesiredDeliveryMediums"`
	ForceAliasCreation     bool               `json:"ForceAliasCreation"`
}

type adminDeleteUserRequest struct {
	UserPoolID string `json:"UserPoolId"`
	Username   string `json:"Username"`
}

type getUserRequest struct {
	AccessToken string `json:"AccessToken"`
}

// AdminCreateUser preserves the caller's username and email as independent
// identities. Password-backed invitations start in FORCE_CHANGE_PASSWORD;
// RESEND replaces only the temporary credential and preserves the account.
func (s *Handler) handleAdminCreateUser(w http.ResponseWriter, r *http.Request) {
	var req adminCreateUserRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	if req.UserPoolID == "" || req.Username == "" || utf8.RuneCountInString(req.Username) > 128 || strings.ContainsFunc(req.Username, unicode.IsSpace) {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "UserPoolId and a valid Username are required")
		return
	}
	if req.MessageAction != "" && req.MessageAction != "SUPPRESS" && req.MessageAction != "RESEND" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "MessageAction must be SUPPRESS or RESEND")
		return
	}
	config, err := s.cognito.GetPoolSignInConfig(r.Context(), req.UserPoolID)
	if errors.Is(err, sql.ErrNoRows) {
		cognitoJSONError(w, http.StatusBadRequest, "ResourceNotFoundException", fmt.Sprintf("User pool %s does not exist", req.UserPoolID))
		return
	}
	if err != nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}
	attributes := map[string]string{}
	for _, attribute := range req.UserAttributes {
		if attribute.Name == "" || attribute.Name == "sub" {
			cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "Attribute names must be non-empty and sub is immutable")
			return
		}
		attributes[attribute.Name] = attribute.Value
	}
	email := attributes["email"]
	if config.EmailAsUsername {
		if email == "" {
			email = req.Username
			attributes["email"] = email
		}
		if email != req.Username {
			cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "Username must match the email sign-in attribute")
			return
		}
	}
	if attributes["email_verified"] == "true" && email == "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "email is required when email_verified is true")
		return
	}
	if attributes["email_verified"] != "" && attributes["email_verified"] != "true" && attributes["email_verified"] != "false" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "email_verified must be true or false")
		return
	}
	if config.EmailAlias && strings.Contains(req.Username, "@") {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "Email aliases cannot also be used as canonical usernames")
		return
	}
	if req.ForceAliasCreation {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "ForceAliasCreation is not implemented by the local emulator")
		return
	}
	pool, err := s.cognito.LookupPool(r.Context(), req.UserPoolID)
	if err != nil {
		authInternalError(w, err, "AdminCreateUser")
		return
	}
	if pool.SchemaAttributes != nil {
		if err = ValidateUserAttributes(pool.SchemaAttributes, nil, attributes, false, true, true); err != nil {
			writeWorkflowError(w, err)
			return
		}
	}
	if pool.Native || !s.allowLegacyFixturePool(pool) {
		if err := validateInvitationMediums(req.DesiredDeliveryMediums); err != nil {
			writeWorkflowError(w, err)
			return
		}
	}
	deliver := req.MessageAction != "SUPPRESS" && (pool.Native || !s.allowLegacyFixturePool(pool))
	var resendUser *CognitoUser
	if deliver {
		deliveryAttributes := attributes
		if req.MessageAction == "RESEND" {
			resendUser, err = s.cognito.LookupPoolUser(r.Context(), req.UserPoolID, req.Username)
			if errors.Is(err, sql.ErrNoRows) {
				cognitoJSONError(w, 400, "UserNotFoundException", "User does not exist")
				return
			}
			if err != nil {
				authInternalError(w, err, "AdminCreateUser")
				return
			}
			deliveryAttributes, err = s.cognito.LoadUserAttributes(r.Context(), resendUser.Sub)
			if err != nil {
				authInternalError(w, err, "AdminCreateUser")
				return
			}
		}
		if err = validateInvitationDelivery(deliveryAttributes, req.DesiredDeliveryMediums); err != nil {
			writeWorkflowError(w, err)
			return
		}
		if s.notifications == nil {
			writeWorkflowError(w, workflowCodeError("CodeDeliveryFailureException", "Invitation notification capture failed"))
			return
		}
	}
	policy, err := loadPoolPasswordPolicy(r.Context(), s.cognito, req.UserPoolID)
	if err != nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}
	password := req.TemporaryPassword
	if password == "" {
		password, err = generatePolicyPassword(policy)
		if err != nil {
			cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", "failed to generate temporary password")
			return
		}
	}
	if message := lifecyclePasswordValidation(password, policy); message != "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidPasswordException", message)
		return
	}
	var user *CognitoUser
	if req.MessageAction == "RESEND" {
		user = resendUser
		if user == nil {
			user, err = s.cognito.LookupPoolUser(r.Context(), req.UserPoolID, req.Username)
		}
		if errors.Is(err, sql.ErrNoRows) {
			cognitoJSONError(w, http.StatusBadRequest, "UserNotFoundException", "User does not exist")
			return
		}
		if err == nil {
			err = s.cognito.SetUserPassword(r.Context(), user.Sub, password, "FORCE_CHANGE_PASSWORD")
		}
		if err == nil {
			user, err = s.cognito.LookupUserBySub(r.Context(), user.Sub)
		}
	} else {
		user, err = s.cognito.CreateUserIdentity(r.Context(), req.UserPoolID, req.Username, email, password, "FORCE_CHANGE_PASSWORD", attributes)
	}
	if errors.Is(err, errUsernameExists) {
		cognitoJSONError(w, http.StatusBadRequest, "UsernameExistsException", fmt.Sprintf("User account already exists for %q", req.Username))
		return
	}
	if errors.Is(err, errEmailAliasExists) {
		cognitoJSONError(w, http.StatusBadRequest, "AliasExistsException", "An account with the email alias already exists")
		return
	}
	if err != nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}
	body, err := s.userProfile(r, user, "Attributes")
	if err != nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}
	if deliver {
		if err = s.deliverInvitation(r.Context(), "", user, password, req.DesiredDeliveryMediums); err != nil {
			writeWorkflowError(w, err)
			return
		}
	}
	cognitoJSONResponse(w, http.StatusOK, map[string]interface{}{"User": body})
}

func (s *Handler) handleAdminDeleteUser(w http.ResponseWriter, r *http.Request) {
	var req adminDeleteUserRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	if req.UserPoolID == "" || req.Username == "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "UserPoolId and Username are required")
		return
	}
	exists, err := s.cognito.PoolExists(r.Context(), req.UserPoolID)
	if err != nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}
	if !exists {
		cognitoJSONError(w, http.StatusBadRequest, "ResourceNotFoundException", fmt.Sprintf("User pool %s does not exist", req.UserPoolID))
		return
	}
	if _, err = s.cognito.DeletePoolUser(r.Context(), req.UserPoolID, req.Username); err != nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}
	cognitoJSONResponse(w, http.StatusOK, map[string]interface{}{})
}

func (s *Handler) handleGetUser(w http.ResponseWriter, r *http.Request) {
	var req getUserRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	user, client, ok := s.authorizeClientAccessToken(w, r, req.AccessToken, "GetUser")
	if !ok {
		return
	}
	attributes, err := s.cognito.LoadUserAttributes(r.Context(), user.Sub)
	if err != nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}
	if client.Native || client.ReadAttributes != nil {
		pool, err := s.cognito.LookupPool(r.Context(), user.PoolID)
		if err != nil {
			authInternalError(w, err, "GetUser")
			return
		}
		attributes = FilterClientReadAttributes(pool.SchemaAttributes, client.ReadAttributes, attributes)
	}
	body := map[string]interface{}{"Username": user.Username, "UserAttributes": orderedAttributes(attributes)}
	if user.MFAEnabled {
		body["UserMFASettingList"] = []string{"SOFTWARE_TOKEN_MFA"}
		body["PreferredMfaSetting"] = "SOFTWARE_TOKEN_MFA"
	}
	cognitoJSONResponse(w, http.StatusOK, body)
}

func isUsersConstraintViolation(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "constraint failed")
}

func generatePolicyPassword(policy *PasswordPolicy) (string, error) {
	length := 24
	if policy != nil && policy.MinLength > length {
		length = policy.MinLength
	}
	if length > 256 {
		return "", errors.New("password policy exceeds the password length limit")
	}
	entropy := make([]byte, length)
	if _, err := rand.Read(entropy); err != nil {
		return "", err
	}
	return "Aa1!" + base64.RawURLEncoding.EncodeToString(entropy)[:length-4], nil
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

func newULIDSub() string {
	// crypto/rand for entropy — never math/rand. We don't depend on a
	// monotonic generator since AdminCreateUser is not in a hot loop.
	return ulid.MustNew(ulid.Timestamp(time.Now()), rand.Reader).String()
}
