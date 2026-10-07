package cognito

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/rs/zerolog/log"
)

func (s *Handler) userProfile(r *http.Request, user *CognitoUser, attributeField string) (map[string]interface{}, error) {
	attributes, err := s.cognito.LoadUserAttributes(r.Context(), user.Sub)
	if err != nil {
		return nil, err
	}
	body := map[string]interface{}{
		"Username": user.Username, "Enabled": user.Enabled, "UserStatus": user.Status,
		"UserCreateDate": float64(user.CreatedAt), "UserLastModifiedDate": float64(user.UpdatedAt),
		attributeField: orderedAttributes(attributes),
	}
	if attributeField == "UserAttributes" && user.MFAEnabled {
		body["UserMFASettingList"] = []string{"SOFTWARE_TOKEN_MFA"}
		body["PreferredMfaSetting"] = "SOFTWARE_TOKEN_MFA"
	}
	return body, nil
}

// lookupAdminUser resolves an admin action's pool and Username (sub or
// email), writing Cognito's error on failure.
func (s *Handler) lookupAdminUser(w http.ResponseWriter, r *http.Request, poolID, username, action string) (*CognitoUser, bool) {
	if poolID == "" || username == "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "UserPoolId and Username are required")
		return nil, false
	}
	exists, err := s.cognito.PoolExists(r.Context(), poolID)
	if err != nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return nil, false
	}
	if !exists {
		cognitoJSONError(w, http.StatusBadRequest, "ResourceNotFoundException", "User pool "+poolID+" does not exist")
		return nil, false
	}
	user, err := s.cognito.LookupPoolUser(r.Context(), poolID, username)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			cognitoJSONError(w, http.StatusBadRequest, "UserNotFoundException", "User does not exist")
			return nil, false
		}
		log.Error().Err(err).Str("action", action).Msg("LookupPoolUser failed")
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return nil, false
	}
	return user, true
}

func (s *Handler) handleAdminGetUser(w http.ResponseWriter, r *http.Request) {
	var req adminDeleteUserRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	user, ok := s.lookupAdminUser(w, r, req.UserPoolID, req.Username, "AdminGetUser")
	if !ok {
		return
	}
	body, err := s.userProfile(r, user, "UserAttributes")
	if err != nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}
	cognitoJSONResponse(w, http.StatusOK, body)
}

type adminSetUserPasswordRequest struct {
	UserPoolID string `json:"UserPoolId"`
	Username   string `json:"Username"`
	Password   string `json:"Password"`
	Permanent  bool   `json:"Permanent"`
}

func lifecyclePasswordValidation(password string, policy *PasswordPolicy) string {
	if password == "" || utf8.RuneCountInString(password) > 256 || strings.ContainsFunc(password, unicode.IsSpace) {
		return "Password must contain 1 to 256 non-whitespace characters"
	}
	if policy != nil {
		if violation := policy.CognitoViolation(password); violation != "" {
			return changePasswordPolicyPrefix + violation
		}
	}
	return ""
}

func (s *Handler) handleAdminSetUserPassword(w http.ResponseWriter, r *http.Request) {
	var req adminSetUserPasswordRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	user, ok := s.lookupAdminUser(w, r, req.UserPoolID, req.Username, "AdminSetUserPassword")
	if !ok {
		return
	}
	policy, err := loadPoolPasswordPolicy(r.Context(), s.cognito, req.UserPoolID)
	if err != nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}
	if message := lifecyclePasswordValidation(req.Password, policy); message != "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidPasswordException", message)
		return
	}
	status := "FORCE_CHANGE_PASSWORD"
	if req.Permanent {
		status = "CONFIRMED"
	}
	if err = s.cognito.SetUserPassword(r.Context(), user.Sub, req.Password, status); err != nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}
	cognitoJSONResponse(w, http.StatusOK, map[string]interface{}{})
}

func (s *Handler) handleAdminDisableUser(w http.ResponseWriter, r *http.Request) {
	s.handleSetUserEnabled(w, r, false, "AdminDisableUser")
}
func (s *Handler) handleAdminEnableUser(w http.ResponseWriter, r *http.Request) {
	s.handleSetUserEnabled(w, r, true, "AdminEnableUser")
}
func (s *Handler) handleSetUserEnabled(w http.ResponseWriter, r *http.Request, enabled bool, action string) {
	var req adminDeleteUserRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	user, ok := s.lookupAdminUser(w, r, req.UserPoolID, req.Username, action)
	if !ok {
		return
	}
	if err := s.cognito.SetUserEnabled(r.Context(), user.Sub, enabled); err != nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}
	cognitoJSONResponse(w, http.StatusOK, map[string]interface{}{})
}

type listUsersRequest struct {
	UserPoolID      string   `json:"UserPoolId"`
	Filter          string   `json:"Filter"`
	Limit           *int     `json:"Limit"`
	PaginationToken string   `json:"PaginationToken"`
	AttributesToGet []string `json:"AttributesToGet"`
}

var emailFilterRE = regexp.MustCompile(`^\s*email\s*(\^=|=)\s*("(?:[^"\\]|\\.)*")\s*$`)

func parseEmailFilter(filter string) (string, string, error) {
	match := emailFilterRE.FindStringSubmatch(filter)
	if match == nil {
		return "", "", &invalidUserListParameter{"Only email equality (=) and prefix (^=) filters are supported"}
	}
	var value string
	if err := json.Unmarshal([]byte(match[2]), &value); err != nil {
		return "", "", &invalidUserListParameter{"Invalid quoted email filter"}
	}
	return match[1], value, nil
}

type invalidUserListParameter struct{ message string }

func (e *invalidUserListParameter) Error() string { return e.message }

func (s *Handler) handleListUsers(w http.ResponseWriter, r *http.Request) {
	var req listUsersRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	if req.UserPoolID == "" || utf8.RuneCountInString(req.Filter) > 256 {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "UserPoolId is required and Filter must be at most 256 characters")
		return
	}
	exists, err := s.cognito.PoolExists(r.Context(), req.UserPoolID)
	if err != nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		return
	}
	if !exists {
		cognitoJSONError(w, http.StatusBadRequest, "ResourceNotFoundException", "User pool does not exist")
		return
	}
	limit := 60
	if req.Limit != nil {
		limit = *req.Limit
	}
	if limit < 0 || limit > 60 {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "Limit must be between 0 and 60")
		return
	}
	if req.Filter != "" {
		if _, _, err = parseEmailFilter(req.Filter); err != nil {
			cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", err.Error())
			return
		}
	}
	for _, attribute := range req.AttributesToGet {
		if attribute == "" || utf8.RuneCountInString(attribute) > 32 || strings.ContainsRune(attribute, 0) {
			cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "AttributesToGet names must contain 1 to 32 characters")
			return
		}
	}
	users, token, err := s.cognito.ListPoolUsers(r.Context(), req.UserPoolID, UserListOptions{Filter: req.Filter, Limit: limit, PaginationToken: req.PaginationToken, AttributesToGet: req.AttributesToGet})
	if err != nil {
		var invalid *invalidUserListParameter
		if errors.As(err, &invalid) {
			cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", err.Error())
		} else {
			cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
		}
		return
	}

	profiles := make([]map[string]interface{}, 0, len(users))
	for _, user := range users {
		profile, err := s.userProfile(r, user, "Attributes")
		if err != nil {
			cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
			return
		}
		if len(req.AttributesToGet) != 0 {
			attributes, err := s.cognito.LoadUserAttributes(r.Context(), user.Sub)
			if err != nil {
				cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", err.Error())
				return
			}
			selected := map[string]string{}
			for _, name := range req.AttributesToGet {
				value, exists := attributes[name]
				if !exists {
					cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "An attribute requested in AttributesToGet is missing from a returned user")
					return
				}
				selected[name] = value
			}
			profile["Attributes"] = orderedAttributes(selected)
		}
		profiles = append(profiles, profile)
	}
	body := map[string]interface{}{"Users": profiles}
	if token != "" {
		body["PaginationToken"] = token
	}
	cognitoJSONResponse(w, http.StatusOK, body)
}
