package cognito

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// Signup and recovery requests share public client-secret admission. Metadata
// has no effect without the corresponding Lambda trigger; risk/analytics and
// immediate USER_AUTH signup sessions are explicitly outside this subset.
type publicWorkflowRequest struct {
	ClientID           string             `json:"ClientId"`
	Username           string             `json:"Username"`
	SecretHash         string             `json:"SecretHash"`
	Password           string             `json:"Password"`
	ConfirmationCode   string             `json:"ConfirmationCode"`
	UserAttributes     []cognitoAttribute `json:"UserAttributes"`
	ClientMetadata     map[string]string  `json:"ClientMetadata"`
	ValidationData     []cognitoAttribute `json:"ValidationData"`
	AnalyticsMetadata  json.RawMessage    `json:"AnalyticsMetadata"`
	UserContextData    json.RawMessage    `json:"UserContextData"`
	Session            string             `json:"Session"`
	ForceAliasCreation bool               `json:"ForceAliasCreation"`
}

func writeWorkflowError(w http.ResponseWriter, err error) {
	code, message := "InternalErrorException", err.Error()
	status := http.StatusInternalServerError
	var workflow *workflowError
	var validation *AttributeValidationError
	switch {
	case errors.As(err, &workflow):
		code, message, status = workflow.Code, workflow.Message, http.StatusBadRequest
	case errors.As(err, &validation):
		code, message, status = validation.Code, validation.Message, http.StatusBadRequest
	case errors.Is(err, errTokenRevoked), errors.Is(err, errUserDisabled):
		code, message, status = "NotAuthorizedException", "User is disabled or this authorization is no longer valid", http.StatusBadRequest
	case errors.Is(err, errEmailAliasExists):
		code, message, status = "AliasExistsException", "An account with this verified alias already exists", http.StatusBadRequest
	case errors.Is(err, errUsernameExists):
		code, message, status = "UsernameExistsException", "User account already exists", http.StatusBadRequest
	case errors.Is(err, sql.ErrNoRows):
		code, message, status = "UserNotFoundException", "User does not exist", http.StatusBadRequest
	}
	cognitoJSONError(w, status, code, message)
}
func selectedWorkflowOption(raw json.RawMessage) bool {
	return len(raw) != 0 && string(raw) != "null" && string(raw) != "{}"
}
func (s *Handler) readPublicWorkflow(w http.ResponseWriter, r *http.Request) (publicWorkflowRequest, *CognitoClient, *CognitoPool, bool) {
	var req publicWorkflowRequest
	if !readCognitoJSON(w, r, &req) {
		return req, nil, nil, false
	}
	if req.ClientID == "" || req.Username == "" || utf8.RuneCountInString(req.Username) > 128 || strings.ContainsFunc(req.Username, unicode.IsSpace) {
		writeWorkflowError(w, workflowCodeError("InvalidParameterException", "ClientId and a valid Username are required"))
		return req, nil, nil, false
	}
	if req.ForceAliasCreation || req.Session != "" || selectedWorkflowOption(req.AnalyticsMetadata) || selectedWorkflowOption(req.UserContextData) || len(req.ValidationData) > 0 {
		writeWorkflowError(w, workflowCodeError("InvalidParameterException", "Alias migration, immediate signup sessions, analytics, threat protection and pre-signup ValidationData are not supported"))
		return req, nil, nil, false
	}
	client, ok := s.authClient(w, r, req.ClientID, "")
	if !ok {
		return req, nil, nil, false
	}
	if err := verifySecretHash(client.Secret, req.Username, client.ID, req.SecretHash); err != nil {
		writeWorkflowError(w, workflowCodeError("NotAuthorizedException", "Unable to verify secret hash for client"))
		return req, nil, nil, false
	}
	pool, err := s.cognito.LookupPool(r.Context(), client.PoolID)
	if err != nil {
		writeWorkflowError(w, err)
		return req, nil, nil, false
	}
	return req, client, pool, true
}
func workflowAttributes(input []cognitoAttribute) (map[string]string, error) {
	attributes := make(map[string]string, len(input))
	for _, attribute := range input {
		if attribute.Name == "" {
			return nil, workflowCodeError("InvalidParameterException", "Attribute names must not be empty")
		}
		if _, exists := attributes[attribute.Name]; exists {
			return nil, workflowCodeError("InvalidParameterException", "Duplicate user attribute "+attribute.Name)
		}
		attributes[attribute.Name] = attribute.Value
	}
	return attributes, nil
}
func (s *Handler) workflowUser(ctx context.Context, poolID, username string) (*CognitoUser, error) {
	user, err := s.cognito.ResolveSignInUser(ctx, poolID, username)
	if err != nil {
		return nil, err
	}
	if !user.Enabled {
		return nil, errUserDisabled
	}
	return user, nil
}
func validWorkflowCode(code string) bool {
	return code != "" && utf8.RuneCountInString(code) <= 2048 && !strings.ContainsFunc(code, unicode.IsSpace)
}
func selectSignupDestination(pool *CognitoPool, attributes map[string]string) (string, string) {
	// Cognito chooses SMS first when both contact attributes auto-verify.
	for _, name := range []string{"phone_number", "email"} {
		for _, configured := range pool.AutoVerifiedAttributes {
			if configured == name && attributes[name] != "" {
				return name, attributes[name]
			}
		}
	}
	return "", ""
}
func selectRecoveryDestination(pool *CognitoPool, attributes map[string]string) (string, string, error) {
	mechanisms := []RecoveryMechanism{{"verified_phone_number", 1}, {"verified_email", 2}}
	if pool.AccountRecoverySetting != nil {
		mechanisms = append([]RecoveryMechanism(nil), pool.AccountRecoverySetting.RecoveryMechanisms...)
		sort.Slice(mechanisms, func(i, j int) bool { return mechanisms[i].Priority < mechanisms[j].Priority })
	}
	for _, mechanism := range mechanisms {
		if mechanism.Name == "admin_only" {
			return "", "", workflowCodeError("InvalidParameterException", "Self-service account recovery is not enabled for this user pool")
		}
		name := strings.TrimPrefix(mechanism.Name, "verified_")
		if attributes[name] != "" && attributes[name+"_verified"] == "true" {
			return name, attributes[name], nil
		}
	}
	return "", "", workflowCodeError("InvalidParameterException", "Cannot reset password: no eligible verified recovery destination")
}
func (s *Handler) issueVerification(ctx context.Context, operation, clientID string, user *CognitoUser, purpose, attribute, destination string, ttl time.Duration) (codeDeliveryDetails, error) {
	details := deliveryDetails(attribute, destination)
	code, err := generateVerificationCode()
	if err != nil {
		return details, err
	}
	notification := Notification{Operation: operation, PoolID: user.PoolID, ClientID: clientID, Username: user.Username, UserSub: user.Sub, Destination: destination, DeliveryMedium: details.DeliveryMedium, AttributeName: attribute, Purpose: purpose, Code: code, Timestamp: s.cognito.now().UTC()}
	if s.notifications == nil || s.notifications.Deliver(ctx, notification) != nil {
		return details, workflowCodeError("CodeDeliveryFailureException", "Failed to deliver verification message")
	}
	if err = s.cognito.putVerification(ctx, user, verificationSpec{purpose, attribute, destination, code, s.cognito.now().Add(ttl)}); err != nil {
		return details, err
	}
	return details, nil
}

func (s *Handler) handleSignUp(w http.ResponseWriter, r *http.Request) {
	req, client, pool, ok := s.readPublicWorkflow(w, r)
	if !ok {
		return
	}
	if pool.AdminCreateUserConfig != nil && pool.AdminCreateUserConfig.AllowAdminCreateUserOnly {
		writeWorkflowError(w, workflowCodeError("NotAuthorizedException", "SignUp is not permitted for this user pool"))
		return
	}
	attributes, err := workflowAttributes(req.UserAttributes)
	if err != nil {
		writeWorkflowError(w, err)
		return
	}
	config, err := s.cognito.GetPoolSignInConfig(r.Context(), pool.ID)
	if err != nil {
		writeWorkflowError(w, err)
		return
	}
	if config.EmailAsUsername {
		if attributes["email"] == "" {
			attributes["email"] = req.Username
		}
		if normalizeSignIn(attributes["email"], config) != normalizeSignIn(req.Username, config) {
			writeWorkflowError(w, workflowCodeError("InvalidParameterException", "Username must match the email sign-in attribute"))
			return
		}
	}
	if config.EmailAlias && strings.Contains(req.Username, "@") {
		writeWorkflowError(w, workflowCodeError("InvalidParameterException", "Email aliases cannot also be canonical usernames"))
		return
	}
	if err = ValidateClientWriteAttributes(pool.SchemaAttributes, client.WriteAttributes, attributes); err != nil {
		writeWorkflowError(w, err)
		return
	}
	if err = ValidateUserAttributes(pool.SchemaAttributes, nil, attributes, true, true, false); err != nil {
		writeWorkflowError(w, err)
		return
	}
	policy, err := loadPoolPasswordPolicy(r.Context(), s.cognito, pool.ID)
	if err != nil {
		writeWorkflowError(w, err)
		return
	}
	if message := lifecyclePasswordValidation(req.Password, policy); message != "" {
		writeWorkflowError(w, workflowCodeError("InvalidPasswordException", message))
		return
	}
	for _, name := range []string{"email", "phone_number"} {
		if attributes[name] != "" {
			attributes[name+"_verified"] = "false"
		}
	}
	user, err := s.cognito.CreateUserIdentity(r.Context(), pool.ID, req.Username, attributes["email"], req.Password, "UNCONFIRMED", attributes)
	if err != nil {
		writeWorkflowError(w, err)
		return
	}
	body := map[string]interface{}{"UserConfirmed": false, "UserSub": user.Sub}
	if attribute, destination := selectSignupDestination(pool, attributes); attribute != "" {
		// Store creation normalizes case-insensitive emails, so capture the persisted
		// destination rather than the unnormalized submitted spelling.
		if attribute == "email" {
			destination = user.Email
		}
		details, err := s.issueVerification(r.Context(), "SignUp", client.ID, user, "signup", attribute, destination, 24*time.Hour)
		if err != nil {
			writeWorkflowError(w, err)
			return
		}
		body["CodeDeliveryDetails"] = details
	}
	cognitoJSONResponse(w, http.StatusOK, body)
}
func (s *Handler) handleConfirmSignUp(w http.ResponseWriter, r *http.Request) {
	req, _, pool, ok := s.readPublicWorkflow(w, r)
	if !ok {
		return
	}
	if !validWorkflowCode(req.ConfirmationCode) {
		writeWorkflowError(w, workflowCodeError("InvalidParameterException", "ConfirmationCode is required and must contain 1 to 2048 non-whitespace characters"))
		return
	}
	user, err := s.workflowUser(r.Context(), pool.ID, req.Username)
	if err != nil {
		writeWorkflowError(w, err)
		return
	}
	if err = s.cognito.confirmVerification(r.Context(), user, "signup", req.ConfirmationCode, true); err != nil {
		writeWorkflowError(w, err)
		return
	}
	cognitoJSONResponse(w, http.StatusOK, map[string]interface{}{})
}
func (s *Handler) handleResendConfirmationCode(w http.ResponseWriter, r *http.Request) {
	req, client, pool, ok := s.readPublicWorkflow(w, r)
	if !ok {
		return
	}
	user, err := s.workflowUser(r.Context(), pool.ID, req.Username)
	if err != nil {
		writeWorkflowError(w, err)
		return
	}
	if user.Status != "UNCONFIRMED" {
		writeWorkflowError(w, workflowCodeError("InvalidParameterException", "User is already confirmed"))
		return
	}
	attrs, err := s.cognito.LoadUserAttributes(r.Context(), user.Sub)
	if err != nil {
		writeWorkflowError(w, err)
		return
	}
	attribute, destination := selectSignupDestination(pool, attrs)
	if attribute == "" {
		writeWorkflowError(w, workflowCodeError("InvalidParameterException", "No auto-verified contact attribute is available"))
		return
	}
	details, err := s.issueVerification(r.Context(), "ResendConfirmationCode", client.ID, user, "signup", attribute, destination, 24*time.Hour)
	if err != nil {
		writeWorkflowError(w, err)
		return
	}
	cognitoJSONResponse(w, http.StatusOK, map[string]interface{}{"CodeDeliveryDetails": details})
}
func (s *Handler) handleForgotPassword(w http.ResponseWriter, r *http.Request) {
	req, client, pool, ok := s.readPublicWorkflow(w, r)
	if !ok {
		return
	}
	user, err := s.workflowUser(r.Context(), pool.ID, req.Username)
	if err != nil {
		writeWorkflowError(w, err)
		return
	}
	if user.Status == "UNCONFIRMED" {
		writeWorkflowError(w, workflowCodeError("InvalidParameterException", "Cannot reset password for an unconfirmed user"))
		return
	}
	attrs, err := s.cognito.LoadUserAttributes(r.Context(), user.Sub)
	if err != nil {
		writeWorkflowError(w, err)
		return
	}
	attribute, destination, err := selectRecoveryDestination(pool, attrs)
	if err != nil {
		writeWorkflowError(w, err)
		return
	}
	details, err := s.issueVerification(r.Context(), "ForgotPassword", client.ID, user, "recovery", attribute, destination, time.Hour)
	if err != nil {
		writeWorkflowError(w, err)
		return
	}
	cognitoJSONResponse(w, http.StatusOK, map[string]interface{}{"CodeDeliveryDetails": details})
}
func (s *Handler) handleConfirmForgotPassword(w http.ResponseWriter, r *http.Request) {
	req, _, pool, ok := s.readPublicWorkflow(w, r)
	if !ok {
		return
	}
	if !validWorkflowCode(req.ConfirmationCode) {
		writeWorkflowError(w, workflowCodeError("InvalidParameterException", "ConfirmationCode is required"))
		return
	}
	user, err := s.workflowUser(r.Context(), pool.ID, req.Username)
	if err != nil {
		writeWorkflowError(w, err)
		return
	}
	// Recheck recovery policy so changing to admin_only cannot admit an old code.
	attrs, err := s.cognito.LoadUserAttributes(r.Context(), user.Sub)
	if err != nil {
		writeWorkflowError(w, err)
		return
	}
	if _, _, err = selectRecoveryDestination(pool, attrs); err != nil {
		writeWorkflowError(w, err)
		return
	}
	policy, err := loadPoolPasswordPolicy(r.Context(), s.cognito, pool.ID)
	if err != nil {
		writeWorkflowError(w, err)
		return
	}
	if message := lifecyclePasswordValidation(req.Password, policy); message != "" {
		writeWorkflowError(w, workflowCodeError("InvalidPasswordException", message))
		return
	}
	if err = s.cognito.confirmPasswordRecovery(r.Context(), user, req.ConfirmationCode, req.Password); err != nil {
		writeWorkflowError(w, err)
		return
	}
	cognitoJSONResponse(w, http.StatusOK, map[string]interface{}{})
}
func (s *Handler) handleAdminConfirmSignUp(w http.ResponseWriter, r *http.Request) {
	var req adminDeleteUserRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	user, ok := s.lookupAdminUser(w, r, req.UserPoolID, req.Username, "AdminConfirmSignUp")
	if !ok {
		return
	}
	if err := s.cognito.adminConfirmSignUp(r.Context(), user); err != nil {
		writeWorkflowError(w, err)
		return
	}
	cognitoJSONResponse(w, http.StatusOK, map[string]interface{}{})
}
