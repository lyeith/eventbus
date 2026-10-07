package cognito

import (
	"net/http"
	"time"
)

type attributeVerificationRequest struct {
	AccessToken    string            `json:"AccessToken"`
	AttributeName  string            `json:"AttributeName"`
	Code           string            `json:"Code"`
	ClientMetadata map[string]string `json:"ClientMetadata"`
}

func (s *Handler) handleGetUserAttributeVerificationCode(w http.ResponseWriter, r *http.Request) {
	var req attributeVerificationRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	if req.AttributeName != "email" && req.AttributeName != "phone_number" {
		writeWorkflowError(w, workflowCodeError("InvalidParameterException", "Only email and phone_number support verification"))
		return
	}
	user, client, ok := s.authorizeClientAccessToken(w, r, req.AccessToken, "GetUserAttributeVerificationCode")
	if !ok {
		return
	}
	attrs, err := s.cognito.LoadUserAttributes(r.Context(), user.Sub)
	if err != nil {
		writeWorkflowError(w, err)
		return
	}
	if attrs[req.AttributeName] == "" {
		writeWorkflowError(w, workflowCodeError("InvalidParameterException", "User has no "+req.AttributeName+" attribute"))
		return
	}
	details, err := s.issueVerification(r.Context(), "GetUserAttributeVerificationCode", client.ID, user, "attribute:"+req.AttributeName, req.AttributeName, attrs[req.AttributeName], 24*time.Hour)
	if err != nil {
		writeWorkflowError(w, err)
		return
	}
	cognitoJSONResponse(w, http.StatusOK, map[string]interface{}{"CodeDeliveryDetails": details})
}
func (s *Handler) handleVerifyUserAttribute(w http.ResponseWriter, r *http.Request) {
	var req attributeVerificationRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	if (req.AttributeName != "email" && req.AttributeName != "phone_number") || !validWorkflowCode(req.Code) {
		writeWorkflowError(w, workflowCodeError("InvalidParameterException", "AttributeName must be email or phone_number and Code is required"))
		return
	}
	user, _, ok := s.authorizeClientAccessToken(w, r, req.AccessToken, "VerifyUserAttribute")
	if !ok {
		return
	}
	if err := s.cognito.confirmVerification(r.Context(), user, "attribute:"+req.AttributeName, req.Code, false); err != nil {
		writeWorkflowError(w, err)
		return
	}
	cognitoJSONResponse(w, http.StatusOK, map[string]interface{}{})
}

type updateUserAttributesRequest struct {
	AccessToken    string             `json:"AccessToken"`
	UserPoolID     string             `json:"UserPoolId"`
	Username       string             `json:"Username"`
	UserAttributes []cognitoAttribute `json:"UserAttributes"`
	ClientMetadata map[string]string  `json:"ClientMetadata"`
}

func (s *Handler) handleUpdateUserAttributes(w http.ResponseWriter, r *http.Request) {
	s.handleAttributeUpdate(w, r, false)
}
func (s *Handler) handleAdminUpdateUserAttributes(w http.ResponseWriter, r *http.Request) {
	s.handleAttributeUpdate(w, r, true)
}
func (s *Handler) handleAttributeUpdate(w http.ResponseWriter, r *http.Request, administrator bool) {
	var req updateUserAttributesRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	var user *CognitoUser
	var client *CognitoClient
	var ok bool
	operation := "UpdateUserAttributes"
	if administrator {
		operation = "AdminUpdateUserAttributes"
		user, ok = s.lookupAdminUser(w, r, req.UserPoolID, req.Username, operation)
	} else {
		user, client, ok = s.authorizeClientAccessToken(w, r, req.AccessToken, operation)
	}
	if !ok {
		return
	}
	updates, err := workflowAttributes(req.UserAttributes)
	if err != nil {
		writeWorkflowError(w, err)
		return
	}
	if len(updates) == 0 {
		writeWorkflowError(w, workflowCodeError("InvalidParameterException", "UserAttributes must not be empty"))
		return
	}
	pool, err := s.cognito.LookupPool(r.Context(), user.PoolID)
	if err != nil {
		writeWorkflowError(w, err)
		return
	}
	if !administrator {
		if err = ValidateClientWriteAttributes(pool.SchemaAttributes, client.WriteAttributes, updates); err != nil {
			writeWorkflowError(w, err)
			return
		}
	}
	if err = s.cognito.updateWorkflowAttributes(r.Context(), user, pool.SchemaAttributes, updates, administrator); err != nil {
		writeWorkflowError(w, err)
		return
	}
	details := []codeDeliveryDetails{}
	attrs, err := s.cognito.LoadUserAttributes(r.Context(), user.Sub)
	if err != nil {
		writeWorkflowError(w, err)
		return
	}
	for _, attribute := range []string{"phone_number", "email"} {
		if _, updated := updates[attribute]; !updated || attrs[attribute] == "" || attrs[attribute+"_verified"] == "true" {
			continue
		}
		selected := false
		for _, configured := range pool.AutoVerifiedAttributes {
			if configured == attribute {
				selected = true
			}
		}
		if !selected {
			continue
		}
		clientID := ""
		if client != nil {
			clientID = client.ID
		}
		delivery, err := s.issueVerification(r.Context(), operation, clientID, user, "attribute:"+attribute, attribute, attrs[attribute], 24*time.Hour)
		if err != nil {
			writeWorkflowError(w, err)
			return
		}
		details = append(details, delivery)
	}
	body := map[string]interface{}{}
	if !administrator {
		body["CodeDeliveryDetailsList"] = details
	}
	cognitoJSONResponse(w, http.StatusOK, body)
}
