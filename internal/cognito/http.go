// AWS Cognito JSON-1.1 protocol dispatch and envelopes.
package cognito

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/lyeith/eventbus/internal/awsprotocol"
	"github.com/rs/zerolog/log"
)

const cognitoTargetPrefix = "AWSCognitoIdentityProviderService."

// IsTarget reports whether the X-Amz-Target header is a Cognito IDP op.
func IsTarget(target string) bool {
	return strings.HasPrefix(target, cognitoTargetPrefix)
}

// Action strips the service prefix off the X-Amz-Target header.
// Returns "" if the target does not have the expected shape.
func Action(target string) string {
	if !strings.HasPrefix(target, cognitoTargetPrefix) {
		return ""
	}
	return target[len(cognitoTargetPrefix):]
}

// ServeAction dispatches an AWS Cognito JSON-1.1 operation.
// Unknown actions return InvalidAction; a nil store retains the configured-store error.
func (s *Handler) ServeAction(w http.ResponseWriter, r *http.Request, action string) {
	log.Debug().Str("action", action).Msg("Cognito IDP JSON API request")

	if action == "" {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidAction", "Missing or malformed X-Amz-Target")
		return
	}

	var handler http.HandlerFunc
	switch action {
	case "AdminCreateUser":
		handler = s.handleAdminCreateUser
	case "AdminGetUser":
		handler = s.handleAdminGetUser
	case "ListUsers":
		handler = s.handleListUsers
	case "AdminSetUserPassword":
		handler = s.handleAdminSetUserPassword
	case "AdminDisableUser":
		handler = s.handleAdminDisableUser
	case "AdminEnableUser":
		handler = s.handleAdminEnableUser
	case "AdminRespondToAuthChallenge":
		handler = s.handleAdminRespondToAuthChallenge
	case "AdminDeleteUser":
		handler = s.handleAdminDeleteUser
	case "GetUser":
		handler = s.handleGetUser
	case "InitiateAuth":
		handler = s.handleInitiateAuth
	case "RespondToAuthChallenge":
		handler = s.handleRespondToAuthChallenge
	case "AdminInitiateAuth":
		handler = s.handleAdminInitiateAuth
	case "CreateUserPool":
		handler = s.handleCreateUserPool
	case "CreateUserPoolClient":
		handler = s.handleCreateUserPoolClient
	case "DeleteUserPool":
		handler = s.handleDeleteUserPool
	case "DeleteUserPoolClient":
		handler = s.handleDeleteUserPoolClient
	case "AssociateSoftwareToken":
		handler = s.handleAssociateSoftwareToken
	case "VerifySoftwareToken":
		handler = s.handleVerifySoftwareToken
	case "SetUserMFAPreference":
		handler = s.handleSetUserMFAPreference
	case "AdminSetUserMFAPreference":
		handler = s.handleAdminSetUserMFAPreference
	case "GlobalSignOut":
		handler = s.handleGlobalSignOut
	case "AdminUserGlobalSignOut":
		handler = s.handleAdminUserGlobalSignOut
	case "RevokeToken":
		handler = s.handleRevokeToken
	case "ChangePassword":
		handler = s.handleChangePassword
	}

	if handler == nil {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidAction", fmt.Sprintf("Unknown Cognito IDP action: %s", action))
		return
	}
	if s.cognito == nil {
		cognitoJSONError(w, http.StatusInternalServerError, "InternalErrorException", "cognito store not configured")
		return
	}
	handler(w, r)
}

// cognitoJSONError writes the AWS JSON-1.1 typed-error envelope.
func cognitoJSONError(w http.ResponseWriter, statusCode int, code, message string) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	if w.Header().Get("X-Amzn-RequestId") == "" {
		w.Header().Set("X-Amzn-RequestId", awsprotocol.RequestID())
	}
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"__type":  code,
		"message": message,
	})
}

// cognitoJSONResponse writes a successful JSON-1.1 response.
func cognitoJSONResponse(w http.ResponseWriter, statusCode int, body interface{}) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	if w.Header().Get("X-Amzn-RequestId") == "" {
		w.Header().Set("X-Amzn-RequestId", awsprotocol.RequestID())
	}
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(body)
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
