package secrets

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/lyeith/eventbus/internal/awsprotocol"
)

// maxJSONRequestBytes retains this adapter's existing local transport budget.
const maxJSONRequestBytes int64 = 1 << 20

// Handler owns the Secrets Manager AWS JSON adapter.
type Handler struct {
	secrets *SecretsStore
}

func NewHandler(store *SecretsStore) *Handler {
	return &Handler{secrets: store}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.ServeAction(w, r, awsprotocol.TargetAction(r.Header.Get("X-Amz-Target")))
}

func (s *Handler) ServeAction(w http.ResponseWriter, r *http.Request, action string) {
	log.Debug().Str("action", action).Msg("Secrets Manager JSON API request")

	switch action {
	case "CreateSecret":
		s.handleCreateSecret(w, r)
	case "PutSecretValue":
		s.handlePutSecretValue(w, r)
	case "GetSecretValue":
		s.handleGetSecretValue(w, r)
	case "UpdateSecret":
		s.handleUpdateSecret(w, r)
	case "DeleteSecret":
		s.handleDeleteSecret(w, r)
	default:
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidAction", fmt.Sprintf("Unknown Secrets Manager action: %s", action))
	}
}

func (s *Handler) handleCreateSecret(w http.ResponseWriter, r *http.Request) {
	data, err := awsprotocol.ReadJSONBody(r, maxJSONRequestBytes)
	if err != nil {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidInput", "Invalid JSON body")
		return
	}

	name, _ := data["Name"].(string)
	secretString, _ := data["SecretString"].(string)

	if name == "" {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidParameter", "Name is required")
		return
	}

	// Parse tags
	tags := make(map[string]string)
	if rawTags, ok := data["Tags"].([]interface{}); ok {
		for _, rt := range rawTags {
			if tag, ok := rt.(map[string]interface{}); ok {
				key, _ := tag["Key"].(string)
				value, _ := tag["Value"].(string)
				if key != "" {
					tags[key] = value
				}
			}
		}
	}

	secret, err := s.secrets.CreateSecret(
		name,
		secretString,
		stringValue(data, "ClientRequestToken"),
		tags,
	)
	if err != nil {
		if strings.Contains(err.Error(), "ResourceExistsException") {
			awsprotocol.JSONError(w, http.StatusBadRequest, "ResourceExistsException", "Secret already exists: "+name)
			return
		}
		awsprotocol.JSONError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	log.Debug().Str("name", name).Msg("Secret created")

	awsprotocol.JSONResponse(w, http.StatusOK, map[string]interface{}{
		"ARN":       secret.ARN,
		"Name":      secret.Name,
		"VersionId": secret.VersionID,
	})
}

func (s *Handler) handleGetSecretValue(w http.ResponseWriter, r *http.Request) {
	data, err := awsprotocol.ReadJSONBody(r, maxJSONRequestBytes)
	if err != nil {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidInput", "Invalid JSON body")
		return
	}

	secretID, _ := data["SecretId"].(string)
	if secretID == "" {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidParameter", "SecretId is required")
		return
	}

	secret, err := s.secrets.GetSecretValue(secretID, stringValue(data, "VersionId"))
	if err != nil {
		awsprotocol.JSONError(w, http.StatusBadRequest, "ResourceNotFoundException", "Secret not found: "+secretID)
		return
	}

	awsprotocol.JSONResponse(w, http.StatusOK, map[string]interface{}{
		"ARN":          secret.ARN,
		"Name":         secret.Name,
		"SecretString": secret.SecretString,
		"VersionId":    secret.VersionID,
		"CreatedDate":  secret.CreatedDate.Unix(),
	})
}

func (s *Handler) handleUpdateSecret(w http.ResponseWriter, r *http.Request) {
	data, err := awsprotocol.ReadJSONBody(r, maxJSONRequestBytes)
	if err != nil {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidInput", "Invalid JSON body")
		return
	}

	secretID, _ := data["SecretId"].(string)
	secretString, _ := data["SecretString"].(string)

	if secretID == "" {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidParameter", "SecretId is required")
		return
	}

	secret, err := s.secrets.UpdateSecret(secretID, secretString, stringValue(data, "ClientRequestToken"))
	if err != nil {
		awsprotocol.JSONError(w, http.StatusBadRequest, "ResourceNotFoundException", "Secret not found: "+secretID)
		return
	}

	log.Debug().Str("name", secret.Name).Msg("Secret updated")

	awsprotocol.JSONResponse(w, http.StatusOK, map[string]interface{}{
		"ARN":       secret.ARN,
		"Name":      secret.Name,
		"VersionId": secret.VersionID,
	})
}
func (s *Handler) handlePutSecretValue(w http.ResponseWriter, r *http.Request) {
	data, err := awsprotocol.ReadJSONBody(r, maxJSONRequestBytes)
	if err != nil {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidInput", "Invalid JSON body")
		return
	}

	secretID, _ := data["SecretId"].(string)
	if secretID == "" {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidParameter", "SecretId is required")
		return
	}

	secret, err := s.secrets.PutSecretValue(
		secretID,
		stringValue(data, "SecretString"),
		stringValue(data, "ClientRequestToken"),
	)
	if err != nil {
		errorType := "ResourceNotFoundException"
		if strings.Contains(err.Error(), "ResourceExistsException") {
			errorType = "ResourceExistsException"
		}
		awsprotocol.JSONError(w, http.StatusBadRequest, errorType, err.Error())
		return
	}

	awsprotocol.JSONResponse(w, http.StatusOK, map[string]interface{}{
		"ARN":       secret.ARN,
		"Name":      secret.Name,
		"VersionId": secret.VersionID,
	})
}

func stringValue(data map[string]interface{}, key string) string {
	value, _ := data[key].(string)
	return value
}

func (s *Handler) handleDeleteSecret(w http.ResponseWriter, r *http.Request) {
	data, err := awsprotocol.ReadJSONBody(r, maxJSONRequestBytes)
	if err != nil {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidInput", "Invalid JSON body")
		return
	}

	secretID, _ := data["SecretId"].(string)
	if secretID == "" {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidParameter", "SecretId is required")
		return
	}

	forceDelete, _ := data["ForceDeleteWithoutRecovery"].(bool)

	err = s.secrets.DeleteSecret(secretID, forceDelete)
	if err != nil {
		awsprotocol.JSONError(w, http.StatusBadRequest, "ResourceNotFoundException", "Secret not found: "+secretID)
		return
	}

	log.Debug().Str("secretId", secretID).Msg("Secret deleted")

	awsprotocol.JSONResponse(w, http.StatusOK, map[string]interface{}{
		"ARN":          secretID,
		"Name":         secretID,
		"DeletionDate": float64(time.Now().Unix()),
	})
}
