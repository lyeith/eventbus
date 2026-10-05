package main

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

func (s *Server) handleSecretsJSON(w http.ResponseWriter, r *http.Request, action string) {
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
		jsonError(w, http.StatusBadRequest, "InvalidAction", fmt.Sprintf("Unknown Secrets Manager action: %s", action))
	}
}

func (s *Server) handleCreateSecret(w http.ResponseWriter, r *http.Request) {
	data, err := readJSONBody(r)
	if err != nil {
		jsonError(w, http.StatusBadRequest, "InvalidInput", "Invalid JSON body")
		return
	}

	name, _ := data["Name"].(string)
	secretString, _ := data["SecretString"].(string)

	if name == "" {
		jsonError(w, http.StatusBadRequest, "InvalidParameter", "Name is required")
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
		s.broker.region,
		s.broker.accountID,
	)
	if err != nil {
		if strings.Contains(err.Error(), "ResourceExistsException") {
			jsonError(w, http.StatusBadRequest, "ResourceExistsException", "Secret already exists: "+name)
			return
		}
		jsonError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	log.Debug().Str("name", name).Msg("Secret created")

	jsonResponse(w, http.StatusOK, map[string]interface{}{
		"ARN":       secret.ARN,
		"Name":      secret.Name,
		"VersionId": secret.VersionID,
	})
}

func (s *Server) handleGetSecretValue(w http.ResponseWriter, r *http.Request) {
	data, err := readJSONBody(r)
	if err != nil {
		jsonError(w, http.StatusBadRequest, "InvalidInput", "Invalid JSON body")
		return
	}

	secretID, _ := data["SecretId"].(string)
	if secretID == "" {
		jsonError(w, http.StatusBadRequest, "InvalidParameter", "SecretId is required")
		return
	}

	secret, err := s.secrets.GetSecretValue(secretID, stringValue(data, "VersionId"))
	if err != nil {
		jsonError(w, http.StatusBadRequest, "ResourceNotFoundException", "Secret not found: "+secretID)
		return
	}

	jsonResponse(w, http.StatusOK, map[string]interface{}{
		"ARN":          secret.ARN,
		"Name":         secret.Name,
		"SecretString": secret.SecretString,
		"VersionId":    secret.VersionID,
		"CreatedDate":  secret.CreatedDate.Unix(),
	})
}

func (s *Server) handleUpdateSecret(w http.ResponseWriter, r *http.Request) {
	data, err := readJSONBody(r)
	if err != nil {
		jsonError(w, http.StatusBadRequest, "InvalidInput", "Invalid JSON body")
		return
	}

	secretID, _ := data["SecretId"].(string)
	secretString, _ := data["SecretString"].(string)

	if secretID == "" {
		jsonError(w, http.StatusBadRequest, "InvalidParameter", "SecretId is required")
		return
	}

	secret, err := s.secrets.UpdateSecret(secretID, secretString, stringValue(data, "ClientRequestToken"))
	if err != nil {
		jsonError(w, http.StatusBadRequest, "ResourceNotFoundException", "Secret not found: "+secretID)
		return
	}

	log.Debug().Str("name", secret.Name).Msg("Secret updated")

	jsonResponse(w, http.StatusOK, map[string]interface{}{
		"ARN":       secret.ARN,
		"Name":      secret.Name,
		"VersionId": secret.VersionID,
	})
}
func (s *Server) handlePutSecretValue(w http.ResponseWriter, r *http.Request) {
	data, err := readJSONBody(r)
	if err != nil {
		jsonError(w, http.StatusBadRequest, "InvalidInput", "Invalid JSON body")
		return
	}

	secretID, _ := data["SecretId"].(string)
	if secretID == "" {
		jsonError(w, http.StatusBadRequest, "InvalidParameter", "SecretId is required")
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
		jsonError(w, http.StatusBadRequest, errorType, err.Error())
		return
	}

	jsonResponse(w, http.StatusOK, map[string]interface{}{
		"ARN":       secret.ARN,
		"Name":      secret.Name,
		"VersionId": secret.VersionID,
	})
}

func stringValue(data map[string]interface{}, key string) string {
	value, _ := data[key].(string)
	return value
}

func (s *Server) handleDeleteSecret(w http.ResponseWriter, r *http.Request) {
	data, err := readJSONBody(r)
	if err != nil {
		jsonError(w, http.StatusBadRequest, "InvalidInput", "Invalid JSON body")
		return
	}

	secretID, _ := data["SecretId"].(string)
	if secretID == "" {
		jsonError(w, http.StatusBadRequest, "InvalidParameter", "SecretId is required")
		return
	}

	forceDelete, _ := data["ForceDeleteWithoutRecovery"].(bool)

	err = s.secrets.DeleteSecret(secretID, forceDelete)
	if err != nil {
		jsonError(w, http.StatusBadRequest, "ResourceNotFoundException", "Secret not found: "+secretID)
		return
	}

	log.Debug().Str("secretId", secretID).Msg("Secret deleted")

	jsonResponse(w, http.StatusOK, map[string]interface{}{
		"ARN":          secretID,
		"Name":         secretID,
		"DeletionDate": float64(time.Now().Unix()),
	})
}

// isSecretsManagerTarget checks if the X-Amz-Target is a Secrets Manager operation.
func isSecretsManagerTarget(target string) bool {
	return strings.HasPrefix(target, "secretsmanager.")
}

func extractSecretsAction(target string) string {
	parts := strings.SplitN(target, ".", 2)
	if len(parts) == 2 {
		return parts[1]
	}
	return ""
}
