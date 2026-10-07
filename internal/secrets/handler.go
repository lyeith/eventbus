package secrets

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/lyeith/eventbus/internal/awsprotocol"
)

const maxJSONRequestBytes int64 = 1 << 20

// Handler owns the native Secrets Manager AWS JSON 1.1 adapter.
type Handler struct {
	secrets  *SecretsStore
	rotation *RotationService
}

func NewHandler(store *SecretsStore, rotation ...*RotationService) *Handler {
	handler := &Handler{secrets: store}
	if len(rotation) > 0 {
		handler.rotation = rotation[0]
	}
	return handler
}
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.ServeAction(w, r, awsprotocol.TargetAction(r.Header.Get("X-Amz-Target")))
}
func readInput(r *http.Request, target any) error {
	data, err := awsprotocol.ReadJSONBody(r, maxJSONRequestBytes)
	if err != nil || data == nil {
		return invalidParameter("Request body must contain one JSON object")
	}
	encoded, err := json.Marshal(data)
	if err != nil {
		return invalidParameter("Invalid request body")
	}
	if json.Unmarshal(encoded, target) != nil {
		return invalidParameter("Invalid request parameter types")
	}
	return nil
}
func writeError(w http.ResponseWriter, err error) {
	var failure *APIError
	if !errors.As(err, &failure) {
		failure = &APIError{"InternalServiceError", "An internal service error occurred"}
	}
	status := http.StatusBadRequest
	if failure.Code == "InternalServiceError" {
		status = http.StatusInternalServerError
	}
	awsprotocol.JSON11.Error(w, status, failure.Code, failure.Message)
}
func identityOutput(secret *Secret) map[string]any {
	output := map[string]any{"ARN": secret.ARN, "Name": secret.Name}
	if secret.VersionID != "" {
		output["VersionId"] = secret.VersionID
	}
	return output
}
func epoch(t time.Time) float64 { return float64(t.UnixNano()) / 1e9 }
func valueOutput(secret *Secret) map[string]any {
	output := identityOutput(secret)
	output["CreatedDate"] = epoch(secret.CreatedDate)
	output["VersionStages"] = secret.VersionStages
	if secret.StringValue {
		output["SecretString"] = secret.SecretString
	} else {
		output["SecretBinary"] = secret.SecretBinary
	}
	return output
}
func metadataOutput(secret *Secret) map[string]any {
	output := identityOutput(secret)
	output["CreatedDate"] = epoch(secret.CreatedDate)
	output["LastChangedDate"] = epoch(secret.LastChangedDate)
	output["VersionIdsToStages"] = secret.VersionIdsToStages
	if secret.Description != "" {
		output["Description"] = secret.Description
	}
	if secret.KmsKeyID != "" {
		output["KmsKeyId"] = secret.KmsKeyID
	}
	if secret.RotationEnabled != nil {
		output["RotationEnabled"] = *secret.RotationEnabled
	}
	if secret.RotationLambdaARN != "" {
		output["RotationLambdaARN"] = secret.RotationLambdaARN
	}
	if secret.RotationRules != nil {
		output["RotationRules"] = secret.RotationRules
	}
	if !secret.LastRotatedDate.IsZero() {
		output["LastRotatedDate"] = epoch(secret.LastRotatedDate)
	}
	tags := []map[string]string{}
	for key, value := range secret.Tags {
		tags = append(tags, map[string]string{"Key": key, "Value": value})
	}
	output["Tags"] = tags
	return output
}

type valueRequest struct {
	SecretID           string `json:"SecretId"`
	ClientRequestToken string
	SecretString       *string
	SecretBinary       []byte
	VersionStages      []string
	RotationToken      *string
}

func (h *Handler) ServeAction(w http.ResponseWriter, r *http.Request, action string) {
	var output any
	var err error
	switch action {
	case "CreateSecret":
		var input struct {
			Name, ClientRequestToken, Description string
			KmsKeyID                              string `json:"KmsKeyId"`
			SecretString                          *string
			SecretBinary                          []byte
			Tags                                  []struct{ Key, Value string }
			AddReplicaRegions                     []json.RawMessage
			ForceOverwriteReplicaSecret           *bool
			Type                                  string
		}
		if err = readInput(r, &input); err != nil {
			break
		}
		if len(input.AddReplicaRegions) > 0 || enabled(input.ForceOverwriteReplicaSecret) || input.Type != "" {
			err = invalidParameter("Secret replication and managed external secrets are not supported")
			break
		}
		tags := map[string]string{}
		for _, tag := range input.Tags {
			if _, found := tags[tag.Key]; found {
				err = invalidParameter("Duplicate secret tag")
				break
			}
			tags[tag.Key] = tag.Value
		}
		if err != nil {
			break
		}
		var secret *Secret
		secret, err = h.secrets.Create(CreateInput{Name: input.Name, ClientRequestToken: input.ClientRequestToken, Description: input.Description, KmsKeyID: input.KmsKeyID, Value: SecretValue{String: input.SecretString, Binary: input.SecretBinary}, Tags: tags})
		if err == nil {
			output = identityOutput(secret)
		}
	case "PutSecretValue":
		var input valueRequest
		if err = readInput(r, &input); err != nil {
			break
		}
		if input.RotationToken != nil {
			err = invalidParameter("Cross-account rotation tokens are not supported")
			break
		}
		if input.SecretID == "" {
			err = invalidParameter("SecretId is required")
			break
		}
		var secret *Secret
		secret, err = h.secrets.PutValue(PutInput{SecretID: input.SecretID, ClientRequestToken: input.ClientRequestToken, Value: SecretValue{String: input.SecretString, Binary: input.SecretBinary}, VersionStages: input.VersionStages})
		if err == nil {
			out := identityOutput(secret)
			out["VersionStages"] = secret.VersionStages
			output = out
		}
	case "GetSecretValue":
		var input struct {
			SecretID     string `json:"SecretId"`
			VersionID    string `json:"VersionId"`
			VersionStage string
		}
		if err = readInput(r, &input); err != nil {
			break
		}
		var secret *Secret
		secret, err = h.secrets.GetValue(input.SecretID, input.VersionID, input.VersionStage)
		if err == nil {
			output = valueOutput(secret)
		}
	case "DescribeSecret":
		var input struct {
			SecretID string `json:"SecretId"`
		}
		if err = readInput(r, &input); err != nil {
			break
		}
		if input.SecretID == "" {
			err = invalidParameter("SecretId is required")
			break
		}
		var secret *Secret
		secret, err = h.secrets.Describe(input.SecretID)
		if err == nil {
			output = metadataOutput(secret)
		}
	case "UpdateSecret":
		var input struct {
			SecretID           string `json:"SecretId"`
			ClientRequestToken string
			SecretString       *string
			SecretBinary       []byte
			Description        *string
			KmsKeyID           *string `json:"KmsKeyId"`
		}
		if err = readInput(r, &input); err != nil {
			break
		}
		if input.SecretID == "" {
			err = invalidParameter("SecretId is required")
			break
		}
		var secret *Secret
		secret, err = h.secrets.Update(UpdateInput{SecretID: input.SecretID, ClientRequestToken: input.ClientRequestToken, Value: SecretValue{String: input.SecretString, Binary: input.SecretBinary}, Description: input.Description, KmsKeyID: input.KmsKeyID})
		if err == nil {
			output = identityOutput(secret)
		}
	case "UpdateSecretVersionStage":
		var input struct {
			SecretID            string `json:"SecretId"`
			VersionStage        string
			MoveToVersionID     string `json:"MoveToVersionId"`
			RemoveFromVersionID string `json:"RemoveFromVersionId"`
		}
		if err = readInput(r, &input); err != nil {
			break
		}
		if input.SecretID == "" {
			err = invalidParameter("SecretId is required")
			break
		}
		var secret *Secret
		secret, err = h.secrets.UpdateVersionStage(StageInput{SecretID: input.SecretID, VersionStage: input.VersionStage, MoveToVersionID: input.MoveToVersionID, RemoveFromVersionID: input.RemoveFromVersionID})
		if err == nil {
			output = identityOutput(secret)
		}
	case "GetRandomPassword":
		var input RandomPasswordInput
		if err = readInput(r, &input); err != nil {
			break
		}
		var password string
		password, err = GenerateRandomPassword(input)
		if err == nil {
			output = map[string]string{"RandomPassword": password}
		}
	case "RotateSecret":
		var input struct {
			SecretID                              string `json:"SecretId"`
			ClientRequestToken, RotationLambdaARN string
			RotationRules                         *RotationRules
			RotateImmediately                     *bool
			ExternalSecretRotationMetadata        []json.RawMessage
			ExternalSecretRotationRoleARN         string `json:"ExternalSecretRotationRoleArn"`
		}
		if err = readInput(r, &input); err != nil {
			break
		}
		if len(input.ExternalSecretRotationMetadata) > 0 || input.ExternalSecretRotationRoleARN != "" {
			err = invalidParameter("Managed external rotation is not supported")
			break
		}
		if h.rotation == nil {
			err = invalidRequest("No rotation function invoker is configured")
			break
		}
		var secret *Secret
		secret, err = h.rotation.Rotate(r.Context(), RotateInput{SecretID: input.SecretID, ClientRequestToken: input.ClientRequestToken, RotationLambdaARN: input.RotationLambdaARN, RotationRules: input.RotationRules, RotateImmediately: input.RotateImmediately})
		if err == nil {
			output = identityOutput(secret)
		}
	case "CancelRotateSecret":
		var input struct {
			SecretID string `json:"SecretId"`
		}
		if err = readInput(r, &input); err != nil {
			break
		}
		if input.SecretID == "" {
			err = invalidParameter("SecretId is required")
			break
		}
		var secret *Secret
		if h.rotation != nil {
			secret, err = h.rotation.Cancel(input.SecretID)
		} else {
			secret, err = h.secrets.cancelRotation(input.SecretID)
		}
		if err == nil {
			output = identityOutput(secret)
		}
	case "DeleteSecret":
		var input struct {
			SecretID                   string `json:"SecretId"`
			ForceDeleteWithoutRecovery bool
			RecoveryWindowInDays       *int
		}
		if err = readInput(r, &input); err != nil {
			break
		}
		if input.SecretID == "" {
			err = invalidParameter("SecretId is required")
			break
		}
		var secret *Secret
		secret, err = h.secrets.Describe(input.SecretID)
		if err != nil {
			break
		}
		if input.ForceDeleteWithoutRecovery && input.RecoveryWindowInDays != nil {
			err = invalidParameter("Recovery window cannot accompany force deletion")
			break
		}
		if input.RecoveryWindowInDays != nil && (*input.RecoveryWindowInDays < 7 || *input.RecoveryWindowInDays > 30) {
			err = invalidParameter("RecoveryWindowInDays must be 7..30")
			break
		}
		err = h.secrets.DeleteSecret(input.SecretID, input.ForceDeleteWithoutRecovery)
		if err == nil {
			output = identityOutput(secret)
			output.(map[string]any)["DeletionDate"] = epoch(time.Now())
		}
	default:
		err = &APIError{"InvalidAction", fmt.Sprintf("Unknown Secrets Manager action: %s", action)}
	}
	if err != nil {
		writeError(w, err)
		return
	}
	awsprotocol.JSON11.Response(w, http.StatusOK, output)
}
