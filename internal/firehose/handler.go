package firehose

import (
	"encoding/base64"
	"fmt"
	"net/http"

	"github.com/lyeith/eventbus/internal/awsprotocol"
)

// Four MiB of decoded records require base64 overhead plus the JSON envelope.
const maxJSONRequestBytes int64 = 6 << 20

type Handler struct{ firehose *FirehoseManager }

func NewHandler(manager *FirehoseManager) *Handler { return &Handler{firehose: manager} }
func (handler *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	handler.ServeAction(w, r, awsprotocol.TargetAction(r.Header.Get("X-Amz-Target")))
}
func (handler *Handler) ServeAction(w http.ResponseWriter, r *http.Request, action string) {
	switch action {
	case "CreateDeliveryStream":
		handler.handleCreateDeliveryStream(w, r)
	case "DescribeDeliveryStream":
		handler.handleDescribeDeliveryStream(w, r)
	case "DeleteDeliveryStream":
		handler.handleDeleteDeliveryStream(w, r)
	case "PutRecord":
		handler.handleFirehosePutRecord(w, r)
	case "PutRecordBatch":
		handler.handleFirehosePutRecordBatch(w, r)
	default:
		awsprotocol.JSON11.Error(w, http.StatusBadRequest, "UnknownOperationException", "Unknown Firehose action: "+action)
	}
}

func readFirehoseJSON(w http.ResponseWriter, r *http.Request) (map[string]interface{}, bool) {
	data, err := awsprotocol.ReadJSONBody(r, maxJSONRequestBytes)
	if err != nil || data == nil {
		awsprotocol.JSON11.Error(w, http.StatusBadRequest, "InvalidArgumentException", "Invalid JSON request body")
		return nil, false
	}
	return data, true
}
func firehoseName(w http.ResponseWriter, data map[string]interface{}) (string, bool) {
	name, ok := data["DeliveryStreamName"].(string)
	if !ok || !streamNameRE.MatchString(name) {
		awsprotocol.JSON11.Error(w, http.StatusBadRequest, "InvalidArgumentException", "DeliveryStreamName must match [a-zA-Z0-9_.-]{1,64}")
		return "", false
	}
	return name, true
}

func (handler *Handler) handleCreateDeliveryStream(w http.ResponseWriter, r *http.Request) {
	data, ok := readFirehoseJSON(w, r)
	if !ok {
		return
	}
	config, err := configFromRequest(data)
	if err != nil {
		awsprotocol.JSON11.Error(w, http.StatusBadRequest, "InvalidArgumentException", err.Error())
		return
	}
	stream, err := handler.firehose.CreateConfiguredStream(r.Context(), config)
	if err != nil {
		awsprotocol.JSON11.Error(w, http.StatusBadRequest, "InvalidArgumentException", err.Error())
		return
	}
	awsprotocol.JSON11.Response(w, http.StatusOK, map[string]string{"DeliveryStreamARN": stream.ARN})
}

func (handler *Handler) handleDeleteDeliveryStream(w http.ResponseWriter, r *http.Request) {
	data, ok := readFirehoseJSON(w, r)
	if !ok {
		return
	}
	name, ok := firehoseName(w, data)
	if !ok {
		return
	}
	if handler.firehose.GetStream(name) == nil {
		awsprotocol.JSON11.Error(w, http.StatusBadRequest, "ResourceNotFoundException", "Stream not found: "+name)
		return
	}
	if err := handler.firehose.DeleteStream(r.Context(), name); err != nil {
		awsprotocol.JSON11.Error(w, http.StatusInternalServerError, "ServiceUnavailableException", err.Error())
		return
	}
	awsprotocol.JSON11.Response(w, http.StatusOK, map[string]interface{}{})
}

func (handler *Handler) handleDescribeDeliveryStream(w http.ResponseWriter, r *http.Request) {
	data, ok := readFirehoseJSON(w, r)
	if !ok {
		return
	}
	name, ok := firehoseName(w, data)
	if !ok {
		return
	}
	snapshot, exists := handler.firehose.Snapshot(name)
	if !exists {
		awsprotocol.JSON11.Error(w, http.StatusBadRequest, "ResourceNotFoundException", "Stream not found: "+name)
		return
	}
	config := snapshot.Config
	destination := map[string]interface{}{"BucketARN": config.BucketARN, "RoleARN": config.RoleARN, "Prefix": config.Prefix, "ErrorOutputPrefix": config.ErrorOutputPrefix, "BufferingHints": config.BufferingHints, "CompressionFormat": config.CompressionFormat, "CustomTimeZone": config.CustomTimeZone, "FileExtension": config.FileExtension, "ProcessingConfiguration": config.ProcessingConfiguration, "DynamicPartitioningConfiguration": config.DynamicPartitioningConfiguration, "EncryptionConfiguration": map[string]string{"NoEncryptionConfig": "NoEncryption"}, "S3BackupMode": "Disabled"}
	awsprotocol.JSON11.Response(w, http.StatusOK, map[string]interface{}{"DeliveryStreamDescription": map[string]interface{}{"DeliveryStreamARN": snapshot.ARN, "DeliveryStreamName": snapshot.Name, "DeliveryStreamStatus": snapshot.Status, "DeliveryStreamType": "DirectPut", "VersionId": "1", "CreateTimestamp": float64(snapshot.CreatedAt.UnixMilli()) / 1000, "HasMoreDestinations": false, "Destinations": []interface{}{map[string]interface{}{"DestinationId": "destinationId-000000000001", "ExtendedS3DestinationDescription": destination}}}})
}

func (handler *Handler) handleFirehosePutRecord(w http.ResponseWriter, r *http.Request) {
	data, ok := readFirehoseJSON(w, r)
	if !ok {
		return
	}
	name, ok := firehoseName(w, data)
	if !ok {
		return
	}
	record, ok := data["Record"].(map[string]interface{})
	if !ok {
		awsprotocol.JSON11.Error(w, http.StatusBadRequest, "InvalidArgumentException", "Record is required")
		return
	}
	decoded, err := extractRecordData(record)
	if err != nil {
		awsprotocol.JSON11.Error(w, http.StatusBadRequest, "InvalidArgumentException", err.Error())
		return
	}
	if err := validateRecords([][]byte{decoded}); err != nil {
		awsprotocol.JSON11.Error(w, http.StatusBadRequest, "InvalidArgumentException", err.Error())
		return
	}
	stream := handler.firehose.GetStream(name)
	if stream == nil {
		awsprotocol.JSON11.Error(w, http.StatusBadRequest, "ResourceNotFoundException", "Stream not found: "+name)
		return
	}
	results, err := handler.firehose.AcceptRecordBatch(r.Context(), stream, [][]byte{decoded})
	if err != nil {
		awsprotocol.JSON11.Error(w, http.StatusInternalServerError, "ServiceUnavailableException", err.Error())
		return
	}
	if result := results[0]; result.ErrorCode != "" {
		awsprotocol.JSON11.Error(w, http.StatusInternalServerError, result.ErrorCode, result.ErrorMessage)
		return
	}
	awsprotocol.JSON11.Response(w, http.StatusOK, map[string]interface{}{"RecordId": results[0].RecordID, "Encrypted": false})
}

func (handler *Handler) handleFirehosePutRecordBatch(w http.ResponseWriter, r *http.Request) {
	data, ok := readFirehoseJSON(w, r)
	if !ok {
		return
	}
	name, ok := firehoseName(w, data)
	if !ok {
		return
	}
	rawRecords, ok := data["Records"].([]interface{})
	if !ok {
		awsprotocol.JSON11.Error(w, http.StatusBadRequest, "InvalidArgumentException", "Records is required")
		return
	}
	// Decode and validate the entire modeled request before any state mutation.
	records := make([][]byte, len(rawRecords))
	for index, raw := range rawRecords {
		record, ok := raw.(map[string]interface{})
		if !ok {
			awsprotocol.JSON11.Error(w, http.StatusBadRequest, "InvalidArgumentException", fmt.Sprintf("Records[%d] must be an object", index))
			return
		}
		decoded, err := extractRecordData(record)
		if err != nil {
			awsprotocol.JSON11.Error(w, http.StatusBadRequest, "InvalidArgumentException", fmt.Sprintf("Records[%d]: %v", index, err))
			return
		}
		records[index] = decoded
	}
	if err := validateRecords(records); err != nil {
		awsprotocol.JSON11.Error(w, http.StatusBadRequest, "InvalidArgumentException", err.Error())
		return
	}
	stream := handler.firehose.GetStream(name)
	if stream == nil {
		awsprotocol.JSON11.Error(w, http.StatusBadRequest, "ResourceNotFoundException", "Stream not found: "+name)
		return
	}
	results, err := handler.firehose.AcceptRecordBatch(r.Context(), stream, records)
	if err != nil {
		awsprotocol.JSON11.Error(w, http.StatusInternalServerError, "ServiceUnavailableException", err.Error())
		return
	}
	responses := make([]map[string]interface{}, len(results))
	failures := 0
	for index, result := range results {
		if result.ErrorCode != "" {
			failures++
			responses[index] = map[string]interface{}{"ErrorCode": result.ErrorCode, "ErrorMessage": result.ErrorMessage}
		} else {
			responses[index] = map[string]interface{}{"RecordId": result.RecordID}
		}
	}
	awsprotocol.JSON11.Response(w, http.StatusOK, map[string]interface{}{"FailedPutCount": failures, "Encrypted": false, "RequestResponses": responses})
}

func extractRecordData(record map[string]interface{}) ([]byte, error) {
	data, ok := record["Data"].(string)
	if !ok {
		return nil, fmt.Errorf("Record.Data is required")
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(data)
	if err != nil {
		return nil, fmt.Errorf("Record.Data must be valid base64")
	}
	return decoded, nil
}
