package firehose

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/lyeith/eventbus/internal/awsprotocol"
)

// maxJSONRequestBytes retains this adapter's existing local transport budget.
const maxJSONRequestBytes int64 = 1 << 20

// Handler owns the Firehose AWS JSON adapter; the manager owns delivery state.
type Handler struct {
	firehose *FirehoseManager
}

func NewHandler(manager *FirehoseManager) *Handler {
	return &Handler{firehose: manager}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.ServeAction(w, r, awsprotocol.TargetAction(r.Header.Get("X-Amz-Target")))
}

// ServeAction handles Firehose requests using the AWS JSON 1.1 protocol.
// X-Amz-Target: Firehose_20150804.<Action>
func (s *Handler) ServeAction(w http.ResponseWriter, r *http.Request, action string) {
	log.Debug().Str("action", action).Msg("Firehose JSON API request")

	switch action {
	case "CreateDeliveryStream":
		s.handleCreateDeliveryStream(w, r)
	case "DeleteDeliveryStream":
		s.handleDeleteDeliveryStream(w, r)
	case "DescribeDeliveryStream":
		s.handleDescribeDeliveryStream(w, r)
	case "PutRecord":
		s.handleFirehosePutRecord(w, r)
	case "PutRecordBatch":
		s.handleFirehosePutRecordBatch(w, r)
	default:
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidAction", fmt.Sprintf("Unknown Firehose action: %s", action))
	}
}

func (s *Handler) handleCreateDeliveryStream(w http.ResponseWriter, r *http.Request) {
	data, err := awsprotocol.ReadJSONBody(r, maxJSONRequestBytes)
	if err != nil {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidInput", "Invalid JSON body")
		return
	}

	name, _ := data["DeliveryStreamName"].(string)
	if name == "" {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidParameter", "DeliveryStreamName is required")
		return
	}

	// Parse S3 destination config
	bucketName := ""
	prefix := ""
	errorPrefix := ""
	bufferSizeMB := 1
	bufferIntervalSec := 60

	if s3Config, ok := data["ExtendedS3DestinationConfiguration"].(map[string]interface{}); ok {
		if bucketARN, ok := s3Config["BucketARN"].(string); ok {
			// Extract bucket name from ARN: arn:aws:s3:::bucket-name
			parts := strings.Split(bucketARN, ":::")
			if len(parts) == 2 {
				bucketName = parts[1]
			}
		}
		if p, ok := s3Config["Prefix"].(string); ok {
			prefix = p
		}
		if ep, ok := s3Config["ErrorOutputPrefix"].(string); ok {
			errorPrefix = ep
		}
		if hints, ok := s3Config["BufferingHints"].(map[string]interface{}); ok {
			if size, ok := hints["SizeInMBs"].(float64); ok {
				bufferSizeMB = int(size)
			}
			if interval, ok := hints["IntervalInSeconds"].(float64); ok {
				bufferIntervalSec = int(interval)
			}
		}
	}

	// Also check S3DestinationConfiguration (simpler form)
	if bucketName == "" {
		if s3Config, ok := data["S3DestinationConfiguration"].(map[string]interface{}); ok {
			if bucketARN, ok := s3Config["BucketARN"].(string); ok {
				parts := strings.Split(bucketARN, ":::")
				if len(parts) == 2 {
					bucketName = parts[1]
				}
			}
			if p, ok := s3Config["Prefix"].(string); ok {
				prefix = p
			}
		}
	}

	if bucketName == "" {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidParameter", "S3 destination bucket is required")
		return
	}

	ds, err := s.firehose.CreateStream(name, bucketName, prefix, errorPrefix, bufferSizeMB, bufferIntervalSec)
	if err != nil {
		awsprotocol.JSONError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	log.Info().Str("stream", name).Str("bucket", bucketName).Str("prefix", prefix).Msg("Created delivery stream")

	awsprotocol.JSONResponse(w, http.StatusOK, map[string]string{
		"DeliveryStreamARN": ds.ARN,
	})
}

func (s *Handler) handleDeleteDeliveryStream(w http.ResponseWriter, r *http.Request) {
	data, err := awsprotocol.ReadJSONBody(r, maxJSONRequestBytes)
	if err != nil {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidInput", "Invalid JSON body")
		return
	}

	name, _ := data["DeliveryStreamName"].(string)
	if name == "" {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidParameter", "DeliveryStreamName is required")
		return
	}

	ds := s.firehose.GetStream(name)
	if ds == nil {
		awsprotocol.JSONError(w, http.StatusBadRequest, "ResourceNotFoundException", "Stream not found: "+name)
		return
	}

	if err := s.firehose.DeleteStream(r.Context(), name); err != nil {
		awsprotocol.JSONError(w, http.StatusServiceUnavailable, "ServiceUnavailableException", err.Error())
		return
	}
	log.Info().Str("stream", name).Msg("Deleted delivery stream")

	awsprotocol.JSONResponse(w, http.StatusOK, map[string]interface{}{})
}

func (s *Handler) handleDescribeDeliveryStream(w http.ResponseWriter, r *http.Request) {
	data, err := awsprotocol.ReadJSONBody(r, maxJSONRequestBytes)
	if err != nil {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidInput", "Invalid JSON body")
		return
	}

	name, _ := data["DeliveryStreamName"].(string)
	if name == "" {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidParameter", "DeliveryStreamName is required")
		return
	}

	ds := s.firehose.GetStream(name)
	if ds == nil {
		awsprotocol.JSONError(w, http.StatusBadRequest, "ResourceNotFoundException", "Stream not found: "+name)
		return
	}

	awsprotocol.JSONResponse(w, http.StatusOK, map[string]interface{}{
		"DeliveryStreamDescription": map[string]interface{}{
			"DeliveryStreamARN":    ds.ARN,
			"DeliveryStreamName":   ds.Name,
			"DeliveryStreamStatus": ds.Status,
			"DeliveryStreamType":   "DirectPut",
		},
	})
}

func (s *Handler) handleFirehosePutRecord(w http.ResponseWriter, r *http.Request) {
	data, err := awsprotocol.ReadJSONBody(r, maxJSONRequestBytes)
	if err != nil {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidInput", "Invalid JSON body")
		return
	}

	name, _ := data["DeliveryStreamName"].(string)
	if name == "" {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidParameter", "DeliveryStreamName is required")
		return
	}

	ds := s.firehose.GetStream(name)
	if ds == nil {
		awsprotocol.JSONError(w, http.StatusBadRequest, "ResourceNotFoundException", "Stream not found: "+name)
		return
	}

	record, ok := data["Record"].(map[string]interface{})
	if !ok {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidParameter", "Record is required")
		return
	}

	recordData, err := extractRecordData(record)
	if err != nil {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidParameter", err.Error())
		return
	}

	recordID, err := s.firehose.PutRecord(ds, recordData)
	if err != nil {
		awsprotocol.JSONError(w, http.StatusServiceUnavailable, "ServiceUnavailableException", err.Error())
		return
	}

	awsprotocol.JSONResponse(w, http.StatusOK, map[string]interface{}{
		"RecordId":  recordID,
		"Encrypted": false,
	})
}

func (s *Handler) handleFirehosePutRecordBatch(w http.ResponseWriter, r *http.Request) {
	data, err := awsprotocol.ReadJSONBody(r, maxJSONRequestBytes)
	if err != nil {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidInput", "Invalid JSON body")
		return
	}

	name, _ := data["DeliveryStreamName"].(string)
	if name == "" {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidParameter", "DeliveryStreamName is required")
		return
	}

	ds := s.firehose.GetStream(name)
	if ds == nil {
		awsprotocol.JSONError(w, http.StatusBadRequest, "ResourceNotFoundException", "Stream not found: "+name)
		return
	}

	rawRecords, ok := data["Records"].([]interface{})
	if !ok {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidParameter", "Records is required")
		return
	}

	records := make([][]byte, 0, len(rawRecords))
	for _, raw := range rawRecords {
		record, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		recordData, err := extractRecordData(record)
		if err != nil {
			continue
		}
		records = append(records, recordData)
	}

	ids, err := s.firehose.PutRecordBatch(ds, records)
	if err != nil {
		awsprotocol.JSONError(w, http.StatusServiceUnavailable, "ServiceUnavailableException", err.Error())
		return
	}

	responses := make([]map[string]interface{}, len(ids))
	for i, id := range ids {
		responses[i] = map[string]interface{}{
			"RecordId": id,
		}
	}

	awsprotocol.JSONResponse(w, http.StatusOK, map[string]interface{}{
		"FailedPutCount":   0,
		"Encrypted":        false,
		"RequestResponses": responses,
	})
}

// extractRecordData gets the raw bytes from a Firehose record.
// The SDK sends Data as base64-encoded bytes.
func extractRecordData(record map[string]interface{}) ([]byte, error) {
	dataStr, ok := record["Data"].(string)
	if !ok {
		return nil, fmt.Errorf("Record.Data is required")
	}

	// AWS SDK sends base64-encoded data
	decoded, err := base64.StdEncoding.DecodeString(dataStr)
	if err != nil {
		// Might be raw string (from tests)
		return []byte(dataStr), nil
	}
	return decoded, nil
}
