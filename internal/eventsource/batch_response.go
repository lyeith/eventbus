package eventsource

import (
	"encoding/json"
	"errors"
)

const reportBatchItemFailures = "ReportBatchItemFailures"

var errInvalidBatchResponse = errors.New("invalid SQS batch response")

type failedBatchItems map[string]struct{}

func (items failedBatchItems) includes(record Record) bool {
	_, failed := items[record.MessageID]
	return failed
}

// Validate the complete response before any acknowledgment. Failed IDs must
// refer to records in this invocation; receipt handles are never accepted here.
func batchFailures(responseTypes []string, records []Record, response []byte) (failedBatchItems, error) {
	if len(responseTypes) == 0 || len(response) == 0 {
		return nil, nil
	}
	var result map[string]json.RawMessage
	if err := json.Unmarshal(response, &result); err != nil {
		return nil, errInvalidBatchResponse
	}
	failures, reported := result["batchItemFailures"]
	if !reported {
		return nil, nil // Includes a null or empty response object.
	}
	var failuresList []map[string]json.RawMessage
	if err := json.Unmarshal(failures, &failuresList); err != nil {
		return nil, errInvalidBatchResponse
	}
	if len(failuresList) == 0 {
		return nil, nil
	}
	received := make(map[string]struct{}, len(records))
	for _, record := range records {
		received[record.MessageID] = struct{}{}
	}
	failed := make(failedBatchItems)
	for _, entry := range failuresList {
		var id string
		if err := json.Unmarshal(entry["itemIdentifier"], &id); err != nil || id == "" {
			return nil, errInvalidBatchResponse
		}
		if _, ok := received[id]; !ok {
			return nil, errInvalidBatchResponse
		}
		failed[id] = struct{}{}
	}
	return failed, nil
}

func batchProcessingResult(failures failedBatchItems, acknowledged bool) string {
	switch {
	case !acknowledged:
		return "Source acknowledgment failed"
	case len(failures) != 0:
		return "Partial batch failure"
	default:
		return "OK"
	}
}
