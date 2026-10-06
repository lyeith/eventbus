package messaging

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/lyeith/eventbus/internal/awsprotocol"
)

// ServeAction uses the same operation engine as the legacy Query protocol.
func (s *Handler) ServeAction(w http.ResponseWriter, r *http.Request, action string) {
	// JSON/base64 encoding and batch metadata can exceed the 1 MiB message limit.
	// The actual decoded message and batch limits are enforced by the engine.
	body, err := io.ReadAll(io.LimitReader(r.Body, 12*1048576+1))
	if err != nil || len(body) > 12*1048576 {
		s.writeSQSError(w, newSQSError("InvalidParameterValue", "Request body is too large"), true)
		return
	}
	if trimmed := bytes.TrimSpace(body); len(trimmed) == 0 || trimmed[0] != '{' {
		s.writeSQSError(w, newSQSError("SerializationException", "Invalid JSON request"), true)
		return
	}
	var input sqsRequest
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(&input); err != nil {
		s.writeSQSError(w, newSQSError("SerializationException", "Invalid JSON request"), true)
		return
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF {
		s.writeSQSError(w, newSQSError("SerializationException", "Invalid JSON request"), true)
		return
	}
	result, apiErr := s.executeSQS(r.Context(), action, input)
	if apiErr != nil {
		s.writeSQSError(w, apiErr, true)
		return
	}
	w.Header().Set("x-amzn-RequestId", awsprotocol.RequestID())
	awsprotocol.JSONResponse(w, http.StatusOK, result)
}
func (s *Handler) writeSQSError(w http.ResponseWriter, err *sqsError, jsonProtocol bool) {
	requestID := awsprotocol.RequestID()
	w.Header().Set("x-amzn-RequestId", requestID)
	if jsonProtocol {
		w.Header().Set("x-amzn-ErrorType", err.Code)
		awsprotocol.JSONError(w, http.StatusBadRequest, err.Code, err.Message)
		return
	}
	code := err.Code
	if code == "QueueDoesNotExist" {
		code = "AWS.SimpleQueueService.NonExistentQueue"
	}
	body := `<ErrorResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/"><Error><Type>Sender</Type><Code>` + awsprotocol.XMLEscape(code) + `</Code><Message>` + awsprotocol.XMLEscape(err.Message) + `</Message></Error><RequestId>` + requestID + `</RequestId></ErrorResponse>`
	awsprotocol.XMLResponse(w, http.StatusBadRequest, body)
}
func extractSQSJSONAction(target string) string {
	parts := strings.SplitN(target, ".", 2)
	if len(parts) == 2 && parts[0] == "AmazonSQS" {
		return parts[1]
	}
	return ""
}
