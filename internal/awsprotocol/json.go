package awsprotocol

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
)

// JSONProtocol selects the media type owned by the native service adapter.
type JSONProtocol string

const (
	JSON10 JSONProtocol = "application/x-amz-json-1.0"
	JSON11 JSONProtocol = "application/x-amz-json-1.1"
)

// ReadJSONBody reads at most the caller-owned budget plus one byte, so a
// complete JSON prefix cannot hide a truncated oversized request. Native
// operation limits and error mapping remain with the service adapter.
func ReadJSONBody(r *http.Request, maxBytes int64) (map[string]interface{}, error) {
	if maxBytes <= 0 || maxBytes == math.MaxInt64 {
		return nil, fmt.Errorf("JSON body limit must be positive and bounded")
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > maxBytes {
		return nil, fmt.Errorf("JSON body exceeds %d bytes", maxBytes)
	}
	var data map[string]interface{}
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, err
	}
	if data == nil {
		return nil, fmt.Errorf("JSON body must be an object")
	}
	return data, nil
}

func (protocol JSONProtocol) Response(w http.ResponseWriter, statusCode int, data interface{}) {
	body, err := json.Marshal(data)
	if err != nil {
		protocol.Error(w, http.StatusInternalServerError, "InternalFailure", "Failed to encode response")
		return
	}
	w.Header().Set("Content-Type", string(protocol))
	EnsureRequestID(w)
	w.WriteHeader(statusCode)
	_, _ = w.Write(append(body, '\n'))
}

func (protocol JSONProtocol) Error(w http.ResponseWriter, statusCode int, code, message string) {
	w.Header().Set("X-Amzn-ErrorType", code)
	protocol.Response(w, statusCode, map[string]string{"__type": code, "Message": message})
}

// Legacy wrappers retain the AWS JSON 1.0 choice made by existing adapters.
func JSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	JSON10.Response(w, statusCode, data)
}

func JSONError(w http.ResponseWriter, statusCode int, code, message string) {
	JSON10.Error(w, statusCode, code, message)
}
