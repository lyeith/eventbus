package awsprotocol

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// ReadJSONBody enforces the caller-owned transport budget before decoding.
// Native operation limits and error mapping remain with the service adapter.
func ReadJSONBody(r *http.Request, maxBytes int64) (map[string]interface{}, error) {
	if maxBytes <= 0 {
		return nil, fmt.Errorf("JSON body limit must be positive")
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
	return data, nil
}

func JSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(data)
}

func JSONError(w http.ResponseWriter, statusCode int, code, message string) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"__type":  code,
		"Message": message,
	})
}
