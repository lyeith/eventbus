// Development HTTP controls have a separate namespace and explicit opt-in.
// Native Invoke never accepts a fault flag or changes its request/response.
package lambda

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/lyeith/eventbus/internal/awsprotocol"
)

const devAsyncPrefix = "/__eventbus/dev/lambda/async/"

// NewDevAsyncHandler is a named harness adapter for an existing native owner.
// Hosts attach it separately from the AWS Lambda HTTP surface.
func NewDevAsyncHandler(service *Service) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writeError := func(status int, code string) {
			writer.WriteHeader(status)
			_ = json.NewEncoder(writer).Encode(map[string]string{"error": code})
		}
		if service == nil || !service.asyncCompletionLossEnabled {
			writeError(http.StatusNotFound, "completion_loss_disabled")
			return
		}
		if request.URL.Path == devAsyncPrefix+"events/completion-loss" {
			if request.Method != http.MethodPost {
				writer.Header().Set("Allow", http.MethodPost)
				writeError(http.StatusMethodNotAllowed, "completion_loss_admission_requires_post")
				return
			}
			var target struct {
				FunctionName string          `json:"function_name"`
				Qualifier    string          `json:"qualifier,omitempty"`
				Payload      json.RawMessage `json:"payload"`
			}
			if err := decodeDevAsyncControlBody(request, maxEventPayload+4096, &target); err != nil {
				writeError(http.StatusBadRequest, "invalid_completion_loss_request")
				return
			}
			admission, err := service.DevAdmitAsyncCompletionLoss(request.Context(), InvokeInput{
				FunctionName: target.FunctionName, Qualifier: target.Qualifier, Payload: target.Payload,
			})
			if err != nil {
				status, code := devAsyncHTTPError(err)
				if status == http.StatusTooManyRequests {
					writer.Header().Set("Retry-After", "1")
				}
				writeError(status, code)
				return
			}
			writer.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(writer).Encode(struct {
				RequestID string `json:"request_id"`
				Armed     bool   `json:"armed"`
			}{RequestID: admission.RequestID, Armed: true})
			return
		}
		if request.URL.Path == devAsyncPrefix+"events" {
			if request.Method != http.MethodGet {
				writer.Header().Set("Allow", http.MethodGet)
				writeError(http.StatusMethodNotAllowed, "async_snapshot_requires_get")
				return
			}
			_ = json.NewEncoder(writer).Encode(struct {
				Events []AsyncRecord `json:"events"`
			}{Events: service.AsyncSnapshot()})
			return
		}
		prefix := devAsyncPrefix + "events/"
		requestID, matched := strings.CutSuffix(strings.TrimPrefix(request.URL.Path, prefix), "/completion-loss")
		if !strings.HasPrefix(request.URL.Path, prefix) || !matched || requestID == "" || strings.Contains(requestID, "/") {
			writeError(http.StatusNotFound, "async_control_route_not_found")
			return
		}
		if request.Method != http.MethodPut && request.Method != http.MethodDelete {
			writer.Header().Set("Allow", http.MethodPut+", "+http.MethodDelete)
			writeError(http.StatusMethodNotAllowed, "completion_loss_requires_put_or_delete")
			return
		}
		var target struct {
			FunctionName string `json:"function_name"`
			Attempt      int    `json:"attempt"`
		}
		if err := decodeDevAsyncControlBody(request, 4096, &target); err != nil {
			writeError(http.StatusBadRequest, "invalid_completion_loss_request")
			return
		}
		var err error
		var result DevAsyncCompletionLoss
		if request.Method == http.MethodPut {
			result, err = service.DevArmAsyncCompletionLoss(request.Context(), target.FunctionName, requestID, target.Attempt)
		} else {
			result, err = service.DevClearAsyncCompletionLoss(request.Context(), target.FunctionName, requestID, target.Attempt)
		}
		if err != nil {
			status, code := devAsyncHTTPError(err)
			writeError(status, code)
			return
		}
		_ = json.NewEncoder(writer).Encode(result)
	})
}

func decodeDevAsyncControlBody(request *http.Request, limit int64, target any) error {
	body, err := awsprotocol.ReadBoundedBody(request.Body, request.ContentLength, limit)
	if err != nil {
		return err
	}
	if int64(len(body)) > limit {
		return errors.New("development control body exceeds its limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return errors.New("development control body must contain one JSON object")
	}
	return nil
}
