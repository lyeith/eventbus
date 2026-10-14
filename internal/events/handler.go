package events

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/lyeith/eventbus/internal/awsprotocol"
	"io"
	"net/http"
)

type Handler struct{ service *Service }

func NewHandler(service *Service) *Handler { return &Handler{service} }

func decodeRequest(r *http.Request, input any) error {
	data, err := awsprotocol.ReadBoundedBody(r.Body, r.ContentLength, 1<<20)
	if err != nil || len(data) > 1<<20 {
		return validation("Invalid or oversized EventBridge request")
	}
	// Native operations require exactly one object. Strict field decoding makes
	// unsupported AWS combinations explicit instead of silently dropping them.
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return validation("Request must contain one JSON object")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(input) != nil {
		return validation("Invalid or unsupported EventBridge request fields")
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return validation("Request must contain one JSON object")
	}
	return nil
}
func (h *Handler) ServeAction(w http.ResponseWriter, r *http.Request, action string) {
	if h.service == nil {
		awsprotocol.JSON11.Error(w, 503, "InternalException", "EventBridge is not configured")
		return
	}
	var response any = map[string]any{}
	var err error
	switch action {
	case "PutRule":
		var input PutRuleInput
		if err = decodeRequest(r, &input); err == nil {
			var arn string
			arn, err = h.service.PutRule(r.Context(), input)
			response = map[string]string{"RuleArn": arn}
		}
	case "DescribeRule", "EnableRule", "DisableRule", "DeleteRule":
		var input RuleInput
		if err = decodeRequest(r, &input); err == nil {
			switch action {
			case "DescribeRule":
				response, err = h.service.DescribeRule(input)
			case "EnableRule":
				err = h.service.EnableRule(r.Context(), input)
			case "DisableRule":
				err = h.service.DisableRule(r.Context(), input)
			case "DeleteRule":
				err = h.service.DeleteRule(r.Context(), input)
			}
		}
	case "ListRules":
		var input ListRulesInput
		if err = decodeRequest(r, &input); err == nil {
			response, err = h.service.ListRules(input)
		}
	case "PutTargets":
		var input PutTargetsInput
		if err = decodeRequest(r, &input); err == nil {
			response, err = h.service.PutTargets(r.Context(), input)
		}
	case "RemoveTargets":
		var input RemoveTargetsInput
		if err = decodeRequest(r, &input); err == nil {
			response, err = h.service.RemoveTargets(r.Context(), input)
		}
	case "ListTargetsByRule":
		var input ListTargetsInput
		if err = decodeRequest(r, &input); err == nil {
			response, err = h.service.ListTargetsByRule(input)
		}
	default:
		err = &APIError{"UnknownOperationException", 400, "EventBridge operation is not supported"}
	}
	if err != nil {
		native := &APIError{"InternalException", 500, "EventBridge operation failed"}
		var failure *APIError
		if errors.As(err, &failure) {
			native = failure
		}
		awsprotocol.JSON11.Error(w, native.Status, native.Code, native.Message)
		return
	}
	awsprotocol.JSON11.Response(w, http.StatusOK, response)
}
