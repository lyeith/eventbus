package ses

import (
	"net/http"
	"strings"
	"time"

	"github.com/lyeith/eventbus/internal/awsprotocol"
	"github.com/rs/zerolog/log"
)

// QueryBodyLimit allows the dispatcher to bound transport data before parsing
// AWS Query forms. Decoded MIME quotas are validated separately by SES.
const QueryBodyLimit = sesWireLimit

// Handler owns both SES protocol adapters, including capture before response.
type Handler struct {
	ses *SESManager
}

func NewHandler(manager *SESManager) *Handler {
	return &Handler{ses: manager}
}

// ServeHTTP handles SES v2 REST requests. Unknown routes use the SES error
// envelope and capture stream just like implemented sending operations.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	action, matched := sesV2Action(r)
	limit := sesWireLimit
	if action == "SendBulkEmail" {
		limit = sesBulkWireLimit
	}
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	var apiErr *sesAPIError
	if !matched {
		apiErr = &sesAPIError{
			Code: "NotFoundException", Message: "SES operation is not implemented",
			Status: http.StatusNotFound,
		}
	}
	h.handleSES(w, r, "v2", action, apiErr)
}

// ServeQuery handles SES v1 after the dispatcher parses the bounded form.
// Parsing errors use the SES v1 envelope and are captured before responding.
func (h *Handler) ServeQuery(w http.ResponseWriter, r *http.Request, action string, parseErr error) {
	var apiErr *sesAPIError
	if parseErr != nil {
		apiErr = sesInvalid("v1", "Malformed Query request: "+parseErr.Error())
	}
	h.handleSES(w, r, "v1", action, apiErr)
}

// MatchQuery recognizes SES v1 operations and service-identifying transport
// metadata, including unsupported operations that need an SES error envelope.
func (h *Handler) MatchQuery(r *http.Request, action string) bool {
	return matchQuery(r, action)
}

const sesWireLimit int64 = 64 << 20
const sesBulkWireLimit int64 = 256 << 20

func sesV1SendingAction(action string) bool {
	switch action {
	case "SendEmail", "SendRawEmail", "SendTemplatedEmail", "SendBulkTemplatedEmail", "SendCustomVerificationEmail", "SendBounce":
		return true
	}
	return false
}
func matchQuery(r *http.Request, action string) bool {
	return sesV1SendingAction(action) || r.FormValue("Version") == "2010-12-01" || strings.Contains(r.Header.Get("Authorization"), "/ses/aws4_request")
}
func (s *Handler) handleSES(w http.ResponseWriter, r *http.Request, api, action string, parseError *sesAPIError) {
	id := awsprotocol.RequestID()
	input := map[string]any{}
	apiErr := parseError
	if apiErr == nil {
		if api == "v1" {
			input, apiErr = sesV1Decode(r, action)
		} else {
			input, apiErr = sesV2Decode(r)
		}
	}
	result := sesSendResult{}
	if apiErr == nil {
		if s.ses.fixtures.SendingEnabled != nil && !*s.ses.fixtures.SendingEnabled {
			code := "SendingPausedException"
			if api == "v1" {
				code = "AccountSendingPausedException"
			}
			apiErr = &sesAPIError{Code: code, Message: "Email sending is paused", Status: http.StatusBadRequest}
		} else if api == "v1" {
			result, apiErr = s.ses.sendV1(action, input)
		} else {
			result, apiErr = s.ses.sendV2(action, input)
		}
	}
	status := http.StatusOK
	outcome := map[string]any{"http_status": status, "response": result.Output}
	if apiErr != nil {
		status = apiErr.Status
		outcome["http_status"] = status
		outcome["error"] = map[string]any{"code": apiErr.Code, "message": apiErr.Message}
	}
	service, version := "ses", "2010-12-01"
	if api == "v2" {
		service, version = "sesv2", "2019-09-27"
	}
	record := map[string]any{"schema_version": 1, "timestamp": time.Now().UTC().Format(time.RFC3339Nano), "request_id": id, "api": service, "api_version": version, "operation": action, "request": input, "emails": result.Emails, "outcome": outcome}
	if err := s.ses.capture.append(record); err != nil {
		log.Error().Err(err).Str("operation", action).Str("request_id", id).Msg("SES capture failed; request not accepted")
		apiErr = &sesAPIError{Code: "InternalFailure", Message: "Unable to capture email request", Status: http.StatusInternalServerError}
	}
	if api == "v1" {
		sesV1Write(w, action, id, result.Output, apiErr)
	} else {
		sesV2Write(w, id, result.Output, apiErr)
	}
}
