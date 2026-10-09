package ses

import (
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"time"

	"github.com/lyeith/eventbus/internal/awsprotocol"
)

// DevOutcomePath is an explicitly local control, outside both SES AWS APIs.
const DevOutcomePath = "/__eventbus/dev/ses/outcomes"

// DevOutcome names emulator evidence; it is never inferred from a successful
// capture or native handler execution. Native SendBounce remains separate.
type DevOutcome struct {
	MessageID     string   `json:"message_id"`
	EventType     string   `json:"event_type"`
	Recipients    []string `json:"recipients,omitempty"`
	IPAddress     string   `json:"ip_address,omitempty"`
	UserAgent     string   `json:"user_agent,omitempty"`
	BounceType    string   `json:"bounce_type,omitempty"`
	BounceSubType string   `json:"bounce_sub_type,omitempty"`
}

// ServeDevOutcome emits an explicit Open/Bounce using the same current native
// destination rules and immutable mail correlation as automatic Send events.
func (handler *Handler) ServeDevOutcome(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	writeError := func(status int, code, message string) {
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]any{"code": code, "message": message})
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(http.StatusMethodNotAllowed, "MethodNotAllowed", "Use POST for explicit local SES outcomes")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var outcome DevOutcome
	if err := decoder.Decode(&outcome); err != nil {
		writeError(http.StatusBadRequest, "InvalidOutcome", "Outcome must be a JSON object with supported fields")
		return
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		writeError(http.StatusBadRequest, "InvalidOutcome", "Outcome must contain exactly one JSON object")
		return
	}
	if outcome.MessageID == "" || outcome.EventType != "Open" && outcome.EventType != "Bounce" {
		writeError(http.StatusBadRequest, "InvalidOutcome", "message_id is required; event_type must be Open or Bounce")
		return
	}
	now := time.Now().UTC()
	manager := handler.ses
	manager.mu.Lock()
	manager.pruneAccepted(now)
	accepted := manager.accepted[outcome.MessageID]
	if accepted == nil {
		manager.mu.Unlock()
		writeError(http.StatusNotFound, "MessageNotFound", "No retained accepted SES message has this message_id")
		return
	}
	// A same-name replacement configuration set cannot inherit old messages.
	configuration := accepted.configuration
	if configuration != nil && manager.configurationSets[configuration.name] != configuration {
		configuration = nil
	}
	matching := "open"
	if outcome.EventType == "Bounce" {
		matching = "bounce"
	}
	topics := eventTopics(configuration, matching)
	manager.mu.Unlock()
	recipients := outcome.Recipients
	if len(recipients) == 0 {
		recipients = accepted.recipients
	}
	for _, recipient := range recipients {
		if !slices.Contains(accepted.recipients, recipient) {
			writeError(http.StatusBadRequest, "InvalidOutcome", "Outcome recipients must belong to the accepted message")
			return
		}
	}
	details := map[string]any{"timestamp": now.Format(time.RFC3339Nano)}
	if outcome.EventType == "Open" {
		if len(outcome.Recipients) != 0 || outcome.BounceType != "" || outcome.BounceSubType != "" {
			writeError(http.StatusBadRequest, "InvalidOutcome", "Bounce fields cannot be supplied for Open")
			return
		}
		ip, userAgent := outcome.IPAddress, outcome.UserAgent
		if ip == "" {
			ip = "127.0.0.1"
		}
		if userAgent == "" {
			userAgent = "EventBus explicit local outcome"
		}
		details["ipAddress"], details["userAgent"] = ip, userAgent
	} else {
		if outcome.IPAddress != "" || outcome.UserAgent != "" {
			writeError(http.StatusBadRequest, "InvalidOutcome", "Open fields cannot be supplied for Bounce")
			return
		}
		bounceType, subtype := outcome.BounceType, outcome.BounceSubType
		if bounceType == "" {
			bounceType = "Permanent"
		}
		if subtype == "" {
			subtype = "General"
		}
		valid := map[string][]string{
			"Undetermined": {"Undetermined"},
			"Permanent":    {"General", "NoEmail", "Suppressed", "OnAccountSuppressionList", "EmailValidationSuppressed", "OnTenantSuppressionList"},
			"Transient":    {"General", "MailboxFull", "MessageTooLarge", "ContentRejected", "AttachmentRejected", "CustomTimeoutExceeded"},
		}
		if !slices.Contains(valid[bounceType], subtype) {
			writeError(http.StatusBadRequest, "InvalidOutcome", "Invalid bounce_type/bounce_sub_type combination")
			return
		}
		bounced := make([]map[string]string, 0, len(recipients))
		for _, recipient := range recipients {
			bounced = append(bounced, map[string]string{"emailAddress": recipient})
		}
		details["bounceType"], details["bounceSubType"] = bounceType, subtype
		details["bouncedRecipients"], details["feedbackId"] = bounced, "eventbus-local-"+awsprotocol.RequestID()
	}
	requestID := awsprotocol.RequestID()
	admissions := manager.publishEvent(r.Context(), requestID, accepted, outcome.EventType, details, topics)
	failed := false
	for _, admission := range admissions {
		failed = failed || admission.Error != ""
	}
	status := http.StatusOK
	if failed {
		status = http.StatusBadGateway
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"local": true, "message_id": outcome.MessageID,
		"event_type": outcome.EventType, "request_id": requestID, "destinations": admissions})
}
