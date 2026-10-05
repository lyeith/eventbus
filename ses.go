package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/mail"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
)

const sesWireLimit int64 = 64 << 20
const sesBulkWireLimit int64 = 256 << 20

type sesAPIError struct {
	Code, Message string
	Status        int
}
type sesSendResult struct {
	Output map[string]any
	Emails []map[string]any
}

type SESTemplate struct {
	SubjectPart string `json:"SubjectPart" yaml:"SubjectPart"`
	TextPart    string `json:"TextPart" yaml:"TextPart"`
	HtmlPart    string `json:"HtmlPart" yaml:"HtmlPart"`
}
type SESCustomVerificationTemplate struct {
	FromEmailAddress      string `json:"FromEmailAddress" yaml:"FromEmailAddress"`
	TemplateSubject       string `json:"TemplateSubject" yaml:"TemplateSubject"`
	TemplateContent       string `json:"TemplateContent" yaml:"TemplateContent"`
	SuccessRedirectionURL string `json:"SuccessRedirectionURL" yaml:"SuccessRedirectionURL"`
	FailureRedirectionURL string `json:"FailureRedirectionURL" yaml:"FailureRedirectionURL"`
}
type SESReceivedMessage struct {
	From       string    `json:"from" yaml:"from"`
	ReceivedAt time.Time `json:"received_at" yaml:"received_at"`
	Recipients []string  `json:"recipients" yaml:"recipients"`
}

// Fixtures supply sending prerequisites without pretending to implement SES's
// management APIs or performing DNS checks, account changes, or email delivery.
type SESFixtures struct {
	Templates                   map[string]SESTemplate                   `json:"templates" yaml:"templates"`
	CustomVerificationTemplates map[string]SESCustomVerificationTemplate `json:"custom_verification_templates" yaml:"custom_verification_templates"`
	ConfigurationSets           []string                                 `json:"configuration_sets" yaml:"configuration_sets"`
	VerifiedIdentities          []string                                 `json:"verified_identities" yaml:"verified_identities"`
	RequireVerifiedIdentities   bool                                     `json:"require_verified_identities" yaml:"require_verified_identities"`
	SendingEnabled              *bool                                    `json:"sending_enabled" yaml:"sending_enabled"`
	ReceivedMessages            map[string]SESReceivedMessage            `json:"received_messages" yaml:"received_messages"`
}

func LoadSESFixtures(path string) (SESFixtures, error) {
	var fixtures SESFixtures
	file, err := os.Open(path)
	if err != nil {
		return fixtures, err
	}
	defer file.Close()
	decoder := yaml.NewDecoder(file)
	decoder.KnownFields(true)
	if err := decoder.Decode(&fixtures); err != nil {
		return fixtures, fmt.Errorf("load SES fixtures: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return fixtures, errors.New("SES fixtures must contain exactly one document")
	}
	for id, message := range fixtures.ReceivedMessages {
		if id == "" || message.ReceivedAt.IsZero() || message.From == "" {
			return fixtures, errors.New("SES received-message fixtures require id, from, and received_at")
		}
	}
	return fixtures, nil
}

// SESCapture is synchronous: no response succeeds ahead of its complete append.
// A failed append is terminal, so subsequent requests cannot append after a
// partial record and turn the remainder of the stream into invalid JSONL.
type SESCapture struct {
	mu        sync.Mutex
	writer    io.Writer
	syncFile  func() error
	closeFile func() error
	closed    bool
	failure   error
	closeErr  error
}

func OpenSESCapture(path string) (*SESCapture, error) {
	if path == "-" {
		return &SESCapture{writer: os.Stdout}, nil
	}
	if path == "" {
		return nil, errors.New("SES log path must not be empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err == nil && !info.Mode().IsRegular() {
		err = errors.New("SES log must be a regular file; use '-' for stdout")
	}
	if err == nil && info.Size() > 0 {
		last := make([]byte, 1)
		_, err = file.ReadAt(last, info.Size()-1)
		if err == nil && last[0] != '\n' {
			err = errors.New("SES log ends with an incomplete record; repair it or select a new log")
		}
	}
	if err != nil {
		file.Close()
		return nil, err
	}
	return &SESCapture{writer: file, syncFile: file.Sync, closeFile: file.Close}, nil
}

func (capture *SESCapture) append(record any) error {
	if capture == nil {
		return errors.New("SES capture is not configured")
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	capture.mu.Lock()
	defer capture.mu.Unlock()
	if capture.closed {
		return errors.New("SES capture is closed")
	}
	if capture.failure != nil {
		return capture.failure
	}
	if capture.writer == nil {
		return errors.New("SES capture writer is not configured")
	}
	written, err := capture.writer.Write(encoded)
	if err == nil && written != len(encoded) {
		err = io.ErrShortWrite
	}
	if err == nil && capture.syncFile != nil {
		err = capture.syncFile()
	}
	if err != nil {
		capture.failure = fmt.Errorf("append SES capture: %w", err)
	}
	return capture.failure
}

func (capture *SESCapture) Close() error {
	if capture == nil {
		return nil
	}
	capture.mu.Lock()
	defer capture.mu.Unlock()
	if capture.closed {
		return capture.closeErr
	}
	capture.closed = true
	var err error
	if capture.closeFile != nil {
		err = capture.closeFile()
	}
	capture.closeErr = errors.Join(capture.failure, err)
	return capture.closeErr
}

type SESManager struct {
	fixtures SESFixtures
	capture  *SESCapture
}

func NewSESManager(fixtures SESFixtures, capture *SESCapture) *SESManager {
	// Fixtures contain no mutable runtime state; make the caller's maps/pointers
	// independent before concurrent HTTP readers are admitted.
	owned := fixtures
	owned.Templates = maps.Clone(fixtures.Templates)
	owned.CustomVerificationTemplates = maps.Clone(fixtures.CustomVerificationTemplates)
	owned.ConfigurationSets = slices.Clone(fixtures.ConfigurationSets)
	owned.VerifiedIdentities = slices.Clone(fixtures.VerifiedIdentities)
	owned.ReceivedMessages = maps.Clone(fixtures.ReceivedMessages)
	for id, message := range owned.ReceivedMessages {
		message.Recipients = slices.Clone(message.Recipients)
		owned.ReceivedMessages[id] = message
	}
	if fixtures.SendingEnabled != nil {
		enabled := *fixtures.SendingEnabled
		owned.SendingEnabled = &enabled
	}
	return &SESManager{fixtures: owned, capture: capture}
}
func (manager *SESManager) Close() error {
	if manager == nil {
		return nil
	}
	return manager.capture.Close()
}
func (s *Server) SetSES(manager *SESManager) { s.ses = manager }

func sesV1SendingAction(action string) bool {
	switch action {
	case "SendEmail", "SendRawEmail", "SendTemplatedEmail", "SendBulkTemplatedEmail", "SendCustomVerificationEmail", "SendBounce":
		return true
	}
	return false
}
func sesQueryRequest(r *http.Request, action string) bool {
	return sesV1SendingAction(action) || r.FormValue("Version") == "2010-12-01" || strings.Contains(r.Header.Get("Authorization"), "/ses/aws4_request")
}
func (s *Server) handleSES(w http.ResponseWriter, r *http.Request, api, action string, parseError *sesAPIError) {
	id := requestID()
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

func sesObject(value any) map[string]any { object, _ := value.(map[string]any); return object }
func sesString(value any) string         { text, _ := value.(string); return text }
func sesStrings(value any) []string {
	if values, ok := value.([]string); ok {
		return values
	}
	values, _ := value.([]any)
	result := make([]string, 0, len(values))
	for _, value := range values {
		if text, ok := value.(string); ok {
			result = append(result, text)
		}
	}
	return result
}
func sesMessageID() string { return uuid.NewString() }
func sesInvalid(api, message string) *sesAPIError {
	code := "BadRequestException"
	if api == "v1" {
		code = "InvalidParameterValue"
	}
	return &sesAPIError{Code: code, Message: message, Status: http.StatusBadRequest}
}
func sesValidateAddress(api, field, address string) *sesAPIError {
	if address == "" || strings.ContainsAny(address, "\r\n") {
		return sesInvalid(api, field+" must be a valid email address")
	}
	for _, character := range address {
		if character > 127 {
			return sesInvalid(api, field+" must use ASCII email addresses and MIME-encoded display names")
		}
	}
	parsed, err := mail.ParseAddress(address)
	if err != nil || !strings.Contains(parsed.Address, "@") {
		return sesInvalid(api, field+" must be a valid email address")
	}
	return nil
}
func sesValidateRecipients(api string, destination map[string]any) *sesAPIError {
	count := 0
	for _, field := range []string{"ToAddresses", "CcAddresses", "BccAddresses"} {
		if value, present := destination[field]; present {
			values, ok := value.([]any)
			if !ok {
				if stringsValue, yes := value.([]string); yes {
					values = make([]any, len(stringsValue))
					for i, text := range stringsValue {
						values[i] = text
					}
				} else {
					return sesInvalid(api, field+" must be a list")
				}
			}
			for _, value := range values {
				address, ok := value.(string)
				if !ok {
					return sesInvalid(api, field+" must contain email addresses")
				}
				if err := sesValidateAddress(api, field, address); err != nil {
					return err
				}
				count++
			}
		}
	}
	if count < 1 || count > 50 {
		return sesInvalid(api, "A message requires between 1 and 50 recipients")
	}
	return nil
}
func (manager *SESManager) checkSender(api, sender string) *sesAPIError {
	if err := sesValidateAddress(api, "Sender", sender); err != nil {
		return err
	}
	if !manager.fixtures.RequireVerifiedIdentities {
		return nil
	}
	parsed, _ := mail.ParseAddress(sender)
	domain := parsed.Address[strings.LastIndex(parsed.Address, "@")+1:]
	for _, identity := range manager.fixtures.VerifiedIdentities {
		if strings.EqualFold(identity, parsed.Address) || strings.EqualFold(identity, domain) {
			return nil
		}
	}
	return &sesAPIError{Code: "MessageRejected", Message: "Email address is not verified", Status: http.StatusBadRequest}
}
func (manager *SESManager) checkConfigurationSet(api, name string) *sesAPIError {
	if name == "" {
		return nil
	}
	for _, configured := range manager.fixtures.ConfigurationSets {
		if configured == name {
			return nil
		}
	}
	code, status := "NotFoundException", http.StatusNotFound
	if api == "v1" {
		code, status = "ConfigurationSetDoesNotExist", http.StatusBadRequest
	}
	return &sesAPIError{Code: code, Message: "Configuration set does not exist: " + name, Status: status}
}
func (manager *SESManager) template(name string) (SESTemplate, bool) {
	if strings.HasPrefix(name, "arn:") {
		if index := strings.LastIndex(name, "/"); index >= 0 {
			name = name[index+1:]
		}
	}
	template, ok := manager.fixtures.Templates[name]
	return template, ok
}
func (manager *SESManager) receivedMessage(id string) (SESReceivedMessage, bool) {
	value, ok := manager.fixtures.ReceivedMessages[id]
	return value, ok
}

var sesSubstitution = regexp.MustCompile(`\{\{\{?\s*([^{}]+?)\s*\}?\}\}`)

// This is an optional capture view. The request and original template data stay
// intact; unsupported rich Handlebars expressions do not change API acceptance.
func sesRenderTemplate(template SESTemplate, data string) (map[string]any, error) {
	var values map[string]any
	decoder := json.NewDecoder(strings.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&values); err != nil {
		return nil, fmt.Errorf("capture template data: %w", err)
	}
	if values == nil {
		return nil, errors.New("capture template data must be a JSON object")
	}
	result := map[string]any{}
	for field, text := range map[string]string{"subject": template.SubjectPart, "text": template.TextPart, "html": template.HtmlPart} {
		var renderErr error
		rendered := sesSubstitution.ReplaceAllStringFunc(text, func(match string) string {
			parts := sesSubstitution.FindStringSubmatch(match)
			path := strings.TrimSpace(parts[1])
			if strings.ContainsAny(path, "#/@() \t") {
				renderErr = fmt.Errorf("capture view does not render rich Handlebars expression %q", path)
				return match
			}
			var value any = values
			for _, component := range strings.Split(path, ".") {
				object, ok := value.(map[string]any)
				if !ok {
					renderErr = fmt.Errorf("capture substitution %q is unavailable", path)
					return match
				}
				value, ok = object[component]
				if !ok {
					renderErr = fmt.Errorf("capture substitution %q is unavailable", path)
					return match
				}
			}
			if value == nil {
				return ""
			}
			return fmt.Sprint(value)
		})
		if renderErr != nil {
			return nil, renderErr
		}
		result[field] = rendered
	}
	return result, nil
}
