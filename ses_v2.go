package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"unicode/utf8"
)

const sesV2MaxMessageBytes int64 = 40 * 1024 * 1024

func sesV2Action(r *http.Request) (string, bool) {
	if r.Method != http.MethodPost {
		return "", false
	}
	switch r.URL.Path {
	case "/v2/email/outbound-emails":
		return "SendEmail", true
	case "/v2/email/outbound-bulk-emails":
		return "SendBulkEmail", true
	case "/v2/email/outbound-custom-verification-emails":
		return "SendCustomVerificationEmail", true
	default:
		return "", false
	}
}

func sesV2Decode(r *http.Request) (map[string]any, *sesAPIError) {
	decoder := json.NewDecoder(r.Body)
	decoder.UseNumber()
	var input map[string]any
	if err := decoder.Decode(&input); err != nil || input == nil {
		return nil, sesInvalid("v2", "Request body must be a JSON object")
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, sesInvalid("v2", "Request body must contain exactly one JSON object")
	}
	return input, nil
}

func sesV2Write(w http.ResponseWriter, requestID string, output map[string]any, apiErr *sesAPIError) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("x-amzn-requestid", requestID)
	if apiErr != nil {
		w.Header().Set("x-amzn-errortype", apiErr.Code)
		w.WriteHeader(apiErr.Status)
		_ = json.NewEncoder(w).Encode(map[string]any{"message": apiErr.Message})
		return
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(output)
}

func (m *SESManager) sendV2(action string, input map[string]any) (sesSendResult, *sesAPIError) {
	switch action {
	case "SendEmail":
		return m.sendV2Email(input)
	case "SendBulkEmail":
		return m.sendV2Bulk(input)
	case "SendCustomVerificationEmail":
		return m.sendV2CustomVerification(input)
	default:
		return sesSendResult{}, &sesAPIError{Code: "NotFoundException", Message: "Unknown SES operation", Status: http.StatusNotFound}
	}
}

func (m *SESManager) sendV2Email(input map[string]any) (sesSendResult, *sesAPIError) {
	if apiErr := sesV2ValidateCommon(input); apiErr != nil {
		return sesSendResult{}, apiErr
	}
	content := sesObject(input["Content"])
	if content == nil {
		return sesSendResult{}, sesInvalid("v2", "Content is required and must be an object")
	}
	kind := ""
	for _, candidate := range []string{"Simple", "Raw", "Template"} {
		if value, present := content[candidate]; present && value != nil {
			if kind != "" || sesObject(value) == nil {
				return sesSendResult{}, sesInvalid("v2", "Content must contain exactly one of Simple, Raw or Template")
			}
			kind = candidate
		}
	}
	if kind == "" {
		return sesSendResult{}, sesInvalid("v2", "Content must contain exactly one of Simple, Raw or Template")
	}
	var email map[string]any
	var apiErr *sesAPIError
	switch kind {
	case "Simple":
		email, apiErr = sesV2Simple(sesObject(content[kind]), "Content.Simple")
	case "Raw":
		raw := sesObject(content[kind])
		encoded, ok := raw["Data"].(string)
		if !ok {
			apiErr = sesInvalid("v2", "Content.Raw.Data is required and must be a base64 string")
			break
		}
		email, apiErr = sesValidateRaw("v2", encoded, sesV2MaxMessageBytes)
		if email != nil {
			email["content_type"] = "raw"
			email["request_content_path"] = "Content.Raw.Data"
		}
	case "Template":
		email, apiErr = m.sesV2Template(sesObject(content[kind]), "Content.Template")
	}
	if apiErr != nil {
		return sesSendResult{}, apiErr
	}
	if apiErr = m.sesV2Envelope(input, email); apiErr != nil {
		return sesSendResult{}, apiErr
	}
	if apiErr = m.checkConfigurationSet("v2", sesString(input["ConfigurationSetName"])); apiErr != nil {
		return sesSendResult{}, apiErr
	}
	if kind != "Raw" {
		if apiErr = sesCheckMessageSize("v2", email, sesObject(content[kind]), sesV2MaxMessageBytes); apiErr != nil {
			return sesSendResult{}, apiErr
		}
	}
	id := sesMessageID()
	email["message_id"] = id
	return sesSendResult{Output: map[string]any{"MessageId": id}, Emails: []map[string]any{email}}, nil
}

func (m *SESManager) sesV2Envelope(input, email map[string]any) *sesAPIError {
	if from := sesString(input["FromEmailAddress"]); from != "" {
		email["from"] = from
	}
	if apiErr := m.checkSender("v2", sesString(email["from"])); apiErr != nil {
		return apiErr
	}
	if destination, present := input["Destination"]; present && destination != nil {
		if apiErr := sesV2ValidateDestination(destination); apiErr != nil {
			return apiErr
		}
		email["destination"] = destination
	}
	if apiErr := sesValidateRecipients("v2", sesObject(email["destination"])); apiErr != nil {
		return apiErr
	}
	if replyTo, present := input["ReplyToAddresses"]; present && replyTo != nil {
		email["reply_to"] = replyTo
	}
	if feedback := sesString(input["FeedbackForwardingEmailAddress"]); feedback != "" {
		email["return_path"] = feedback
	}
	return nil
}

func (m *SESManager) sendV2Bulk(input map[string]any) (sesSendResult, *sesAPIError) {
	if apiErr := sesV2ValidateCommon(input); apiErr != nil {
		return sesSendResult{}, apiErr
	}
	entries, ok := input["BulkEmailEntries"].([]any)
	if !ok || len(entries) == 0 || len(entries) > 50 {
		return sesSendResult{}, sesInvalid("v2", "BulkEmailEntries must contain between 1 and 50 entries")
	}
	content := sesObject(input["DefaultContent"])
	if content == nil || sesObject(content["Template"]) == nil {
		return sesSendResult{}, sesInvalid("v2", "DefaultContent.Template is required")
	}
	if apiErr := sesV2ValidateTags(input["DefaultEmailTags"], "DefaultEmailTags"); apiErr != nil {
		return sesSendResult{}, apiErr
	}
	defaultTemplate := sesObject(content["Template"])
	// Shape errors are request errors; a missing named resource is reflected in
	// every entry, using the SES v2 result enum rather than the v1 enum names.
	defaultEmail, templateErr := m.sesV2Template(defaultTemplate, "DefaultContent.Template")
	if templateErr != nil && templateErr.Code != "NotFoundException" {
		return sesSendResult{}, templateErr
	}
	senderErr := m.checkSender("v2", sesString(input["FromEmailAddress"]))
	if senderErr != nil {
		return sesSendResult{}, senderErr
	}
	configurationErr := m.checkConfigurationSet("v2", sesString(input["ConfigurationSetName"]))
	results := make([]any, 0, len(entries))
	emails := make([]map[string]any, 0, len(entries))
	for index, value := range entries {
		entry := sesObject(value)
		var apiErr *sesAPIError
		if entry == nil {
			apiErr = sesInvalid("v2", "BulkEmailEntries must contain objects")
		} else if apiErr = sesV2ValidateDestination(entry["Destination"]); apiErr == nil {
			apiErr = sesValidateRecipients("v2", sesObject(entry["Destination"]))
		}
		if apiErr == nil {
			apiErr = sesV2ValidateTags(entry["ReplacementTags"], "ReplacementTags")
		}
		if apiErr == nil {
			apiErr = sesV2ValidateHeaders(entry["ReplacementHeaders"], "ReplacementHeaders")
		}
		template := make(map[string]any, len(defaultTemplate))
		for key, value := range defaultTemplate {
			template[key] = value
		}
		// Validate and inspect shared attachment data only once. The request is
		// retained by the caller and every successful email references it.
		delete(template, "Attachments")
		if apiErr == nil {
			if replacement, exists := entry["ReplacementEmailContent"]; exists && replacement != nil {
				object := sesObject(replacement)
				if object == nil || sesObject(object["ReplacementTemplate"]) == nil {
					apiErr = sesInvalid("v2", "ReplacementEmailContent.ReplacementTemplate must be an object")
				} else if data, exists := sesObject(object["ReplacementTemplate"])["ReplacementTemplateData"]; exists {
					template["TemplateData"] = data
				}
			}
		}
		if headers, exists := entry["ReplacementHeaders"]; exists && headers != nil {
			template["Headers"] = sesV2MergeHeaders(defaultTemplate["Headers"], headers)
		}
		var email map[string]any
		if apiErr == nil {
			email, apiErr = m.sesV2Template(template, "DefaultContent.Template")
			if email != nil && defaultEmail != nil && defaultEmail["attachments"] != nil {
				email["attachments"] = defaultEmail["attachments"]
			}
		}
		status := "INVALID_PARAMETER"
		if apiErr == nil && templateErr != nil {
			apiErr, status = templateErr, "TEMPLATE_NOT_FOUND"
		}
		if apiErr != nil && apiErr.Code == "NotFoundException" {
			status = "TEMPLATE_NOT_FOUND"
		}
		if apiErr == nil && configurationErr != nil {
			apiErr, status = configurationErr, "CONFIGURATION_SET_NOT_FOUND"
		}
		if apiErr == nil {
			email["from"] = input["FromEmailAddress"]
			email["destination"] = entry["Destination"]
			email["reply_to"] = input["ReplyToAddresses"]
			email["return_path"] = input["FeedbackForwardingEmailAddress"]
			email["entry_index"] = index
			// Attachments and template source live in DefaultContent; replacements
			// are captured once in the corresponding BulkEmailEntries object.
			email["request_content_path"] = "DefaultContent.Template"
			sizeContent := make(map[string]any, len(template)+1)
			for key, value := range template {
				sizeContent[key] = value
			}
			sizeContent["Attachments"] = defaultTemplate["Attachments"]
			apiErr = sesCheckMessageSize("v2", email, sizeContent, sesV2MaxMessageBytes)
			if apiErr != nil {
				status = "MESSAGE_REJECTED"
			}
		}
		if apiErr != nil {
			results = append(results, map[string]any{"Status": status, "Error": apiErr.Message})
			continue
		}
		id := sesMessageID()
		email["message_id"] = id
		emails = append(emails, email)
		results = append(results, map[string]any{"Status": "SUCCESS", "MessageId": id})
	}
	return sesSendResult{Output: map[string]any{"BulkEmailEntryResults": results}, Emails: emails}, nil
}

func (m *SESManager) sendV2CustomVerification(input map[string]any) (sesSendResult, *sesAPIError) {
	for _, field := range []string{"EmailAddress", "TemplateName"} {
		if value, ok := input[field].(string); !ok || value == "" {
			return sesSendResult{}, sesInvalid("v2", field+" is required and must be a non-empty string")
		}
	}
	if apiErr := sesValidateAddress("v2", "EmailAddress", sesString(input["EmailAddress"])); apiErr != nil {
		return sesSendResult{}, apiErr
	}
	if apiErr := sesV2StringFields(input, "ConfigurationSetName"); apiErr != nil {
		return sesSendResult{}, apiErr
	}
	template, found := m.fixtures.CustomVerificationTemplates[sesString(input["TemplateName"])]
	if !found {
		return sesSendResult{}, &sesAPIError{Code: "NotFoundException", Message: "Custom verification email template does not exist", Status: http.StatusNotFound}
	}
	if apiErr := m.checkSender("v2", template.FromEmailAddress); apiErr != nil {
		return sesSendResult{}, apiErr
	}
	if apiErr := m.checkConfigurationSet("v2", sesString(input["ConfigurationSetName"])); apiErr != nil {
		return sesSendResult{}, apiErr
	}
	id := sesMessageID()
	email := map[string]any{
		"message_id": id, "content_type": "custom_verification", "from": template.FromEmailAddress,
		"destination": map[string]any{"ToAddresses": []any{input["EmailAddress"]}},
		"subject":     template.TemplateSubject, "html": template.TemplateContent,
		"template": input["TemplateName"], "request_content_path": "TemplateName",
		"success_redirection_url": template.SuccessRedirectionURL,
		"failure_redirection_url": template.FailureRedirectionURL,
	}
	if apiErr := sesCheckMessageSize("v2", email, nil, sesV2MaxMessageBytes); apiErr != nil {
		return sesSendResult{}, apiErr
	}
	return sesSendResult{Output: map[string]any{"MessageId": id}, Emails: []map[string]any{email}}, nil
}

func sesV2ValidateCommon(input map[string]any) *sesAPIError {
	if apiErr := sesV2StringFields(input, "FromEmailAddress", "FromEmailAddressIdentityArn", "ConfigurationSetName", "FeedbackForwardingEmailAddress", "FeedbackForwardingEmailAddressIdentityArn", "EndpointId", "TenantName"); apiErr != nil {
		return apiErr
	}
	if apiErr := sesV2StringList(input["ReplyToAddresses"], "ReplyToAddresses"); apiErr != nil {
		return apiErr
	}
	for _, address := range sesStrings(input["ReplyToAddresses"]) {
		if apiErr := sesValidateAddress("v2", "ReplyToAddresses", address); apiErr != nil {
			return apiErr
		}
	}
	if feedback := sesString(input["FeedbackForwardingEmailAddress"]); feedback != "" {
		if apiErr := sesValidateAddress("v2", "FeedbackForwardingEmailAddress", feedback); apiErr != nil {
			return apiErr
		}
	}
	if apiErr := sesV2ValidateTags(input["EmailTags"], "EmailTags"); apiErr != nil {
		return apiErr
	}
	if value, present := input["ListManagementOptions"]; present && value != nil {
		options := sesObject(value)
		if options == nil || sesString(options["ContactListName"]) == "" {
			return sesInvalid("v2", "ListManagementOptions.ContactListName is required")
		}
		if apiErr := sesV2StringFields(options, "ContactListName", "TopicName"); apiErr != nil {
			return apiErr
		}
	}
	if value, present := input["ConfigurationOverrides"]; present && value != nil {
		overrides := sesObject(value)
		if overrides == nil {
			return sesInvalid("v2", "ConfigurationOverrides must be an object")
		}
		if value, present := overrides["Tracking"]; present && value != nil {
			tracking := sesObject(value)
			if tracking == nil {
				return sesInvalid("v2", "ConfigurationOverrides.Tracking must be an object")
			}
			for _, key := range []string{"ClickTrackingEnabled", "OpenTrackingEnabled"} {
				if value, present := tracking[key]; present && value != nil && value != "ENABLED" && value != "DISABLED" {
					return sesInvalid("v2", key+" must be ENABLED or DISABLED")
				}
			}
		}
	}
	return nil
}

func sesV2StringFields(input map[string]any, fields ...string) *sesAPIError {
	for _, field := range fields {
		if value, present := input[field]; present && value != nil {
			if _, ok := value.(string); !ok {
				return sesInvalid("v2", field+" must be a string")
			}
		}
	}
	return nil
}

func sesV2StringList(value any, field string) *sesAPIError {
	if value == nil {
		return nil
	}
	if _, ok := value.([]string); ok {
		return nil
	}
	values, ok := value.([]any)
	if !ok {
		return sesInvalid("v2", field+" must be an array of strings")
	}
	for _, value := range values {
		if _, ok := value.(string); !ok {
			return sesInvalid("v2", field+" must be an array of strings")
		}
	}
	return nil
}

func sesV2ValidateDestination(value any) *sesAPIError {
	destination := sesObject(value)
	if destination == nil {
		return sesInvalid("v2", "Destination is required and must be an object")
	}
	for _, key := range []string{"ToAddresses", "CcAddresses", "BccAddresses"} {
		if apiErr := sesV2StringList(destination[key], "Destination."+key); apiErr != nil {
			return apiErr
		}
	}
	return nil
}

func sesV2ValidateTags(value any, field string) *sesAPIError {
	if value == nil {
		return nil
	}
	values, ok := value.([]any)
	if !ok {
		return sesInvalid("v2", field+" must be an array")
	}
	for _, value := range values {
		tag := sesObject(value)
		if tag == nil {
			return sesInvalid("v2", field+" must contain objects")
		}
		for _, key := range []string{"Name", "Value"} {
			text, ok := tag[key].(string)
			if !ok || len(text) > 256 {
				return sesInvalid("v2", field+"."+key+" must be a string of at most 256 characters")
			}
			for _, char := range text {
				if !((char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '_' || char == '-') {
					return sesInvalid("v2", field+"."+key+" contains an invalid character")
				}
			}
		}
	}
	return nil
}

func sesV2ValidateHeaders(value any, field string) *sesAPIError {
	if value == nil {
		return nil
	}
	values, ok := value.([]any)
	if !ok || len(values) > 15 {
		return sesInvalid("v2", field+" must be an array containing at most 15 headers")
	}
	for _, value := range values {
		header := sesObject(value)
		if header == nil {
			return sesInvalid("v2", field+" must contain header objects")
		}
		name, nameOK := header["Name"].(string)
		text, textOK := header["Value"].(string)
		if !nameOK || !textOK || name == "" || len(name) > 126 || text == "" || len(text) > 995 || len(name)+len(text) > 996 {
			return sesInvalid("v2", "Invalid message header name or value length")
		}
		for _, char := range name {
			if char < 33 || char > 126 || char == ':' {
				return sesInvalid("v2", "Invalid message header name")
			}
		}
		for _, char := range text {
			if char < 32 || char > 126 {
				return sesInvalid("v2", "Invalid message header value")
			}
		}
		switch strings.ToLower(name) {
		case "bcc", "cc", "content-disposition", "content-type", "date", "from", "message-id", "mime-version", "reply-to", "return-path", "subject", "to":
			return sesInvalid("v2", "Message header is set by SES and cannot be supplied as a custom header: "+name)
		}
	}
	return nil
}

func sesV2MergeHeaders(defaults, replacements any) []any {
	base, _ := defaults.([]any)
	replacement, _ := replacements.([]any)
	result := make([]any, 0, len(base)+len(replacement))
	names := make(map[string]bool, len(replacement))
	for _, value := range replacement {
		names[strings.ToLower(sesString(sesObject(value)["Name"]))] = true
	}
	for _, value := range base {
		if !names[strings.ToLower(sesString(sesObject(value)["Name"]))] {
			result = append(result, value)
		}
	}
	return append(result, replacement...)
}

func sesV2Content(value any, field string) (map[string]any, *sesAPIError) {
	content := sesObject(value)
	if content == nil {
		return nil, sesInvalid("v2", field+" is required and must be an object")
	}
	if _, ok := content["Data"].(string); !ok {
		return nil, sesInvalid("v2", field+".Data is required and must be a string")
	}
	return content, sesV2StringFields(content, "Charset")
}

func sesV2Simple(message map[string]any, path string) (map[string]any, *sesAPIError) {
	subject, apiErr := sesV2Content(message["Subject"], path+".Subject")
	if apiErr != nil {
		return nil, apiErr
	}
	body := sesObject(message["Body"])
	if body == nil || (body["Text"] == nil && body["Html"] == nil) {
		return nil, sesInvalid("v2", path+".Body must contain Text or Html")
	}
	email := map[string]any{"content_type": "simple", "subject": subject["Data"], "request_content_path": path}
	for key, field := range map[string]string{"Text": "text", "Html": "html"} {
		if value := body[key]; value != nil {
			content, apiErr := sesV2Content(value, path+".Body."+key)
			if apiErr != nil {
				return nil, apiErr
			}
			email[field] = content["Data"]
		}
	}
	if apiErr := sesV2ContentExtras(message, email, path); apiErr != nil {
		return nil, apiErr
	}
	return email, nil
}

func (m *SESManager) sesV2Template(template map[string]any, path string) (map[string]any, *sesAPIError) {
	if apiErr := sesV2StringFields(template, "TemplateName", "TemplateArn", "TemplateData"); apiErr != nil {
		return nil, apiErr
	}
	name := sesString(template["TemplateName"])
	external := name != ""
	if arn := sesString(template["TemplateArn"]); arn != "" {
		external = true
		parts := strings.SplitN(arn, ":", 6)
		if len(parts) != 6 || parts[0] != "arn" || parts[2] != "ses" || !strings.HasPrefix(parts[5], "template/") || len(parts[5]) <= len("template/") {
			return nil, sesInvalid("v2", "TemplateArn must identify an SES email template")
		}
		arnName := strings.TrimPrefix(parts[5], "template/")
		if name != "" && name != arnName {
			return nil, sesInvalid("v2", "TemplateName and TemplateArn must identify the same email template")
		}
		name = arnName
	}
	inline := sesObject(template["TemplateContent"])
	if value, present := template["TemplateContent"]; present && value != nil {
		if inline == nil {
			return nil, sesInvalid("v2", "TemplateContent must be an object")
		}
	}
	if external == (inline != nil) {
		return nil, sesInvalid("v2", "Specify a TemplateName or TemplateArn, or provide exclusive inline TemplateContent")
	}
	var content SESTemplate
	if value, present := template["TemplateContent"]; present && value != nil {
		if apiErr := sesV2StringFields(inline, "Subject", "Text", "Html"); apiErr != nil {
			return nil, apiErr
		}
		content = SESTemplate{SubjectPart: sesString(inline["Subject"]), TextPart: sesString(inline["Text"]), HtmlPart: sesString(inline["Html"])}
	} else if name != "" {
		var found bool
		content, found = m.template(name)
		if !found {
			return nil, &sesAPIError{Code: "NotFoundException", Message: "Email template does not exist: " + name, Status: http.StatusNotFound}
		}
	}
	data := sesString(template["TemplateData"])
	if data == "" {
		if value, present := template["TemplateData"]; !present || value == nil {
			data = "{}"
		}
	}
	if utf8.RuneCountInString(data) > 262144 || !json.Valid([]byte(data)) {
		return nil, sesInvalid("v2", "TemplateData must be valid JSON of at most 262144 characters")
	}
	var values map[string]any
	if err := json.Unmarshal([]byte(data), &values); err != nil || values == nil {
		return nil, sesInvalid("v2", "TemplateData must be a JSON object")
	}
	email := map[string]any{"content_type": "template", "template": name, "template_data": data, "request_content_path": path}
	if rendered, err := sesRenderTemplate(content, data); err != nil {
		email["capture_rendering_error"] = err.Error()
		// Retain the fixture source as a useful view when the optional capture
		// renderer does not support an expression accepted by real SES.
		email["subject"], email["text"], email["html"] = content.SubjectPart, content.TextPart, content.HtmlPart
	} else {
		email["rendered"] = rendered
		for _, key := range []string{"subject", "text", "html"} {
			email[key] = rendered[key]
		}
	}
	if apiErr := sesV2ContentExtras(template, email, path); apiErr != nil {
		return nil, apiErr
	}
	return email, nil
}

// The transport always uses base64 for blob fields. ContentTransferEncoding
// controls the later MIME encoding and therefore the 40 MiB message limit.
func sesV2ContentExtras(content, email map[string]any, path string) *sesAPIError {
	if apiErr := sesV2ValidateHeaders(content["Headers"], path+".Headers"); apiErr != nil {
		return apiErr
	}
	if content["Headers"] != nil {
		email["headers"] = content["Headers"]
	}
	if content["Attachments"] == nil {
		return nil
	}
	attachments, ok := content["Attachments"].([]any)
	if !ok {
		return sesInvalid("v2", path+".Attachments must be an array")
	}
	metadata := make([]any, 0, len(attachments))
	for index, value := range attachments {
		attachment := sesObject(value)
		if attachment == nil {
			return sesInvalid("v2", "Attachments must contain objects")
		}
		if apiErr := sesV2StringFields(attachment, "FileName", "RawContent", "ContentDescription", "ContentDisposition", "ContentId", "ContentTransferEncoding", "ContentType"); apiErr != nil {
			return apiErr
		}
		filename := sesString(attachment["FileName"])
		if filename == "" || utf8.RuneCountInString(filename) > 255 || strings.ContainsAny(filename, "\r\n") {
			return sesInvalid("v2", "Attachment.FileName is required and must be at most 255 characters")
		}
		if sesUnsupportedAttachment(filename) {
			return &sesAPIError{Code: "MessageRejected", Message: "Unsupported attachment file type: " + filename, Status: http.StatusBadRequest}
		}
		encoded, present := attachment["RawContent"].(string)
		if !present {
			return sesInvalid("v2", "Attachment.RawContent is required")
		}
		decodedSize, err := io.Copy(io.Discard, base64.NewDecoder(base64.StdEncoding, strings.NewReader(encoded)))
		if err != nil {
			return sesInvalid("v2", "Attachment.RawContent must be valid base64")
		}
		for _, field := range []string{"ContentId", "ContentType"} {
			if value, present := attachment[field]; present && value != nil {
				text := sesString(value)
				if text == "" || utf8.RuneCountInString(text) > 78 || strings.ContainsAny(text, "\r\n") {
					return sesInvalid("v2", "Attachment."+field+" must contain between 1 and 78 characters without newlines")
				}
			}
		}
		if contentType := sesString(attachment["ContentType"]); contentType != "" {
			if mediaType, _, err := mime.ParseMediaType(contentType); err != nil || !strings.Contains(mediaType, "/") {
				return sesInvalid("v2", "Attachment.ContentType must be a valid MIME media type")
			}
		}
		if description := sesString(attachment["ContentDescription"]); utf8.RuneCountInString(description) > 1000 || strings.ContainsAny(description, "\r\n") {
			return sesInvalid("v2", "Attachment.ContentDescription must be at most 1000 characters without newlines")
		}
		if disposition := sesString(attachment["ContentDisposition"]); disposition != "" && disposition != "ATTACHMENT" && disposition != "INLINE" {
			return sesInvalid("v2", "Attachment.ContentDisposition must be ATTACHMENT or INLINE")
		}
		if encoding := sesString(attachment["ContentTransferEncoding"]); encoding != "" && encoding != "BASE64" && encoding != "QUOTED_PRINTABLE" && encoding != "SEVEN_BIT" {
			return sesInvalid("v2", "Attachment.ContentTransferEncoding must be BASE64, QUOTED_PRINTABLE or SEVEN_BIT")
		}
		view := make(map[string]any, len(attachment)+1)
		for key, value := range attachment {
			if key != "RawContent" {
				view[key] = value
			}
		}
		view["content_bytes"] = decodedSize
		view["raw_content_path"] = fmt.Sprintf("%s.Attachments[%d].RawContent", path, index)
		metadata = append(metadata, view)
	}
	email["attachments"] = metadata
	return nil
}
