package ses

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/mail"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const sesV1Namespace = "http://ses.amazonaws.com/doc/2010-12-01/"
const sesV1MaxMessageSize int64 = 10 * 1024 * 1024

// sesV1Decode reconstructs the AWS Query structures without dropping optional
// fields. Binary data remains the exact base64 string received on the wire.
func sesV1Decode(r *http.Request, action string) (map[string]any, *sesAPIError) {
	if err := r.ParseForm(); err != nil {
		return nil, sesInvalid("v1", "Invalid query request: "+err.Error())
	}
	root := &sesQueryNode{children: map[string]*sesQueryNode{}}
	for key, values := range r.Form {
		if sesV1ProtocolField(key) {
			continue
		}
		if len(values) != 1 {
			return nil, sesInvalid("v1", "Duplicate parameter: "+key)
		}
		path := strings.Split(key, ".")
		if len(path) > 16 {
			return nil, sesInvalid("v1", "Parameter is nested too deeply: "+key)
		}
		node := root
		for _, component := range path {
			if component == "" || node.value != nil {
				return nil, sesInvalid("v1", "Invalid parameter structure: "+key)
			}
			if node.children == nil {
				node.children = map[string]*sesQueryNode{}
			}
			if node.children[component] == nil {
				node.children[component] = &sesQueryNode{}
			}
			node = node.children[component]
		}
		if len(node.children) != 0 {
			return nil, sesInvalid("v1", "Invalid parameter structure: "+key)
		}
		value := values[0]
		node.value = &value
	}
	value, err := root.decode()
	if err != nil {
		return nil, sesInvalid("v1", err.Error())
	}
	input := value.(map[string]any)
	sesV1NormalizeQueryLists(action, input)
	return input, nil
}

// Authentication parameters belong to the transport, including presigned
// SigV4 URL parameters merged by net/http into the form.
func sesV1ProtocolField(key string) bool {
	switch strings.ToLower(key) {
	case "action", "version", "signature", "signatureversion", "signaturemethod",
		"awsaccesskeyid", "securitytoken", "timestamp", "expires",
		"x-amz-algorithm", "x-amz-credential", "x-amz-date", "x-amz-expires",
		"x-amz-security-token", "x-amz-signedheaders", "x-amz-signature":
		return true
	}
	return false
}

type sesQueryNode struct {
	value    *string
	children map[string]*sesQueryNode
}

func (n *sesQueryNode) decode() (any, error) {
	if n.value != nil {
		return *n.value, nil
	}
	if members := n.children["member"]; members != nil {
		if len(n.children) != 1 || members.value != nil {
			return nil, fmt.Errorf("Invalid list parameter structure")
		}
		indices := make([]int, 0, len(members.children))
		for index := range members.children {
			number, err := strconv.Atoi(index)
			if err != nil || number < 1 || strconv.Itoa(number) != index {
				return nil, fmt.Errorf("Invalid list member index: %s", index)
			}
			indices = append(indices, number)
		}
		sort.Ints(indices)
		values := make([]any, 0, len(indices))
		for _, index := range indices {
			value, err := members.children[strconv.Itoa(index)].decode()
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		return values, nil
	}
	result := make(map[string]any, len(n.children))
	for key, child := range n.children {
		value, err := child.decode()
		if err != nil {
			return nil, err
		}
		result[key] = value
	}
	return result, nil
}

func sesV1Write(w http.ResponseWriter, action, requestID string, output map[string]any, apiErr *sesAPIError) {
	var body bytes.Buffer
	body.WriteString(xml.Header)
	encoder := xml.NewEncoder(&body)
	if apiErr != nil {
		start := xml.StartElement{Name: xml.Name{Local: "ErrorResponse"}, Attr: []xml.Attr{{Name: xml.Name{Local: "xmlns"}, Value: sesV1Namespace}}}
		_ = encoder.EncodeToken(start)
		errType := "Sender"
		if apiErr.Status >= 500 {
			errType = "Receiver"
		}
		_ = sesV1Encode(encoder, "Error", map[string]any{"Type": errType, "Code": apiErr.Code, "Message": apiErr.Message})
		_ = sesV1Encode(encoder, "RequestId", requestID)
		_ = encoder.EncodeToken(start.End())
	} else {
		start := xml.StartElement{Name: xml.Name{Local: action + "Response"}, Attr: []xml.Attr{{Name: xml.Name{Local: "xmlns"}, Value: sesV1Namespace}}}
		_ = encoder.EncodeToken(start)
		_ = sesV1Encode(encoder, action+"Result", output)
		_ = sesV1Encode(encoder, "ResponseMetadata", map[string]any{"RequestId": requestID})
		_ = encoder.EncodeToken(start.End())
	}
	_ = encoder.Flush()
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.Header().Set("x-amzn-RequestId", requestID)
	if apiErr != nil {
		status := apiErr.Status
		if status == 0 {
			status = http.StatusBadRequest
		}
		w.WriteHeader(status)
	}
	_, _ = w.Write(body.Bytes())
}

func sesV1Encode(encoder *xml.Encoder, name string, value any) error {
	start := xml.StartElement{Name: xml.Name{Local: name}}
	if err := encoder.EncodeToken(start); err != nil {
		return err
	}
	switch typed := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if err := sesV1Encode(encoder, key, typed[key]); err != nil {
				return err
			}
		}
	case []any:
		for _, item := range typed {
			if err := sesV1Encode(encoder, "member", item); err != nil {
				return err
			}
		}
	case []map[string]any:
		for _, item := range typed {
			if err := sesV1Encode(encoder, "member", item); err != nil {
				return err
			}
		}
	default:
		if err := encoder.EncodeToken(xml.CharData(fmt.Sprint(value))); err != nil {
			return err
		}
	}
	return encoder.EncodeToken(start.End())
}

func (m *SESManager) sendV1(action string, input map[string]any) (sesSendResult, *sesAPIError) {
	if err := sesV1ValidateInput(action, input); err != nil {
		return sesSendResult{}, err
	}
	switch action {
	case "SendEmail", "SendTemplatedEmail", "SendBulkTemplatedEmail":
		if err := m.sesV1Envelope(input); err != nil {
			return sesSendResult{}, err
		}
	case "SendRawEmail", "SendCustomVerificationEmail":
		if err := m.checkConfigurationSet("v1", sesString(input["ConfigurationSetName"])); err != nil {
			return sesSendResult{}, err
		}
	}
	switch action {
	case "SendEmail":
		destination := sesObject(input["Destination"])
		if err := sesValidateRecipients("v1", destination); err != nil {
			return sesSendResult{}, err
		}
		message := sesObject(input["Message"])
		subject := sesObject(message["Subject"])
		body := sesObject(message["Body"])
		if _, exists := subject["Data"]; !exists {
			return sesSendResult{}, sesInvalid("v1", "Message.Subject.Data is required")
		}
		if len(body) == 0 || (body["Text"] == nil && body["Html"] == nil) {
			return sesSendResult{}, sesInvalid("v1", "Message.Body must contain Text or Html")
		}
		for _, name := range []string{"Text", "Html"} {
			if body[name] != nil {
				part := sesObject(body[name])
				if _, exists := part["Data"]; !exists {
					return sesSendResult{}, sesInvalid("v1", "Message.Body."+name+".Data is required")
				}
			}
		}
		email := sesV1Email(input, "simple", destination)
		email["subject"] = sesString(subject["Data"])
		if text := sesObject(body["Text"]); text != nil {
			email["text"] = sesString(text["Data"])
		}
		if html := sesObject(body["Html"]); html != nil {
			email["html"] = sesString(html["Data"])
		}
		if err := sesCheckMessageSize("v1", email, nil, sesV1MaxMessageSize); err != nil {
			return sesSendResult{}, err
		}
		return sesV1Success(email), nil
	case "SendRawEmail":
		raw := sesObject(input["RawMessage"])
		metadata, apiErr := sesValidateRaw("v1", sesString(raw["Data"]), sesV1MaxMessageSize)
		if apiErr != nil {
			return sesSendResult{}, apiErr
		}
		configurationSet := sesString(input["ConfigurationSetName"])
		// SendRawEmail's API selector takes precedence over the MIME header.
		// Select from parsed headers without rewriting the submitted request.
		// https://aws.amazon.com/blogs/messaging-and-targeting/introducing-sending-metrics/
		if _, provided := input["ConfigurationSetName"]; !provided {
			configurationSet = sesString(metadata["configuration_set"])
			if apiErr := m.checkConfigurationSet("v1", configurationSet); apiErr != nil {
				return sesSendResult{}, apiErr
			}
		}
		source := sesString(input["Source"])
		if source == "" {
			source = sesString(metadata["from"])
		}
		if err := m.checkSender("v1", source); err != nil {
			return sesSendResult{}, err
		}
		destination := sesObject(metadata["destination"])
		if destinations := sesStrings(input["Destinations"]); len(destinations) != 0 {
			destination = map[string]any{"ToAddresses": destinations}
		}
		if err := sesValidateRecipients("v1", destination); err != nil {
			return sesSendResult{}, err
		}
		if err := sesV1ValidateTags(input["Tags"]); err != nil {
			return sesSendResult{}, err
		}
		email := sesV1Email(input, "raw", destination)
		email["from"] = source
		email["subject"] = metadata["subject"]
		email["headers"] = metadata["headers"]
		email["configuration_set"] = configurationSet
		email["request_content_path"] = "RawMessage.Data"
		return sesV1Success(email), nil
	case "SendTemplatedEmail":
		destination := sesObject(input["Destination"])
		if err := sesValidateRecipients("v1", destination); err != nil {
			return sesSendResult{}, err
		}
		template, apiErr := m.sesV1Template(input, "TemplateData")
		if apiErr != nil {
			return sesSendResult{}, apiErr
		}
		email := sesV1Email(input, "template", destination)
		email["template"] = sesString(input["Template"])
		email["template_data"] = sesString(input["TemplateData"])
		sesV1Render(email, template, sesString(input["TemplateData"]))
		if err := sesV1CheckRenderedSize(email); err != nil {
			return sesSendResult{}, err
		}
		return sesV1Success(email), nil
	case "SendBulkTemplatedEmail":
		template, apiErr := m.sesV1Template(input, "DefaultTemplateData")
		if apiErr != nil {
			return sesSendResult{}, apiErr
		}
		if err := sesV1ValidateTags(input["DefaultTags"]); err != nil {
			return sesSendResult{}, err
		}
		destinations, ok := input["Destinations"].([]any)
		if !ok || len(destinations) == 0 || len(destinations) > 50 {
			return sesSendResult{}, sesInvalid("v1", "Destinations must contain between 1 and 50 destinations")
		}
		statuses := make([]any, 0, len(destinations))
		emails := make([]map[string]any, 0, len(destinations))
		for index, value := range destinations {
			entry := sesObject(value)
			destination := sesObject(entry["Destination"])
			entryErr := sesValidateRecipients("v1", destination)
			if entryErr == nil {
				entryErr = sesV1ValidateTags(entry["ReplacementTags"])
			}
			data := sesString(input["DefaultTemplateData"])
			if replacement, exists := entry["ReplacementTemplateData"]; exists {
				data = sesString(replacement)
			}
			if err := sesV1ValidateTemplateData(data, "ReplacementTemplateData"); err != nil {
				entryErr = err
			}
			if entryErr != nil {
				statuses = append(statuses, map[string]any{"Status": "InvalidParameterValue", "Error": entryErr.Message})
				emails = append(emails, map[string]any{"index": index, "content_type": "template", "destination": destination, "status": "InvalidParameterValue", "error": entryErr.Message})
				continue
			}
			email := sesV1Email(input, "template", destination)
			email["index"] = index
			email["template"] = sesString(input["Template"])
			email["template_data"] = data
			email["tags"] = input["DefaultTags"]
			email["replacement_tags"] = entry["ReplacementTags"]
			sesV1Render(email, template, data)
			if err := sesV1CheckRenderedSize(email); err != nil {
				statuses = append(statuses, map[string]any{"Status": err.Code, "Error": err.Message})
				delete(email, "message_id")
				email["status"] = err.Code
				email["error"] = err.Message
				emails = append(emails, email)
				continue
			}
			statuses = append(statuses, map[string]any{"Status": "Success", "MessageId": email["message_id"]})
			emails = append(emails, email)
		}
		return sesSendResult{Output: map[string]any{"Status": statuses}, Emails: emails}, nil
	case "SendCustomVerificationEmail":
		address := sesString(input["EmailAddress"])
		if err := sesValidateAddress("v1", "EmailAddress", address); err != nil {
			return sesSendResult{}, err
		}
		name := sesString(input["TemplateName"])
		if name == "" {
			return sesSendResult{}, sesInvalid("v1", "TemplateName is required")
		}
		template, exists := m.fixtures.CustomVerificationTemplates[name]
		if !exists {
			return sesSendResult{}, &sesAPIError{Code: "CustomVerificationEmailTemplateDoesNotExist", Message: "Custom verification email template does not exist: " + name, Status: http.StatusBadRequest}
		}
		if err := m.checkSender("v1", template.FromEmailAddress); err != nil {
			return sesSendResult{}, &sesAPIError{Code: "FromEmailAddressNotVerified", Message: err.Message, Status: http.StatusBadRequest}
		}
		email := map[string]any{"message_id": sesMessageID(), "content_type": "custom_verification", "from": template.FromEmailAddress, "destination": map[string]any{"ToAddresses": []string{address}}, "subject": template.TemplateSubject, "html": template.TemplateContent, "template": name, "success_redirection_url": template.SuccessRedirectionURL, "failure_redirection_url": template.FailureRedirectionURL}
		if err := sesCheckMessageSize("v1", email, nil, sesV1MaxMessageSize); err != nil {
			return sesSendResult{}, err
		}
		return sesV1Success(email), nil
	case "SendBounce":
		return m.sesV1Bounce(input)
	default:
		return sesSendResult{}, &sesAPIError{Code: "InvalidAction", Message: "Unknown SES sending action: " + action, Status: http.StatusBadRequest}
	}
}

func (m *SESManager) sesV1Envelope(input map[string]any) *sesAPIError {
	if err := m.checkSender("v1", sesString(input["Source"])); err != nil {
		return err
	}
	if err := m.checkConfigurationSet("v1", sesString(input["ConfigurationSetName"])); err != nil {
		return err
	}
	if path := sesString(input["ReturnPath"]); path != "" {
		if err := m.checkSender("v1", path); err != nil {
			return err
		}
	}
	for _, address := range sesStrings(input["ReplyToAddresses"]) {
		if err := sesValidateAddress("v1", "ReplyToAddresses", address); err != nil {
			return err
		}
	}
	return sesV1ValidateTags(input["Tags"])
}

func (m *SESManager) sesV1Template(input map[string]any, dataField string) (SESTemplate, *sesAPIError) {
	name := sesString(input["Template"])
	if name == "" {
		return SESTemplate{}, sesInvalid("v1", "Template is required")
	}
	if _, exists := input[dataField]; !exists {
		return SESTemplate{}, sesInvalid("v1", dataField+" is required")
	}
	if err := sesV1ValidateTemplateData(sesString(input[dataField]), dataField); err != nil {
		return SESTemplate{}, err
	}
	template, exists := m.template(name)
	if !exists {
		return SESTemplate{}, &sesAPIError{Code: "TemplateDoesNotExist", Message: "Template does not exist: " + name, Status: http.StatusBadRequest}
	}
	return template, nil
}

func sesV1ValidateTemplateData(data, field string) *sesAPIError {
	if utf8.RuneCountInString(data) > 262144 || !json.Valid([]byte(data)) {
		return sesInvalid("v1", field+" must be valid JSON of at most 262144 characters")
	}
	var object map[string]any
	if err := json.Unmarshal([]byte(data), &object); err != nil || object == nil {
		return sesInvalid("v1", field+" must be a JSON object")
	}
	return nil
}

func sesV1CheckRenderedSize(email map[string]any) *sesAPIError {
	rendered := sesObject(email["rendered"])
	if rendered == nil {
		return nil
	}
	sizeEmail := make(map[string]any, len(email)+3)
	for key, value := range email {
		sizeEmail[key] = value
	}
	for _, field := range []string{"subject", "text", "html"} {
		sizeEmail[field] = rendered[field]
	}
	return sesCheckMessageSize("v1", sizeEmail, nil, sesV1MaxMessageSize)
}

func sesV1Render(email map[string]any, template SESTemplate, data string) {
	rendered, err := sesRenderTemplate(template, data)
	if err != nil {
		// SES has already accepted templated messages before rendering occurs.
		email["capture_rendering_error"] = err.Error()
		return
	}
	email["rendered"] = rendered
}

func sesV1Email(input map[string]any, kind string, destination map[string]any) map[string]any {
	return map[string]any{"message_id": sesMessageID(), "content_type": kind, "from": sesString(input["Source"]), "destination": destination, "reply_to": sesStrings(input["ReplyToAddresses"]), "return_path": sesString(input["ReturnPath"]), "configuration_set": sesString(input["ConfigurationSetName"]), "tags": input["Tags"]}
}

func sesV1Success(email map[string]any) sesSendResult {
	return sesSendResult{Output: map[string]any{"MessageId": email["message_id"]}, Emails: []map[string]any{email}}
}

func sesV1ValidateTags(value any) *sesAPIError {
	if value == nil {
		return nil
	}
	tags, ok := value.([]any)
	if !ok {
		return sesInvalid("v1", "Tags must be a list")
	}
	for _, value := range tags {
		tag := sesObject(value)
		for _, field := range []string{"Name", "Value"} {
			value, exists := tag[field]
			if !exists {
				return sesInvalid("v1", "Tag."+field+" is required")
			}
			text := sesString(value)
			if len(text) > 256 {
				return sesInvalid("v1", "Tag."+field+" exceeds the 256 character limit")
			}
			for _, char := range text {
				if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '_' || char == '-') {
					return sesInvalid("v1", "Tag."+field+" must contain only ASCII letters, numbers, underscores and dashes")
				}
			}
		}
	}
	return nil
}

func (m *SESManager) sesV1Bounce(input map[string]any) (sesSendResult, *sesAPIError) {
	source := sesString(input["BounceSender"])
	if err := m.checkSender("v1", source); err != nil {
		return sesSendResult{}, err
	}
	if sesString(input["OriginalMessageId"]) == "" {
		return sesSendResult{}, sesInvalid("v1", "OriginalMessageId is required")
	}
	received, exists := m.receivedMessage(sesString(input["OriginalMessageId"]))
	if !exists || received.ReceivedAt.IsZero() || time.Since(received.ReceivedAt) >= 24*time.Hour {
		return sesSendResult{}, &sesAPIError{Code: "MessageRejected", Message: "Original message was not received through SES within the last 24 hours", Status: http.StatusBadRequest}
	}
	recipients, ok := input["BouncedRecipientInfoList"].([]any)
	if !ok || len(recipients) == 0 {
		return sesSendResult{}, sesInvalid("v1", "BouncedRecipientInfoList must contain at least one recipient")
	}
	for _, value := range recipients {
		recipient := sesObject(value)
		if err := sesValidateAddress("v1", "Recipient", sesString(recipient["Recipient"])); err != nil {
			return sesSendResult{}, err
		}
		bounceType := sesString(recipient["BounceType"])
		dsn := sesObject(recipient["RecipientDsnFields"])
		if bounceType == "" && len(dsn) == 0 {
			return sesSendResult{}, sesInvalid("v1", "Each bounced recipient requires BounceType or RecipientDsnFields")
		}
		if bounceType != "" {
			switch bounceType {
			case "DoesNotExist", "MessageTooLarge", "ExceededQuota", "ContentRejected", "Undefined", "TemporaryFailure":
			default:
				return sesSendResult{}, sesInvalid("v1", "Invalid BounceType: "+bounceType)
			}
		}
		if len(received.Recipients) != 0 {
			matched := false
			for _, original := range received.Recipients {
				parsed, err := mail.ParseAddress(original)
				if err != nil {
					continue
				}
				for _, candidate := range []string{sesString(recipient["Recipient"]), sesString(dsn["FinalRecipient"])} {
					address, err := mail.ParseAddress(candidate)
					if err == nil && strings.EqualFold(parsed.Address, address.Address) {
						matched = true
					}
				}
			}
			if !matched {
				return sesSendResult{}, &sesAPIError{Code: "MessageRejected", Message: "Bounced recipient was not a recipient of the original message", Status: http.StatusBadRequest}
			}
		}
		if len(dsn) != 0 {
			switch sesString(dsn["Action"]) {
			case "failed", "delayed", "delivered", "relayed", "expanded":
			default:
				return sesSendResult{}, sesInvalid("v1", "RecipientDsnFields.Action is required and must be a valid DSN action")
			}
			if sesString(dsn["Status"]) == "" {
				return sesSendResult{}, sesInvalid("v1", "RecipientDsnFields.Status is required")
			}
			if final := sesString(dsn["FinalRecipient"]); final != "" {
				if err := sesValidateAddress("v1", "FinalRecipient", final); err != nil {
					return sesSendResult{}, err
				}
			}
		}
	}
	if dsn := sesObject(input["MessageDsn"]); input["MessageDsn"] != nil && sesString(dsn["ReportingMta"]) == "" {
		return sesSendResult{}, sesInvalid("v1", "MessageDsn.ReportingMta is required")
	}
	email := map[string]any{"message_id": sesMessageID(), "content_type": "bounce", "from": source, "destination": map[string]any{"ToAddresses": []string{received.From}}, "original_message_id": sesString(input["OriginalMessageId"]), "bounced_recipients": recipients, "explanation": sesString(input["Explanation"]), "message_dsn": input["MessageDsn"]}
	// Count a generated DSN view and the explanation against the v1 MIME
	// quota. Capture retains the original structured fields rather than
	// claiming an AWS-generated bounce payload.
	var status strings.Builder
	if dsn := sesObject(input["MessageDsn"]); dsn != nil {
		for _, field := range []struct{ member, header string }{
			{"ReportingMta", "Reporting-MTA"}, {"ArrivalDate", "Arrival-Date"},
		} {
			if value := sesString(dsn[field.member]); value != "" {
				fmt.Fprintf(&status, "%s: %s\r\n", field.header, value)
			}
		}
		sesV1DSNExtensions(&status, dsn["ExtensionFields"])
	}
	for _, value := range recipients {
		recipient := sesObject(value)
		fmt.Fprintf(&status, "\r\nFinal-Recipient: rfc822; %s\r\n", sesString(recipient["Recipient"]))
		dsn := sesObject(recipient["RecipientDsnFields"])
		if dsn == nil {
			fmt.Fprintf(&status, "Action: failed\r\nStatus: %s\r\n", sesString(recipient["BounceType"]))
		} else {
			for _, field := range []struct{ member, header string }{
				{"FinalRecipient", "Final-Recipient"}, {"Action", "Action"},
				{"Status", "Status"}, {"RemoteMta", "Remote-MTA"},
				{"DiagnosticCode", "Diagnostic-Code"}, {"LastAttemptDate", "Last-Attempt-Date"},
			} {
				if value := sesString(dsn[field.member]); value != "" {
					fmt.Fprintf(&status, "%s: %s\r\n", field.header, value)
				}
			}
			sesV1DSNExtensions(&status, dsn["ExtensionFields"])
		}
	}
	sizeEmail := map[string]any{
		"from": source, "destination": email["destination"],
		"subject": "Delivery Status Notification", "text": email["explanation"],
		"delivery_status": status.String(),
	}
	if err := sesCheckMessageSize("v1", sizeEmail, nil, sesV1MaxMessageSize); err != nil {
		return sesSendResult{}, err
	}
	return sesV1Success(email), nil
}

func sesV1DSNExtensions(status *strings.Builder, value any) {
	for _, item := range sesAnyList(value) {
		field := sesObject(item)
		fmt.Fprintf(status, "%s: %s\r\n", sesString(field["Name"]), sesString(field["Value"]))
	}
}

// Known sending members follow their AWS Query shapes. Unknown extension
// members remain untouched in the decoded request for capture.
func sesV1ValidateInput(action string, input map[string]any) *sesAPIError {
	fields := []string{}
	switch action {
	case "SendEmail", "SendTemplatedEmail", "SendBulkTemplatedEmail":
		fields = []string{"Source", "SourceArn", "ReturnPath", "ReturnPathArn", "ConfigurationSetName"}
		if err := sesV1StringList(input["ReplyToAddresses"], "ReplyToAddresses"); err != nil {
			return err
		}
	case "SendRawEmail":
		fields = []string{"Source", "SourceArn", "FromArn", "ReturnPathArn", "ConfigurationSetName"}
		if err := sesV1StringList(input["Destinations"], "Destinations"); err != nil {
			return err
		}
		if err := sesV1ObjectStrings(input["RawMessage"], "RawMessage", "Data"); err != nil {
			return err
		}
	case "SendCustomVerificationEmail":
		fields = []string{"EmailAddress", "TemplateName", "ConfigurationSetName"}
	case "SendBounce":
		fields = []string{"BounceSender", "BounceSenderArn", "Explanation", "OriginalMessageId"}
		if err := sesV1ObjectStrings(input["MessageDsn"], "MessageDsn", "ReportingMta", "ArrivalDate"); err != nil {
			return err
		}
		if err := sesV1ExtensionFields(sesObject(input["MessageDsn"])["ExtensionFields"]); err != nil {
			return err
		}
		if value, exists := input["BouncedRecipientInfoList"]; exists {
			recipients, ok := value.([]any)
			if !ok {
				return sesInvalid("v1", "BouncedRecipientInfoList must be a list")
			}
			for _, value := range recipients {
				if err := sesV1ObjectStrings(value, "BouncedRecipientInfo", "Recipient", "RecipientArn", "BounceType"); err != nil {
					return err
				}
				recipient := sesObject(value)
				if err := sesV1ObjectStrings(recipient["RecipientDsnFields"], "RecipientDsnFields", "Action", "Status", "DiagnosticCode", "FinalRecipient", "LastAttemptDate", "RemoteMta"); err != nil {
					return err
				}
				if err := sesV1ExtensionFields(sesObject(recipient["RecipientDsnFields"])["ExtensionFields"]); err != nil {
					return err
				}
			}
		}
	}
	if err := sesV1ObjectStrings(input, "Request", fields...); err != nil {
		return err
	}
	if action == "SendTemplatedEmail" {
		if err := sesV1ObjectStrings(input, "Request", "Template", "TemplateArn", "TemplateData"); err != nil {
			return err
		}
	}
	if action == "SendBulkTemplatedEmail" {
		if err := sesV1ObjectStrings(input, "Request", "Template", "TemplateArn", "DefaultTemplateData"); err != nil {
			return err
		}
	}
	if action == "SendEmail" || action == "SendTemplatedEmail" {
		if err := sesV1DestinationShape(input["Destination"]); err != nil {
			return err
		}
	}
	if action == "SendEmail" {
		if err := sesV1ObjectStrings(input["Message"], "Message"); err != nil {
			return err
		}
		message := sesObject(input["Message"])
		if err := sesV1ObjectStrings(message["Subject"], "Message.Subject", "Data", "Charset"); err != nil {
			return err
		}
		if err := sesV1ObjectStrings(message["Body"], "Message.Body"); err != nil {
			return err
		}
		for _, field := range []string{"Text", "Html"} {
			if err := sesV1ObjectStrings(sesObject(message["Body"])[field], "Message.Body."+field, "Data", "Charset"); err != nil {
				return err
			}
		}
	}
	if action == "SendBulkTemplatedEmail" && input["Destinations"] != nil {
		destinations, ok := input["Destinations"].([]any)
		if !ok {
			return sesInvalid("v1", "Destinations must be a list")
		}
		for _, value := range destinations {
			if err := sesV1ObjectStrings(value, "BulkEmailDestination", "ReplacementTemplateData"); err != nil {
				return err
			}
			if err := sesV1DestinationShape(sesObject(value)["Destination"]); err != nil {
				return err
			}
		}
	}
	return nil
}

func sesV1ObjectStrings(value any, field string, members ...string) *sesAPIError {
	if value == nil {
		return nil
	}
	object := sesObject(value)
	if object == nil {
		return sesInvalid("v1", field+" must be an object")
	}
	for _, member := range members {
		if value, exists := object[member]; exists {
			if _, ok := value.(string); !ok {
				return sesInvalid("v1", field+"."+member+" must be a string")
			}
		}
	}
	return nil
}

func sesV1StringList(value any, field string) *sesAPIError {
	if value == nil {
		return nil
	}
	if _, ok := value.([]string); ok {
		return nil
	}
	values, ok := value.([]any)
	if !ok {
		return sesInvalid("v1", field+" must be a list of strings")
	}
	for _, item := range values {
		if _, ok := item.(string); !ok {
			return sesInvalid("v1", field+" must be a list of strings")
		}
	}
	return nil
}

func sesV1DestinationShape(value any) *sesAPIError {
	if err := sesV1ObjectStrings(value, "Destination"); err != nil {
		return err
	}
	for _, field := range []string{"ToAddresses", "CcAddresses", "BccAddresses"} {
		if err := sesV1StringList(sesObject(value)[field], "Destination."+field); err != nil {
			return err
		}
	}
	return nil
}

func sesV1ExtensionFields(value any) *sesAPIError {
	if value == nil {
		return nil
	}
	fields, ok := value.([]any)
	if !ok {
		return sesInvalid("v1", "ExtensionFields must be a list")
	}
	for _, value := range fields {
		if err := sesV1ObjectStrings(value, "ExtensionField", "Name", "Value"); err != nil {
			return err
		}
		field := sesObject(value)
		name := sesString(field["Name"])
		text, hasValue := field["Value"].(string)
		if name == "" || len(name) > 50 || !hasValue || len(text) > 2048 || strings.ContainsAny(text, "\r\n") {
			return sesInvalid("v1", "Invalid DSN extension field name or value")
		}
		for _, character := range name {
			if !(character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || character == '-') {
				return sesInvalid("v1", "DSN extension field names must contain only ASCII letters, numbers and dashes")
			}
		}
	}
	return nil
}

// AWS Query SDKs serialize an explicitly empty list as Field= rather than
// member entries. Only known list fields have this spelling; empty content,
// template data and opaque extension strings stay strings.
func sesV1NormalizeQueryLists(action string, input map[string]any) {
	normalize := func(object map[string]any, fields ...string) {
		for _, field := range fields {
			if text, ok := object[field].(string); ok && text == "" {
				object[field] = []any{}
			}
		}
	}
	destination := func(value any) { normalize(sesObject(value), "ToAddresses", "CcAddresses", "BccAddresses") }
	switch action {
	case "SendEmail", "SendTemplatedEmail", "SendBulkTemplatedEmail":
		normalize(input, "ReplyToAddresses")
		if action == "SendBulkTemplatedEmail" {
			normalize(input, "DefaultTags", "Destinations")
			for _, value := range sesV1List(input["Destinations"]) {
				entry := sesObject(value)
				normalize(entry, "ReplacementTags")
				destination(entry["Destination"])
			}
		} else {
			normalize(input, "Tags")
			destination(input["Destination"])
		}
	case "SendRawEmail":
		normalize(input, "Destinations", "Tags")
	case "SendBounce":
		normalize(input, "BouncedRecipientInfoList")
		normalize(sesObject(input["MessageDsn"]), "ExtensionFields")
		for _, value := range sesV1List(input["BouncedRecipientInfoList"]) {
			normalize(sesObject(sesObject(value)["RecipientDsnFields"]), "ExtensionFields")
		}
	}
}

func sesV1List(value any) []any { list, _ := value.([]any); return list }
