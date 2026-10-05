package main

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func sesV2TestInput(t *testing.T, body string) map[string]any {
	t.Helper()
	input, apiErr := sesV2Decode(httptest.NewRequest(http.MethodPost, "/v2/email/outbound-emails", strings.NewReader(body)))
	if apiErr != nil {
		t.Fatalf("decode request: %+v", apiErr)
	}
	return input
}

func sesV2TestManager() *SESManager {
	return NewSESManager(SESFixtures{
		ConfigurationSets: []string{"test"},
		Templates: map[string]SESTemplate{
			"welcome": {SubjectPart: "Hello {{name}}", TextPart: "Welcome, {{name}}", HtmlPart: "<p>{{name}}</p>"},
		},
		CustomVerificationTemplates: map[string]SESCustomVerificationTemplate{
			"verify": {FromEmailAddress: "sender@example.com", TemplateSubject: "Verify your email", TemplateContent: "<p>Verify</p>", SuccessRedirectionURL: "https://example.com/success", FailureRedirectionURL: "https://example.com/failure"},
		},
	}, nil)
}

func sesV2RequireError(t *testing.T, apiErr *sesAPIError, code string, status int) {
	t.Helper()
	if apiErr == nil || apiErr.Code != code || apiErr.Status != status {
		t.Fatalf("error = %+v, want %s / %d", apiErr, code, status)
	}
}

func TestSESV2WireRoutesAndJSON(t *testing.T) {
	for path, expected := range map[string]string{
		"/v2/email/outbound-emails":                     "SendEmail",
		"/v2/email/outbound-bulk-emails":                "SendBulkEmail",
		"/v2/email/outbound-custom-verification-emails": "SendCustomVerificationEmail",
	} {
		t.Run(expected, func(t *testing.T) {
			action, matched := sesV2Action(httptest.NewRequest(http.MethodPost, path+"?ignored=true", nil))
			if !matched || action != expected {
				t.Fatalf("route = %s / %v", action, matched)
			}
			_, matched = sesV2Action(httptest.NewRequest(http.MethodGet, path, nil))
			if matched {
				t.Fatal("GET must not match a send operation")
			}
		})
	}
	if _, matched := sesV2Action(httptest.NewRequest(http.MethodPost, "/v2/email/identities", nil)); matched {
		t.Fatal("management route matched a send operation")
	}
	for _, body := range []string{"", "null", "[]", "{", "{} {}", "{} trailing"} {
		_, apiErr := sesV2Decode(httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body)))
		sesV2RequireError(t, apiErr, "BadRequestException", http.StatusBadRequest)
	}
	// A valid request body may be much larger than the old server form limit.
	large := `{"Content":{"Simple":{"Body":{"Text":{"Data":"` + strings.Repeat("x", 2<<20) + `"}}}}}`
	input := sesV2TestInput(t, large)
	if len(sesString(sesObject(sesObject(sesObject(sesObject(input["Content"])["Simple"])["Body"])["Text"])["Data"])) != 2<<20 {
		t.Fatal("large body was truncated")
	}
	input = sesV2TestInput(t, `{"FutureField":9007199254740993}`)
	if input["FutureField"].(json.Number).String() != "9007199254740993" {
		t.Fatal("decoder lost number precision")
	}
	response := httptest.NewRecorder()
	sesV2Write(response, "request-id", nil, &sesAPIError{Code: "NotFoundException", Message: "missing", Status: http.StatusNotFound})
	if response.Code != http.StatusNotFound || response.Header().Get("x-amzn-errortype") != "NotFoundException" || response.Header().Get("x-amzn-requestid") != "request-id" {
		t.Fatalf("error wire response: %d / %v", response.Code, response.Header())
	}
	var errorBody map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &errorBody); err != nil || errorBody["message"] != "missing" {
		t.Fatalf("error body = %s / %v", response.Body.String(), err)
	}
	response = httptest.NewRecorder()
	sesV2Write(response, "request-id", map[string]any{"MessageId": "message-id"}, nil)
	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("success wire response: %d / %v", response.Code, response.Header())
	}
}

func TestSESV2SimpleCapturesCurrentSendingFields(t *testing.T) {
	input := sesV2TestInput(t, `{
	  "FromEmailAddress":"Sender <sender@example.com>",
	  "FromEmailAddressIdentityArn":"arn:aws:ses:us-east-1:123456789012:identity/example.com",
	  "Destination":{"ToAddresses":["recipient@example.com"],"CcAddresses":["copy@example.com"],"BccAddresses":["blind@example.com"]},
	  "ReplyToAddresses":["reply@example.com"],
	  "FeedbackForwardingEmailAddress":"feedback@example.com",
	  "FeedbackForwardingEmailAddressIdentityArn":"arn:aws:ses:us-east-1:123456789012:identity/example.com",
	  "ConfigurationSetName":"test", "EmailTags":[{"Name":"campaign","Value":"spring_2026"}],
	  "EndpointId":"example.global", "TenantName":"testing",
	  "ConfigurationOverrides":{"Tracking":{"ClickTrackingEnabled":"DISABLED","OpenTrackingEnabled":"ENABLED"}},
	  "ListManagementOptions":{"ContactListName":"customers","TopicName":"updates"},
	  "Content":{"Simple":{
	    "Subject":{"Data":"Hello","Charset":"UTF-8"},
	    "Body":{"Text":{"Data":"Text ✓","Charset":"UTF-8"},"Html":{"Data":"<p>Hello</p>","Charset":"UTF-8"}},
	    "Headers":[{"Name":"X-Correlation-ID","Value":"example"}],
	    "Attachments":[{"FileName":"data.bin","RawContent":"AAECA//+","ContentDisposition":"INLINE","ContentTransferEncoding":"BASE64","ContentType":"application/octet-stream","ContentDescription":"binary fixture","ContentId":"binary"}]
	  }},
	  "FutureField":{"enabled":true}
	}`)
	before, _ := json.Marshal(input)
	result, apiErr := sesV2TestManager().sendV2("SendEmail", input)
	if apiErr != nil || len(result.Emails) != 1 {
		t.Fatalf("send = %+v / %+v", result, apiErr)
	}
	after, _ := json.Marshal(input)
	if string(before) != string(after) {
		t.Fatal("sending mutated the request retained for capture")
	}
	email := result.Emails[0]
	if result.Output["MessageId"] == "" || result.Output["MessageId"] != email["message_id"] || email["content_type"] != "simple" {
		t.Fatalf("message identifiers/metadata = %+v", email)
	}
	if email["from"] != input["FromEmailAddress"] || !reflect.DeepEqual(email["destination"], input["Destination"]) || !reflect.DeepEqual(email["reply_to"], input["ReplyToAddresses"]) || email["return_path"] != "feedback@example.com" {
		t.Fatalf("envelope = %+v", email)
	}
	if email["subject"] != "Hello" || email["text"] != "Text ✓" || email["html"] != "<p>Hello</p>" {
		t.Fatalf("message content = %+v", email)
	}
	attachments := email["attachments"].([]any)
	attachment := sesObject(attachments[0])
	if _, duplicated := attachment["RawContent"]; duplicated || attachment["content_bytes"] != int64(6) || attachment["raw_content_path"] != "Content.Simple.Attachments[0].RawContent" {
		t.Fatalf("attachment view = %+v", attachment)
	}
	if attachment["ContentId"] != "binary" || attachment["ContentDescription"] != "binary fixture" {
		t.Fatal("attachment metadata was omitted")
	}
}

func TestSESV2RawEnvelopeFallbackAndOverride(t *testing.T) {
	raw := "From: Sender <sender@example.com>\r\nTo: raw@example.com\r\nCc: copy@example.com\r\nBcc: blind@example.com\r\nSubject: Raw test\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\nHello\r\n"
	input := map[string]any{"Content": map[string]any{"Raw": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte(raw))}}}
	manager := sesV2TestManager()
	result, apiErr := manager.sendV2("SendEmail", input)
	if apiErr != nil || len(result.Emails) != 1 {
		t.Fatalf("raw send = %+v / %+v", result, apiErr)
	}
	email := result.Emails[0]
	if email["content_type"] != "raw" || email["subject"] != "Raw test" || email["request_content_path"] != "Content.Raw.Data" || len(sesStrings(sesObject(email["destination"])["BccAddresses"])) != 1 {
		t.Fatalf("raw metadata = %+v", email)
	}
	if strings.Contains(stringMustJSON(t, email), base64.StdEncoding.EncodeToString([]byte(raw))) {
		t.Fatal("raw MIME was duplicated into normalized email")
	}
	input["FromEmailAddress"] = "override@example.com"
	input["Destination"] = map[string]any{"ToAddresses": []any{"override-recipient@example.com"}}
	result, apiErr = manager.sendV2("SendEmail", input)
	if apiErr != nil || result.Emails[0]["from"] != "override@example.com" || !reflect.DeepEqual(result.Emails[0]["destination"], input["Destination"]) {
		t.Fatalf("raw override = %+v / %+v", result, apiErr)
	}
	_, apiErr = manager.sendV2("SendEmail", map[string]any{"Content": map[string]any{"Raw": map[string]any{"Data": "not base64"}}})
	sesV2RequireError(t, apiErr, "BadRequestException", http.StatusBadRequest)
}

func stringMustJSON(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func TestSESV2TemplateSourcesAndRendering(t *testing.T) {
	manager := sesV2TestManager()
	for name, template := range map[string]map[string]any{
		"name":         {"TemplateName": "welcome", "TemplateData": `{"name":"Ada"}`},
		"arn":          {"TemplateArn": "arn:aws:ses:us-east-1:123456789012:template/welcome", "TemplateData": `{"name":"Ada"}`},
		"name and arn": {"TemplateName": "welcome", "TemplateArn": "arn:aws:ses:us-east-1:123456789012:template/welcome", "TemplateData": `{"name":"Ada"}`},
		"inline":       {"TemplateContent": map[string]any{"Subject": "Hello {{name}}", "Text": "Welcome, {{name}}", "Html": "<p>{{name}}</p>"}, "TemplateData": `{"name":"Ada"}`},
	} {
		t.Run(name, func(t *testing.T) {
			input := map[string]any{"FromEmailAddress": "sender@example.com", "Destination": map[string]any{"ToAddresses": []any{"recipient@example.com"}}, "Content": map[string]any{"Template": template}}
			result, apiErr := manager.sendV2("SendEmail", input)
			if apiErr != nil || len(result.Emails) != 1 {
				t.Fatalf("template send = %+v / %+v", result, apiErr)
			}
			if email := result.Emails[0]; email["subject"] != "Hello Ada" || email["text"] != "Welcome, Ada" || email["html"] != "<p>Ada</p>" || email["content_type"] != "template" {
				t.Fatalf("rendered template = %+v", email)
			}
		})
	}
	input := map[string]any{"FromEmailAddress": "sender@example.com", "Destination": map[string]any{"ToAddresses": []any{"recipient@example.com"}}, "Content": map[string]any{"Template": map[string]any{"TemplateName": "missing"}}}
	_, apiErr := manager.sendV2("SendEmail", input)
	sesV2RequireError(t, apiErr, "NotFoundException", http.StatusNotFound)
	template := map[string]any{"TemplateName": "welcome", "TemplateData": "{}"}
	input["Content"] = map[string]any{"Template": template}
	result, apiErr := manager.sendV2("SendEmail", input)
	if apiErr != nil || len(result.Emails) != 1 || result.Emails[0]["capture_rendering_error"] == nil {
		t.Fatalf("missing render data should be accepted with optional renderer diagnostic: %+v / %+v", result, apiErr)
	}
	for _, invalid := range []any{"", "not JSON", "[]", "null", `{ "name": `} {
		template["TemplateData"] = invalid
		_, apiErr = manager.sendV2("SendEmail", input)
		sesV2RequireError(t, apiErr, "BadRequestException", http.StatusBadRequest)
	}
	template["TemplateData"] = "{}"
	template["TemplateContent"] = map[string]any{"Text": "body"}
	_, apiErr = manager.sendV2("SendEmail", input)
	sesV2RequireError(t, apiErr, "BadRequestException", http.StatusBadRequest)
	delete(template, "TemplateContent")
	template["TemplateArn"] = "arn:aws:ses:us-east-1:123456789012:template/other"
	_, apiErr = manager.sendV2("SendEmail", input)
	sesV2RequireError(t, apiErr, "BadRequestException", http.StatusBadRequest)
}

func TestSESV2BulkOrderedPartialResultsAndReplacements(t *testing.T) {
	input := sesV2TestInput(t, `{
	 "FromEmailAddress":"sender@example.com","ConfigurationSetName":"test",
	 "DefaultEmailTags":[{"Name":"kind","Value":"default"}],
	 "DefaultContent":{"Template":{"TemplateName":"welcome","TemplateData":"{\"name\":\"Default\"}","Headers":[{"Name":"X-Shared","Value":"default"},{"Name":"X-Keep","Value":"keep"}],"Attachments":[{"FileName":"info.txt","RawContent":"aW5mbw==","ContentTransferEncoding":"BASE64"}]}},
	 "BulkEmailEntries":[
	  {"Destination":{"ToAddresses":["first@example.com"]},"ReplacementEmailContent":{"ReplacementTemplate":{"ReplacementTemplateData":"{\"name\":\"First\"}"}},"ReplacementTags":[{"Name":"kind","Value":"replacement"}],"ReplacementHeaders":[{"Name":"x-shared","Value":"first"}]},
	  {"Destination":{"ToAddresses":["invalid address"]}},
	  {"Destination":{"ToAddresses":["third@example.com"]}},
	  {"Destination":{"ToAddresses":["fourth@example.com"]},"ReplacementEmailContent":{"ReplacementTemplate":{"ReplacementTemplateData":"[1]"}}}
	 ]
	}`)
	before := stringMustJSON(t, input)
	result, apiErr := sesV2TestManager().sendV2("SendBulkEmail", input)
	if apiErr != nil || len(result.Emails) != 2 {
		t.Fatalf("bulk send = %+v / %+v", result, apiErr)
	}
	if before != stringMustJSON(t, input) {
		t.Fatal("bulk sending mutated capture input")
	}
	entries := result.Output["BulkEmailEntryResults"].([]any)
	if len(entries) != 4 {
		t.Fatalf("bulk result count = %d", len(entries))
	}
	for index, expected := range []string{"SUCCESS", "INVALID_PARAMETER", "SUCCESS", "INVALID_PARAMETER"} {
		entry := sesObject(entries[index])
		if entry["Status"] != expected {
			t.Fatalf("result[%d] = %+v, want %s", index, entry, expected)
		}
		if expected != "SUCCESS" && (entry["Error"] == nil || entry["MessageId"] != nil) {
			t.Fatalf("failed entry must have Error and no MessageId: %+v", entry)
		}
	}
	if result.Emails[0]["subject"] != "Hello First" || result.Emails[1]["subject"] != "Hello Default" || result.Emails[0]["entry_index"] != 0 || result.Emails[1]["entry_index"] != 2 {
		t.Fatalf("replacement/default email views = %+v", result.Emails)
	}
	headers := result.Emails[0]["headers"].([]any)
	if len(headers) != 2 || sesObject(headers[0])["Name"] != "X-Keep" || sesObject(headers[1])["Value"] != "first" {
		t.Fatalf("replacement headers = %+v", headers)
	}
	if sesObject(result.Emails[0]["attachments"].([]any)[0])["raw_content_path"] != "DefaultContent.Template.Attachments[0].RawContent" {
		t.Fatal("bulk attachment must reference shared default request content")
	}
	if sesObject(entries[0])["MessageId"] != result.Emails[0]["message_id"] || sesObject(entries[2])["MessageId"] != result.Emails[1]["message_id"] || sesObject(entries[0])["MessageId"] == sesObject(entries[2])["MessageId"] {
		t.Fatal("bulk message identifiers don't align with ordered results")
	}
}

func TestSESV2BulkMissingResourcesAndBounds(t *testing.T) {
	manager := sesV2TestManager()
	input := sesV2TestInput(t, `{"FromEmailAddress":"sender@example.com","DefaultContent":{"Template":{"TemplateName":"missing","TemplateData":"{}"}},"BulkEmailEntries":[{"Destination":{"ToAddresses":["recipient@example.com"]}},{"Destination":{"ToAddresses":["other@example.com"]}}]}`)
	result, apiErr := manager.sendV2("SendBulkEmail", input)
	if apiErr != nil || len(result.Emails) != 0 {
		t.Fatalf("missing bulk template = %+v / %+v", result, apiErr)
	}
	for _, entry := range result.Output["BulkEmailEntryResults"].([]any) {
		if sesObject(entry)["Status"] != "TEMPLATE_NOT_FOUND" {
			t.Fatalf("missing template result = %+v", entry)
		}
	}
	sesObject(sesObject(input["DefaultContent"])["Template"])["TemplateName"] = "welcome"
	input["ConfigurationSetName"] = "missing"
	result, apiErr = manager.sendV2("SendBulkEmail", input)
	if apiErr != nil {
		t.Fatalf("missing bulk configuration set error = %+v", apiErr)
	}
	for _, entry := range result.Output["BulkEmailEntryResults"].([]any) {
		if sesObject(entry)["Status"] != "CONFIGURATION_SET_NOT_FOUND" {
			t.Fatalf("missing configuration result = %+v", entry)
		}
	}
	for _, entries := range []any{nil, "not an array", []any{}, make([]any, 51)} {
		input["BulkEmailEntries"] = entries
		_, apiErr := manager.sendV2("SendBulkEmail", input)
		sesV2RequireError(t, apiErr, "BadRequestException", http.StatusBadRequest)
	}
}

func TestSESV2CustomVerificationFixture(t *testing.T) {
	manager := sesV2TestManager()
	input := map[string]any{"EmailAddress": "recipient@example.com", "TemplateName": "verify", "ConfigurationSetName": "test"}
	result, apiErr := manager.sendV2("SendCustomVerificationEmail", input)
	if apiErr != nil || len(result.Emails) != 1 {
		t.Fatalf("custom verification = %+v / %+v", result, apiErr)
	}
	email := result.Emails[0]
	if email["message_id"] != result.Output["MessageId"] || email["content_type"] != "custom_verification" || email["from"] != "sender@example.com" || email["subject"] != "Verify your email" || email["html"] != "<p>Verify</p>" || email["success_redirection_url"] != "https://example.com/success" {
		t.Fatalf("custom verification view = %+v", email)
	}
	input["TemplateName"] = "missing"
	_, apiErr = manager.sendV2("SendCustomVerificationEmail", input)
	sesV2RequireError(t, apiErr, "NotFoundException", http.StatusNotFound)
	input["TemplateName"] = "verify"
	input["EmailAddress"] = "invalid"
	_, apiErr = manager.sendV2("SendCustomVerificationEmail", input)
	sesV2RequireError(t, apiErr, "BadRequestException", http.StatusBadRequest)
}

func TestSESV2Validation(t *testing.T) {
	for name, modify := range map[string]func(map[string]any){
		"missing content":   func(input map[string]any) { delete(input, "Content") },
		"ambiguous content": func(input map[string]any) { sesObject(input["Content"])["Raw"] = map[string]any{"Data": ""} },
		"missing subject":   func(input map[string]any) { delete(sesObject(sesObject(input["Content"])["Simple"]), "Subject") },
		"missing body": func(input map[string]any) {
			sesObject(sesObject(input["Content"])["Simple"])["Body"] = map[string]any{}
		},
		"bad destination type": func(input map[string]any) { input["Destination"] = "recipient@example.com" },
		"bad recipient array":  func(input map[string]any) { sesObject(input["Destination"])["ToAddresses"] = []any{12} },
		"missing recipients":   func(input map[string]any) { input["Destination"] = map[string]any{} },
		"over 50 recipients": func(input map[string]any) {
			sesObject(input["Destination"])["ToAddresses"] = strings.Split(strings.Repeat("a@example.com,", 50)+"a@example.com", ",")
		},
		"bad reply-to":          func(input map[string]any) { input["ReplyToAddresses"] = []any{"invalid"} },
		"bad identity arn type": func(input map[string]any) { input["FromEmailAddressIdentityArn"] = true },
		"bad tracking": func(input map[string]any) {
			input["ConfigurationOverrides"] = map[string]any{"Tracking": map[string]any{"ClickTrackingEnabled": true}}
		},
		"bad list options": func(input map[string]any) { input["ListManagementOptions"] = map[string]any{"TopicName": "topic"} },
		"bad email tag": func(input map[string]any) {
			input["EmailTags"] = []any{map[string]any{"Name": "invalid tag", "Value": "value"}}
		},
		"header injection": func(input map[string]any) {
			sesObject(sesObject(input["Content"])["Simple"])["Headers"] = []any{map[string]any{"Name": "X-Test", "Value": "value\r\nBcc: other@example.com"}}
		},
		"SES owned header": func(input map[string]any) {
			sesObject(sesObject(input["Content"])["Simple"])["Headers"] = []any{map[string]any{"Name": "sUbJeCt", "Value": "override"}}
		},
		"too many headers": func(input map[string]any) {
			sesObject(sesObject(input["Content"])["Simple"])["Headers"] = make([]any, 16)
		},
		"bad attachment base64": func(input map[string]any) {
			sesObject(sesObject(input["Content"])["Simple"])["Attachments"] = []any{map[string]any{"FileName": "file.txt", "RawContent": "!"}}
		},
		"bad attachment transfer encoding": func(input map[string]any) {
			sesObject(sesObject(input["Content"])["Simple"])["Attachments"] = []any{map[string]any{"FileName": "file.txt", "RawContent": "ZmlsZQ==", "ContentTransferEncoding": "binary"}}
		},
		"bad attachment MIME type": func(input map[string]any) {
			sesObject(sesObject(input["Content"])["Simple"])["Attachments"] = []any{map[string]any{"FileName": "file.txt", "RawContent": "ZmlsZQ==", "ContentType": "invalid"}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			input := sesV2TestInput(t, `{"FromEmailAddress":"sender@example.com","Destination":{"ToAddresses":["recipient@example.com"]},"Content":{"Simple":{"Subject":{"Data":"subject"},"Body":{"Text":{"Data":"body"}}}}}`)
			modify(input)
			result, apiErr := sesV2TestManager().sendV2("SendEmail", input)
			sesV2RequireError(t, apiErr, "BadRequestException", http.StatusBadRequest)
			if len(result.Emails) != 0 {
				t.Fatal("invalid message produced captured email IDs")
			}
		})
	}
}

func TestSESV2AttachmentLimitsUseMIMEEncoding(t *testing.T) {
	manager := sesV2TestManager()
	input := sesV2TestInput(t, `{"FromEmailAddress":"sender@example.com","Destination":{"ToAddresses":["recipient@example.com"]},"Content":{"Simple":{"Subject":{"Data":"subject"},"Body":{"Text":{"Data":"body"}}}}}`)
	message := sesObject(sesObject(input["Content"])["Simple"])
	attachment := map[string]any{"FileName": "file.bin", "RawContent": base64.StdEncoding.EncodeToString([]byte(strings.Repeat("a", 2<<20))), "ContentTransferEncoding": "BASE64"}
	message["Attachments"] = []any{attachment}
	if _, apiErr := manager.sendV2("SendEmail", input); apiErr != nil {
		t.Fatalf("2 MiB attachment must be accepted: %+v", apiErr)
	}
	attachment["FileName"] = "malware.EXE"
	_, apiErr := manager.sendV2("SendEmail", input)
	sesV2RequireError(t, apiErr, "MessageRejected", http.StatusBadRequest)
	attachment["FileName"] = "file.bin"
	// Decoded bytes are under 40 MiB, but MIME base64 expansion exceeds it.
	attachment["RawContent"] = base64.StdEncoding.EncodeToString([]byte(strings.Repeat("a", 31<<20)))
	_, apiErr = manager.sendV2("SendEmail", input)
	sesV2RequireError(t, apiErr, "MessageRejected", http.StatusBadRequest)
	// Seven-bit MIME stores these ASCII bytes directly, so the same attachment
	// is accepted rather than applying the JSON transport size to the quota.
	attachment["ContentTransferEncoding"] = "SEVEN_BIT"
	if _, apiErr := manager.sendV2("SendEmail", input); apiErr != nil {
		t.Fatalf("31 MiB seven-bit attachment must fit: %+v", apiErr)
	}
}

func TestSESV2UnicodeFieldLengthsCountCharacters(t *testing.T) {
	input := sesV2TestInput(t, `{"FromEmailAddress":"sender@example.com","Destination":{"ToAddresses":["recipient@example.com"]},"Content":{"Simple":{"Subject":{"Data":"subject"},"Body":{"Text":{"Data":"body"}}}}}`)
	simple := sesObject(sesObject(input["Content"])["Simple"])
	attachment := map[string]any{
		"FileName":           strings.Repeat("文", 251) + ".txt",
		"RawContent":         "eA==",
		"ContentDescription": strings.Repeat("文", 1000),
		"ContentId":          strings.Repeat("文", 78),
	}
	simple["Attachments"] = []any{attachment}
	if _, apiErr := sesV2TestManager().sendV2("SendEmail", input); apiErr != nil {
		t.Fatalf("valid Unicode character limits: %+v", apiErr)
	}
	attachment["FileName"] = strings.Repeat("文", 252) + ".txt"
	_, apiErr := sesV2TestManager().sendV2("SendEmail", input)
	sesV2RequireError(t, apiErr, "BadRequestException", http.StatusBadRequest)

	// JSON template data limits count characters, independently of UTF-8 bytes.
	sesObject(input["Content"])["Simple"] = nil
	data := `{"name":"` + strings.Repeat("文", 100000) + `"}`
	sesObject(input["Content"])["Template"] = map[string]any{
		"TemplateContent": map[string]any{"Subject": "subject", "Text": "body"},
		"TemplateData":    data,
	}
	if _, apiErr := sesV2TestManager().sendV2("SendEmail", input); apiErr != nil {
		t.Fatalf("Unicode template data below character limit: %+v", apiErr)
	}
}

func TestSESV2VerificationMIMEQuota(t *testing.T) {
	manager := sesV2TestManager()
	manager.fixtures.CustomVerificationTemplates["large"] = SESCustomVerificationTemplate{
		FromEmailAddress: "sender@example.com", TemplateContent: strings.Repeat("a", int(sesV2MaxMessageBytes)),
	}
	_, apiErr := manager.sendV2("SendCustomVerificationEmail", map[string]any{"EmailAddress": "to@example.com", "TemplateName": "large"})
	sesV2RequireError(t, apiErr, "MessageRejected", http.StatusBadRequest)
}
