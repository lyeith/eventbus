package main

import (
	"encoding/base64"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSESV1QueryDecodePreservesNestedSendingFields(t *testing.T) {
	form := url.Values{
		"Action": {"SendBulkTemplatedEmail"}, "Version": {"2010-12-01"}, "Source": {"Sender <sender@example.test>"},
		"SourceArn": {"arn:aws:ses:us-east-1:123456789012:identity/example.test"}, "Template": {"welcome"}, "TemplateArn": {"arn:aws:ses:us-east-1:123456789012:template/welcome"},
		"DefaultTemplateData": {`{"name":"fallback"}`}, "ReplyToAddresses.member.1": {"reply@example.test"},
		"DefaultTags.member.1.Name": {"campaign"}, "DefaultTags.member.1.Value": {"test"},
		"Destinations.member.2.Destination.ToAddresses.member.1": {"two@example.test"},
		"Destinations.member.2.ReplacementTemplateData":          {`{"name":"two"}`},
		"Destinations.member.1.Destination.ToAddresses.member.1": {"one@example.test"},
		"Destinations.member.1.Destination.CcAddresses.member.1": {"copy@example.test"},
		"Destinations.member.1.ReplacementTags.member.1.Name":    {"override"}, "Destinations.member.1.ReplacementTags.member.1.Value": {"yes"},
		"Opaque.ProviderExtension.member.1.Value": {"kept"},
	}
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	input, apiErr := sesV1Decode(r, "SendBulkTemplatedEmail")
	if apiErr != nil {
		t.Fatalf("decode failed: %+v", apiErr)
	}
	if input["SourceArn"] != form.Get("SourceArn") || input["TemplateArn"] != form.Get("TemplateArn") {
		t.Fatalf("authorization/template metadata dropped: %#v", input)
	}
	destinations := input["Destinations"].([]any)
	if len(destinations) != 2 || sesStrings(sesObject(sesObject(destinations[0])["Destination"])["ToAddresses"])[0] != "one@example.test" {
		t.Fatalf("Query member order lost: %#v", destinations)
	}
	if got := sesObject(sesObject(input["Opaque"])["ProviderExtension"].([]any)[0])["Value"]; got != "kept" {
		t.Fatalf("unknown metadata dropped: %#v", input)
	}
	if _, exists := input["Action"]; exists {
		t.Fatal("protocol Action included in sending request")
	}
}

func TestSESV1QueryDecodeRejectsAmbiguousStructures(t *testing.T) {
	for _, form := range []url.Values{
		{"Source": {"one@example.test", "two@example.test"}},
		{"Destination": {"scalar"}, "Destination.ToAddresses.member.1": {"one@example.test"}},
		{"Destinations.member.0.Source": {"bad"}},
		{"Tags.member.01.Name": {"bad"}},
	} {
		r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		if _, apiErr := sesV1Decode(r, "SendEmail"); apiErr == nil {
			t.Fatalf("accepted ambiguous Query structure: %v", form)
		}
	}
}

func TestSESV1XMLResponses(t *testing.T) {
	response := httptest.NewRecorder()
	sesV1Write(response, "SendBulkTemplatedEmail", "request-123", map[string]any{"Status": []any{
		map[string]any{"Status": "Success", "MessageId": "one"},
		map[string]any{"Status": "InvalidParameterValue", "Error": "Invalid <address> & recipient"},
	}}, nil)
	var decoded struct {
		XMLName xml.Name
		Result  struct {
			Statuses []struct{ Status, MessageId, Error string } `xml:"Status>member"`
		} `xml:"SendBulkTemplatedEmailResult"`
		Metadata struct{ RequestId string } `xml:"ResponseMetadata"`
	}
	if err := xml.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.XMLName.Space != sesV1Namespace || decoded.XMLName.Local != "SendBulkTemplatedEmailResponse" || decoded.Metadata.RequestId != "request-123" || len(decoded.Result.Statuses) != 2 || decoded.Result.Statuses[1].Error != "Invalid <address> & recipient" {
		t.Fatalf("SDK response structure incorrect: %s", response.Body.String())
	}
	failure := httptest.NewRecorder()
	sesV1Write(failure, "SendEmail", "failure-123", nil, &sesAPIError{Code: "MessageRejected", Message: "Rejected <message>", Status: 400})
	var failed struct {
		XMLName   xml.Name
		Error     struct{ Type, Code, Message string }
		RequestId string
	}
	if err := xml.Unmarshal(failure.Body.Bytes(), &failed); err != nil {
		t.Fatal(err)
	}
	if failure.Code != 400 || failed.XMLName.Space != sesV1Namespace || failed.Error.Type != "Sender" || failed.Error.Code != "MessageRejected" || failed.RequestId != "failure-123" {
		t.Fatalf("SDK error response incorrect: %s", failure.Body.String())
	}
}

func sesV1TestManager() *SESManager {
	return &SESManager{fixtures: SESFixtures{
		Templates:                   map[string]SESTemplate{"welcome": {SubjectPart: "Hello {{name}}", TextPart: "Welcome {{name}}", HtmlPart: "<p>{{name}}</p>"}},
		CustomVerificationTemplates: map[string]SESCustomVerificationTemplate{"verify": {FromEmailAddress: "sender@example.test", TemplateSubject: "Verify your address", TemplateContent: "<p>Verify</p>", SuccessRedirectionURL: "https://example.test/success", FailureRedirectionURL: "https://example.test/failure"}},
		ReceivedMessages:            map[string]SESReceivedMessage{"received-123": {From: "original@example.test", ReceivedAt: time.Now()}},
	}}
}

func sesV1TestDestination(address string) map[string]any {
	return map[string]any{"ToAddresses": []any{address}}
}

func TestSESV1AllSendingOperations(t *testing.T) {
	manager := sesV1TestManager()
	raw := base64.StdEncoding.EncodeToString([]byte("From: Sender <sender@example.test>\r\nTo: Recipient <to@example.test>\r\nSubject: raw subject\r\nMIME-Version: 1.0\r\nContent-Type: text/plain\r\n\r\nraw body\r\n"))
	cases := []struct {
		action      string
		input       map[string]any
		contentType string
	}{
		{"SendEmail", map[string]any{"Source": "sender@example.test", "Destination": sesV1TestDestination("to@example.test"), "Message": map[string]any{"Subject": map[string]any{"Data": "subject", "Charset": "UTF-8"}, "Body": map[string]any{"Text": map[string]any{"Data": "body"}, "Html": map[string]any{"Data": "<p>body</p>"}}}}, "simple"},
		{"SendRawEmail", map[string]any{"RawMessage": map[string]any{"Data": raw}}, "raw"},
		{"SendTemplatedEmail", map[string]any{"Source": "sender@example.test", "Destination": sesV1TestDestination("to@example.test"), "Template": "welcome", "TemplateData": `{"name":"World"}`}, "template"},
		{"SendBulkTemplatedEmail", map[string]any{"Source": "sender@example.test", "Template": "welcome", "DefaultTemplateData": `{"name":"World"}`, "Destinations": []any{map[string]any{"Destination": sesV1TestDestination("to@example.test")}}}, "template"},
		{"SendCustomVerificationEmail", map[string]any{"EmailAddress": "to@example.test", "TemplateName": "verify"}, "custom_verification"},
		{"SendBounce", map[string]any{"BounceSender": "sender@example.test", "OriginalMessageId": "received-123", "BouncedRecipientInfoList": []any{map[string]any{"Recipient": "to@example.test", "BounceType": "DoesNotExist"}}}, "bounce"},
	}
	for _, item := range cases {
		t.Run(item.action, func(t *testing.T) {
			result, apiErr := manager.sendV1(item.action, item.input)
			if apiErr != nil {
				t.Fatalf("valid send rejected: %+v", apiErr)
			}
			if len(result.Emails) != 1 || result.Emails[0]["content_type"] != item.contentType || sesString(result.Emails[0]["message_id"]) == "" {
				t.Fatalf("missing normalized capture: %#v", result)
			}
			if item.action != "SendBulkTemplatedEmail" && result.Output["MessageId"] != result.Emails[0]["message_id"] {
				t.Fatalf("response/capture message IDs differ: %#v", result)
			}
			if item.action == "SendRawEmail" {
				if sesObject(item.input["RawMessage"])["Data"] != raw || result.Emails[0]["request_content_path"] != "RawMessage.Data" {
					t.Fatal("raw MIME not preserved by reference")
				}
				if _, duplicated := result.Emails[0]["raw"]; duplicated {
					t.Fatal("raw MIME duplicated in normalized view")
				}
			}
		})
	}
}

func TestSESV1BulkOrderedPartialResults(t *testing.T) {
	manager := sesV1TestManager()
	input := map[string]any{"Source": "sender@example.test", "Template": "welcome", "DefaultTemplateData": `{"name":"fallback"}`, "Destinations": []any{
		map[string]any{"Destination": sesV1TestDestination("one@example.test"), "ReplacementTemplateData": `{"name":"one"}`},
		map[string]any{"Destination": sesV1TestDestination("invalid-address")},
		map[string]any{"Destination": sesV1TestDestination("three@example.test")},
	}}
	result, apiErr := manager.sendV1("SendBulkTemplatedEmail", input)
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	statuses := result.Output["Status"].([]any)
	if got := []any{sesObject(statuses[0])["Status"], sesObject(statuses[1])["Status"], sesObject(statuses[2])["Status"]}; !reflect.DeepEqual(got, []any{"Success", "InvalidParameterValue", "Success"}) {
		t.Fatalf("partial results out of order: %#v", statuses)
	}
	if result.Emails[0]["template_data"] != `{"name":"one"}` || result.Emails[2]["template_data"] != `{"name":"fallback"}` || len(result.Emails) != 3 {
		t.Fatalf("replacement/default data not preserved: %#v", result.Emails)
	}
}

func TestSESV1TemplateMissingAndCaptureRenderingFailure(t *testing.T) {
	manager := sesV1TestManager()
	input := map[string]any{"Source": "sender@example.test", "Destination": sesV1TestDestination("to@example.test"), "Template": "missing", "TemplateData": "{}"}
	if _, apiErr := manager.sendV1("SendTemplatedEmail", input); apiErr == nil || apiErr.Code != "TemplateDoesNotExist" {
		t.Fatalf("missing template error incorrect: %+v", apiErr)
	}
	input["Template"] = "welcome"
	input["TemplateData"] = "not JSON"
	if _, apiErr := manager.sendV1("SendTemplatedEmail", input); apiErr == nil || apiErr.Code != "InvalidParameterValue" {
		t.Fatalf("malformed template data accepted: %+v", apiErr)
	}
	input["TemplateData"] = "{}"
	result, apiErr := manager.sendV1("SendTemplatedEmail", input)
	if apiErr != nil || result.Output["MessageId"] == "" || result.Emails[0]["capture_rendering_error"] == nil {
		t.Fatalf("rendering failure incorrectly rejects accepted message: %#v %+v", result, apiErr)
	}
}

func TestSESV1BounceReceiptAndDSNValidation(t *testing.T) {
	manager := sesV1TestManager()
	input := map[string]any{"BounceSender": "sender@example.test", "OriginalMessageId": "unknown", "BouncedRecipientInfoList": []any{map[string]any{"Recipient": "to@example.test", "BounceType": "DoesNotExist"}}}
	if _, apiErr := manager.sendV1("SendBounce", input); apiErr == nil || apiErr.Code != "MessageRejected" {
		t.Fatalf("unknown received message accepted: %+v", apiErr)
	}
	manager.fixtures.ReceivedMessages["expired"] = SESReceivedMessage{From: "original@example.test", ReceivedAt: time.Now().Add(-25 * time.Hour)}
	input["OriginalMessageId"] = "expired"
	if _, apiErr := manager.sendV1("SendBounce", input); apiErr == nil || apiErr.Code != "MessageRejected" {
		t.Fatalf("expired received message accepted: %+v", apiErr)
	}
	manager.fixtures.ReceivedMessages["received-123"] = SESReceivedMessage{From: "original@example.test", ReceivedAt: time.Now(), Recipients: []string{"to@example.test"}}
	input["OriginalMessageId"] = "received-123"
	input["BouncedRecipientInfoList"] = []any{map[string]any{"Recipient": "other@example.test", "BounceType": "DoesNotExist"}}
	if _, apiErr := manager.sendV1("SendBounce", input); apiErr == nil || apiErr.Code != "MessageRejected" {
		t.Fatalf("recipient outside original message accepted: %+v", apiErr)
	}
	input["BouncedRecipientInfoList"] = []any{map[string]any{"Recipient": "to@example.test", "RecipientDsnFields": map[string]any{"Action": "failed", "Status": "5.1.1", "ExtensionFields": []any{map[string]any{"Name": "X-Reason", "Value": "gone"}}}}}
	if _, apiErr := manager.sendV1("SendBounce", input); apiErr != nil {
		t.Fatalf("valid DSN rejected: %+v", apiErr)
	}
	input["BouncedRecipientInfoList"] = []any{map[string]any{"Recipient": "to@example.test"}}
	if _, apiErr := manager.sendV1("SendBounce", input); apiErr == nil {
		t.Fatal("bounce without BounceType/DSN accepted")
	}
}

func TestSESRawValidation(t *testing.T) {
	valid := "From: sender@example.test\r\nTo: one@example.test\r\nCc: copy@example.test\r\nSubject: =?UTF-8?B?SGVsbG8=?=\r\nContent-Type: multipart/mixed; boundary=boundary\r\n\r\n--boundary\r\nContent-Type: text/plain\r\n\r\nbody\r\n--boundary\r\nContent-Type: application/octet-stream\r\nContent-Transfer-Encoding: base64\r\n\r\nAAEC\r\n--boundary--\r\n"
	metadata, apiErr := sesValidateRaw("v1", base64.StdEncoding.EncodeToString([]byte(valid)), sesV1MaxMessageSize)
	if apiErr != nil || metadata["subject"] != "Hello" || len(sesStrings(sesObject(metadata["destination"])["CcAddresses"])) != 1 {
		t.Fatalf("valid multipart MIME rejected: %#v %+v", metadata, apiErr)
	}
	for name, value := range map[string]string{
		"missing From":      "To: to@example.test\r\n\r\nbody",
		"missing separator": "From: sender@example.test\r\nTo: to@example.test\r\nbody",
		"invalid recipient": "From: sender@example.test\r\nTo: invalid\r\n\r\nbody",
		"long line":         "From: sender@example.test\r\nTo: to@example.test\r\n\r\n" + strings.Repeat("a", 999) + "\r\n",
		"broken multipart":  "From: sender@example.test\r\nTo: to@example.test\r\nContent-Type: multipart/mixed; boundary=missing\r\n\r\nbody",
	} {
		t.Run(name, func(t *testing.T) {
			if _, apiErr := sesValidateRaw("v1", base64.StdEncoding.EncodeToString([]byte(value)), sesV1MaxMessageSize); apiErr == nil {
				t.Fatal("invalid raw MIME accepted")
			}
		})
	}
	blocked := strings.Replace(valid, "Content-Type: application/octet-stream\r\n", "Content-Type: application/octet-stream; name=program.EXE\r\n", 1)
	if _, apiErr := sesValidateRaw("v1", base64.StdEncoding.EncodeToString([]byte(blocked)), sesV1MaxMessageSize); apiErr == nil || apiErr.Code != "MessageRejected" {
		t.Fatalf("unsupported raw MIME attachment accepted: %+v", apiErr)
	}
	invalidEncoding := strings.Replace(valid, "AAEC\r\n", "invalid!\r\n", 1)
	if _, apiErr := sesValidateRaw("v1", base64.StdEncoding.EncodeToString([]byte(invalidEncoding)), sesV1MaxMessageSize); apiErr == nil {
		t.Fatal("malformed MIME part base64 accepted")
	}
	if _, apiErr := sesValidateRaw("v1", "not base64!", sesV1MaxMessageSize); apiErr == nil {
		t.Fatal("invalid base64 accepted")
	}
	if _, apiErr := sesValidateRaw("v2", base64.StdEncoding.EncodeToString([]byte(valid)), 32); apiErr == nil {
		t.Fatal("message size limit ignored")
	}
}

func TestSESV1QueryEmptyListSerialization(t *testing.T) {
	form := url.Values{"Action": {"SendEmail"}, "Source": {"sender@example.test"}, "Tags": {""}, "ReplyToAddresses": {""}, "Destination.ToAddresses.member.1": {"to@example.test"}, "Destination.CcAddresses": {""}, "Destination.BccAddresses": {""}, "Message.Subject.Data": {""}, "Message.Body.Text.Data": {""}, "Opaque.Unknown": {""}}
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	input, apiErr := sesV1Decode(r, "SendEmail")
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	for field, value := range map[string]any{"Tags": input["Tags"], "ReplyToAddresses": input["ReplyToAddresses"], "CcAddresses": sesObject(input["Destination"])["CcAddresses"], "BccAddresses": sesObject(input["Destination"])["BccAddresses"]} {
		if list, ok := value.([]any); !ok || len(list) != 0 {
			t.Fatalf("%s empty list decoded incorrectly: %#v", field, value)
		}
	}
	if sesObject(sesObject(input["Message"])["Subject"])["Data"] != "" || sesObject(sesObject(sesObject(input["Message"])["Body"])["Text"])["Data"] != "" || sesObject(input["Opaque"])["Unknown"] != "" {
		t.Fatal("empty string data changed to list")
	}
	if _, apiErr := sesV1TestManager().sendV1("SendEmail", input); apiErr != nil {
		t.Fatalf("valid empty-list SDK request rejected: %+v", apiErr)
	}
	for action, input := range map[string]map[string]any{
		"SendBulkTemplatedEmail": {"Destinations": []any{map[string]any{"ReplacementTags": "", "Destination": map[string]any{"CcAddresses": ""}}}, "DefaultTags": ""},
		"SendRawEmail":           {"Destinations": "", "Tags": ""},
		"SendBounce":             {"BouncedRecipientInfoList": []any{map[string]any{"RecipientDsnFields": map[string]any{"ExtensionFields": ""}}}, "MessageDsn": map[string]any{"ExtensionFields": ""}},
	} {
		sesV1NormalizeQueryLists(action, input)
		if apiErr := sesV1ValidateInput(action, input); apiErr != nil {
			t.Fatalf("%s normalized nested empty lists failed shape validation: %+v", action, apiErr)
		}
	}
}

func TestSESV1KnownMalformedQueryShapes(t *testing.T) {
	for _, item := range []struct {
		action string
		input  map[string]any
	}{
		{"SendEmail", map[string]any{"ReplyToAddresses": "reply@example.test"}},
		{"SendRawEmail", map[string]any{"Destinations": "to@example.test"}},
		{"SendRawEmail", map[string]any{"Source": map[string]any{"Bad": "sender@example.test"}}},
		{"SendEmail", map[string]any{"Message": map[string]any{"Body": "body"}}},
		{"SendEmail", map[string]any{"Message": map[string]any{"Subject": map[string]any{"Data": []any{"subject"}}}}},
		{"SendBounce", map[string]any{"BouncedRecipientInfoList": "to@example.test"}},
		{"SendBounce", map[string]any{"MessageDsn": map[string]any{"ReportingMta": []any{"dns; example.test"}}}},
		{"SendBounce", map[string]any{"MessageDsn": map[string]any{"ExtensionFields": []any{map[string]any{"Name": "X-Reason", "Value": "bad\nvalue"}}}}},
	} {
		if apiErr := sesV1ValidateInput(item.action, item.input); apiErr == nil || apiErr.Code != "InvalidParameterValue" {
			t.Fatalf("malformed %s shape accepted: %#v %+v", item.action, item.input, apiErr)
		}
	}
}

func TestSESV1EncodedMIMEMessageQuota(t *testing.T) {
	manager := sesV1TestManager()
	large := strings.Repeat("a", int(sesV1MaxMessageSize))
	destination := sesV1TestDestination("to@example.test")
	if _, apiErr := manager.sendV1("SendEmail", map[string]any{"Source": "sender@example.test", "Destination": destination, "Message": map[string]any{"Subject": map[string]any{"Data": ""}, "Body": map[string]any{"Text": map[string]any{"Data": large}}}}); apiErr == nil || apiErr.Code != "MessageRejected" {
		t.Fatalf("10 MiB body plus MIME overhead accepted: %+v", apiErr)
	}
	manager.fixtures.Templates["large"] = SESTemplate{SubjectPart: "large", TextPart: large}
	if _, apiErr := manager.sendV1("SendTemplatedEmail", map[string]any{"Source": "sender@example.test", "Destination": destination, "Template": "large", "TemplateData": "{}"}); apiErr == nil || apiErr.Code != "MessageRejected" {
		t.Fatalf("oversized rendered template accepted: %+v", apiErr)
	}
	result, apiErr := manager.sendV1("SendBulkTemplatedEmail", map[string]any{"Source": "sender@example.test", "Template": "large", "DefaultTemplateData": "{}", "Destinations": []any{map[string]any{"Destination": destination}}})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	status := sesObject(result.Output["Status"].([]any)[0])
	if status["Status"] != "MessageRejected" || status["MessageId"] != nil {
		t.Fatalf("oversized bulk result incorrect: %#v", status)
	}
}

func TestSESV1VerificationAndBounceMIMEQuota(t *testing.T) {
	manager := sesV1TestManager()
	manager.fixtures.CustomVerificationTemplates["large"] = SESCustomVerificationTemplate{
		FromEmailAddress: "sender@example.test", TemplateContent: strings.Repeat("a", int(sesV1MaxMessageSize)),
	}
	if _, apiErr := manager.sendV1("SendCustomVerificationEmail", map[string]any{"EmailAddress": "to@example.test", "TemplateName": "large"}); apiErr == nil || apiErr.Code != "MessageRejected" {
		t.Fatalf("oversized verification accepted: %+v", apiErr)
	}
	input := map[string]any{
		"BounceSender": "sender@example.test", "OriginalMessageId": "received-123",
		"BouncedRecipientInfoList": []any{map[string]any{
			"Recipient": "to@example.test", "RecipientDsnFields": map[string]any{
				"Action": "failed", "Status": "5.1.1", "DiagnosticCode": strings.Repeat("a", int(sesV1MaxMessageSize)),
			},
		}},
	}
	if _, apiErr := manager.sendV1("SendBounce", input); apiErr == nil || apiErr.Code != "MessageRejected" {
		t.Fatalf("oversized bounce DSN accepted: %+v", apiErr)
	}
}

func TestSESV1QueryCaptureExcludesTransportCredentials(t *testing.T) {
	form := url.Values{"Source": {"sender@example.test"}, "X-Amz-Credential": {"fake-access-key"}, "X-Amz-Security-Token": {"fake-session-token"}}
	r := httptest.NewRequest(http.MethodPost, "/?X-Amz-Signature=fake-signature&x-amz-algorithm=AWS4-HMAC-SHA256", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	input, apiErr := sesV1Decode(r, "SendEmail")
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	if len(input) != 1 || input["Source"] != "sender@example.test" {
		t.Fatalf("transport credentials reached captured request: %#v", input)
	}
}
