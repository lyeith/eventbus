package messaging

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func mobileTestApplication(t *testing.T, broker *Broker, name string, extra map[string]string) string {
	t.Helper()
	attributes := map[string]string{"PlatformCredential": "local-credential"}
	for key, value := range extra {
		attributes[key] = value
	}
	arn, err := broker.mobileCreateApplication(name, "GCM", attributes)
	if err != nil {
		t.Fatal(err)
	}
	return arn
}

func mobileTestEndpoint(t *testing.T, broker *Broker, application, token string) string {
	t.Helper()
	arn, err := broker.mobileCreateEndpoint(application, token, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return arn
}

func mobileTestErrorCode(t *testing.T, err error, code string) {
	t.Helper()
	var sns *snsError
	if !errors.As(err, &sns) || sns.Code != code {
		t.Fatalf("want SNS %s, got %v", code, err)
	}
}

func mobileTestQuery(t *testing.T, handler *Handler, action string, values url.Values) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(values.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	if !handler.handleSNSMobileQuery(response, request, action) {
		t.Fatalf("mobile operation %s was not handled", action)
	}
	return response
}

func mobileTestElement(t *testing.T, body, name string) string {
	t.Helper()
	decoder := xml.NewDecoder(strings.NewReader(body))
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			return ""
		}
		if err != nil {
			t.Fatal(err)
		}
		if start, ok := token.(xml.StartElement); ok && start.Name.Local == name {
			var value string
			if err := decoder.DecodeElement(&value, &start); err != nil {
				t.Fatal(err)
			}
			return value
		}
	}
}

func TestSNSMobileQueryLifecycle(t *testing.T) {
	broker := NewBroker("", "", 4100)
	handler := NewHandler(broker)
	create := mobileTestQuery(t, handler, "CreatePlatformApplication", url.Values{
		"Name": {"agent_push"}, "Platform": {"GCM"},
		"Attributes.entry.1.key": {"PlatformCredential"}, "Attributes.entry.1.value": {"private-local-key"},
	})
	if create.Code != http.StatusOK {
		t.Fatal(create.Body.String())
	}
	application := mobileTestElement(t, create.Body.String(), "PlatformApplicationArn")
	if application != "arn:aws:sns:us-east-1:000000000000:app/GCM/agent_push" {
		t.Fatalf("application ARN %q", application)
	}
	getApplication := mobileTestQuery(t, handler, "GetPlatformApplicationAttributes", url.Values{"PlatformApplicationArn": {application}})
	if getApplication.Code != http.StatusOK || strings.Contains(getApplication.Body.String(), "private-local-key") || !strings.Contains(getApplication.Body.String(), "AuthenticationMethod") {
		t.Fatal(getApplication.Body.String())
	}
	setApplication := mobileTestQuery(t, handler, "SetPlatformApplicationAttributes", url.Values{
		"PlatformApplicationArn": {application}, "Attributes.entry.1.key": {"SuccessFeedbackSampleRate"}, "Attributes.entry.1.value": {"25"},
	})
	if setApplication.Code != http.StatusOK {
		t.Fatal(setApplication.Body.String())
	}
	listApplications := mobileTestQuery(t, handler, "ListPlatformApplications", nil)
	if listApplications.Code != http.StatusOK || !strings.Contains(listApplications.Body.String(), application) || strings.Contains(listApplications.Body.String(), "private-local-key") {
		t.Fatal(listApplications.Body.String())
	}
	createEndpoint := mobileTestQuery(t, handler, "CreatePlatformEndpoint", url.Values{"PlatformApplicationArn": {application}, "Token": {"device-one"}, "CustomUserData": {"agent & fixture"}})
	if createEndpoint.Code != http.StatusOK {
		t.Fatal(createEndpoint.Body.String())
	}
	endpoint := mobileTestElement(t, createEndpoint.Body.String(), "EndpointArn")
	if !strings.HasPrefix(endpoint, "arn:aws:sns:us-east-1:000000000000:endpoint/GCM/agent_push/") {
		t.Fatalf("endpoint ARN %q", endpoint)
	}
	getEndpoint := mobileTestQuery(t, handler, "GetEndpointAttributes", url.Values{"EndpointArn": {endpoint}})
	if getEndpoint.Code != http.StatusOK || !strings.Contains(getEndpoint.Body.String(), "agent &amp; fixture") || !strings.Contains(getEndpoint.Body.String(), "device-one") {
		t.Fatal(getEndpoint.Body.String())
	}
	setEndpoint := mobileTestQuery(t, handler, "SetEndpointAttributes", url.Values{"EndpointArn": {endpoint}, "Attributes.entry.1.key": {"Enabled"}, "Attributes.entry.1.value": {"false"}})
	if setEndpoint.Code != http.StatusOK {
		t.Fatal(setEndpoint.Body.String())
	}
	listEndpoints := mobileTestQuery(t, handler, "ListEndpointsByPlatformApplication", url.Values{"PlatformApplicationArn": {application}})
	if listEndpoints.Code != http.StatusOK || !strings.Contains(listEndpoints.Body.String(), endpoint) || !strings.Contains(listEndpoints.Body.String(), "false") {
		t.Fatal(listEndpoints.Body.String())
	}
	for index := 0; index < 2; index++ {
		deleted := mobileTestQuery(t, handler, "DeleteEndpoint", url.Values{"EndpointArn": {endpoint}})
		if deleted.Code != http.StatusOK {
			t.Fatal(deleted.Body.String())
		}
	}
	missing := mobileTestQuery(t, handler, "GetEndpointAttributes", url.Values{"EndpointArn": {endpoint}})
	if missing.Code != http.StatusNotFound || mobileTestElement(t, missing.Body.String(), "Code") != "NotFound" {
		t.Fatal(missing.Body.String())
	}
	secondEndpoint := mobileTestEndpoint(t, broker, application, "device-two")
	deleted := mobileTestQuery(t, handler, "DeletePlatformApplication", url.Values{"PlatformApplicationArn": {application}})
	if deleted.Code != http.StatusOK {
		t.Fatal(deleted.Body.String())
	}
	_, err := broker.mobileGetEndpointAttributes(secondEndpoint)
	mobileTestErrorCode(t, err, "NotFound")
}

func TestSNSMobileEndpointIdempotencyAndAtomicUpdates(t *testing.T) {
	broker := NewBroker("", "", 4100)
	application := mobileTestApplication(t, broker, "idempotent", nil)
	userdata := "fixture"
	endpoint, err := broker.mobileCreateEndpoint(application, "token", &userdata, nil)
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := broker.mobileCreateEndpoint(application, "token", &userdata, nil)
	if err != nil || duplicate != endpoint {
		t.Fatalf("duplicate endpoint %q, %v", duplicate, err)
	}
	different := "different"
	_, err = broker.mobileCreateEndpoint(application, "token", &different, nil)
	mobileTestErrorCode(t, err, "InvalidParameter")
	if err := broker.mobileSetEndpointAttributes(endpoint, map[string]string{"Enabled": "false"}); err != nil {
		t.Fatal(err)
	}
	duplicate, err = broker.mobileCreateEndpoint(application, "token", &userdata, nil)
	if err != nil || duplicate != endpoint {
		t.Fatalf("disabled duplicate %q, %v", duplicate, err)
	}
	attributes, err := broker.mobileGetEndpointAttributes(endpoint)
	if err != nil || attributes["Enabled"] != "false" {
		t.Fatalf("create re-enabled endpoint: %#v, %v", attributes, err)
	}
	_, err = broker.publishMobile(SNSPublishInput{TargetARN: endpoint, Message: "blocked"})
	mobileTestErrorCode(t, err, "EndpointDisabled")
	second := mobileTestEndpoint(t, broker, application, "other-token")
	err = broker.mobileSetEndpointAttributes(second, map[string]string{"Token": "token", "Enabled": "false"})
	mobileTestErrorCode(t, err, "InvalidParameter")
	attributes, _ = broker.mobileGetEndpointAttributes(second)
	if attributes["Token"] != "other-token" || attributes["Enabled"] != "true" {
		t.Fatalf("failed update partially mutated endpoint: %#v", attributes)
	}
	err = broker.mobileSetEndpointAttributes(endpoint, map[string]string{"Token": "fresh-token", "Enabled": "true", "unknown": "reject"})
	mobileTestErrorCode(t, err, "InvalidParameter")
	attributes, _ = broker.mobileGetEndpointAttributes(endpoint)
	if attributes["Token"] != "token" || attributes["Enabled"] != "false" {
		t.Fatalf("invalid attribute update partially mutated endpoint: %#v", attributes)
	}
}

func TestSNSMobileDirectPublishCapture(t *testing.T) {
	broker := NewBroker("", "", 4100)
	var buffer bytes.Buffer
	broker.SetSNSCapture(&SNSCapture{writer: &buffer})
	application := mobileTestApplication(t, broker, "capture", nil)
	endpoint := mobileTestEndpoint(t, broker, application, "token")
	message := `{"default":"fallback","GCM":"{\"notification\":{\"body\":\"hello\"}}"}`
	result, err := broker.publishMobile(SNSPublishInput{
		TargetARN: endpoint, Message: message, MessageStructure: "json", Subject: "fixture subject", RequestID: "request-one",
		Attributes: map[string]MessageAttribute{"binary": {DataType: "Binary", BinaryValue: []byte{0, 255, 10}}},
	})
	if err != nil || result.MessageID == "" {
		t.Fatalf("publish: %#v, %v", result, err)
	}
	var record SNSCaptureRecord
	if err := json.Unmarshal(bytes.TrimSpace(buffer.Bytes()), &record); err != nil {
		t.Fatal(err)
	}
	if record.Operation != "Publish" || record.RequestID != "request-one" || record.Message != message || record.Subject != "fixture subject" || record.MessageID != result.MessageID || record.TargetARN != endpoint {
		t.Fatalf("capture lost request: %#v", record)
	}
	if record.Details["resolved_message"] != `{"notification":{"body":"hello"}}` || len(record.Deliveries) != 1 || record.Deliveries[0].Status != "captured" {
		t.Fatalf("capture resolution: %#v", record)
	}
	if !bytes.Equal(record.MessageAttributes["binary"].BinaryValue, []byte{0, 255, 10}) {
		t.Fatalf("binary attribute changed: %#v", record.MessageAttributes)
	}
	buffer.Reset()
	if _, err := broker.publishMobile(SNSPublishInput{TargetARN: endpoint, Message: `{"default":"fallback","GCM":null}`, MessageStructure: "json"}); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(bytes.TrimSpace(buffer.Bytes()), &record); err != nil || record.Details["resolved_message"] != "fallback" {
		t.Fatalf("non-string platform key must fall back: %#v, %v", record, err)
	}
	broker.SetSNSCapture(&SNSCapture{writer: mobileFailWriter{}})
	_, err = broker.publishMobile(SNSPublishInput{TargetARN: endpoint, Message: "cannot capture"})
	mobileTestErrorCode(t, err, "InternalError")
	broker.SetSNSCapture(nil)
	if err := broker.mobileDeleteEndpoint(endpoint); err != nil {
		t.Fatal(err)
	}
	_, err = broker.publishMobile(SNSPublishInput{TargetARN: endpoint, Message: "gone"})
	mobileTestErrorCode(t, err, "NotFound")
}

type mobileFailWriter struct{}

func (mobileFailWriter) Write([]byte) (int, error) { return 0, errors.New("test capture failure") }

func TestSNSMobilePaginationScopesAndDeletion(t *testing.T) {
	broker := NewBroker("", "", 4100)
	for index := 0; index < 101; index++ {
		mobileTestApplication(t, broker, fmt.Sprintf("app_%03d", index), nil)
	}
	first, err := broker.mobileListApplicationsXML("")
	if err != nil || strings.Count(first, "<member>") != 100 {
		t.Fatalf("first app page: %s, %v", first, err)
	}
	next := mobileTestElement(t, first, "NextToken")
	if next == "" {
		t.Fatal("missing next token")
	}
	if err := broker.mobileDeleteApplication("arn:aws:sns:us-east-1:000000000000:app/GCM/app_000"); err != nil {
		t.Fatal(err)
	}
	last, err := broker.mobileListApplicationsXML(next)
	if err != nil || strings.Count(last, "<member>") != 1 || !strings.Contains(last, "app_100") || strings.Contains(last, "NextToken") {
		t.Fatalf("last app page: %s, %v", last, err)
	}
	application := mobileTestApplication(t, broker, "endpoints", nil)
	for index := 0; index < 101; index++ {
		mobileTestEndpoint(t, broker, application, fmt.Sprintf("device-%d", index))
	}
	first, err = broker.mobileListEndpointsXML(application, "")
	if err != nil || strings.Count(first, "<member>") != 100 {
		t.Fatalf("first endpoint page: %s, %v", first, err)
	}
	endpointCursor := mobileTestElement(t, first, "NextToken")
	last, err = broker.mobileListEndpointsXML(application, endpointCursor)
	if err != nil || strings.Count(last, "<member>") != 1 || strings.Contains(last, "NextToken") {
		t.Fatalf("last endpoint page: %s, %v", last, err)
	}
	_, err = broker.mobileListEndpointsXML(application, next)
	mobileTestErrorCode(t, err, "InvalidParameter")
	_, err = broker.mobileListApplicationsXML(endpointCursor)
	mobileTestErrorCode(t, err, "InvalidParameter")
	_, err = broker.mobileListApplicationsXML("garbage")
	mobileTestErrorCode(t, err, "InvalidParameter")
}

func TestSNSMobileValidation(t *testing.T) {
	broker := NewBroker("", "", 4100)
	for _, test := range []struct {
		name, platform string
		attributes     map[string]string
	}{
		{"space name", "GCM", map[string]string{"PlatformCredential": "key"}},
		{"valid", "FCM", map[string]string{"PlatformCredential": "key"}},
		{"valid", "GCM", nil},
		{"valid", "APNS", map[string]string{"PlatformCredential": "key"}},
		{"valid", "GCM", map[string]string{"PlatformCredential": "key", "SuccessFeedbackSampleRate": "101"}},
		{"valid", "GCM", map[string]string{"PlatformCredential": "key", "EventEndpointCreated": "not-an-arn"}},
	} {
		_, err := broker.mobileCreateApplication(test.name, test.platform, test.attributes)
		mobileTestErrorCode(t, err, "InvalidParameter")
	}
	application := mobileTestApplication(t, broker, "valid", nil)
	for _, attributes := range []map[string]string{
		{"Enabled": "yes"}, {"CustomUserData": strings.Repeat("é", 1024)}, {"CustomUserData": string([]byte{0xff})}, {"Token": ""}, {"UserId": "not-baidu"},
	} {
		_, err := broker.mobileCreateEndpoint(application, "token", nil, attributes)
		mobileTestErrorCode(t, err, "InvalidParameter")
	}
	for _, arn := range []string{"", "bad", "arn:aws:sns:us-west-2:000000000000:app/GCM/valid", "arn:aws:sns:us-east-1:000000000000:app/GCM/valid/extra"} {
		_, err := broker.mobileGetApplicationAttributes(arn)
		mobileTestErrorCode(t, err, "InvalidParameter")
	}
	baidu, err := broker.mobileCreateApplication("baidu", "BAIDU", map[string]string{"PlatformPrincipal": "key", "PlatformCredential": "secret"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = broker.mobileCreateEndpoint(baidu, "channel", nil, nil)
	mobileTestErrorCode(t, err, "InvalidParameter")
	endpoint, err := broker.mobileCreateEndpoint(baidu, "channel", nil, map[string]string{"ChannelId": "channel", "UserId": "user"})
	if err != nil {
		t.Fatal(err)
	}
	if err := broker.validateSNSApplicationEndpoint(endpoint); err != nil {
		t.Fatal(err)
	}
}

func TestSNSMobileEventCaptureFailureDoesNotMutateEndpoint(t *testing.T) {
	broker := NewBroker("", "", 4100)
	topic := broker.CreateTopic("mobile-events")
	application := mobileTestApplication(t, broker, "events", map[string]string{"EventEndpointCreated": topic.ARN, "EventEndpointUpdated": topic.ARN, "EventEndpointDeleted": topic.ARN})
	broker.SetSNSCapture(&SNSCapture{writer: mobileFailWriter{}})
	_, err := broker.mobileCreateEndpoint(application, "token", nil, nil)
	mobileTestErrorCode(t, err, "InternalError")
	state := broker.snsState()
	state.mu.Lock()
	count := len(state.mobile.endpoints)
	state.mu.Unlock()
	if count != 0 {
		t.Fatal("create committed before event capture")
	}
	broker.SetSNSCapture(nil)
	endpoint := mobileTestEndpoint(t, broker, application, "token")
	broker.SetSNSCapture(&SNSCapture{writer: mobileFailWriter{}})
	err = broker.mobileSetEndpointAttributes(endpoint, map[string]string{"Enabled": "false"})
	mobileTestErrorCode(t, err, "InternalError")
	attributes, _ := broker.mobileGetEndpointAttributes(endpoint)
	if attributes["Enabled"] != "true" {
		t.Fatal("set committed before event capture")
	}
	err = broker.mobileDeleteEndpoint(endpoint)
	mobileTestErrorCode(t, err, "InternalError")
	if _, err := broker.mobileGetEndpointAttributes(endpoint); err != nil {
		t.Fatal("delete committed before event capture")
	}
}

func TestSNSMobileApplicationEventsFanOutToSQS(t *testing.T) {
	broker := NewBroker("", "", 4100)
	var capture bytes.Buffer
	broker.SetSNSCapture(&SNSCapture{writer: &capture})
	topic := broker.CreateTopic("mobile-events")
	queue := broker.CreateQueue("mobile-events", 0, 0)
	if _, err := broker.Subscribe(topic.ARN, "sqs", queue.ARN, nil); err != nil {
		t.Fatal(err)
	}
	application := mobileTestApplication(t, broker, "fanout", map[string]string{"EventEndpointCreated": topic.ARN, "EventEndpointUpdated": topic.ARN, "EventEndpointDeleted": topic.ARN})
	endpoint := mobileTestEndpoint(t, broker, application, "token")
	if duplicate := mobileTestEndpoint(t, broker, application, "token"); duplicate != endpoint {
		t.Fatal("duplicate endpoint")
	}
	if err := broker.mobileSetEndpointAttributes(endpoint, map[string]string{"Enabled": "true"}); err != nil {
		t.Fatal(err)
	}
	if err := broker.mobileSetEndpointAttributes(endpoint, map[string]string{"Enabled": "false"}); err != nil {
		t.Fatal(err)
	}
	if err := broker.mobileDeleteEndpoint(endpoint); err != nil {
		t.Fatal(err)
	}
	messages := broker.ReceiveMessages(queue, 10, 0)
	if len(messages) != 3 {
		t.Fatalf("want create/update/delete events, got %d", len(messages))
	}
	for index, want := range []string{"EndpointCreated", "EndpointUpdated", "EndpointDeleted"} {
		var envelope struct{ Message string }
		if err := json.Unmarshal([]byte(messages[index].Body), &envelope); err != nil {
			t.Fatal(err)
		}
		var event map[string]string
		if err := json.Unmarshal([]byte(envelope.Message), &event); err != nil {
			t.Fatal(err)
		}
		if event["EventType"] != want || event["Type"] != want || event["Resource"] != application || event["EndpointArn"] != endpoint || event["Service"] != "SNS" || event["Time"] == "" {
			t.Fatalf("unexpected application event %#v", event)
		}
	}
	if strings.Contains(capture.String(), "local-credential") || strings.Contains(capture.String(), "PlatformCredential") {
		t.Fatal("provider credentials leaked into event capture")
	}
}

func TestSNSMobileEventFanoutFailureDoesNotUndoPrimaryState(t *testing.T) {
	broker := NewBroker("", "", 4100)
	var capture bytes.Buffer
	broker.SetSNSCapture(&SNSCapture{writer: &capture})
	topic := broker.CreateTopic("disappearing-events")
	application := mobileTestApplication(t, broker, "failedfanout", map[string]string{"EventEndpointCreated": topic.ARN})
	broker.DeleteTopic(topic.ARN)
	endpoint := mobileTestEndpoint(t, broker, application, "token")
	if _, err := broker.mobileGetEndpointAttributes(endpoint); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(capture.String(), `"status":"failed"`) || !strings.Contains(capture.String(), endpoint) {
		t.Fatalf("missing fanout failure evidence: %s", capture.String())
	}
}

func TestSNSMobileAPNSTokenNormalization(t *testing.T) {
	broker := NewBroker("", "", 4100)
	application, err := broker.mobileCreateApplication("ios", "APNS_SANDBOX", map[string]string{"PlatformPrincipal": "local-key-id", "PlatformCredential": "local-key", "ApplePlatformTeamID": "local-team", "ApplePlatformBundleID": "app.fixture"})
	if err != nil {
		t.Fatal(err)
	}
	upperToken := strings.Repeat("AB", 32)
	endpoint := mobileTestEndpoint(t, broker, application, upperToken)
	lowerToken := strings.ToLower(upperToken)
	duplicate := mobileTestEndpoint(t, broker, application, lowerToken)
	if duplicate != endpoint {
		t.Fatal("APNS token case created a duplicate endpoint")
	}
	attributes, err := broker.mobileGetEndpointAttributes(endpoint)
	if err != nil || attributes["Token"] != lowerToken {
		t.Fatalf("APNS token must be returned in lowercase: %#v, %v", attributes, err)
	}
	update := map[string]string{"Token": strings.Repeat("CD", 32)}
	if err := broker.mobileSetEndpointAttributes(endpoint, update); err != nil {
		t.Fatal(err)
	}
	attributes, _ = broker.mobileGetEndpointAttributes(endpoint)
	if attributes["Token"] != strings.Repeat("cd", 32) || update["Token"] != strings.Repeat("CD", 32) {
		t.Fatalf("APNS update normalization mutated caller: %#v, %#v", attributes, update)
	}
}
