package ses

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const eventTestTopic = "arn:aws:sns:us-east-1:000000000000:tracking"

type recordedSESEvent struct {
	topic, requestID string
	payload          map[string]any
}
type recordingSESPublisher struct {
	mu          sync.Mutex
	events      []recordedSESEvent
	failPublish bool
	validate    func(string) error
}

func (publisher *recordingSESPublisher) ValidateTopic(topic string) error {
	if publisher.validate != nil {
		return publisher.validate(topic)
	}
	if topic != eventTestTopic {
		return errors.New("topic does not exist")
	}
	return nil
}
func (publisher *recordingSESPublisher) PublishEvent(_ context.Context, topic, message, requestID string) error {
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	if publisher.failPublish {
		return errors.New("native SNS admission unavailable")
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(message), &payload); err != nil {
		return err
	}
	publisher.events = append(publisher.events, recordedSESEvent{topic: topic, requestID: requestID, payload: payload})
	return nil
}
func (publisher *recordingSESPublisher) snapshot() []recordedSESEvent {
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	return append([]recordedSESEvent(nil), publisher.events...)
}
func newEventManager(capture *SESCapture, publisher *recordingSESPublisher) *SESManager {
	return NewSESManager(SESFixtures{ConfigurationSets: []string{"tracking", "other"}}, capture,
		WithEventPublisher(publisher, "us-east-1", "000000000000"))
}
func configureEvent(t *testing.T, manager *SESManager, action, name string, enabled any, eventTypes ...string) {
	t.Helper()
	_, apiErr := manager.configurationV1(action, map[string]any{"ConfigurationSetName": "tracking", "EventDestination": map[string]any{
		"Name": name, "Enabled": enabled, "MatchingEventTypes": eventTypes, "SNSDestination": map[string]any{"TopicARN": eventTestTopic}}})
	require.Nil(t, apiErr)
}
func eventQuery(handler *Handler, action string, form url.Values) *httptest.ResponseRecorder {
	form.Set("Action", action)
	form.Set("Version", "2010-12-01")
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	handler.ServeQuery(response, request, action, nil)
	return response
}
func eventSimpleForm(configuration string) url.Values {
	form := url.Values{"Source": {"Sender <sender@example.test>"}, "Destination.ToAddresses.member.1": {"recipient@example.test"},
		"Destination.CcAddresses.member.1": {"copy@example.test"}, "Message.Subject.Data": {"tracking subject"}, "Message.Body.Text.Data": {"body"}}
	if configuration != "" {
		form.Set("ConfigurationSetName", configuration)
	}
	return form
}
func eventMessageID(t *testing.T, response *httptest.ResponseRecorder) string {
	t.Helper()
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var wire struct {
		Result struct {
			ID string `xml:"MessageId"`
		} `xml:",any"`
	}
	require.NoError(t, xml.Unmarshal(response.Body.Bytes(), &wire))
	require.NotEmpty(t, wire.Result.ID)
	return wire.Result.ID
}
func eventOutcome(handler *Handler, input any) *httptest.ResponseRecorder {
	data, _ := json.Marshal(input)
	request := httptest.NewRequest(http.MethodPost, DevOutcomePath, bytes.NewReader(data))
	response := httptest.NewRecorder()
	handler.ServeDevOutcome(response, request)
	return response
}

func TestSESConfigurationNativeCRUDAndErrors(t *testing.T) {
	publisher := &recordingSESPublisher{}
	// Management remains available when sending is paused or its capture fails.
	disabled := false
	manager := NewSESManager(SESFixtures{SendingEnabled: &disabled}, NewSESCapture(&sesShortWriter{}),
		WithEventPublisher(publisher, "us-east-1", "000000000000"))
	handler := NewHandler(manager)
	create := eventQuery(handler, "CreateConfigurationSet", url.Values{"ConfigurationSet.Name": {"sdk-set"}})
	require.Equal(t, http.StatusOK, create.Code, create.Body.String())
	require.True(t, handler.MatchQuery(httptest.NewRequest(http.MethodPost, "/", nil), "CreateConfigurationSet"))
	duplicate := eventQuery(handler, "CreateConfigurationSet", url.Values{"ConfigurationSet.Name": {"sdk-set"}})
	require.Equal(t, http.StatusBadRequest, duplicate.Code)
	require.Contains(t, duplicate.Body.String(), "<Code>ConfigurationSetAlreadyExists</Code>")
	require.Contains(t, duplicate.Body.String(), "<ConfigurationSetName>sdk-set</ConfigurationSetName>")
	invalid := eventQuery(handler, "CreateConfigurationSet", url.Values{"ConfigurationSet.Name": {"invalid name"}})
	require.Contains(t, invalid.Body.String(), "<Code>InvalidConfigurationSet</Code>")
	form := url.Values{"ConfigurationSetName": {"sdk-set"}, "EventDestination.Name": {"native"},
		"EventDestination.MatchingEventTypes.member.1": {"send"}, "EventDestination.MatchingEventTypes.member.2": {"open"},
		"EventDestination.SNSDestination.TopicARN": {eventTestTopic}}
	response := eventQuery(handler, "CreateConfigurationSetEventDestination", form)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	response = eventQuery(handler, "CreateConfigurationSetEventDestination", form)
	require.Contains(t, response.Body.String(), "<Code>EventDestinationAlreadyExists</Code>")
	response = eventQuery(handler, "DescribeConfigurationSet", url.Values{"ConfigurationSetName": {"sdk-set"}, "ConfigurationSetAttributeNames.member.1": {"eventDestinations"}})
	require.Equal(t, http.StatusOK, response.Code)
	require.Contains(t, response.Body.String(), "<Enabled>false</Enabled>")
	require.Contains(t, response.Body.String(), "<MatchingEventTypes><member>send</member><member>open</member></MatchingEventTypes>")
	require.Contains(t, response.Body.String(), "<TopicARN>"+eventTestTopic+"</TopicARN>")
	response = eventQuery(handler, "DescribeConfigurationSet", url.Values{"ConfigurationSetName": {"sdk-set"}})
	require.NotContains(t, response.Body.String(), "EventDestinations")
	form.Set("EventDestination.Enabled", "true")
	response = eventQuery(handler, "UpdateConfigurationSetEventDestination", form)
	require.Equal(t, http.StatusOK, response.Code)
	response = eventQuery(handler, "DeleteConfigurationSetEventDestination", url.Values{"ConfigurationSetName": {"sdk-set"}, "EventDestinationName": {"native"}})
	require.Equal(t, http.StatusOK, response.Code)
	response = eventQuery(handler, "DeleteConfigurationSetEventDestination", url.Values{"ConfigurationSetName": {"sdk-set"}, "EventDestinationName": {"native"}})
	require.Contains(t, response.Body.String(), "<Code>EventDestinationDoesNotExist</Code>")
	response = eventQuery(handler, "UpdateConfigurationSetEventDestination", form)
	require.Contains(t, response.Body.String(), "<Code>EventDestinationDoesNotExist</Code>")
	response = eventQuery(handler, "DeleteConfigurationSet", url.Values{"ConfigurationSetName": {"sdk-set"}})
	require.Equal(t, http.StatusOK, response.Code)
	response = eventQuery(handler, "DescribeConfigurationSet", url.Values{"ConfigurationSetName": {"sdk-set"}})
	require.Contains(t, response.Body.String(), "<Code>ConfigurationSetDoesNotExist</Code>")
}

func TestSESEventDestinationValidationAndOwnedCopies(t *testing.T) {
	manager := newEventManager(NewSESCapture(io.Discard), &recordingSESPublisher{})
	cases := []struct {
		name   string
		mutate func(map[string]any)
		code   string
	}{
		{"empty types", func(d map[string]any) { d["MatchingEventTypes"] = []string{} }, "InvalidParameterValue"},
		{"capitalized types", func(d map[string]any) { d["MatchingEventTypes"] = []string{"Send"} }, "InvalidParameterValue"},
		{"bad enabled", func(d map[string]any) { d["Enabled"] = "enabled" }, "InvalidParameterValue"},
		{"two destinations", func(d map[string]any) { d["CloudWatchDestination"] = map[string]any{} }, "InvalidParameterValue"},
		{"unavailable topic", func(d map[string]any) { d["SNSDestination"] = map[string]any{"TopicARN": eventTestTopic + "-absent"} }, "InvalidSNSDestination"},
		{"FIFO", func(d map[string]any) { d["SNSDestination"] = map[string]any{"TopicARN": eventTestTopic + ".fifo"} }, "InvalidSNSDestination"},
		{"wrong account", func(d map[string]any) {
			d["SNSDestination"] = map[string]any{"TopicARN": strings.Replace(eventTestTopic, "000000000000", "123456789012", 1)}
		}, "InvalidSNSDestination"},
		{"wrong region", func(d map[string]any) {
			d["SNSDestination"] = map[string]any{"TopicARN": strings.Replace(eventTestTopic, "us-east-1", "eu-west-1", 1)}
		}, "InvalidSNSDestination"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			destination := map[string]any{"Name": "destination", "Enabled": "true", "MatchingEventTypes": []string{"send"}, "SNSDestination": map[string]any{"TopicARN": eventTestTopic}}
			test.mutate(destination)
			_, apiErr := manager.configurationV1("CreateConfigurationSetEventDestination", map[string]any{"ConfigurationSetName": "tracking", "EventDestination": destination})
			require.NotNil(t, apiErr)
			require.Equal(t, test.code, apiErr.Code)
		})
	}
	types := []string{"send"}
	destination := map[string]any{"Name": "owned", "Enabled": true, "MatchingEventTypes": types, "SNSDestination": map[string]any{"TopicARN": eventTestTopic}}
	_, apiErr := manager.configurationV1("CreateConfigurationSetEventDestination", map[string]any{"ConfigurationSetName": "tracking", "EventDestination": destination})
	require.Nil(t, apiErr)
	types[0] = "bounce"
	output, apiErr := manager.configurationV1("DescribeConfigurationSet", map[string]any{"ConfigurationSetName": "tracking", "ConfigurationSetAttributeNames": []string{"eventDestinations"}})
	require.Nil(t, apiErr)
	returned := output["EventDestinations"].([]map[string]any)[0]
	require.Equal(t, []any{"send"}, returned["MatchingEventTypes"])
	returned["MatchingEventTypes"].([]any)[0] = "open"
	output, apiErr = manager.configurationV1("DescribeConfigurationSet", map[string]any{"ConfigurationSetName": "tracking", "ConfigurationSetAttributeNames": []string{"eventDestinations"}})
	require.Nil(t, apiErr)
	require.Equal(t, []any{"send"}, output["EventDestinations"].([]map[string]any)[0]["MatchingEventTypes"])
}

func TestSESSendEventAcceptanceSelectionAndOriginalCapture(t *testing.T) {
	cases := []struct{ name, apiSet, header, effective string }{
		{"simple", "tracking", "", "tracking"}, {"MIME", "", "tracking", "tracking"},
		{"API over MIME", "tracking", "other", "tracking"}, {"unrelated set", "other", "tracking", "other"},
		{"absent set", "", "", ""},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var captured bytes.Buffer
			publisher := &recordingSESPublisher{}
			manager := newEventManager(NewSESCapture(&captured), publisher)
			configureEvent(t, manager, "CreateConfigurationSetEventDestination", "send", true, "send")
			handler := NewHandler(manager)
			action, form := "SendEmail", eventSimpleForm(test.apiSet)
			if test.header != "" {
				action = "SendRawEmail"
				raw := []byte("From: Sender <sender@example.test>\r\nTo: recipient@example.test\r\nSubject: original\r\nx-sEs-CoNfIgUrAtIoN-SeT: " + test.header + "\r\n\r\nbody")
				form = url.Values{"RawMessage.Data": {base64.StdEncoding.EncodeToString(raw)}}
				if test.apiSet != "" {
					form.Set("ConfigurationSetName", test.apiSet)
				}
			}
			response := eventQuery(handler, action, form)
			id := eventMessageID(t, response)
			var record map[string]any
			require.NoError(t, json.Unmarshal(bytes.TrimSpace(captured.Bytes()), &record))
			require.EqualValues(t, 1, record["schema_version"])
			if test.header != "" {
				require.Equal(t, form.Get("RawMessage.Data"), sesObject(sesObject(record["request"])["RawMessage"])["Data"])
			}
			events := publisher.snapshot()
			if test.effective != "tracking" {
				require.Empty(t, events)
				return
			}
			require.Len(t, events, 1)
			event := events[0]
			require.Equal(t, eventTestTopic, event.topic)
			require.Equal(t, response.Header().Get("x-amzn-requestid"), event.requestID)
			require.Equal(t, "Send", event.payload["eventType"])
			require.Equal(t, id, sesObject(event.payload["mail"])["messageId"])
			require.Equal(t, "sender@example.test", sesObject(event.payload["mail"])["source"])
			require.Equal(t, []any{"tracking"}, sesObject(sesObject(event.payload["mail"])["tags"])["ses:configuration-set"])
			require.Equal(t, map[string]any{}, event.payload["send"])
			require.Equal(t, id, sesObject(sesObject(record["outcome"])["response"])["MessageId"])
		})
	}
	publisher := &recordingSESPublisher{}
	manager := newEventManager(NewSESCapture(&sesShortWriter{}), publisher)
	configureEvent(t, manager, "CreateConfigurationSetEventDestination", "send", true, "send")
	response := eventQuery(NewHandler(manager), "SendEmail", eventSimpleForm("tracking"))
	require.Equal(t, http.StatusInternalServerError, response.Code)
	require.Empty(t, publisher.snapshot())
	require.Empty(t, manager.accepted)
}

func TestSESExplicitOutcomesCurrentFilteringAndNoRecreatedSetInheritance(t *testing.T) {
	publisher := &recordingSESPublisher{}
	manager := newEventManager(NewSESCapture(io.Discard), publisher)
	configureEvent(t, manager, "CreateConfigurationSetEventDestination", "tracking", false, "send", "open", "bounce")
	handler := NewHandler(manager)
	id := eventMessageID(t, eventQuery(handler, "SendEmail", eventSimpleForm("tracking")))
	require.Empty(t, publisher.snapshot())
	outcome := map[string]any{"message_id": id, "event_type": "Open"}
	require.Equal(t, http.StatusOK, eventOutcome(handler, outcome).Code)
	require.Empty(t, publisher.snapshot())
	configureEvent(t, manager, "UpdateConfigurationSetEventDestination", "tracking", true, "bounce")
	require.Equal(t, http.StatusOK, eventOutcome(handler, outcome).Code)
	require.Empty(t, publisher.snapshot())
	outcome["event_type"] = "Bounce"
	outcome["recipients"] = []string{"foreign@example.test"}
	require.Equal(t, http.StatusBadRequest, eventOutcome(handler, outcome).Code)
	outcome["recipients"] = []string{"copy@example.test"}
	require.Equal(t, http.StatusOK, eventOutcome(handler, outcome).Code)
	events := publisher.snapshot()
	require.Len(t, events, 1)
	require.Equal(t, "Bounce", events[0].payload["eventType"])
	require.Equal(t, id, sesObject(events[0].payload["mail"])["messageId"])
	require.Equal(t, []any{map[string]any{"emailAddress": "copy@example.test"}}, sesObject(events[0].payload["bounce"])["bouncedRecipients"])
	configureEvent(t, manager, "UpdateConfigurationSetEventDestination", "tracking", true, "open")
	require.Equal(t, http.StatusOK, eventOutcome(handler, map[string]any{"message_id": id, "event_type": "Open"}).Code)
	require.Len(t, publisher.snapshot(), 2)
	_, apiErr := manager.configurationV1("DeleteConfigurationSet", map[string]any{"ConfigurationSetName": "tracking"})
	require.Nil(t, apiErr)
	_, apiErr = manager.configurationV1("CreateConfigurationSet", map[string]any{"ConfigurationSet": map[string]any{"Name": "tracking"}})
	require.Nil(t, apiErr)
	configureEvent(t, manager, "CreateConfigurationSetEventDestination", "tracking", true, "open", "bounce")
	require.Equal(t, http.StatusOK, eventOutcome(handler, map[string]any{"message_id": id, "event_type": "Open"}).Code)
	require.Len(t, publisher.snapshot(), 2, "same-name replacement cannot route an old message")
	manager.mu.Lock()
	manager.accepted[id].retainedAt = time.Now().Add(-acceptedMessageTTL)
	manager.mu.Unlock()
	require.Equal(t, http.StatusNotFound, eventOutcome(handler, map[string]any{"message_id": id, "event_type": "Open"}).Code)
}

func TestSESNativeSendRemainsAcceptedWhenNotificationAdmissionFails(t *testing.T) {
	publisher := &recordingSESPublisher{failPublish: true}
	var captured bytes.Buffer
	manager := newEventManager(NewSESCapture(&captured), publisher)
	configureEvent(t, manager, "CreateConfigurationSetEventDestination", "all", true, "send", "open")
	handler := NewHandler(manager)
	id := eventMessageID(t, eventQuery(handler, "SendEmail", eventSimpleForm("tracking")))
	require.Contains(t, captured.String(), `"http_status":200`)
	response := eventOutcome(handler, map[string]any{"message_id": id, "event_type": "Open"})
	require.Equal(t, http.StatusBadGateway, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), `"local":true`)
	require.Contains(t, response.Body.String(), "native SNS admission unavailable")
}

func TestSESConfigurationValidationCannotMutateRecreatedGeneration(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	publisher := &recordingSESPublisher{validate: func(string) error { close(started); <-release; return nil }}
	manager := newEventManager(NewSESCapture(io.Discard), publisher)
	result := make(chan *sesAPIError, 1)
	go func() {
		_, err := manager.configurationV1("CreateConfigurationSetEventDestination", map[string]any{"ConfigurationSetName": "tracking", "EventDestination": map[string]any{
			"Name": "racing", "Enabled": true, "MatchingEventTypes": []string{"send"}, "SNSDestination": map[string]any{"TopicARN": eventTestTopic}}})
		result <- err
	}()
	<-started
	_, err := manager.configurationV1("DeleteConfigurationSet", map[string]any{"ConfigurationSetName": "tracking"})
	require.Nil(t, err)
	_, err = manager.configurationV1("CreateConfigurationSet", map[string]any{"ConfigurationSet": map[string]any{"Name": "tracking"}})
	require.Nil(t, err)
	close(release)
	require.Equal(t, "ConfigurationSetDoesNotExist", (<-result).Code)
	output, err := manager.configurationV1("DescribeConfigurationSet", map[string]any{"ConfigurationSetName": "tracking", "ConfigurationSetAttributeNames": []string{"eventDestinations"}})
	require.Nil(t, err)
	require.Empty(t, output["EventDestinations"])
}

func TestSESConcurrentSendCRUDAndOutcomes(t *testing.T) {
	publisher := &recordingSESPublisher{}
	manager := newEventManager(NewSESCapture(io.Discard), publisher)
	configureEvent(t, manager, "CreateConfigurationSetEventDestination", "tracking", true, "send", "open")
	handler := NewHandler(manager)
	var workers sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := 0; index < 25; index++ {
				response := eventQuery(handler, "SendEmail", eventSimpleForm("tracking"))
				if response.Code != http.StatusOK {
					t.Errorf("send: %s", response.Body.String())
					return
				}
				var wire struct {
					Result struct {
						ID string `xml:"MessageId"`
					} `xml:",any"`
				}
				if err := xml.Unmarshal(response.Body.Bytes(), &wire); err != nil {
					t.Error(err)
					return
				}
				outcome := eventOutcome(handler, map[string]any{"message_id": wire.Result.ID, "event_type": "Open"})
				if outcome.Code != http.StatusOK {
					t.Errorf("outcome: %s", outcome.Body.String())
					return
				}
			}
		}()
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		for index := 0; index < 100; index++ {
			_, err := manager.configurationV1("UpdateConfigurationSetEventDestination", map[string]any{"ConfigurationSetName": "tracking", "EventDestination": map[string]any{
				"Name": "tracking", "Enabled": index%2 == 0, "MatchingEventTypes": []string{"send", "open"}, "SNSDestination": map[string]any{"TopicARN": eventTestTopic}}})
			if err != nil {
				t.Errorf("update: %#v", err)
				return
			}
		}
	}()
	workers.Wait()
	manager.mu.Lock()
	defer manager.mu.Unlock()
	require.Len(t, manager.accepted, 100)
	require.Equal(t, len(manager.accepted), manager.acceptedOrder.Len())
	for _, event := range publisher.snapshot() {
		require.Contains(t, manager.accepted, sesString(sesObject(event.payload["mail"])["messageId"]))
	}
}

func TestSESBoundedCorrelationEvictsOldestWithoutLosingCapture(t *testing.T) {
	manager := newEventManager(NewSESCapture(io.Discard), &recordingSESPublisher{})
	first := ""
	for index := 0; index < acceptedMessageLimit+1; index++ {
		result, apiErr := manager.sendV1("SendEmail", map[string]any{"Source": "sender@example.test", "Destination": map[string]any{"ToAddresses": []string{"recipient@example.test"}},
			"ConfigurationSetName": "tracking", "Message": map[string]any{"Subject": map[string]any{"Data": fmt.Sprint(index)}, "Body": map[string]any{"Text": map[string]any{"Data": "body"}}}})
		require.Nil(t, apiErr)
		if index == 0 {
			first = sesString(result.Output["MessageId"])
		}
		prepared, apiErr := manager.prepareEvents("v1", map[string]any{"ConfigurationSetName": "tracking"}, result.Emails)
		require.Nil(t, apiErr)
		manager.acceptEvents(context.Background(), "bounded", prepared)
	}
	response := eventOutcome(NewHandler(manager), map[string]any{"message_id": first, "event_type": "Open"})
	require.Equal(t, http.StatusNotFound, response.Code)
	require.Len(t, manager.accepted, acceptedMessageLimit)
	require.LessOrEqual(t, manager.acceptedBytes, acceptedMessageByteLimit)
}

type cancelAfterSESCapture struct{ cancel context.CancelFunc }

func (writer cancelAfterSESCapture) Write(data []byte) (int, error) {
	writer.cancel()
	return len(data), nil
}

type cancellationCheckingPublisher struct{ recordingSESPublisher }

func (publisher *cancellationCheckingPublisher) PublishEvent(ctx context.Context, topic, message, requestID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return publisher.recordingSESPublisher.PublishEvent(ctx, topic, message, requestID)
}
func TestSESAcceptedSendEventSurvivesCallerCancellationAfterCapture(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	publisher := &cancellationCheckingPublisher{}
	manager := NewSESManager(SESFixtures{ConfigurationSets: []string{"tracking"}}, NewSESCapture(cancelAfterSESCapture{cancel}),
		WithEventPublisher(publisher, "us-east-1", "000000000000"))
	configureEvent(t, manager, "CreateConfigurationSetEventDestination", "send", true, "send")
	handler := NewHandler(manager)
	form := eventSimpleForm("tracking")
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode())).WithContext(ctx)
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	handler.ServeQuery(response, request, "SendEmail", nil)
	id := eventMessageID(t, response)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	events := publisher.snapshot()
	require.Len(t, events, 1)
	require.Equal(t, id, sesObject(events[0].payload["mail"])["messageId"])
}

func TestSESConfiguredBulkEventsOnlyUseAcceptedEntryMessageIDs(t *testing.T) {
	for _, api := range []string{"v1", "v2"} {
		t.Run(api, func(t *testing.T) {
			publisher := &recordingSESPublisher{}
			manager := newEventManager(NewSESCapture(io.Discard), publisher)
			manager.fixtures.Templates = map[string]SESTemplate{"welcome": {SubjectPart: "Subject", TextPart: "Hello {{name}}"}}
			configureEvent(t, manager, "CreateConfigurationSetEventDestination", "send", true, "send")
			good := map[string]any{"ToAddresses": []any{"recipient@example.test"}}
			bad := map[string]any{"ToAddresses": []any{"invalid-address"}}
			var result sesSendResult
			var apiErr *sesAPIError
			var input map[string]any
			if api == "v1" {
				input = map[string]any{"Source": "sender@example.test", "ConfigurationSetName": "tracking", "Template": "welcome",
					"DefaultTemplateData": `{"name":"SDK"}`, "DefaultTags": []any{map[string]any{"Name": "campaign", "Value": "default"}},
					"Destinations": []any{map[string]any{"Destination": good, "ReplacementTags": []any{map[string]any{"Name": "campaign", "Value": "override"}}}, map[string]any{"Destination": bad}}}
				result, apiErr = manager.sendV1("SendBulkTemplatedEmail", input)
			} else {
				input = map[string]any{"FromEmailAddress": "sender@example.test", "ConfigurationSetName": "tracking",
					"DefaultContent":   map[string]any{"Template": map[string]any{"TemplateName": "welcome", "TemplateData": `{"name":"SDK"}`}},
					"DefaultEmailTags": []any{map[string]any{"Name": "campaign", "Value": "default"}},
					"BulkEmailEntries": []any{map[string]any{"Destination": good, "ReplacementTags": []any{map[string]any{"Name": "campaign", "Value": "override"}}}, map[string]any{"Destination": bad}}}
				result, apiErr = manager.sendV2("SendBulkEmail", input)
			}
			require.Nil(t, apiErr)
			prepared, apiErr := manager.prepareEvents(api, input, result.Emails)
			require.Nil(t, apiErr)
			require.Len(t, prepared, 1)
			manager.acceptEvents(context.Background(), "bulk-request", prepared)
			events := publisher.snapshot()
			require.Len(t, events, 1)
			mail := sesObject(events[0].payload["mail"])
			require.Equal(t, []any{"override"}, sesObject(mail["tags"])["campaign"])
			require.Equal(t, []any{"recipient@example.test"}, mail["destination"])
			field := "Status"
			if api == "v2" {
				field = "BulkEmailEntryResults"
			}
			entries := result.Output[field].([]any)
			require.Equal(t, sesObject(entries[0])["MessageId"], mail["messageId"])
			require.NotContains(t, sesObject(entries[1]), "MessageId")
			require.Len(t, manager.accepted, 1)
		})
	}
}

func TestSESConcurrentDestinationUpdatesPreserveResourceGeneration(t *testing.T) {
	publisher := &recordingSESPublisher{}
	manager := newEventManager(NewSESCapture(io.Discard), publisher)
	configureEvent(t, manager, "CreateConfigurationSetEventDestination", "tracking", true, "send")
	started, release := make(chan struct{}), make(chan struct{})
	var first sync.Once
	publisher.validate = func(string) error {
		blocked := false
		first.Do(func() { blocked = true; close(started) })
		if blocked {
			<-release
		}
		return nil
	}
	update := func(types []string) *sesAPIError {
		_, err := manager.configurationV1("UpdateConfigurationSetEventDestination", map[string]any{
			"ConfigurationSetName": "tracking", "EventDestination": map[string]any{"Name": "tracking", "Enabled": true,
				"MatchingEventTypes": types, "SNSDestination": map[string]any{"TopicARN": eventTestTopic}}})
		return err
	}
	earlier := make(chan *sesAPIError, 1)
	go func() { earlier <- update([]string{"open"}) }()
	<-started
	require.Nil(t, update([]string{"bounce"}))
	close(release)
	require.Nil(t, <-earlier, "ordinary concurrent update cannot report a missing existing destination")
	output, apiErr := manager.configurationV1("DescribeConfigurationSet", map[string]any{"ConfigurationSetName": "tracking", "ConfigurationSetAttributeNames": []string{"eventDestinations"}})
	require.Nil(t, apiErr)
	require.Equal(t, []any{"open"}, output["EventDestinations"].([]map[string]any)[0]["MatchingEventTypes"])
}
