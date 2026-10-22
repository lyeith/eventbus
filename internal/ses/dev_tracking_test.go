package ses

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"image/gif"
	"io"
	"mime"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type devTrackingDiscovery struct {
	Local       bool      `json:"local"`
	MessageID   string    `json:"message_id"`
	ViewURL     string    `json:"view_url"`
	TrackingURL string    `json:"tracking_url"`
	ExpiresAt   time.Time `json:"expires_at"`
}

func newDevTrackingTestHandler(t *testing.T, capture *SESCapture) (*Handler, *recordingSESPublisher) {
	t.Helper()
	publisher := &recordingSESPublisher{}
	manager := NewSESManager(SESFixtures{ConfigurationSets: []string{"tracking", "other"},
		Templates: map[string]SESTemplate{"welcome": {HtmlPart: "<body>Hello {{name}} {{ses:openTracker}}</body>"}}},
		capture, WithEventPublisher(publisher, "us-east-1", "000000000000"), WithDevTracking())
	configureEvent(t, manager, "CreateConfigurationSetEventDestination", "opens", true, "open")
	t.Cleanup(func() {
		if captureErr := capture.Err(); captureErr != nil {
			require.ErrorIs(t, manager.Close(), captureErr, "capture failure remains sticky through close")
		} else {
			require.NoError(t, manager.Close())
		}
	})
	return NewHandler(manager), publisher
}
func devTrackingRequest(handler *Handler, method, path string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, nil)
	request.RemoteAddr = "192.0.2.47:32000"
	request.Header.Set("User-Agent", "actual local email reader")
	request.Header.Set("X-Forwarded-For", "203.0.113.99")
	response := httptest.NewRecorder()
	handler.ServeDevTracking(response, request)
	return response
}
func readDevTracking(t *testing.T, handler *Handler, id string) devTrackingDiscovery {
	t.Helper()
	response := devTrackingRequest(handler, http.MethodGet, DevTrackingMessagesPrefix+id)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var discovery devTrackingDiscovery
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &discovery))
	require.True(t, discovery.Local)
	require.Equal(t, id, discovery.MessageID)
	require.True(t, strings.HasPrefix(discovery.TrackingURL, DevTrackingPixelPrefix))
	require.True(t, strings.HasSuffix(discovery.TrackingURL, ".gif"))
	var fields map[string]any
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &fields))
	require.Len(t, fields, 5, "discovery must not copy message bodies or original requests")
	return discovery
}
func sendDevTrackingV2(t *testing.T, handler *Handler, input map[string]any) string {
	t.Helper()
	data, err := json.Marshal(input)
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodPost, "/v2/email/outbound-emails", bytes.NewReader(data))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var output map[string]any
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &output))
	require.Len(t, output, 1, "native send response must not gain local tracking fields")
	return sesString(output["MessageId"])
}
func devTrackingV2Input(content map[string]any) map[string]any {
	return map[string]any{"FromEmailAddress": "sender@example.test", "ConfigurationSetName": "tracking",
		"Destination": map[string]any{"ToAddresses": []string{"recipient@example.test"}}, "Content": content}
}

func TestSESDevTrackingPreservesSendAndProducesNativeOpenFromActualGET(t *testing.T) {
	for _, api := range []string{"v1", "v2"} {
		t.Run(api, func(t *testing.T) {
			var capture bytes.Buffer
			handler, publisher := newDevTrackingTestHandler(t, NewSESCapture(&capture))
			html := "<body>before {{ses:openTracker}} after</body>"
			send := func() string {
				if api == "v1" {
					form := eventSimpleForm("tracking")
					form.Set("Message.Body.Html.Data", html)
					return eventMessageID(t, eventQuery(handler, "SendEmail", form))
				}
				return sendDevTrackingV2(t, handler, devTrackingV2Input(map[string]any{
					"Simple": map[string]any{"Subject": map[string]any{"Data": "subject"},
						"Body": map[string]any{"Html": map[string]any{"Data": html}}}}))
			}
			firstID := send()
			captured := bytes.Clone(capture.Bytes())
			first := readDevTracking(t, handler, firstID)
			require.WithinDuration(t, time.Now().Add(acceptedMessageTTL), first.ExpiresAt, 2*time.Second)
			view := devTrackingRequest(handler, http.MethodGet, first.ViewURL)
			require.Equal(t, http.StatusOK, view.Code)
			require.Contains(t, view.Body.String(), `before <img src="`+first.TrackingURL)
			require.Contains(t, view.Body.String(), `alt=""> after`)
			require.NotContains(t, view.Body.String(), "{{ses:openTracker}}")
			require.Contains(t, view.Header().Get("Content-Security-Policy"), "sandbox; default-src 'none'")
			require.Equal(t, "no-store", view.Header().Get("Cache-Control"))
			require.Equal(t, "nosniff", view.Header().Get("X-Content-Type-Options"))
			require.Equal(t, "no-referrer", view.Header().Get("Referrer-Policy"))
			require.Equal(t, captured, capture.Bytes(), "discovery/view cannot alter original durable capture")
			require.Contains(t, capture.String(), "{{ses:openTracker}}")
			require.Empty(t, publisher.snapshot(), "acceptance, capture read and HTML read are not opens")
			require.Equal(t, http.StatusMethodNotAllowed, devTrackingRequest(handler, http.MethodHead, first.TrackingURL).Code)
			require.Empty(t, publisher.snapshot())

			start := time.Now()
			pixel := devTrackingRequest(handler, http.MethodGet, first.TrackingURL)
			require.Equal(t, http.StatusOK, pixel.Code)
			require.Equal(t, "image/gif", pixel.Header().Get("Content-Type"))
			image, err := gif.Decode(bytes.NewReader(pixel.Body.Bytes()))
			require.NoError(t, err)
			require.Equal(t, 1, image.Bounds().Dx())
			require.Equal(t, 1, image.Bounds().Dy())
			events := publisher.snapshot()
			require.Len(t, events, 1)
			require.Equal(t, "Open", events[0].payload["eventType"])
			require.Equal(t, firstID, sesObject(events[0].payload["mail"])["messageId"])
			details := sesObject(events[0].payload["open"])
			require.Equal(t, "192.0.2.47", details["ipAddress"])
			require.Equal(t, "actual local email reader", details["userAgent"])
			when, err := time.Parse(time.RFC3339Nano, sesString(details["timestamp"]))
			require.NoError(t, err)
			require.False(t, when.Before(start))
			require.False(t, when.After(time.Now()))

			secondID := send()
			second := readDevTracking(t, handler, secondID)
			require.NotEqual(t, firstID, secondID)
			require.NotEqual(t, first.TrackingURL, second.TrackingURL)
			require.Equal(t, http.StatusOK, devTrackingRequest(handler, http.MethodGet, first.TrackingURL).Code)
			require.Equal(t, http.StatusOK, devTrackingRequest(handler, http.MethodGet, second.TrackingURL).Code)
			events = publisher.snapshot()
			require.Len(t, events, 3, "each actual fetch is a separate Open")
			require.Equal(t, firstID, sesObject(events[1].payload["mail"])["messageId"])
			require.Equal(t, secondID, sesObject(events[2].payload["mail"])["messageId"])
		})
	}
}

func TestSESDevTrackingRawMIMEAndTemplateViewsPreserveOriginalCapture(t *testing.T) {
	latin := "<body>caf" + string([]byte{0xe9}) + " {{ses:openTracker}}</body>"
	cases := []struct {
		name, api, action, html, charset string
		form                             url.Values
		content                          map[string]any
	}{
		{name: "raw multipart quoted printable", api: "v1", action: "SendRawEmail", html: "actual body", charset: "utf-8",
			form: url.Values{"RawMessage.Data": {base64.StdEncoding.EncodeToString([]byte(
				"From: sender@example.test\r\nTo: recipient@example.test\r\nX-SES-CONFIGURATION-SET: tracking\r\nContent-Type: multipart/mixed; boundary=outer\r\n\r\n" +
					"--outer\r\nContent-Type: text/html\r\nContent-Disposition: attachment; filename=attachment.html\r\n\r\nATTACHMENT MUST NOT RENDER\r\n" +
					"--outer\r\nContent-Type: multipart/alternative; boundary=inner\r\n\r\n--inner\r\nContent-Type: text/plain\r\n\r\nplain\r\n" +
					"--inner\r\nContent-Type: text/html; charset=utf-8\r\nContent-Transfer-Encoding: quoted-printable\r\n\r\n<body>actual=20body {{ses:openTracker}}</body>\r\n--inner--\r\n--outer--\r\n"))}}},
		{name: "raw base64 latin1", api: "v2", html: "caf", charset: "iso-8859-1",
			content: map[string]any{"Raw": map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte(
				"From: sender@example.test\r\nTo: recipient@example.test\r\nContent-Type: text/html; charset=iso-8859-1\r\nContent-Transfer-Encoding: base64\r\n\r\n" +
					base64.StdEncoding.EncodeToString([]byte(latin))))}}},
		{name: "stored template v1", api: "v1", action: "SendTemplatedEmail", html: "Hello native", charset: "utf-8",
			form: url.Values{"Source": {"sender@example.test"}, "Destination.ToAddresses.member.1": {"recipient@example.test"},
				"ConfigurationSetName": {"tracking"}, "Template": {"welcome"}, "TemplateData": {`{"name":"native"}`}}},
		{name: "inline template v2", api: "v2", html: "Hello native", charset: "utf-8",
			content: map[string]any{"Template": map[string]any{"TemplateContent": map[string]any{"Html": "<body>Hello {{name}} {{ses:openTracker}}</body>"},
				"TemplateData": `{"name":"native"}`}}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var capture bytes.Buffer
			handler, publisher := newDevTrackingTestHandler(t, NewSESCapture(&capture))
			var id string
			if test.api == "v1" {
				id = eventMessageID(t, eventQuery(handler, test.action, test.form))
			} else {
				id = sendDevTrackingV2(t, handler, devTrackingV2Input(test.content))
			}
			before := bytes.Clone(capture.Bytes())
			discovery := readDevTracking(t, handler, id)
			view := devTrackingRequest(handler, http.MethodGet, discovery.ViewURL)
			require.Equal(t, http.StatusOK, view.Code)
			require.Contains(t, view.Body.String(), test.html)
			require.NotContains(t, view.Body.String(), "ATTACHMENT MUST NOT RENDER")
			require.Contains(t, view.Body.String(), discovery.TrackingURL)
			_, params, err := mime.ParseMediaType(view.Header().Get("Content-Type"))
			require.NoError(t, err)
			require.Equal(t, test.charset, params["charset"])
			if test.charset == "iso-8859-1" {
				require.True(t, bytes.Contains(view.Body.Bytes(), []byte{0xe9}), "decoded MIME bytes must not undergo lossy JSON conversion")
			}
			require.Equal(t, before, capture.Bytes())
			require.Empty(t, publisher.snapshot())
			if test.action == "SendRawEmail" {
				var record map[string]any
				require.NoError(t, json.Unmarshal(bytes.TrimSpace(before), &record))
				require.Equal(t, test.form.Get("RawMessage.Data"), sesObject(sesObject(record["request"])["RawMessage"])["Data"])
			}
			if strings.Contains(test.name, "template") {
				require.Contains(t, capture.String(), "capture_rendering_error", "native capture view stays unchanged")
				if test.api == "v2" {
					require.Contains(t, capture.String(), "{{ses:openTracker}}")
				}
			}
		})
	}
}

func TestSESDevTrackingEligibilityAndCurrentNativeFiltering(t *testing.T) {
	cases := []struct {
		name   string
		change func(*Handler, url.Values)
	}{
		{"feature disabled", func(h *Handler, _ url.Values) { h.ses.devTracking = nil }},
		{"destination disabled", func(h *Handler, _ url.Values) {
			configureEvent(t, h.ses, "UpdateConfigurationSetEventDestination", "opens", false, "open")
		}},
		{"nonmatching", func(h *Handler, _ url.Values) {
			configureEvent(t, h.ses, "UpdateConfigurationSetEventDestination", "opens", true, "bounce")
		}},
		{"no set", func(_ *Handler, form url.Values) { form.Del("ConfigurationSetName") }},
		{"text only", func(_ *Handler, form url.Values) { form.Del("Message.Body.Html.Data") }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			handler, publisher := newDevTrackingTestHandler(t, NewSESCapture(io.Discard))
			form := eventSimpleForm("tracking")
			form.Set("Message.Body.Html.Data", "<body>hello</body>")
			test.change(handler, form)
			id := eventMessageID(t, eventQuery(handler, "SendEmail", form))
			require.Equal(t, http.StatusNotFound, devTrackingRequest(handler, http.MethodGet, DevTrackingMessagesPrefix+id).Code)
			require.Empty(t, publisher.snapshot())
		})
	}
	handler, publisher := newDevTrackingTestHandler(t, NewSESCapture(io.Discard))
	form := eventSimpleForm("tracking")
	form.Set("Message.Body.Html.Data", "<body>hello</body>")
	id := eventMessageID(t, eventQuery(handler, "SendEmail", form))
	discovery := readDevTracking(t, handler, id)
	view := devTrackingRequest(handler, http.MethodGet, discovery.ViewURL)
	require.True(t, strings.HasSuffix(view.Body.String(), `alt="">`), "default pixel follows submitted HTML")
	for _, types := range [][]string{{"bounce"}, {"open"}} {
		configureEvent(t, handler.ses, "UpdateConfigurationSetEventDestination", "opens", false, types...)
		require.Equal(t, http.StatusOK, devTrackingRequest(handler, http.MethodGet, discovery.TrackingURL).Code)
		require.Empty(t, publisher.snapshot())
	}
	configureEvent(t, handler.ses, "UpdateConfigurationSetEventDestination", "opens", true, "open")
	require.Equal(t, http.StatusOK, devTrackingRequest(handler, http.MethodGet, discovery.TrackingURL).Code)
	require.Len(t, publisher.snapshot(), 1)
	_, apiErr := handler.ses.configurationV1("DeleteConfigurationSet", map[string]any{"ConfigurationSetName": "tracking"})
	require.Nil(t, apiErr)
	require.Equal(t, http.StatusOK, devTrackingRequest(handler, http.MethodGet, discovery.TrackingURL).Code)
	_, apiErr = handler.ses.configurationV1("CreateConfigurationSet", map[string]any{"ConfigurationSet": map[string]any{"Name": "tracking"}})
	require.Nil(t, apiErr)
	configureEvent(t, handler.ses, "CreateConfigurationSetEventDestination", "opens", true, "open")
	require.Equal(t, http.StatusOK, devTrackingRequest(handler, http.MethodGet, discovery.TrackingURL).Code)
	require.Len(t, publisher.snapshot(), 1, "same-name replacement cannot inherit an older accepted email")
}

func TestSESDevTrackingNativeOverrideAndDuplicateLocalMarkers(t *testing.T) {
	for _, policy := range []string{"ENABLED", "DISABLED"} {
		t.Run(policy, func(t *testing.T) {
			handler, publisher := newDevTrackingTestHandler(t, NewSESCapture(io.Discard))
			input := devTrackingV2Input(map[string]any{"Simple": map[string]any{
				"Subject": map[string]any{"Data": "subject"}, "Body": map[string]any{"Html": map[string]any{"Data": "<body>{{ses:openTracker}} middle {{ses:openTracker}}</body>"}}}})
			input["ConfigurationOverrides"] = map[string]any{"Tracking": map[string]any{"OpenTrackingEnabled": policy}}
			id := sendDevTrackingV2(t, handler, input)
			if policy == "DISABLED" {
				require.Equal(t, http.StatusNotFound, devTrackingRequest(handler, http.MethodGet, DevTrackingMessagesPrefix+id).Code)
				require.Equal(t, http.StatusOK, eventOutcome(handler, map[string]any{"message_id": id, "event_type": "Open"}).Code)
				require.Empty(t, publisher.snapshot(), "native per-message policy also owns explicit Open event selection")
				return
			}
			discovery := readDevTracking(t, handler, id)
			view := devTrackingRequest(handler, http.MethodGet, discovery.ViewURL)
			require.Equal(t, 1, strings.Count(view.Body.String(), "<img "))
			require.NotContains(t, view.Body.String(), "{{ses:openTracker}}")
			require.Contains(t, view.Body.String(), " middle </body>")
		})
	}
}

func TestSESDevTrackingExpiryEvictionInvalidResourcesAndFailure(t *testing.T) {
	handler, publisher := newDevTrackingTestHandler(t, NewSESCapture(io.Discard))
	form := eventSimpleForm("tracking")
	form.Set("Message.Body.Html.Data", "<body>hello</body>")
	id := eventMessageID(t, eventQuery(handler, "SendEmail", form))
	discovery := readDevTracking(t, handler, id)
	for _, path := range []string{DevTrackingMessagesPrefix + "unknown", discovery.ViewURL + "/extra",
		DevTrackingPixelPrefix + "unknown.gif", DevTrackingPixelPrefix + ".gif", discovery.TrackingURL + "/extra"} {
		require.Equal(t, http.StatusNotFound, devTrackingRequest(handler, http.MethodGet, path).Code, path)
	}
	require.Empty(t, publisher.snapshot())
	handler.ses.mu.Lock()
	accepted := handler.ses.accepted[id]
	require.GreaterOrEqual(t, accepted.size, len(accepted.devTracking.html)+len(accepted.devTracking.token))
	accepted.retainedAt = time.Now().Add(-acceptedMessageTTL)
	handler.ses.mu.Unlock()
	require.Equal(t, http.StatusNotFound, devTrackingRequest(handler, http.MethodGet, discovery.TrackingURL).Code)
	require.Empty(t, handler.ses.devTracking.tokens)
	require.Empty(t, handler.ses.accepted)
	require.Zero(t, handler.ses.acceptedBytes)
	require.Empty(t, publisher.snapshot())

	// Exercise the existing byte-budget eviction owner without allocating a
	// second 64 MiB buffer. Each synthesized entry represents accounted bytes.
	handler.ses.acceptEvents(context.Background(), "retention", []preparedMessage{
		{accepted: &acceptedMessage{id: "old", size: 10, devTracking: &devTrackingMessage{token: "old-token", html: "old"}}},
		{accepted: &acceptedMessage{id: "new", size: acceptedMessageByteLimit, devTracking: &devTrackingMessage{token: "new-token", html: "new"}}},
	})
	require.NotContains(t, handler.ses.accepted, "old")
	require.NotContains(t, handler.ses.devTracking.tokens, "old-token")
	require.Equal(t, "new", handler.ses.devTracking.tokens["new-token"])
	require.Equal(t, acceptedMessageByteLimit, handler.ses.acceptedBytes)
	require.Equal(t, http.StatusNotFound, devTrackingRequest(handler, http.MethodGet, DevTrackingPixelPrefix+"old-token.gif").Code)

	failedPublisher := &recordingSESPublisher{failPublish: true}
	handler.ses.eventPublisher = failedPublisher
	// Existing valid correlation survives downstream SNS admission failures.
	newID := eventMessageID(t, eventQuery(handler, "SendEmail", form))
	newView := readDevTracking(t, handler, newID)
	require.Equal(t, http.StatusBadGateway, devTrackingRequest(handler, http.MethodGet, newView.TrackingURL).Code)
	require.Contains(t, handler.ses.accepted, newID)
}

func TestSESDevTrackingCaptureMustCommitBeforeResourcePublication(t *testing.T) {
	writer := &devTrackingCaptureGate{started: make(chan struct{}), release: make(chan struct{})}
	handler, publisher := newDevTrackingTestHandler(t, NewSESCapture(writer))
	release := sync.OnceFunc(func() { close(writer.release) })
	t.Cleanup(release)
	form := eventSimpleForm("tracking")
	form.Set("Message.Body.Html.Data", "<body>{{ses:openTracker}}</body>")
	response := make(chan *httptest.ResponseRecorder, 1)
	go func() { response <- eventQuery(handler, "SendEmail", form) }()
	select {
	case <-writer.started:
	case <-time.After(3 * time.Second):
		t.Fatal("send capture did not reach held append")
	}
	handler.ses.mu.Lock()
	acceptedCount, tokenCount := len(handler.ses.accepted), len(handler.ses.devTracking.tokens)
	handler.ses.mu.Unlock()
	require.Zero(t, acceptedCount)
	require.Zero(t, tokenCount)
	require.Empty(t, publisher.snapshot())
	release()
	id := eventMessageID(t, <-response)
	_ = readDevTracking(t, handler, id)

	failed, failedEvents := newDevTrackingTestHandler(t, NewSESCapture(&sesShortWriter{}))
	require.Equal(t, http.StatusInternalServerError, eventQuery(failed, "SendEmail", form).Code)
	require.Empty(t, failed.ses.accepted)
	require.Empty(t, failed.ses.devTracking.tokens)
	require.Empty(t, failedEvents.snapshot())
}

type devTrackingCaptureGate struct {
	started, release chan struct{}
	once             sync.Once
}

func (writer *devTrackingCaptureGate) Write(data []byte) (int, error) {
	writer.once.Do(func() { close(writer.started) })
	<-writer.release
	return len(data), nil
}

func TestSESDevTrackingMIMEBoundsAndUnavailableTemplateDoNotInventViews(t *testing.T) {
	for _, test := range []struct {
		contentType, encoding, disposition, body string
		depth, parts                             int
	}{
		{"text/html", "", "attachment; filename=mail.html", "attachment", 0, 0},
		{"text/html; charset=utf-16", "", "", "unsupported encoding", 0, 0},
		{"text/html", "", "", "too deep", 33, 0},
		{"text/html", "", "", "too many parts", 0, 500},
	} {
		html, _ := devMIMEHTML(test.contentType, test.encoding, test.disposition, strings.NewReader(test.body), test.depth, &test.parts)
		require.Empty(t, html)
	}
	handler, _ := newDevTrackingTestHandler(t, NewSESCapture(io.Discard))
	form := url.Values{"Source": {"sender@example.test"}, "Destination.ToAddresses.member.1": {"recipient@example.test"},
		"ConfigurationSetName": {"tracking"}, "Template": {"welcome"}, "TemplateData": {`{}`}}
	id := eventMessageID(t, eventQuery(handler, "SendTemplatedEmail", form))
	require.Equal(t, http.StatusNotFound, devTrackingRequest(handler, http.MethodGet, DevTrackingMessagesPrefix+id).Code)
}

func TestSESDevTrackingSourceMarkerAndDefaultPlacement(t *testing.T) {
	for _, api := range []string{"v1", "v2"} {
		t.Run(api, func(t *testing.T) {
			var capture bytes.Buffer
			handler, _ := newDevTrackingTestHandler(t, NewSESCapture(&capture))
			handler.ses.fixtures.Templates["welcome"] = SESTemplate{HtmlPart: "<body>Hello {{name}} {{id}} {{ses:openTracker}}</body>"}
			data := `{"name":"native","id":9007199254740993,"ses:openTracker":"USER_OVERRIDE"}`
			var id string
			if api == "v1" {
				id = eventMessageID(t, eventQuery(handler, "SendTemplatedEmail", url.Values{
					"Source": {"sender@example.test"}, "Destination.ToAddresses.member.1": {"recipient@example.test"},
					"ConfigurationSetName": {"tracking"}, "Template": {"welcome"}, "TemplateData": {data}}))
			} else {
				id = sendDevTrackingV2(t, handler, devTrackingV2Input(map[string]any{"Template": map[string]any{
					"TemplateContent": map[string]any{"Html": "<body>Hello {{name}} {{id}} {{ses:openTracker}}</body>"}, "TemplateData": data}}))
			}
			discovery := readDevTracking(t, handler, id)
			view := devTrackingRequest(handler, http.MethodGet, discovery.ViewURL)
			require.Contains(t, view.Body.String(), `Hello native 9007199254740993 <img src="`+discovery.TrackingURL)
			require.NotContains(t, view.Body.String(), "USER_OVERRIDE")
			require.Contains(t, capture.String(), "Hello native 9007199254740993 USER_OVERRIDE", "native optional capture remains unchanged and exact")
		})
	}
	handler, _ := newDevTrackingTestHandler(t, NewSESCapture(io.Discard))
	form := eventSimpleForm("tracking")
	html := "<body>body</body><!-- closing tag text: </body> -->"
	form.Set("Message.Body.Html.Data", html)
	id := eventMessageID(t, eventQuery(handler, "SendEmail", form))
	discovery := readDevTracking(t, handler, id)
	view := devTrackingRequest(handler, http.MethodGet, discovery.ViewURL)
	require.Contains(t, view.Body.String(), html+`<img src="`+discovery.TrackingURL, "default pixel cannot land inside a comment")
}

func TestSESDevTrackingConcurrentFetchAndConfigurationUpdates(t *testing.T) {
	handler, publisher := newDevTrackingTestHandler(t, NewSESCapture(io.Discard))
	form := eventSimpleForm("tracking")
	form.Set("Message.Body.Html.Data", "<body>native</body>")
	id := eventMessageID(t, eventQuery(handler, "SendEmail", form))
	discovery := readDevTracking(t, handler, id)
	var workers sync.WaitGroup
	for worker := 0; worker < 3; worker++ {
		workers.Go(func() {
			for index := 0; index < 20; index++ {
				if response := devTrackingRequest(handler, http.MethodGet, discovery.TrackingURL); response.Code != http.StatusOK {
					t.Errorf("pixel admission: %d %s", response.Code, response.Body.String())
				}
			}
		})
	}
	workers.Go(func() {
		for index := 0; index < 20; index++ {
			_, apiErr := handler.ses.configurationV1("UpdateConfigurationSetEventDestination", map[string]any{
				"ConfigurationSetName": "tracking", "EventDestination": map[string]any{
					"Name": "opens", "Enabled": index%2 == 0, "MatchingEventTypes": []string{"open"},
					"SNSDestination": map[string]any{"TopicARN": eventTestTopic}}})
			if apiErr != nil {
				t.Errorf("native destination update: %v", apiErr)
			}
		}
	})
	workers.Wait()
	for _, event := range publisher.snapshot() {
		require.Equal(t, id, sesObject(event.payload["mail"])["messageId"])
		require.Equal(t, "Open", event.payload["eventType"])
	}
}
