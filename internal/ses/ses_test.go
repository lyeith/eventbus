package ses

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type sesShortWriter struct{ calls int }

func (writer *sesShortWriter) Write(data []byte) (int, error) {
	writer.calls++
	return len(data) / 2, nil
}
func TestSESCaptureHTTPFailureCannotReturnMessageID(t *testing.T) {
	writer := &sesShortWriter{}
	server := NewHandler(NewSESManager(SESFixtures{}, NewSESCapture(writer)))
	request := httptest.NewRequest(http.MethodPost, "/v2/email/outbound-emails", strings.NewReader(`{"FromEmailAddress":"from@example.test","Destination":{"ToAddresses":["to@example.test"]},"Content":{"Simple":{"Subject":{"Data":"subject"},"Body":{"Text":{"Data":"body"}}}}}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	require.Equal(t, http.StatusInternalServerError, response.Code, response.Body.String())
	require.NotContains(t, response.Body.String(), "MessageId")
	require.Contains(t, response.Body.String(), "capture")
}

func TestSESHTTPErrorIsCapturedWithCorrelation(t *testing.T) {
	var buffer bytes.Buffer
	server := NewHandler(NewSESManager(SESFixtures{}, NewSESCapture(&buffer)))
	request := httptest.NewRequest(http.MethodPost, "/v2/email/outbound-emails", strings.NewReader(`{}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	require.Equal(t, http.StatusBadRequest, response.Code)
	var record map[string]any
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(buffer.Bytes()), &record))
	require.EqualValues(t, 1, record["schema_version"])
	require.Equal(t, "sesv2", record["api"])
	require.Equal(t, "SendEmail", record["operation"])
	require.NotEmpty(t, record["request_id"])
	require.Equal(t, record["request_id"], response.Header().Get("x-amzn-requestid"))
	outcome := sesObject(record["outcome"])
	require.EqualValues(t, http.StatusBadRequest, outcome["http_status"])
	require.NotEmpty(t, sesObject(outcome["error"])["code"])
}

func TestSESFixturesAndRenderCaptureView(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ses.yaml")
	require.NoError(t, os.WriteFile(path, []byte("templates:\n  welcome:\n    SubjectPart: 'Hello {{user.name}}'\n    HtmlPart: '<p>{{user.name}}</p>'\nrequire_verified_identities: true\nverified_identities: [example.test]\n"), 0600))
	fixtures, err := LoadSESFixtures(path)
	require.NoError(t, err)
	manager := NewSESManager(fixtures, NewSESCapture(io.Discard))
	fixtures.Templates["welcome"] = SESTemplate{SubjectPart: "changed by caller"}
	template, found := manager.template("arn:aws:ses:us-east-1:000000000000:template/welcome")
	require.True(t, found)
	rendered, err := sesRenderTemplate(template, `{"user":{"name":"A & B"}}`)
	require.NoError(t, err)
	require.Equal(t, "Hello A & B", rendered["subject"])
	require.Equal(t, "<p>A & B</p>", rendered["html"])
	require.Nil(t, manager.checkSender("v2", "Sender <sender@example.test>"))
	require.Equal(t, "MessageRejected", manager.checkSender("v2", "other@unverified.test").Code)
	_, err = sesRenderTemplate(template, `{}`)
	require.Error(t, err)
	require.NoError(t, os.WriteFile(path, []byte("unknown_fixture: true\n"), 0600))
	_, err = LoadSESFixtures(path)
	require.Error(t, err)
}

func TestSESValidationRecipientBounds(t *testing.T) {
	recipients := make([]any, 50)
	for i := range recipients {
		recipients[i] = fmt.Sprintf("%d@example.test", i)
	}
	require.Nil(t, sesValidateRecipients("v2", map[string]any{"ToAddresses": recipients}))
	require.NotNil(t, sesValidateRecipients("v2", map[string]any{"ToAddresses": append(recipients, "extra@example.test")}))
	require.NotNil(t, sesValidateRecipients("v1", map[string]any{"ToAddresses": []any{12}}))
}

func TestSESCaptureTemplatePreservesNumericPrecision(t *testing.T) {
	rendered, err := sesRenderTemplate(SESTemplate{SubjectPart: "Record {{id}}"}, `{"id":9007199254740993}`)
	if err != nil {
		t.Fatal(err)
	}
	if rendered["subject"] != "Record 9007199254740993" {
		t.Fatalf("capture renderer rounded numeric identifier: %#v", rendered)
	}
}
