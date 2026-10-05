package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSESCaptureVisibleConcurrentAppendAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "email.jsonl")
	capture, err := OpenSESCapture(path)
	require.NoError(t, err)
	var workers sync.WaitGroup
	for i := 0; i < 32; i++ {
		workers.Add(1)
		go func(index int) {
			defer workers.Done()
			if err := capture.append(map[string]any{"index": index, "text": "hello\nworld"}); err != nil {
				t.Error(err)
			}
		}(i)
	}
	workers.Wait()
	content, err := os.ReadFile(path)
	require.NoError(t, err)
	decoder := json.NewDecoder(bytes.NewReader(content))
	seen := map[int]bool{}
	for decoder.More() {
		var record map[string]any
		require.NoError(t, decoder.Decode(&record))
		seen[int(record["index"].(float64))] = true
		require.Equal(t, "hello\nworld", record["text"])
	}
	require.Len(t, seen, 32)
	require.NoError(t, capture.Close())
	require.NoError(t, capture.Close())
	require.Error(t, capture.append(map[string]any{"late": true}))
	next, err := OpenSESCapture(path)
	require.NoError(t, err)
	require.NoError(t, next.append(map[string]any{"restart": true}))
	require.NoError(t, next.Close())
	final, err := os.ReadFile(path)
	require.NoError(t, err)
	require.True(t, bytes.HasPrefix(final, content))
	require.Equal(t, 33, bytes.Count(final, []byte{'\n'}))
}

func TestSESCaptureRefusesIncompleteLogWithoutTruncation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "email.jsonl")
	original := []byte(`{"unfinished":`)
	require.NoError(t, os.WriteFile(path, original, 0600))
	_, err := OpenSESCapture(path)
	require.ErrorContains(t, err, "incomplete")
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, original, after)
}

type sesShortWriter struct{ calls int }

func (writer *sesShortWriter) Write(data []byte) (int, error) {
	writer.calls++
	return len(data) / 2, nil
}
func TestSESCaptureFailureIsTerminal(t *testing.T) {
	writer := &sesShortWriter{}
	capture := &SESCapture{writer: writer}
	require.ErrorIs(t, capture.append(map[string]any{"first": true}), io.ErrShortWrite)
	require.ErrorIs(t, capture.append(map[string]any{"second": true}), io.ErrShortWrite)
	require.Equal(t, 1, writer.calls)
	require.ErrorIs(t, capture.Close(), io.ErrShortWrite)
}

func TestSESCaptureHTTPFailureCannotReturnMessageID(t *testing.T) {
	writer := &sesShortWriter{}
	server := NewServer(nil, nil, nil, nil)
	server.SetSES(NewSESManager(SESFixtures{}, &SESCapture{writer: writer}))
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
	server := NewServer(nil, nil, nil, nil)
	server.SetSES(NewSESManager(SESFixtures{}, &SESCapture{writer: &buffer}))
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

func TestSESLifecycleClosesCaptureAfterWorkers(t *testing.T) {
	var buffer bytes.Buffer
	closed := false
	capture := &SESCapture{writer: &buffer, closeFile: func() error { closed = true; return errors.New("close failed") }}
	done := make(chan struct{})
	owned := &eventBusLifecycle{ses: NewSESManager(SESFixtures{}, capture), requeueDone: done}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.Error(t, owned.Close(ctx))
	require.False(t, closed, "a live resource user prevents capture close")
	close(done)
	joined := &eventBusLifecycle{ses: NewSESManager(SESFixtures{}, capture), requeueDone: done}
	require.ErrorContains(t, joined.Close(t.Context()), "close failed")
	require.True(t, closed)
}

func TestSESFixturesAndRenderCaptureView(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ses.yaml")
	require.NoError(t, os.WriteFile(path, []byte("templates:\n  welcome:\n    SubjectPart: 'Hello {{user.name}}'\n    HtmlPart: '<p>{{user.name}}</p>'\nrequire_verified_identities: true\nverified_identities: [example.test]\n"), 0600))
	fixtures, err := LoadSESFixtures(path)
	require.NoError(t, err)
	manager := NewSESManager(fixtures, &SESCapture{writer: io.Discard})
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
