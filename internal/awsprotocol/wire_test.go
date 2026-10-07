package awsprotocol

import (
	"encoding/json"
	"encoding/xml"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestJSONProtocolsUseSelectedVersionAndStableRequestID(t *testing.T) {
	for _, protocol := range []JSONProtocol{JSON10, JSON11} {
		t.Run(string(protocol), func(t *testing.T) {
			response := httptest.NewRecorder()
			response.Header().Set("X-Amzn-RequestId", "owned-request-id")
			protocol.Response(response, http.StatusOK, map[string]string{"Value": "<owned>"})
			if response.Header().Get("Content-Type") != string(protocol) || response.Header().Get("X-Amzn-RequestId") != "owned-request-id" {
				t.Fatalf("protocol/ID mismatch: %v", response.Header())
			}
			var value map[string]string
			if err := json.Unmarshal(response.Body.Bytes(), &value); err != nil || value["Value"] != "<owned>" {
				t.Fatalf("invalid response: %s", response.Body.String())
			}

			failure := httptest.NewRecorder()
			id := EnsureRequestID(failure)
			if id == "" || EnsureRequestID(failure) != id {
				t.Fatal("request ID is not stable")
			}
			protocol.Error(failure, http.StatusBadRequest, "ValidationException", "invalid <field>")
			if failure.Code != http.StatusBadRequest || failure.Header().Get("Content-Type") != string(protocol) ||
				failure.Header().Get("X-Amzn-RequestId") != id || failure.Header().Get("X-Amzn-ErrorType") != "ValidationException" {
				t.Fatalf("invalid error headers/status: %v status=%d", failure.Header(), failure.Code)
			}
			if err := json.Unmarshal(failure.Body.Bytes(), &value); err != nil || value["__type"] != "ValidationException" || value["Message"] != "invalid <field>" {
				t.Fatalf("invalid native error body: %s", failure.Body.String())
			}
		})
	}
	legacy := httptest.NewRecorder()
	JSONError(legacy, http.StatusBadRequest, "Legacy", "message")
	if legacy.Header().Get("Content-Type") != string(JSON10) {
		t.Fatal("legacy wrapper changed media version")
	}
	failure := httptest.NewRecorder()
	JSON11.Response(failure, http.StatusOK, func() {})
	if failure.Code != http.StatusInternalServerError || failure.Header().Get("X-Amzn-ErrorType") != "InternalFailure" {
		t.Fatalf("encoding failure emitted a success: %d %s", failure.Code, failure.Body.String())
	}
}

func TestQueryErrorNamespaceFaultSideAndRequestID(t *testing.T) {
	for _, test := range []struct {
		namespace string
		status    int
		faultSide string
	}{
		{SNSNamespace, 400, "Sender"}, {SQSNamespace, 500, "Receiver"},
		{SESNamespace, 503, "Receiver"}, {"", 400, "Sender"},
	} {
		response := httptest.NewRecorder()
		response.Header().Set("X-Amzn-RequestId", "owned-query-id")
		QueryError(response, test.status, test.namespace, "Error<&", "escaped <message> & details")
		var envelope struct {
			XMLName   xml.Name
			Error     struct{ Type, Code, Message string }
			RequestID string `xml:"RequestId"`
		}
		if err := xml.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
			t.Fatal(err)
		}
		if envelope.XMLName.Space != test.namespace || envelope.Error.Type != test.faultSide ||
			envelope.Error.Code != "Error<&" || envelope.Error.Message != "escaped <message> & details" ||
			envelope.RequestID != "owned-query-id" || response.Header().Get("X-Amzn-RequestId") != envelope.RequestID {
			t.Fatalf("invalid Query envelope: %+v headers=%v", envelope, response.Header())
		}
		if test.namespace == "" && strings.Contains(response.Body.String(), "xmlns=") {
			t.Fatal("unknown service invented a namespace")
		}
	}
	for version, want := range map[string]string{"2010-03-31": SNSNamespace, "2012-11-05": SQSNamespace, "2010-12-01": SESNamespace} {
		got, known := QueryNamespace(version)
		if !known || got != want {
			t.Fatalf("namespace version=%s got=%s known=%t", version, got, known)
		}
	}
	if namespace, known := QueryNamespace("unknown"); known || namespace != "" {
		t.Fatal("unknown version selected a service")
	}
	legacy := httptest.NewRecorder()
	XMLError(legacy, 400, "Legacy", "message")
	if !strings.Contains(legacy.Body.String(), SNSNamespace) {
		t.Fatal("legacy SNS wrapper changed")
	}
}

type budgetReader struct {
	remaining int
	consumed  int
}

func (reader *budgetReader) Read(out []byte) (int, error) {
	n := min(len(out), reader.remaining)
	for i := 0; i < n; i++ {
		out[i] = ' '
	}
	reader.remaining -= n
	reader.consumed += n
	return n, nil
}

func TestReadJSONBodyBoundsReadAndRejectsNullAndOverflowBudget(t *testing.T) {
	reader := &budgetReader{remaining: 1000}
	request := httptest.NewRequest(http.MethodPost, "/", nil)
	request.Body = io.NopCloser(reader)
	if _, err := ReadJSONBody(request, 8); err == nil || reader.consumed != 9 {
		t.Fatalf("read was not bounded: consumed=%d err=%v", reader.consumed, err)
	}
	for _, body := range []string{"null", " null ", "true", "\"object\""} {
		request = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		if _, err := ReadJSONBody(request, 64); err == nil {
			t.Fatalf("non-object accepted: %s", body)
		}
	}
	if _, err := ReadJSONBody(request, math.MaxInt64); err == nil {
		t.Fatal("overflowing caller budget accepted")
	}
}
