package devquiescence

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/lyeith/eventbus/internal/awsprotocol"
	"github.com/stretchr/testify/require"
)

type unreadAdmissionBody struct{}

func (unreadAdmissionBody) Read([]byte) (int, error) { panic("fenced request body was read") }
func (unreadAdmissionBody) Close() error             { return nil }

func TestAdmissionErrorsUseNativeWireWithoutReadingBodies(t *testing.T) {
	for _, scenario := range []struct {
		name, path, target, protocol, code, namespace string
		form                                          url.Values
	}{
		{name: "SNS Query fallback", path: "/", protocol: "query", code: "ServiceUnavailable"},
		{name: "SNS Query URL namespace", path: "/?Version=2010-03-31", protocol: "query", code: "ServiceUnavailable", namespace: awsprotocol.SNSNamespace},
		{name: "SQS Query parsed namespace", path: "/queue/example", protocol: "query", code: "ServiceUnavailable", namespace: awsprotocol.SQSNamespace, form: url.Values{"Version": {"2012-11-05"}}},
		{name: "SES Query parsed namespace", path: "/", protocol: "query", code: "ServiceUnavailable", namespace: awsprotocol.SESNamespace, form: url.Values{"Version": {"2010-12-01"}}},
		{name: "AmazonSQS JSON", path: "/", target: "AmazonSQS.SendMessage", protocol: string(awsprotocol.JSON10), code: "ServiceUnavailable"},
		{name: "Cognito JSON", path: "/", target: "AWSCognitoIdentityProviderService.InitiateAuth", protocol: string(awsprotocol.JSON10), code: "ServiceUnavailable"},
		{name: "Firehose JSON", path: "/", target: "Firehose_20150804.PutRecord", protocol: string(awsprotocol.JSON11), code: "ServiceUnavailable"},
		{name: "SSM JSON", path: "/", target: "AmazonSSM.PutParameter", protocol: string(awsprotocol.JSON11), code: "ServiceUnavailable"},
		{name: "Secrets JSON", path: "/", target: "secretsmanager.RotateSecret", protocol: string(awsprotocol.JSON11), code: "ServiceUnavailable"},
		{name: "Lambda REST precedes target", path: "/2015-03-31/functions/test/invocations", target: "AmazonSQS.SendMessage", protocol: "rest", code: "ServiceException"},
		{name: "Lambda mappings REST", path: "/2015-03-31/event-source-mappings", protocol: "rest", code: "ServiceException"},
		{name: "Scheduler REST", path: "/schedules/example", protocol: "rest", code: "InternalServerException"},
		{name: "Scheduler groups REST", path: "/schedule-groups", protocol: "rest", code: "InternalServerException"},
		{name: "SES REST", path: "/v2/email/outbound-emails", protocol: "rest", code: "ServiceUnavailable"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, scenario.path, nil)
			request.Body = unreadAdmissionBody{}
			request.Form = scenario.form
			request.Header.Set("X-Amz-Target", scenario.target)
			response := httptest.NewRecorder()
			response.Header().Set("X-Amzn-RequestId", "owned-admission-id")
			WriteAdmissionError(response, request, ErrFenced)
			require.Equal(t, http.StatusServiceUnavailable, response.Code)
			require.Equal(t, "owned-admission-id", response.Header().Get("X-Amzn-RequestId"))
			if scenario.protocol == "query" {
				var envelope struct {
					XMLName   xml.Name
					Error     struct{ Type, Code, Message string }
					RequestID string `xml:"RequestId"`
				}
				require.NoError(t, xml.Unmarshal(response.Body.Bytes(), &envelope), "AWS Query parsers require an XML error envelope")
				require.Equal(t, "ErrorResponse", envelope.XMLName.Local)
				require.Equal(t, scenario.namespace, envelope.XMLName.Space)
				require.Equal(t, "Receiver", envelope.Error.Type)
				require.Equal(t, scenario.code, envelope.Error.Code)
				require.Equal(t, ErrFenced.Error(), envelope.Error.Message)
				require.Equal(t, "owned-admission-id", envelope.RequestID)
				return
			}
			var envelope map[string]string
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &envelope))
			require.Equal(t, scenario.code, response.Header().Get("X-Amzn-ErrorType"))
			if scenario.protocol == "rest" {
				require.Equal(t, "application/json", response.Header().Get("Content-Type"))
				require.Equal(t, map[string]string{"message": ErrFenced.Error()}, envelope)
				return
			}
			require.Equal(t, scenario.protocol, response.Header().Get("Content-Type"))
			require.Equal(t, map[string]string{"__type": scenario.code, "Message": ErrFenced.Error()}, envelope)
		})
	}
}

func TestFencedSNSWrapperReturnsQueryErrorAndControlsKeepJSON(t *testing.T) {
	c := New()
	_, err := c.Quiesce(t.Context())
	require.NoError(t, err)
	wrapper := c.Wrap(Source, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("fenced source dispatched") }), nil)
	request := httptest.NewRequest(http.MethodPost, "/", nil)
	request.Body = unreadAdmissionBody{}
	response := httptest.NewRecorder()
	wrapper.ServeHTTP(response, request)
	require.Equal(t, http.StatusServiceUnavailable, response.Code)
	require.Contains(t, response.Body.String(), "<Code>ServiceUnavailable</Code>")
	control := httptest.NewRequest(http.MethodPost, ControlPath+"/quiesce", strings.NewReader(`{"timeout_ms":0}`))
	control.Header.Set("X-Amz-Target", "AmazonSQS.SendMessage")
	response = httptest.NewRecorder()
	NewHandler(c).ServeHTTP(response, control)
	require.Equal(t, http.StatusBadRequest, response.Code)
	require.Equal(t, "application/json", response.Header().Get("Content-Type"))
	var envelope map[string]string
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &envelope))
	require.Equal(t, map[string]string{"error": "timeout_ms must be 1..300000"}, envelope)
}

func TestHeldCleanupEvidenceFailureIsStickyBeforeEnvelopeRelease(t *testing.T) {
	var c *Coordinator
	poisoned := false
	c = New(func() error {
		if poisoned {
			require.Equal(t, 1, c.Snapshot().CleanupEnvelopes, "evidence is checked outside the lock while cleanup remains owned")
			return errors.New("private capture sink failure")
		}
		return nil
	})
	held, err := c.Quiesce(t.Context())
	require.NoError(t, err)
	require.True(t, held.FixtureSafe)
	wrapper := c.Wrap(Callback, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("held cleanup became work") }), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		poisoned = true
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("native-cleanup-error"))
	}))
	response := httptest.NewRecorder()
	wrapper.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/exact-delete", nil))
	require.Equal(t, http.StatusBadRequest, response.Code)
	require.Equal(t, "native-cleanup-error", response.Body.String(), "ownership evidence does not rewrite the native cleanup response")
	snapshot := c.Snapshot()
	require.Zero(t, snapshot.CleanupEnvelopes)
	require.False(t, snapshot.FixtureSafe)
	require.NotEmpty(t, snapshot.EvidenceFailure)
	encoded, err := json.Marshal(snapshot)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "private capture sink failure")
	_, err = c.Resume(held.Generation)
	require.ErrorIs(t, err, ErrEvidence)
	_, err = c.Quiesce(t.Context())
	require.ErrorIs(t, err, ErrEvidence)
	require.NotEqual(t, Shutdown, c.Snapshot().State, "failed evidence fences the owner without closing it")
}
