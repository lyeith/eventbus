package messaging

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/lyeith/eventbus/internal/awsprotocol"
	"github.com/stretchr/testify/require"
)

func TestUnknownQueryActionUsesIdentifiedNamespace(t *testing.T) {
	handler := NewHandler(NewBroker("us-east-1", "000000000000", 4100))
	for _, test := range []struct{ version, namespace string }{
		{"", ""}, {"unknown", ""}, {"2010-03-31", awsprotocol.SNSNamespace}, {"2012-11-05", awsprotocol.SQSNamespace},
	} {
		t.Run(test.version, func(t *testing.T) {
			form := url.Values{"Action": {"UnknownAction"}, "Version": {test.version}}
			r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
			r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			handler.ServeQuery(w, r, "UnknownAction")
			var body struct {
				XMLName   xml.Name
				RequestID string `xml:"RequestId"`
			}
			require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &body))
			require.Equal(t, test.namespace, body.XMLName.Space)
			require.NotEmpty(t, body.RequestID)
			require.Equal(t, w.Header().Get("X-Amzn-RequestId"), body.RequestID)
			require.Equal(t, http.StatusBadRequest, w.Code)
		})
	}
}

func TestSQSQuerySuccessSharesResponseRequestID(t *testing.T) {
	handler := NewHandler(NewBroker("us-east-1", "000000000000", 4100))
	form := url.Values{"Action": {"CreateQueue"}, "Version": {"2012-11-05"}, "QueueName": {"owned-wire"}}
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	handler.ServeQuery(w, r, "CreateQueue")
	var body struct {
		RequestID string `xml:"ResponseMetadata>RequestId"`
	}
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &body))
	require.Equal(t, http.StatusOK, w.Code)
	require.NotEmpty(t, body.RequestID)
	require.Equal(t, w.Header().Get("X-Amzn-RequestId"), body.RequestID)
}
