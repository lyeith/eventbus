package app

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
	"github.com/stretchr/testify/require"
)

func TestLambdaDestinationS3WritesNativeRecord(t *testing.T) {
	type put struct {
		method, path, query string
		body                []byte
		header              http.Header
		err                 error
	}
	requests := make(chan put, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		requests <- put{r.Method, r.URL.EscapedPath(), r.URL.RawQuery, body, r.Header.Clone(), err}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	destination, err := newLambdaDestinationS3(config{
		s3Endpoint: server.URL + "/tenant%2Fbucket?trace=a%2Bb", region: "us-east-1",
		s3AccessKey: "destination-test", s3SecretKey: "destination-secret", s3SessionToken: "destination-token",
	})
	require.NoError(t, err)
	defer destination.CloseIdleConnections()

	record := lambdaservice.AsyncInvocationRecord{
		Version:   "1.0",
		Timestamp: time.Date(2025, time.December, 2, 3, 4, 5, 0, time.FixedZone("UTC plus 8", 8*60*60)),
		RequestContext: lambdaservice.AsyncRequestContext{
			RequestID:   "aa929273-56e8-4d4c-abdc-67f41e206fe1",
			FunctionARN: "arn:aws:lambda:us-east-1:000000000000:function:example:alias",
			Condition:   "RetriesExhausted",
		},
		RequestPayload: json.RawMessage(`{"id":"sample"}`),
	}
	payload, err := json.Marshal(record)
	require.NoError(t, err)
	require.NoError(t, destination.Put(t.Context(), "local-destination", record, payload))
	actual := <-requests
	require.NoError(t, actual.err)
	require.Equal(t, http.MethodPut, actual.method)
	require.Equal(t, "/tenant%2Fbucket/local-destination/aws/lambda/async/example/2025/12/01/2025-12-01T19.04.05-aa929273-56e8-4d4c-abdc-67f41e206fe1", actual.path)
	require.Equal(t, "trace=a%2Bb", actual.query)
	require.Equal(t, payload, actual.body)
	require.Equal(t, "application/json", actual.header.Get("Content-Type"))
	require.Equal(t, "destination-token", actual.header.Get("X-Amz-Security-Token"))
	require.Contains(t, actual.header.Get("Authorization"), "Credential=destination-test/")
	require.Contains(t, actual.header.Get("Authorization"), "/us-east-1/s3/aws4_request")
}
