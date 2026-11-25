package app

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
	"github.com/stretchr/testify/require"
)

func TestLocalFixtureHTTPTrustsOnlySelectedEndpointAndRefusesRedirect(t *testing.T) {
	var calls atomic.Int32
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/followed" {
			calls.Add(1)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		http.Redirect(w, r, "/followed", http.StatusTemporaryRedirect)
	}))
	defer receiver.Close()
	client, err := newFixtureHTTPClient("")
	require.NoError(t, err)
	defer client.CloseIdleConnections()
	transport, err := newSNSFixtureHTTP(receiver.URL+"/owned", client)
	require.NoError(t, err)
	_, err = transport.Post(t.Context(), receiver.URL+"/unselected", nil, "private")
	require.Error(t, err)
	status, err := transport.Post(t.Context(), receiver.URL+"/owned", nil, "fixture")
	require.NoError(t, err)
	require.Equal(t, http.StatusTemporaryRedirect, status)
	require.Zero(t, calls.Load())
	for _, origin := range []string{"https://example.invalid/hook", "http://10.0.0.1/", "http://user@localhost/", "http://localhost/#secret"} {
		_, err := newSNSFixtureHTTP(origin, client)
		require.Error(t, err, origin)
	}
}

func TestLocalLambdaEventBridgeDestinationUsesNativeEntry(t *testing.T) {
	type observation struct {
		Header http.Header
		Body   []byte
	}
	requests := make(chan observation, 1)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		requests <- observation{r.Header.Clone(), body}
		_, _ = io.WriteString(w, `{"FailedEntryCount":0,"Entries":[{"EventId":"owned"}]}`)
	}))
	defer receiver.Close()
	client, err := newFixtureHTTPClient("")
	require.NoError(t, err)
	defer client.CloseIdleConnections()
	endpoint, err := fixtureHTTPURL(receiver.URL)
	require.NoError(t, err)
	adapter := lambdaDestinations{eventsEndpoint: endpoint, client: client, region: "eu-west-1"}
	bus := "arn:aws:events:eu-west-1:000000000000:event-bus/default"
	from := "arn:aws:lambda:eu-west-1:000000000000:function:source:$LATEST"
	record := lambdaservice.AsyncInvocationRecord{Version: "1.0", Timestamp: time.Date(2026, 11, 25, 1, 2, 3, 0, time.UTC),
		RequestContext: lambdaservice.AsyncRequestContext{FunctionARN: from, Condition: "RetriesExhausted"},
		RequestPayload: json.RawMessage(`{"owned":true}`)}
	require.NoError(t, adapter.DeliverAsyncDestination(t.Context(), bus, record))
	received := <-requests
	require.Equal(t, "AWSEvents.PutEvents", received.Header.Get("X-Amz-Target"))
	require.True(t, strings.Contains(received.Header.Get("Authorization"), "/eu-west-1/events/aws4_request"))
	var result struct {
		Entries []struct {
			Source, DetailType, EventBusName, Detail string
			Resources                                []string
		}
	}
	require.NoError(t, json.Unmarshal(received.Body, &result))
	require.Len(t, result.Entries, 1)
	item := result.Entries[0]
	require.Equal(t, "lambda", item.Source)
	require.Equal(t, bus, item.EventBusName)
	require.Equal(t, "Lambda Function Invocation Result - Failure", item.DetailType)
	require.Equal(t, []string{from, bus}, item.Resources)
	expected, err := json.Marshal(record)
	require.NoError(t, err)
	require.JSONEq(t, string(expected), item.Detail)
}
