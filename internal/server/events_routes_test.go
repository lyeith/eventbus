package server_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/lyeith/eventbus/internal/server"
	"github.com/stretchr/testify/require"
)

type eventsActionRecorder struct{ calls []string }

func (port *eventsActionRecorder) ServeAction(w http.ResponseWriter, r *http.Request, operation string) {
	port.calls = append(port.calls, operation)
	w.Header().Set("X-Service-Owner", "events")
	w.WriteHeader(http.StatusAccepted)
}

func TestEventsDispatcherSelectsExactNativeTargetPrefix(t *testing.T) {
	port := &eventsActionRecorder{}
	router := server.New(server.Services{Events: port})
	for _, operation := range []string{"PutRule", "DescribeRule", "PutTargets", "DisableRule", "RemoveTargets", "DeleteRule", "NoSuchOperation"} {
		request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
		request.Header.Set("X-Amz-Target", "AWSEvents."+operation)
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		require.Equal(t, http.StatusAccepted, response.Code)
		require.Equal(t, "events", response.Header().Get("X-Service-Owner"))
		require.Equal(t, operation, port.calls[len(port.calls)-1])
	}
	count := len(port.calls)
	for _, target := range []string{"AWSEvents", "AWSEventss.PutRule", "Other.PutRule"} {
		request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
		request.Header.Set("X-Amz-Target", target)
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		require.Equal(t, http.StatusBadRequest, response.Code)
	}
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("X-Amz-Target", "AWSEvents.PutRule")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	require.Equal(t, http.StatusMethodNotAllowed, response.Code)
	require.Equal(t, "application/x-amz-json-1.1", response.Header().Get("Content-Type"))
	require.Len(t, port.calls, count)
}
