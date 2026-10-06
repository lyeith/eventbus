package cognito

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// A missing store must retain protocol errors without falling through to an
// unrelated emulator or entering an operation against a nil database handle.
func TestHandlerMissingStorePreservesProtocolErrors(t *testing.T) {
	handler := NewHandler(nil, Options{})
	for _, scenario := range []struct {
		name, action, code string
		status             int
	}{
		{"supported action", "AdminCreateUser", "InternalErrorException", http.StatusInternalServerError},
		{"unknown action", "MadeUpOp", "InvalidAction", http.StatusBadRequest},
		{"missing action", "", "InvalidAction", http.StatusBadRequest},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			handler.ServeAction(recorder, httptest.NewRequest(http.MethodPost, "/", nil), scenario.action)
			require.Equal(t, scenario.status, recorder.Code)
			require.Equal(t, "application/x-amz-json-1.1", recorder.Header().Get("Content-Type"))
			var body map[string]string
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
			require.Equal(t, scenario.code, body["__type"])
		})
	}
	recorder := httptest.NewRecorder()
	handler.ServeJWKS(recorder, httptest.NewRequest(http.MethodGet, "/pool/.well-known/jwks.json", nil))
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
}
