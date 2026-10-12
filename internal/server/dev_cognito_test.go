package server_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/lyeith/eventbus/internal/server"
	"github.com/stretchr/testify/require"
)

func TestDevCognitoPortKeepsNativeRoutesIndependent(t *testing.T) {
	services := composedServices(t)
	var calls []string
	services.CognitoDev = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		calls = append(calls, r.Method+" "+r.URL.RequestURI()+" "+string(body))
		w.WriteHeader(http.StatusNoContent)
	})
	router := server.New(services)
	path := "/__eventbus/dev/cognito/pools/us-east-1_local/triggers/CreateAuthChallenge/reload"
	result := request(router, "POST", path+"?probe=owned", "AWSCognitoIdentityProviderService.DeleteUserPool", "{}")
	require.Equal(t, http.StatusNoContent, result.Code)
	require.Equal(t, []string{"POST " + path + "?probe=owned {}"}, calls)

	result = request(router, "POST", "/", "AWSCognitoIdentityProviderService.ListUserPools", `{"MaxResults":1}`)
	require.Equal(t, http.StatusOK, result.Code, result.Body.String())
	require.JSONEq(t, `{"UserPools":[]}`, result.Body.String())
	require.Len(t, calls, 1)

	result = request(router, "POST", "/2015-03-31/functions/cognito_trigger_1/invocations", "", "{}")
	require.Equal(t, http.StatusNotFound, result.Code)
	require.Equal(t, "ResourceNotFoundException", result.Header().Get("X-Amzn-ErrorType"))
	require.Len(t, calls, 1, "the dev port cannot expose private native Invoke targets")

	result = request(router, "GET", "/__eventbus/dev/cognitox/pools/local", "", "")
	require.Equal(t, http.StatusMethodNotAllowed, result.Code)
	require.Len(t, calls, 1, "a lookalike namespace must not reach the dev port")
}

func TestDevCognitoPortAbsenceReturnsNotFound(t *testing.T) {
	router := server.New(composedServices(t))
	for _, method := range []string{"POST", "GET", "DELETE"} {
		result := httptest.NewRecorder()
		router.ServeHTTP(result, httptest.NewRequest(method,
			"/__eventbus/dev/cognito/pools/local/triggers/CreateAuthChallenge/reload", strings.NewReader("{}")))
		require.Equal(t, http.StatusNotFound, result.Code)
	}
	result := request(router, "POST", "/", "AWSCognitoIdentityProviderService.ListUserPools", `{"MaxResults":1}`)
	require.Equal(t, http.StatusOK, result.Code, result.Body.String())
}
