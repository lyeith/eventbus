package app

import (
	"github.com/lyeith/eventbus/internal/devquiescence"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRetainedFenceRefusesLambdaControlsBeforeBodyOrMutation(t *testing.T) {
	owner := devquiescence.New()
	_, err := owner.Quiesce(t.Context())
	require.NoError(t, err)
	called := false
	router := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true })
	for _, path := range []string{
		"/__eventbus/dev/lambda/warm",
		"/__eventbus/dev/lambda/invocations/request/worker-crash",
	} {
		for _, lane := range []devquiescence.Lane{devquiescence.Source, devquiescence.Callback} {
			body := &retainedUnreadBody{}
			method := http.MethodPut
			if path == "/__eventbus/dev/lambda/warm" {
				method = http.MethodPost
			}
			request := httptest.NewRequest(method, path, nil)
			request.Body = body
			response := httptest.NewRecorder()
			owner.Wrap(lane, router, retainedCleanupHandler(router)).ServeHTTP(response, request)
			require.Equal(t, http.StatusServiceUnavailable, response.Code)
			require.Zero(t, body.reads)
			require.False(t, called)
		}
	}
	require.True(t, owner.Snapshot().FixtureSafe)
}
