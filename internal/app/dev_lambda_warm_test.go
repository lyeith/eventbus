package app

import (
	"github.com/lyeith/eventbus/internal/devquiescence"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRetainedFenceRefusesWarmTuningBeforeBodyOrMutation(t *testing.T) {
	owner := devquiescence.New()
	_, err := owner.Quiesce(t.Context())
	require.NoError(t, err)
	called := false
	router := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true })
	for _, lane := range []devquiescence.Lane{devquiescence.Source, devquiescence.Callback} {
		body := &retainedUnreadBody{}
		request := httptest.NewRequest("POST", "/__eventbus/dev/lambda/warm", nil)
		request.Body = body
		response := httptest.NewRecorder()
		owner.Wrap(lane, router, retainedCleanupHandler(router)).ServeHTTP(response, request)
		require.Equal(t, http.StatusServiceUnavailable, response.Code)
		require.Zero(t, body.reads)
		require.False(t, called)
	}
	require.True(t, owner.Snapshot().FixtureSafe)
}
