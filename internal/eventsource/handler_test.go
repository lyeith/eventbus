package eventsource

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func requestMapping(t *testing.T, h *Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	response := httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest(method, path, bytes.NewBufferString(body)))
	assert.NotEmpty(t, response.Header().Get("X-Amzn-RequestId"))
	return response
}

func TestNativeCreateGetDeleteWire(t *testing.T) {
	s := newTestService(t, newFakeQueue(), fakeInvoker{})
	h := NewHandler(s)
	body := `{"EventSourceArn":"` + sourceARN + `","FunctionName":"worker:live","BatchSize":1,"Enabled":false,"MaximumBatchingWindowInSeconds":0,"FunctionResponseTypes":[]}`
	created := requestMapping(t, h, http.MethodPost, mappingPath, body)
	require.Equal(t, 202, created.Code, created.Body.String())
	var mapping Mapping
	require.NoError(t, json.Unmarshal(created.Body.Bytes(), &mapping))
	assert.Equal(t, "Disabled", mapping.State)
	assert.Equal(t, targetARN, mapping.FunctionARN)
	assert.Contains(t, created.Body.String(), `"MaximumBatchingWindowInSeconds":0`)
	got := requestMapping(t, h, http.MethodGet, mappingPath+"/"+mapping.UUID, "")
	require.Equal(t, 200, got.Code)
	assert.JSONEq(t, created.Body.String(), got.Body.String())
	deleted := requestMapping(t, h, http.MethodDelete, mappingPath+"/"+mapping.UUID, "")
	require.Equal(t, 202, deleted.Code)
	assert.Contains(t, deleted.Body.String(), `"State":"Deleting"`)
	absent := requestMapping(t, h, http.MethodGet, mappingPath+"/"+mapping.UUID, "")
	assert.Equal(t, 404, absent.Code)
	assert.Equal(t, "ResourceNotFoundException", absent.Header().Get("X-Amzn-ErrorType"))
}

func TestUnsupportedSettingsNeverSilentlyAccepted(t *testing.T) {
	s := newTestService(t, newFakeQueue(), fakeInvoker{})
	h := NewHandler(s)
	for _, selection := range []string{
		`"ScalingConfig":{"MaximumConcurrency":2}`,
		`"FilterCriteria":{"Filters":[{"Pattern":"{}"}]}`,
		`"ProvisionedPollerConfig":{"MinimumPollers":2}`,
		`"MaximumRetryAttempts":3`,
		`"Tags":{"owner":"fixture"}`,
		`"StartingPosition":"LATEST"`,
		`"dev_poll_interval":1`,
	} {
		t.Run(selection, func(t *testing.T) {
			body := `{"EventSourceArn":"` + sourceARN + `","FunctionName":"worker:live","BatchSize":1,` + selection + `}`
			got := requestMapping(t, h, http.MethodPost, mappingPath, body)
			assert.Equal(t, 400, got.Code)
			assert.Equal(t, "InvalidParameterValueException", got.Header().Get("X-Amzn-ErrorType"))
		})
	}
	for _, body := range []string{"[]", "null", `{}`, `{"BatchSize":1} null`, `{"BatchSize":"1"}`, `{"BatchSize":1.2}`} {
		got := requestMapping(t, h, http.MethodPost, mappingPath, body)
		assert.Equal(t, 400, got.Code, body)
	}
	got := requestMapping(t, h, http.MethodPut, mappingPath+"/123", "{}")
	assert.Equal(t, 400, got.Code)
}
