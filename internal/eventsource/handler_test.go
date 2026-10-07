package eventsource

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/server"

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

func nativeMappingRequest(t *testing.T, client *http.Client, method, url, body string) (*http.Response, []byte) {
	t.Helper()
	request, err := http.NewRequest(method, url, bytes.NewBufferString(body))
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	assert.NotEmpty(t, response.Header.Get("X-Amzn-RequestId"))
	assert.Empty(t, response.Header.Get("Location"), "native operations must not redirect")
	return response, payload
}

func TestNativeCreateGetDeleteWire(t *testing.T) {
	for _, collection := range []string{mappingPath, mappingPath + "/"} {
		t.Run(collection, func(t *testing.T) {
			s := newTestService(t, newFakeQueue(), fakeInvoker{})
			serving := httptest.NewServer(server.New(server.Services{EventSources: NewHandler(s)}))
			defer serving.Close()
			client := serving.Client()
			client.Timeout = 3 * time.Second
			client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
			body := `{"EventSourceArn":"` + sourceARN + `","FunctionName":"worker:live","BatchSize":5,"ScalingConfig":{"MaximumConcurrency":2},"Enabled":false,"MaximumBatchingWindowInSeconds":0,"FunctionResponseTypes":[]}`
			created, createdBody := nativeMappingRequest(t, client, http.MethodPost, serving.URL+collection, body)
			require.Equal(t, 202, created.StatusCode, string(createdBody))
			var mapping Mapping
			require.NoError(t, json.Unmarshal(createdBody, &mapping))
			assert.Equal(t, "Disabled", mapping.State)
			assert.Equal(t, targetARN, mapping.FunctionARN)
			assert.Equal(t, 5, mapping.BatchSize)
			require.NotNil(t, mapping.ScalingConfig)
			assert.Equal(t, 2, *mapping.ScalingConfig.MaximumConcurrency)
			assert.Contains(t, string(createdBody), `"MaximumBatchingWindowInSeconds":0`)
			resource := serving.URL + mappingPath + "/" + mapping.UUID
			got, gotBody := nativeMappingRequest(t, client, http.MethodGet, resource, "")
			require.Equal(t, 200, got.StatusCode)
			assert.JSONEq(t, string(createdBody), string(gotBody))
			deleted, deletedBody := nativeMappingRequest(t, client, http.MethodDelete, resource, "")
			require.Equal(t, 202, deleted.StatusCode)
			assert.Contains(t, string(deletedBody), `"State":"Deleting"`)
			absent, _ := nativeMappingRequest(t, client, http.MethodGet, resource, "")
			assert.Equal(t, 404, absent.StatusCode)
			assert.Equal(t, "ResourceNotFoundException", absent.Header.Get("X-Amzn-ErrorType"))
		})
	}
}

func TestNativeCollectionNeighborsRemainUnsupported(t *testing.T) {
	s := newTestService(t, newFakeQueue(), fakeInvoker{})
	serving := httptest.NewServer(server.New(server.Services{EventSources: NewHandler(s)}))
	defer serving.Close()
	client := serving.Client()
	client.Timeout = 3 * time.Second
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	body := `{"EventSourceArn":"` + sourceARN + `","FunctionName":"worker:live","BatchSize":5,"Enabled":false}`
	for _, selection := range []struct{ method, path string }{
		{http.MethodPost, mappingPath + "//"},
		{http.MethodPost, mappingPath + "/./"},
		{http.MethodPost, mappingPath + "/../"},
		{http.MethodPost, mappingPath + "/123"},
		{http.MethodGet, mappingPath},
		{http.MethodGet, mappingPath + "/"},
		{http.MethodPut, mappingPath},
		{http.MethodPut, mappingPath + "/"},
		{http.MethodDelete, mappingPath + "/"},
	} {
		t.Run(selection.method+" "+selection.path, func(t *testing.T) {
			response, payload := nativeMappingRequest(t, client, selection.method, serving.URL+selection.path, body)
			assert.Equal(t, 400, response.StatusCode, string(payload))
			assert.Equal(t, "InvalidParameterValueException", response.Header.Get("X-Amzn-ErrorType"))
		})
	}
}

func TestUnsupportedSettingsNeverSilentlyAccepted(t *testing.T) {
	s := newTestService(t, newFakeQueue(), fakeInvoker{})
	h := NewHandler(s)
	for _, collection := range []string{mappingPath, mappingPath + "/"} {
		for _, selection := range []string{
			`"ScalingConfig":{"MaximumConcurrency":1}`,
			`"ScalingConfig":{"MaximumConcurrency":1001}`,
			`"ScalingConfig":{"MaximumConcurrency":2,"Unknown":true}`,
			`"FilterCriteria":{"Filters":[{"Pattern":"{}"}]}`,
			`"ProvisionedPollerConfig":{"MinimumPollers":2}`,
			`"MaximumRetryAttempts":3`,
			`"Tags":{"owner":"fixture"}`,
			`"StartingPosition":"LATEST"`,
			`"dev_poll_interval":1`,
		} {
			t.Run(collection+" "+selection, func(t *testing.T) {
				body := `{"EventSourceArn":"` + sourceARN + `","FunctionName":"worker:live","BatchSize":1,` + selection + `}`
				got := requestMapping(t, h, http.MethodPost, collection, body)
				assert.Equal(t, 400, got.Code)
				assert.Equal(t, "InvalidParameterValueException", got.Header().Get("X-Amzn-ErrorType"))
			})
		}
		for _, body := range []string{"[]", "null", `{}`, `{"BatchSize":1} null`, `{"BatchSize":"1"}`, `{"BatchSize":1.2}`} {
			got := requestMapping(t, h, http.MethodPost, collection, body)
			assert.Equal(t, 400, got.Code, body)
		}
	}
	got := requestMapping(t, h, http.MethodPut, mappingPath+"/123", "{}")
	assert.Equal(t, 400, got.Code)
}

func TestNativeBatchAndScalingWire(t *testing.T) {
	s := newTestService(t, newFakeQueue(), fakeInvoker{})
	h := NewHandler(s)
	body := `{"EventSourceArn":"` + sourceARN + `","FunctionName":"worker:live","BatchSize":5,"Enabled":false,"ScalingConfig":{"MaximumConcurrency":2}}`
	created := requestMapping(t, h, http.MethodPost, mappingPath, body)
	require.Equal(t, 202, created.Code, created.Body.String())
	var mapping Mapping
	require.NoError(t, json.Unmarshal(created.Body.Bytes(), &mapping))
	assert.Equal(t, 5, mapping.BatchSize)
	require.NotNil(t, mapping.ScalingConfig)
	assert.Equal(t, 2, *mapping.ScalingConfig.MaximumConcurrency)
	got := requestMapping(t, h, http.MethodGet, mappingPath+"/"+mapping.UUID, "")
	require.Equal(t, 200, got.Code)
	assert.JSONEq(t, created.Body.String(), got.Body.String())
	_, err := s.Delete(t.Context(), mapping.UUID)
	require.NoError(t, err)
	defaults := requestMapping(t, h, http.MethodPost, mappingPath, `{"EventSourceArn":"`+sourceARN+`","FunctionName":"worker:live","Enabled":false,"ScalingConfig":{}}`)
	require.Equal(t, 202, defaults.Code, defaults.Body.String())
	assert.Contains(t, defaults.Body.String(), `"BatchSize":10`)
	assert.NotContains(t, defaults.Body.String(), `"ScalingConfig"`)
}
