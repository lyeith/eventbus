package eventsource

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/lyeith/eventbus/internal/awsprotocol"
	"github.com/lyeith/eventbus/internal/devquiescence"
	"github.com/lyeith/eventbus/internal/server"
	"github.com/stretchr/testify/require"
)

func TestMappingTagsNativeHTTP(t *testing.T) {
	s := newTestService(t, newFakeQueue(), fakeInvoker{})
	input := createInput()
	enabled := false
	input.Enabled = &enabled
	input.Tags = map[string]string{"team": "a", "retain": "value", "delete / me": ""}
	mapping, err := s.Create(t.Context(), input)
	require.NoError(t, err)
	input.Tags["team"] = "mutated by caller"
	edge := server.New(server.Services{EventSources: NewHandler(s)})
	tagPath := func(arn string) string {
		return awsprotocol.LambdaTagsPathPrefix + strings.ReplaceAll(url.PathEscape(arn), ":", "%3A")
	}
	path := tagPath(mapping.EventSourceMappingARN)
	request := func(method, path, body string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		response := httptest.NewRecorder()
		edge.ServeHTTP(response, req)
		require.NotEmpty(t, response.Header().Get("X-Amzn-RequestId"))
		return response
	}
	readTags := func() map[string]string {
		t.Helper()
		got := request(http.MethodGet, path, "")
		require.Equal(t, http.StatusOK, got.Code, got.Body.String())
		var output struct{ Tags map[string]string }
		require.NoError(t, json.Unmarshal(got.Body.Bytes(), &output))
		return output.Tags
	}
	require.Equal(t, map[string]string{"team": "a", "retain": "value", "delete / me": ""}, readTags())

	response := request(http.MethodPost, path, `{"Tags":{"team":"b","東京":"新しい値"}}`)
	require.Equal(t, http.StatusNoContent, response.Code, response.Body.String())
	require.Empty(t, response.Body.String())
	require.Equal(t, "b", readTags()["team"])

	query := url.Values{"tagKeys": {"delete / me", "retain", "absent"}}
	response = request(http.MethodDelete, path+"?"+query.Encode(), "")
	require.Equal(t, http.StatusNoContent, response.Code, response.Body.String())
	require.Empty(t, response.Body.String())
	want := map[string]string{"team": "b", "東京": "新しい値"}
	require.Equal(t, want, readTags())

	for _, body := range []string{`{"Tags":null}`, `{"Tags":{"team":null}}`, `{"Tags":{"AWS:reserved":"x"}}`, `{"Tags":{"team":"c"}} {}`} {
		response := request(http.MethodPost, path, body)
		require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
		require.Equal(t, "InvalidParameterValueException", response.Header().Get("X-Amzn-ErrorType"))
	}
	response = request(http.MethodDelete, path+"?tagKeys=team&tagKeys=", "")
	require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
	require.Equal(t, want, readTags(), "invalid operations cannot change a valid tag")
	foreign := tagPath(strings.Replace(mapping.EventSourceMappingARN, ":000000000000:", ":100000000000:", 1))
	require.NotEqual(t, path, foreign)
	response = request(http.MethodGet, foreign, "")
	require.Equal(t, http.StatusNotFound, response.Code, response.Body.String())

	fenced := httptest.NewRecorder()
	devquiescence.WriteAdmissionError(fenced, httptest.NewRequest(http.MethodPost, path, nil), devquiescence.ErrFenced)
	require.Equal(t, http.StatusServiceUnavailable, fenced.Code)
	require.Equal(t, "ServiceException", fenced.Header().Get("X-Amzn-ErrorType"))
	require.Equal(t, "application/json", fenced.Header().Get("Content-Type"))

	_, err = s.Delete(t.Context(), mapping.UUID)
	require.NoError(t, err)
	response = request(http.MethodGet, path, "")
	require.Equal(t, http.StatusNotFound, response.Code, response.Body.String())
}

func TestMappingTagLimitsApplyToMergedState(t *testing.T) {
	s := newTestService(t, newFakeQueue(), fakeInvoker{})
	input := createInput()
	enabled := false
	input.Enabled = &enabled
	input.Tags = make(map[string]string, maxMappingTags)
	for index := range maxMappingTags {
		input.Tags[fmt.Sprintf("key%d", index)] = ""
	}
	mapping, err := s.Create(t.Context(), input)
	require.NoError(t, err)
	arn := mapping.EventSourceMappingARN

	err = s.TagResource(arn, map[string]string{"key0": "should not overwrite", "extra": ""})
	assertCode(t, err, "InvalidParameterValueException", http.StatusBadRequest)
	tags, err := s.ListTags(arn)
	require.NoError(t, err)
	require.Equal(t, input.Tags, tags)
	tags["key0"] = "mutated by caller"

	require.NoError(t, s.TagResource(arn, map[string]string{"key1": strings.Repeat("界", 256)}))
	tags, err = s.ListTags(arn)
	require.NoError(t, err)
	require.Empty(t, tags["key0"])
	require.Equal(t, strings.Repeat("界", 256), tags["key1"])

	input.Tags = map[string]string{strings.Repeat("界", 129): "too long"}
	_, err = s.Create(t.Context(), input)
	assertCode(t, err, "InvalidParameterValueException", http.StatusBadRequest)
}
