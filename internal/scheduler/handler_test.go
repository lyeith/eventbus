package scheduler

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func schedulerRequest(t *testing.T, handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
	assert.NotEmpty(t, response.Header().Get("X-Amzn-RequestId"), "SDK responses need AWS request metadata")
	return response
}

func requireSchedulerWireError(t *testing.T, response *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	require.Equal(t, status, response.Code, response.Body.String())
	assert.Equal(t, code, response.Header().Get("X-Amzn-ErrorType"))
	var body map[string]any
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	assert.NotEmpty(t, body["message"])
	assert.Len(t, body, 1)
}

func TestSchedulerRESTCreateGetDeleteAndNativeErrorEnvelopes(t *testing.T) {
	service := testService(t, testInvoker{}, DevOptions{Groups: []string{"fixture"}})
	handler := NewHandler(service)
	input := testCreate("path-owned-name")
	input.GroupName = "fixture"
	data, err := json.Marshal(input)
	require.NoError(t, err)
	created := schedulerRequest(t, handler, http.MethodPost, "/schedules/path-owned-name", string(data))
	require.Equal(t, 200, created.Code, created.Body.String())
	var result map[string]string
	require.NoError(t, json.Unmarshal(created.Body.Bytes(), &result))
	assert.Equal(t, "arn:aws:scheduler:us-east-1:000000000000:schedule/fixture/path-owned-name", result["ScheduleArn"])
	retried := schedulerRequest(t, handler, http.MethodPost, "/schedules/path-owned-name", string(data))
	assert.JSONEq(t, created.Body.String(), retried.Body.String())
	readback := schedulerRequest(t, handler, http.MethodGet, "/schedules/path-owned-name?groupName=fixture", "")
	require.Equal(t, 200, readback.Code)
	var schedule Schedule
	require.NoError(t, json.Unmarshal(readback.Body.Bytes(), &schedule))
	assert.Equal(t, input.Name, schedule.Name)
	assert.Equal(t, input.GroupName, schedule.GroupName)
	assert.Equal(t, result["ScheduleArn"], schedule.Arn)
	assert.Equal(t, testTargetARN, schedule.Target.Arn)
	assert.Equal(t, *input.Target.Input, *schedule.Target.Input)
	assert.Equal(t, "UTC", schedule.ScheduleExpressionTimezone)
	assert.Equal(t, "NONE", schedule.ActionAfterCompletion)
	assert.Equal(t, "OFF", schedule.FlexibleTimeWindow.Mode)
	assert.NotZero(t, schedule.CreationDate)
	assert.Equal(t, schedule.CreationDate, schedule.LastModificationDate)
	wrongGroup := schedulerRequest(t, handler, http.MethodGet, "/schedules/path-owned-name", "")
	requireSchedulerWireError(t, wrongGroup, 404, "ResourceNotFoundException")
	input.Description = "conflicting request"
	data, err = json.Marshal(input)
	require.NoError(t, err)
	conflict := schedulerRequest(t, handler, http.MethodPost, "/schedules/path-owned-name", string(data))
	requireSchedulerWireError(t, conflict, 409, "ConflictException")
	deleted := schedulerRequest(t, handler, http.MethodDelete, "/schedules/path-owned-name?groupName=fixture&clientToken=delete-owned", "")
	require.Equal(t, 200, deleted.Code)
	assert.Empty(t, deleted.Body.String())
	deleteRetry := schedulerRequest(t, handler, http.MethodDelete, "/schedules/path-owned-name?groupName=fixture&clientToken=delete-owned", "")
	require.Equal(t, 200, deleteRetry.Code)
	missing := schedulerRequest(t, handler, http.MethodGet, "/schedules/path-owned-name?groupName=fixture", "")
	requireSchedulerWireError(t, missing, 404, "ResourceNotFoundException")
	missingDelete := schedulerRequest(t, handler, http.MethodDelete, "/schedules/path-owned-name?groupName=fixture", "")
	requireSchedulerWireError(t, missingDelete, 404, "ResourceNotFoundException")
}

func TestSchedulerRESTRejectsUnknownFieldsTrailingDataAndUnsupportedOperations(t *testing.T) {
	service := testService(t, testInvoker{}, DevOptions{})
	handler := NewHandler(service)
	input := testCreate("invalid")
	data, err := json.Marshal(input)
	require.NoError(t, err)
	unknownTop := append([]byte(nil), bytes.TrimSuffix(data, []byte("}"))...)
	unknownTop = append(unknownTop, []byte(",\"UnsupportedSetting\":true}")...)
	unknownTarget := strings.Replace(string(data), "\"RoleArn\":", "\"UnknownTargetSetting\":true,\"RoleArn\":", 1)
	for _, fixture := range []struct{ name, method, path, body string }{
		{"malformed", http.MethodPost, "/schedules/invalid", "{"},
		{"null", http.MethodPost, "/schedules/invalid", "null"},
		{"empty", http.MethodPost, "/schedules/invalid", ""},
		{"array", http.MethodPost, "/schedules/invalid", "[]"},
		{"unknown-top", http.MethodPost, "/schedules/invalid", string(unknownTop)},
		{"unknown-target", http.MethodPost, "/schedules/invalid", unknownTarget},
		{"trailing-object", http.MethodPost, "/schedules/invalid", string(data) + " {}"},
		{"trailing-invalid", http.MethodPost, "/schedules/invalid", string(data) + " nonsense"},
		{"body-limit", http.MethodPost, "/schedules/invalid", "{\"Description\":\"" + strings.Repeat("x", 2<<20) + "\"}"},
		{"unsupported-update", http.MethodPut, "/schedules/invalid", string(data)},
		{"unsupported-list", http.MethodGet, "/schedules", ""},
		{"invalid-path", http.MethodPost, "/schedules/invalid/extra", string(data)},
		{"invalid-query-group", http.MethodGet, "/schedules/invalid?groupName=bad%2Fgroup", ""},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			response := schedulerRequest(t, handler, fixture.method, fixture.path, fixture.body)
			requireSchedulerWireError(t, response, 400, "ValidationException")
			assert.Empty(t, service.entries)
			assert.Empty(t, service.creates)
		})
	}
}

func TestSchedulerRESTUnconfiguredAndPressureErrors(t *testing.T) {
	unconfigured := schedulerRequest(t, NewHandler(nil), http.MethodGet, "/schedules/owned", "")
	requireSchedulerWireError(t, unconfigured, 503, "InternalServerException")
	service := testService(t, testInvoker{}, DevOptions{MaxSchedules: 1})
	handler := NewHandler(service)
	for index, name := range []string{"first", "second"} {
		input := testCreate(name)
		data, err := json.Marshal(input)
		require.NoError(t, err)
		response := schedulerRequest(t, handler, http.MethodPost, "/schedules/"+name, string(data))
		if index == 0 {
			require.Equal(t, 200, response.Code)
		} else {
			requireSchedulerWireError(t, response, 402, "ServiceQuotaExceededException")
		}
	}
}

type schedulerGroupUnreadBody struct{ reads int }

func (body *schedulerGroupUnreadBody) Read([]byte) (int, error) {
	body.reads++
	return 0, io.EOF
}
func (*schedulerGroupUnreadBody) Close() error { return nil }

func TestSchedulerRESTOwnsExactGroupManagementRefusalBeforeBodyRead(t *testing.T) {
	service := testService(t, testInvoker{}, DevOptions{Groups: []string{"fixture"}})
	for _, test := range []struct {
		name    string
		handler http.Handler
	}{
		{"configured", NewHandler(service)}, {"unconfigured-service", NewHandler(nil)},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodHead} {
				for _, path := range []string{"/schedule-groups", "/schedule-groups/fixture", "/schedule-groups/fixture/extra", "/schedule-groups/"} {
					body := &schedulerGroupUnreadBody{}
					request := httptest.NewRequest(method, path, nil)
					request.Body = body
					response := httptest.NewRecorder()
					response.Header().Set("X-Amzn-RequestId", "actual-group-request")
					test.handler.ServeHTTP(response, request)
					requireSchedulerWireError(t, response, 400, "ValidationException")
					assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
					assert.Equal(t, "actual-group-request", response.Header().Get("X-Amzn-RequestId"))
					assert.JSONEq(t, `{"message":"Schedule group management is not supported"}`, response.Body.String())
					assert.Zero(t, body.reads, "unsupported group operation read its request body")
				}
			}
		})
	}
	assert.Empty(t, service.entries)
	assert.Empty(t, service.creates)
}
