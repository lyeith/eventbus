package events

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func wire(t *testing.T, h *Handler, operation, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeAction(w, r, operation)
	assert.Equal(t, "application/x-amz-json-1.1", w.Header().Get("Content-Type"))
	assert.NotEmpty(t, w.Header().Get("X-Amzn-RequestId"))
	return w
}
func TestEventsAWSJSONCRUDAndNativeErrors(t *testing.T) {
	s := serviceFor(t, newClock(), invoker{}, DevOptions{})
	h := NewHandler(s)
	created := wire(t, h, "PutRule", `{"Name":"owned","ScheduleExpression":"rate(1 minute)","State":"DISABLED"}`)
	require.Equal(t, 200, created.Code, created.Body.String())
	assert.JSONEq(t, `{"RuleArn":"arn:aws:events:us-east-1:000000000000:rule/owned"}`, created.Body.String())
	template := `{"id":<id>,"event":<aws.events.event.json>}`
	encoded, err := json.Marshal(PutTargetsInput{Rule: "owned", Targets: []Target{{Id: "target", Arn: testARN, InputTransformer: &InputTransformer{InputPathsMap: map[string]string{"id": "$.id"}, InputTemplate: template}}}})
	require.NoError(t, err)
	targets := wire(t, h, "PutTargets", string(encoded))
	require.Equal(t, 200, targets.Code, targets.Body.String())
	assert.JSONEq(t, `{"FailedEntries":[],"FailedEntryCount":0}`, targets.Body.String())
	for _, operation := range []string{"DescribeRule", "EnableRule", "DisableRule"} {
		w := wire(t, h, operation, `{"Name":"owned"}`)
		require.Equal(t, 200, w.Code, w.Body.String())
	}
	listing := wire(t, h, "ListTargetsByRule", `{"Rule":"owned"}`)
	require.Equal(t, 200, listing.Code)
	var out ListTargetsOutput
	require.NoError(t, json.Unmarshal(listing.Body.Bytes(), &out))
	require.Len(t, out.Targets, 1)
	assert.Equal(t, template, out.Targets[0].InputTransformer.InputTemplate)
	for _, test := range []struct{ operation, body, code string }{
		{"PutRule", `{"Name":"unsupported","ScheduleExpression":"cron(0 0 * * ? *)"}`, "ValidationException"},
		{"PutRule", `{"Name":"unsupported","ScheduleExpression":"rate(1 minute)","UnsupportedOption":true}`, "ValidationException"},
		{"PutRule", `{"Name":"unsupported","ScheduleExpression":"rate(1 minute)"} {}`, "ValidationException"},
		{"PutRule", `null`, "ValidationException"},
		{"DescribeRule", `{"Name":"absent"}`, "ResourceNotFoundException"},
		{"PutEvents", `{"Entries":[]}`, "UnknownOperationException"},
		{"DeleteRule", `{"Name":"owned"}`, "ValidationException"},
	} {
		w := wire(t, h, test.operation, test.body)
		require.Equal(t, 400, w.Code, w.Body.String())
		assert.Equal(t, test.code, w.Header().Get("X-Amzn-ErrorType"))
		var body map[string]string
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		assert.Equal(t, test.code, body["__type"])
		assert.NotEmpty(t, body["Message"])
	}
	removed := wire(t, h, "RemoveTargets", `{"Rule":"owned","Ids":["target"]}`)
	require.Equal(t, 200, removed.Code)
	deleted := wire(t, h, "DeleteRule", `{"Name":"owned"}`)
	require.Equal(t, 200, deleted.Code)
	missing := wire(t, NewHandler(nil), "ListRules", "{}")
	require.Equal(t, 503, missing.Code)
	assert.Equal(t, "InternalException", missing.Header().Get("X-Amzn-ErrorType"))
}
