package events

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func transformStringPointer(value string) *string { return &value }

func transformFixtureEvent() []byte {
	return []byte(`{"version":"0","id":"evt-original","detail-type":"Scheduled Event","source":"aws.events","account":"000000000000","time":"2026-10-14T01:02:03Z","region":"us-east-1","resources":["arn:aws:events:us-east-1:000000000000:rule/scheduled"],"detail":{"instance-id":"i-123","nested":{"state_name":"RUNNING"},"items":[{"v":1},{"v":2}],"slash/key":"slash-value","missingNull":null}}`)
}

func TestTransformTargetDefaultAndConstantPreserveExactBytes(t *testing.T) {
	event := append([]byte(" \n"), transformFixtureEvent()...)
	got, err := transformTarget(Target{}, event, "rule-arn", "scheduled", time.Time{})
	if err != nil || !bytes.Equal(got, event) {
		t.Fatalf("identity: got %q, err %v", got, err)
	}
	got[0] = 'x'
	if event[0] == 'x' {
		t.Fatal("identity result aliases caller event")
	}
	constant := "  {\"only\": true}\n"
	got, err = transformTarget(Target{Input: &constant}, event, "rule-arn", "scheduled", time.Time{})
	if err != nil || string(got) != constant {
		t.Fatalf("constant: got %q, err %v", got, err)
	}
}

func TestTransformTargetInputPaths(t *testing.T) {
	for _, test := range []struct{ path, want string }{
		{"$.detail.instance-id", `"i-123"`},
		{"$.detail.nested.state_name", `"RUNNING"`},
		{"$.detail.slash/key", `"slash-value"`},
		{"$.resources[0]", `"arn:aws:events:us-east-1:000000000000:rule/scheduled"`},
		{"$.detail['instance-id']", `"i-123"`},
		{`$.detail["slash/key"]`, `"slash-value"`},
		{"$.detail.items[*].v", `[1,2]`},
		{"$.detail.items.*.v", `[1,2]`},
		{"$.detail.items[9]", `null`},
		{"$.absent", `null`},
		{"$.detail.missingNull", `null`},
	} {
		t.Run(test.path, func(t *testing.T) {
			got, err := transformTarget(Target{InputPath: &test.path}, transformFixtureEvent(), "arn", "rule", time.Time{})
			if err != nil || string(got) != test.want {
				t.Fatalf("got %s, err %v; want %s", got, err, test.want)
			}
		})
	}
}

func TestTransformTargetReservedIdentityAndValues(t *testing.T) {
	ingestedAt := time.Date(2026, 10, 14, 3, 4, 5, 123000000, time.FixedZone("test", 3600))
	target := Target{InputTransformer: &InputTransformer{
		InputPathsMap: map[string]string{"id": "$.id", "timestamp": "$.time", "resource": "$.resources[0]", "detail": "$.detail"},
		InputTemplate: `{"id":<id>,"time":<timestamp>,"resource":<resource>,"detail":<detail>,"arn":<aws.events.rule-arn>,"rule":<aws.events.rule-name>,"ingested":<aws.events.event.ingestion-time>,"event":<aws.events.event>,"full":<aws.events.event.json>}`,
	}}
	event := transformFixtureEvent()
	before := bytes.Clone(event)
	got, err := transformTarget(target, event, "arn:aws:events:us-east-1:000000000000:rule/scheduled", "scheduled", ingestedAt)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(got, &fields); err != nil {
		t.Fatalf("invalid transformed payload %s: %v", got, err)
	}
	for key, want := range map[string]string{"id": `"evt-original"`, "time": `"2026-10-14T01:02:03Z"`, "resource": `"arn:aws:events:us-east-1:000000000000:rule/scheduled"`, "arn": `"arn:aws:events:us-east-1:000000000000:rule/scheduled"`, "rule": `"scheduled"`, "ingested": `"2026-10-14T02:04:05.123Z"`} {
		if string(fields[key]) != want {
			t.Errorf("%s = %s, want %s", key, fields[key], want)
		}
	}
	if !bytes.Equal(fields["full"], event) || !bytes.Equal(event, before) {
		t.Fatal("full event or caller input changed")
	}
	var withoutDetail map[string]json.RawMessage
	if err := json.Unmarshal(fields["event"], &withoutDetail); err != nil {
		t.Fatal(err)
	}
	if _, exists := withoutDetail["detail"]; exists || string(withoutDetail["id"]) != `"evt-original"` {
		t.Fatalf("reserved event detail stripping changed identity: %s", fields["event"])
	}
}

func TestTransformTargetStringsAndMissingValues(t *testing.T) {
	paths := map[string]string{"name": "$.detail.instance-id", "state": "$.detail.nested.state_name", "detail": "$.detail.nested", "missing": "$.absent", "number": "$.detail.items[0].v", "nothing": "$.detail.missingNull"}
	for _, test := range []struct{ name, template, want string }{
		{"string", `"instance <name> in <state>"`, `"instance i-123 in RUNNING"`},
		{"escaped literal", `"instance \"<name>\""`, `"instance \"i-123\""`},
		{"mixed", `{"name":<name>,"state":"<state>","values":[9,<number>,true],"null":<nothing>}`, `{"name":"i-123","state":"RUNNING","values":[9,1,true],"null":null}`},
		{"inline object", `"Detail is <detail>"`, `"Detail is {state_name:RUNNING}"`},
		{"missing", `{"missing":<missing>,"present":true,"inline":"a<missing>b","array":[1,<missing>,2]}`, `{"present":true,"inline":"ab","array":[1,2]}`},
		{"missing root", `<missing>`, `null`},
		{"multiline", "\"<name>\"\n\"<state>\"", "\"i-123\"\n\"RUNNING\""},
		{"reserved inline", `"<aws.events.rule-name> triggered"`, `"scheduled triggered"`},
		{"inline null", `"Value <nothing>"`, `"Value null"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := transformTarget(Target{InputTransformer: &InputTransformer{InputPathsMap: paths, InputTemplate: test.template}}, transformFixtureEvent(), "arn", "scheduled", time.Time{})
			if err != nil || string(got) != test.want {
				t.Fatalf("got %s, err %v; want %s", got, err, test.want)
			}
		})
	}
}

func TestTransformTargetDoesNotEscapeExtractedInlineStrings(t *testing.T) {
	event := []byte(`{"detail":{"text":"a \"quoted\" \\ value"}}`)
	transformer := &InputTransformer{InputPathsMap: map[string]string{"text": "$.detail.text"}, InputTemplate: `{"value":"<text>"}`}
	got, err := transformTarget(Target{InputTransformer: transformer}, event, "arn", "rule", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	// AWS explicitly inserts strings without escaping their quotes/backslashes.
	if string(got) != "{\"value\":\"a \"quoted\" \\ value\"}" || json.Valid(got) {
		t.Fatalf("inline value was unexpectedly escaped: %s", got)
	}
	transformer.InputTemplate = `{"value":<text>}`
	got, err = transformTarget(Target{InputTransformer: transformer}, event, "arn", "rule", time.Time{})
	if err != nil || string(got) != `{"value":"a \"quoted\" \\ value"}` || !json.Valid(got) {
		t.Fatalf("whole JSON value lost its original encoding: %s, %v", got, err)
	}
}

func TestValidateTargetInputRejectsInvalidContracts(t *testing.T) {
	transformer := func(paths map[string]string, template string) Target {
		return Target{InputTransformer: &InputTransformer{InputPathsMap: paths, InputTemplate: template}}
	}
	cases := map[string]Target{
		"input and path":                    {Input: transformStringPointer(`{}`), InputPath: transformStringPointer("$")},
		"input and transformer":             {Input: transformStringPointer(`{}`), InputTransformer: &InputTransformer{InputTemplate: `{}`}},
		"path and transformer":              {InputPath: transformStringPointer("$"), InputTransformer: &InputTransformer{InputTemplate: `{}`}},
		"invalid input":                     {Input: transformStringPointer("not json")},
		"input length":                      {Input: transformStringPointer(`"` + strings.Repeat("x", 8191) + `"`)},
		"empty path":                        {InputPath: transformStringPointer("")},
		"path length":                       {InputPath: transformStringPointer("$." + strings.Repeat("x", 255))},
		"recursive descent":                 {InputPath: transformStringPointer("$..detail")},
		"filter":                            {InputPath: transformStringPointer("$.detail.items[?(@.v==1)]")},
		"slice":                             {InputPath: transformStringPointer("$.detail.items[0:2]")},
		"negative index":                    {InputPath: transformStringPointer("$.resources[-1]")},
		"path trailing junk":                {InputPath: transformStringPointer("$.detail.*junk")},
		"bracket transformer key":           transformer(map[string]string{"value": "$.detail['instance-id']"}, `<value>`),
		"invalid variable key":              transformer(map[string]string{"aws.events.rule-name": "$.id"}, `<aws.events.rule-name>`),
		"reserved AWS prefix":               transformer(map[string]string{"AWS.custom": "$.id"}, `"text"`),
		"slash variable key":                transformer(map[string]string{"a/b": "$.id"}, `"text"`),
		"empty variable key":                transformer(map[string]string{"": "$.id"}, `"text"`),
		"long variable key":                 transformer(map[string]string{strings.Repeat("x", 257): "$.id"}, `"text"`),
		"empty template":                    transformer(nil, ""),
		"whitespace template":               transformer(nil, "  \n"),
		"long template":                     transformer(nil, `"`+strings.Repeat("x", 8191)+`"`),
		"unknown variable":                  transformer(nil, `{"value":<unknown>}`),
		"unknown inline variable":           transformer(nil, `"<unknown>"`),
		"variable object key":               transformer(map[string]string{"id": "$.id"}, `{"<id>":true}`),
		"unquoted variable key":             transformer(map[string]string{"id": "$.id"}, `{<id>:true}`),
		"reserved event inline":             transformer(nil, `{"value":"<aws.events.event>"}`),
		"reserved event root":               transformer(nil, `<aws.events.event.json>`),
		"reserved event array":              transformer(nil, `[<aws.events.event.json>]`),
		"template bare text":                transformer(nil, `some text`),
		"template invalid separator":        transformer(nil, `{"a":true "b":false}`),
		"template trailing comma":           transformer(nil, `{"a":true,}`),
		"template multiple objects":         transformer(nil, `{} {}`),
		"template same-line strings":        transformer(nil, `"a" "b"`),
		"template mixed multiline":          transformer(nil, "\"text\"\n{}"),
		"template invalid escape":           transformer(nil, `"\x"`),
		"template unterminated escape":      transformer(nil, `"text\`),
		"template unterminated placeholder": transformer(nil, `"<abc"`),
	}
	many := make(map[string]string, 101)
	for i := range 101 {
		many[strings.Repeat("x", i+1)] = "$.id"
	}
	cases["too many variables"] = transformer(many, `"text"`)
	for name, target := range cases {
		t.Run(name, func(t *testing.T) {
			err := validateTargetInput(target)
			if err == nil {
				t.Fatal("accepted invalid target input")
			}
			var apiError *APIError
			if !errors.As(err, &apiError) || apiError.Code != "ValidationException" || apiError.Status != 400 {
				t.Fatalf("validation lost native error contract: %v", err)
			}
		})
	}
}

func TestTransformWildcardObjectsHaveDeterministicOrder(t *testing.T) {
	path := "$.*.value"
	got, err := transformTarget(Target{InputPath: &path}, []byte(`{"z":{"value":"last"},"a":{"value":"first"},"m":{"ignored":true}}`), "arn", "rule", time.Time{})
	if err != nil || string(got) != `["first","last"]` {
		t.Fatalf("got %s, err %v", got, err)
	}
}
