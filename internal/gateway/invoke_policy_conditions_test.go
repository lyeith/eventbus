package gateway

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestCompileInvokeConditionsStringAndARN(t *testing.T) {
	tests := []struct {
		name, operator, expected, actual string
		want                             bool
	}{
		{"equals", "StringEquals", "blue", "blue", true},
		{"case sensitive", "StringEquals", "blue", "Blue", false},
		{"not equals", "StringNotEquals", "blue", "red", true},
		{"not equal match", "StringNotEquals", "blue", "blue", false},
		{"ignore case", "StringEqualsIgnoreCase", "bLuE", "Blue", true},
		{"negated ignore case", "StringNotEqualsIgnoreCase", "bLuE", "Blue", false},
		{"like", "StringLike", "home/*/file?.txt", "home/a/b/file1.txt", true},
		{"glob punctuation literal", "StringLike", "file[1].txt", "file1xtxt", false},
		{"glob literal brackets", "StringLike", "file[1].txt", "file[1].txt", true},
		{"question consumes one rune", "StringLike", "?", "雪", true},
		{"question consumes exactly one", "StringLike", "?", "ab", false},
		{"not like", "StringNotLike", "private/*", "public/a", true},
		{"not like match", "StringNotLike", "private/*", "private/a", false},
		{"arn empty region and account", "ArnEquals", "arn:aws:s3:::bucket", "arn:aws:s3:::bucket", true},
		{"arn fixed literal escape", "ArnLike", "arn:aws:s3:::bucket/${*}", "arn:aws:s3:::bucket/*", true},
		{"arn equals supports wildcards", "ArnEquals", "arn:aws:iam::*:role/team-?", "arn:aws:iam::123456789012:role/team-a", true},
		{"arn like", "ArnLike", "arn:aws:execute-api:us-*:123456789012:api/*", "arn:aws:execute-api:us-east-1:123456789012:api/dev/GET/a", true},
		{"arn resource may contain colons", "ArnLike", "arn:aws:lambda:*:123456789012:function:worker:*", "arn:aws:lambda:us-east-1:123456789012:function:worker:live", true},
		{"arn wildcard cannot span fields", "ArnLike", "arn:aws:s3:*:123456789012:bucket", "arn:aws:s3:us-east-1:999:extra:123456789012:bucket", false},
		{"arn is case sensitive", "ArnEquals", "arn:aws:iam::123456789012:role/Worker", "arn:aws:iam::123456789012:role/worker", false},
		{"arn not equals", "ArnNotEquals", "arn:aws:iam::*:role/admin", "arn:aws:iam::123456789012:role/reader", true},
		{"arn not like", "ArnNotLike", "arn:aws:iam::*:role/admin*", "arn:aws:iam::123456789012:role/admin-team", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			condition := invokeConditionJSONForTest(test.operator, test.expected)
			got := evaluateInvokeConditionsForTest(t, condition, map[string][]string{"key": {test.actual}})
			if got != test.want {
				t.Fatalf("condition %s actual %q = %v, want %v", condition, test.actual, got, test.want)
			}
		})
	}
}

func TestCompileInvokeConditionsVariables(t *testing.T) {
	tests := []struct {
		name, operator, expected, actual string
		facts                            map[string][]string
		want                             bool
	}{
		{"exact variable", "StringEquals", "team/${aws:username}", "team/david", map[string][]string{"AWS:UserName": {"david"}}, true},
		{"unresolved exact", "StringEquals", "team/${aws:username}", "team/", nil, false},
		{"unresolved negated exact", "StringNotEquals", "${missing}", "anything", nil, true},
		{"unresolved like", "StringLike", "${missing}*", "anything", nil, false},
		{"unresolved negated like", "StringNotLike", "${missing}*", "anything", nil, true},
		{"multivalue variable cannot choose an entry", "StringEquals", "${name}", "david", map[string][]string{"name": {"david", "amy"}}, false},
		{"variable default", "StringEquals", "${name, 'guest'}", "guest", nil, true},
		{"resolved overrides default", "StringEquals", "${name, 'guest'}", "david", map[string][]string{"name": {"david"}}, true},
		{"exact variable wildcard stays literal", "StringEquals", "${aws:PrincipalTag/project}", "tenant/project", map[string][]string{"aws:PrincipalTag/project": {"*"}}, false},
		{"exact variable wildcard matches itself", "StringEquals", "${aws:PrincipalTag/project}", "*", map[string][]string{"aws:PrincipalTag/project": {"*"}}, true},
		{"principal tag wildcard expands for like", "StringLike", "${aws:PrincipalTag/project}", "tenant/project", map[string][]string{"aws:PrincipalTag/project": {"*"}}, true},
		{"default wildcard expands for like", "StringLike", "${name, 'team-*'}", "team-blue", nil, true},
		{"default wildcard stays literal for equals", "StringEquals", "${name, 'team-*'}", "team-blue", nil, false},
		{"glob variable wildcard expands", "StringLike", "home/${name}/*", "home/admin/data", map[string][]string{"name": {"*"}}, true},
		{"glob variable wildcard matches itself", "StringLike", "home/${name}/*", "home/*/data", map[string][]string{"name": {"*"}}, true},
		{"fixed literal star", "StringLike", "home/${*}", "home/*", nil, true},
		{"fixed star does not expand", "StringLike", "home/${*}", "home/admin", nil, false},
		{"fixed question", "StringLike", "${?}", "?", nil, true},
		{"fixed dollar", "StringEquals", "${$}", "$", nil, true},
		{"full arn variable", "ArnEquals", "${aws:PrincipalArn}", "arn:aws:iam::123456789012:role/reader", map[string][]string{"aws:principalarn": {"arn:aws:iam::123456789012:role/reader"}}, true},
		{"arn variable wildcard expands", "ArnLike", "arn:aws:iam::123456789012:role/${role}", "arn:aws:iam::123456789012:role/admin", map[string][]string{"role": {"*"}}, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			facts := map[string][]string{"key": {test.actual}}
			for key, values := range test.facts {
				facts[key] = values
			}
			if got := evaluateInvokeConditionsForTest(t, invokeConditionJSONForTest(test.operator, test.expected), facts); got != test.want {
				t.Fatalf("got %v, want %v", got, test.want)
			}
		})
	}
}

func TestCompileInvokeConditionsCollectionSemantics(t *testing.T) {
	tests := []struct {
		operator string
		actual   []string
		want     bool
	}{
		{"StringEquals", []string{"allowed", "other"}, true},
		{"StringNotEquals", []string{"allowed", "other"}, false},
		{"StringNotEquals", []string{"other", "outside"}, true},
		{"ForAllValues:StringEquals", []string{"allowed", "second"}, true},
		{"ForAllValues:StringEquals", []string{"allowed", "other"}, false},
		{"ForAnyValue:StringEquals", []string{"allowed", "other"}, true},
		{"ForAnyValue:StringEquals", []string{"other", "outside"}, false},
		{"ForAllValues:StringNotEquals", []string{"allowed", "other"}, false},
		{"ForAllValues:StringNotEquals", []string{"other", "outside"}, true},
		{"ForAnyValue:StringNotEquals", []string{"allowed", "other"}, true},
		{"ForAnyValue:StringNotEquals", []string{"allowed", "second"}, false},
		{"StringNotLike", []string{"allowed", "other"}, false},
		{"ForAllValues:StringNotLike", []string{"allowed", "other"}, false},
		{"ForAnyValue:StringNotLike", []string{"allowed", "other"}, true},
	}
	for _, test := range tests {
		t.Run(test.operator+"/"+strings.Join(test.actual, ","), func(t *testing.T) {
			condition := invokeConditionJSONForTest(test.operator, []string{"allowed", "second"})
			if got := evaluateInvokeConditionsForTest(t, condition, map[string][]string{"key": test.actual}); got != test.want {
				t.Fatalf("actual %v: got %v, want %v", test.actual, got, test.want)
			}
		})
	}
	for _, operator := range []string{"StringNotEquals", "StringNotLike", "NumericNotEquals", "DateNotEquals", "NotIpAddress", "ArnNotEquals", "ArnNotLike"} {
		t.Run(operator+" policy values use NOR", func(t *testing.T) {
			var expected []string
			switch operator {
			case "NumericNotEquals", "DateNotEquals":
				expected = []string{"10", "20"}
			case "NotIpAddress":
				expected = []string{"192.0.2.1", "2001:db8::/32"}
			case "ArnNotEquals", "ArnNotLike":
				expected = []string{"arn:aws:iam::123456789012:role/a", "arn:aws:iam::123456789012:role/b"}
			default:
				expected = []string{"first", "second"}
			}
			for _, actual := range expected {
				if strings.Contains(actual, "/32") {
					actual = "2001:db8::1"
				}
				if evaluateInvokeConditionsForTest(t, invokeConditionJSONForTest(operator, expected), map[string][]string{"key": {actual}}) {
					t.Fatalf("a matching policy value must make %s false", operator)
				}
			}
		})
	}
}

func TestCompileInvokeConditionsMissingAndNull(t *testing.T) {
	tests := []struct {
		operator                             string
		expected                             any
		missing, empty, emptyString, present bool
	}{
		{"StringEquals", "value", false, false, false, true},
		{"StringNotEquals", "value", true, true, true, false},
		{"StringEqualsIfExists", "value", true, false, false, true},
		{"StringNotEqualsIfExists", "value", true, true, true, false},
		{"ForAllValues:StringEquals", "value", true, true, true, true},
		{"ForAnyValue:StringEquals", "value", false, false, false, true},
		{"ForAllValues:StringNotEquals", "value", true, true, true, false},
		{"ForAnyValue:StringNotEquals", "value", false, false, false, false},
		{"Null", true, true, true, true, false},
		{"Null", false, false, false, false, true},
	}
	for _, test := range tests {
		t.Run(test.operator+"/"+invokeConditionJSONForTest("Null", test.expected), func(t *testing.T) {
			condition := invokeConditionJSONForTest(test.operator, test.expected)
			for _, context := range []struct {
				name  string
				facts map[string][]string
				want  bool
			}{
				{"missing", nil, test.missing},
				{"empty", map[string][]string{"key": {}}, test.empty},
				{"empty string", map[string][]string{"key": {""}}, test.emptyString},
				{"present", map[string][]string{"key": {"value"}}, test.present},
			} {
				t.Run(context.name, func(t *testing.T) {
					if got := evaluateInvokeConditionsForTest(t, condition, context.facts); got != context.want {
						t.Fatalf("got %v, want %v", got, context.want)
					}
				})
			}
		})
	}
	if !evaluateInvokeConditionsForTest(t, `{"StringEquals":{"key":""}}`, map[string][]string{"key": {""}}) {
		t.Fatal("ordinary StringEquals must be able to match an explicit empty string")
	}
	if !evaluateInvokeConditionsForTest(t, `{"StringEquals":{"key":"value"},"Null":{"KEY":false}}`, map[string][]string{"key": {"value"}}) {
		t.Fatal("different operators must be able to test the same case-insensitive key")
	}
	if evaluateInvokeConditionsForTest(t, `{"ForAllValues:StringEquals":{"key":"value"},"Null":{"key":false}}`, nil) {
		t.Fatal("Null false must prevent the vacuous set match")
	}
}

func TestCompileInvokeConditionsTypedOperators(t *testing.T) {
	epoch := time.Date(2026, 10, 28, 0, 0, 0, 250000000, time.UTC).Unix()
	tests := []struct {
		name, operator string
		expected       any
		actual         string
		want           bool
	}{
		{"numeric decimal", "NumericEquals", 0.1, ".10", true},
		{"numeric exact integer", "NumericEquals", "9007199254740992", "9007199254740993", false},
		{"numeric leading zeros stay decimal", "NumericEquals", "010", "10", true},
		{"numeric exponent", "NumericEquals", "1e2", "100", true},
		{"numeric greater", "NumericGreaterThan", -3, "-2.9", true},
		{"numeric greater boundary", "NumericGreaterThan", -3, "-3", false},
		{"numeric greater equal", "NumericGreaterThanEquals", -3, "-3.0", true},
		{"numeric less", "NumericLessThan", "3.01", "3.001", true},
		{"numeric less boundary", "NumericLessThan", "3.01", "3.010", false},
		{"numeric less equal", "NumericLessThanEquals", 3, "3.0", true},
		{"numeric invalid actual", "NumericNotEquals", 3, "NaN", false},
		{"numeric invalid fractions", "NumericNotEquals", 3, "1/2", false},
		{"date timezone", "DateEquals", "2026-10-28T01:00:00+01:00", "2026-10-28T00:00:00Z", true},
		{"date epoch", "DateEquals", epoch, "2026-10-28T00:00:00Z", true},
		{"date fraction", "DateEquals", "2026-10-28T00:00:00.250Z", "2026-10-28T00:00:00.25Z", true},
		{"date full calendar", "DateEquals", "2026-10-28", "2026-10-28T00:00:00Z", true},
		{"date minute precision", "DateEquals", "2026-10-28T00:00Z", "2026-10-28T00:00:00Z", true},
		{"date greater", "DateGreaterThan", "2026-10-28T00:00:00Z", "2026-10-28T00:00:00.001Z", true},
		{"date greater equal", "DateGreaterThanEquals", "2026-10-28T00:00:00Z", "2026-10-28T00:00:00Z", true},
		{"date less", "DateLessThan", "2026-10-28T00:00:00Z", "2026-10-27T23:59:59Z", true},
		{"date less equal", "DateLessThanEquals", "2026-10-28T00:00:00Z", "2026-10-28T00:00:00Z", true},
		{"date invalid actual", "DateNotEquals", "2026-10-28T00:00:00Z", "yesterday", false},
		{"date no silent fraction truncation", "DateNotEquals", "2026-10-28T00:00:00Z", "2026-10-28T00:00:00.0000000001Z", false},
		{"boolean native", "Bool", true, "true", true},
		{"boolean string", "Bool", "false", "false", true},
		{"boolean opposite", "Bool", false, "true", false},
		{"boolean no permissive parser", "Bool", true, "1", false},
		{"binary bytes", "BinaryEquals", "/wA=", "/wA=", true},
		{"binary different", "BinaryEquals", "/wA=", "/wE=", false},
		{"binary empty", "BinaryEquals", "", "", true},
		{"binary invalid", "BinaryEquals", "Zg==", "Zh==", false},
		{"ip v4 range", "IpAddress", "192.0.2.0/24", "192.0.2.5", true},
		{"ip v4 range outside", "IpAddress", "192.0.2.0/24", "198.51.100.5", false},
		{"ip v4 exact", "IpAddress", "192.0.2.5", "192.0.2.6", false},
		{"ip v6 range", "IpAddress", "2001:db8::/32", "2001:DB8::5", true},
		{"ip v6 exact", "IpAddress", "2001:db8::5", "2001:db8::6", false},
		{"not ip outside", "NotIpAddress", "192.0.2.0/24", "198.51.100.5", true},
		{"not ip invalid actual", "NotIpAddress", "192.0.2.0/24", "untrusted", false},
		{"not ip zone invalid", "NotIpAddress", "2001:db8::/32", "fe80::1%eth0", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := evaluateInvokeConditionsForTest(t, invokeConditionJSONForTest(test.operator, test.expected), map[string][]string{"key": {test.actual}}); got != test.want {
				t.Fatalf("got %v, want %v", got, test.want)
			}
		})
	}
	for _, test := range []struct {
		operator string
		expected any
		actual   []string
		want     bool
	}{
		{"ForAllValues:Bool", true, []string{"true", "true"}, true},
		{"ForAllValues:Bool", true, []string{"true", "false"}, false},
		{"ForAnyValue:Bool", false, []string{"true", "false"}, true},
		{"IpAddress", []string{"192.0.2.0/24", "2001:db8::/32"}, []string{"2001:db8::1"}, true},
	} {
		if got := evaluateInvokeConditionsForTest(t, invokeConditionJSONForTest(test.operator, test.expected), map[string][]string{"key": test.actual}); got != test.want {
			t.Fatalf("%s got %v, want %v", test.operator, got, test.want)
		}
	}
}

func TestCompileInvokeConditionsAndDetachedData(t *testing.T) {
	data := json.RawMessage(`{"StringEquals":{"AWS:PrincipalTag/Team":["blue","green"],"aws:RequestedRegion":"us-east-1"},"NumericLessThanEquals":{"aws:MultiFactorAuthAge":300},"Bool":{"aws:SecureTransport":true}}`)
	conditions, err := compileInvokeConditions(data)
	if err != nil {
		t.Fatal(err)
	}
	for i := range data {
		data[i] = ' '
	}
	facts := map[string][]string{"aws:principaltag/team": {"green"}, "AWS:REQUESTEDREGION": {"us-east-1"}, "aws:MultiFactorAuthAge": {"300"}, "aws:SecureTransport": {"true"}}
	before, _ := json.Marshal(facts)
	for _, condition := range conditions {
		if !condition(facts) {
			t.Fatal("all initial conditions should match after detaching input JSON")
		}
	}
	after, _ := json.Marshal(facts)
	if string(after) != string(before) {
		t.Fatal("condition evaluation mutated request facts")
	}
	for _, key := range []string{"aws:principaltag/team", "AWS:REQUESTEDREGION", "aws:MultiFactorAuthAge", "aws:SecureTransport"} {
		original := facts[key]
		facts[key] = []string{"wrong"}
		all := true
		for _, condition := range conditions {
			all = condition(facts) && all
		}
		if all {
			t.Fatalf("conditions must AND key %s", key)
		}
		facts[key] = original
	}
}

func TestCompileInvokeConditionsRejectMalformed(t *testing.T) {
	for _, condition := range []string{
		`null`, `[]`, `true`, `"condition"`, `{}`, `{} {}`,
		`{"Unknown":{"key":"value"}}`, `{"StringEquals":{}}`, `{"StringEquals":null}`, `{"StringEquals":[]}`,
		`{"StringEquals":{"":"value"}}`, `{"StringEquals":{"${key}":"value"}}`,
		`{"StringEquals":{"key":[]}}`, `{"StringEquals":{"key":null}}`, `{"StringEquals":{"key":{}}}`, `{"StringEquals":{"key":[["value"]]}}`,
		`{"StringEquals":{"AWS:Name":"one","aws:name":"two"}}`,
		`{"StringEquals":{"AWS:Tag/Σ":"one","aws:tag/ς":"two"}}`,
		`{"StringEquals":{"key":"one","key":"two"}}`, `{"StringEquals":{"key":"one"},"StringEquals":{"key":"two"}}`,
		`{"StringLike":{"key":"${missing"}}`, `{"StringEquals":{"key":"${}"}}`,
		`{"NullIfExists":{"key":true}}`, `{"ForAllValues:Null":{"key":true}}`, `{"ForAnyValue:Null":{"key":false}}`,
		`{"ForAnyValue:StringEqualsIfExists":{"key":"value"}}`, `{"ForAllValues:StringEqualsIfExists":{"key":"value"}}`,
		`{"ForAnyValues:StringEquals":{"key":"value"}}`, `{"ForAllValues:ForAnyValue:StringEquals":{"key":"value"}}`,
		`{"Null":{"key":"TRUE"}}`, `{"Bool":{"key":"1"}}`, `{"Bool":{"key":"${flag}"}}`,
		`{"NumericEquals":{"key":"1/2"}}`, `{"NumericEquals":{"key":"NaN"}}`, `{"NumericEquals":{"key":"1e1025"}}`, `{"NumericEquals":{"key":"1e-1025"}}`,
		`{"NumericEquals":{"key":"${limit}"}}`, `{"DateEquals":{"key":"${time}"}}`, `{"DateEquals":{"key":"today"}}`,
		`{"DateEquals":{"key":"2026-10-28T00:00:00.0000000001Z"}}`,
		`{"BinaryEquals":{"key":"Zh=="}}`, `{"BinaryEquals":{"key":"Zg==\n"}}`, `{"BinaryEquals":{"key":"${binary}"}}`,
		`{"IpAddress":{"key":"192.0.2.1/33"}}`, `{"IpAddress":{"key":"2001:db8::/129"}}`, `{"IpAddress":{"key":"fe80::1%eth0"}}`, `{"IpAddress":{"key":"${ip}"}}`,
	} {
		t.Run(condition, func(t *testing.T) {
			if _, err := compileInvokeConditions(json.RawMessage(condition)); err == nil {
				t.Fatalf("accepted malformed or unsupported condition %s", condition)
			}
		})
	}
	if _, err := compileInvokeConditions(json.RawMessage(invokeConditionJSONForTest("NumericEquals", strings.Repeat("1", 257)))); err == nil {
		t.Fatal("accepted unbounded numeric text")
	}
	for _, data := range []json.RawMessage{nil, {}, json.RawMessage(" \n\t")} {
		conditions, err := compileInvokeConditions(data)
		if err != nil || len(conditions) != 0 {
			t.Fatalf("missing Condition returned %v, %v", conditions, err)
		}
	}
}

func TestCompileInvokeConditionsRejectStaticMalformedARN(t *testing.T) {
	for _, operator := range []string{"ArnEquals", "ArnNotEquals", "ArnLike", "ArnNotLike"} {
		t.Run(operator, func(t *testing.T) {
			for _, value := range []string{
				"*", "", "not-an-arn", "arn:aws:iam", "ARN:aws:iam::123456789012:role/a",
				"arn::iam::123456789012:role/a", "arn:aws:::123456789012:role/a", "arn:aws:iam::123456789012:",
				"${*}", "arn:aws:s3:::",
			} {
				if _, err := compileInvokeConditions(json.RawMessage(invokeConditionJSONForTest(operator, value))); err == nil {
					t.Fatalf("accepted unsupported static ARN pattern %q", value)
				}
			}
		})
	}
}

func TestPolicyFactCaseInsensitivePresence(t *testing.T) {
	facts := map[string][]string{"AWS:Name": {"first"}, "aws:name": {"second"}, "empty": {}, "null": nil}
	if values, present := policyFact(facts, "aWs:nAmE"); !present || !reflect.DeepEqual(values, []string{"first", "second"}) {
		t.Fatalf("case-insensitive aggregate = %v, %v", values, present)
	}
	for _, key := range []string{"EMPTY", "NULL"} {
		if values, present := policyFact(facts, key); !present || len(values) != 0 {
			t.Fatalf("present empty %s = %v, %v", key, values, present)
		}
	}
	if values, present := policyFact(facts, "missing"); present || values != nil {
		t.Fatalf("missing fact = %v, %v", values, present)
	}
}

func invokeConditionJSONForTest(operator string, expected any) string {
	encoded, err := json.Marshal(map[string]any{operator: map[string]any{"key": expected}})
	if err != nil {
		panic(err)
	}
	return string(encoded)
}

func evaluateInvokeConditionsForTest(t *testing.T, data string, facts map[string][]string) bool {
	t.Helper()
	conditions, err := compileInvokeConditions(json.RawMessage(data))
	if err != nil {
		t.Fatal(err)
	}
	for _, condition := range conditions {
		if !condition(facts) {
			return false
		}
	}
	return true
}
