package messaging

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSNSFilterAttributeOperators(t *testing.T) {
	tests := []struct {
		name, policy, dataType, value string
		want                          bool
	}{
		{"string exact", "{\"x\":[\"rugby\"]}", "String", "rugby", true},
		{"case sensitive", "{\"x\":[\"rugby\"]}", "String", "Rugby", false},
		{"string array", "{\"x\":[\"rugby\"]}", "String.Array", "[\"tennis\",\"rugby\"]", true},
		{"array miss", "{\"x\":[\"rugby\"]}", "String.Array", "[\"tennis\"]", false},
		{"invalid array", "{\"x\":[\"rugby\"]}", "String.Array", "[", false},
		{"array objects ignored", "{\"x\":[\"rugby\"]}", "String.Array", "[{\"sport\":\"rugby\"}]", false},
		{"typed boolean", "{\"x\":[true]}", "String.Array", "[false,true]", true},
		{"typed null", "{\"x\":[null]}", "String.Array", "[null]", true},
		{"string not boolean", "{\"x\":[true]}", "String", "true", false},
		{"prefix", "{\"x\":[{\"prefix\":\"bas\"}]}", "String", "baseball", true},
		{"prefix miss", "{\"x\":[{\"prefix\":\"bas\"}]}", "String", "football", false},
		{"suffix", "{\"x\":[{\"suffix\":\"ball\"}]}", "String", "football", true},
		{"ignore case", "{\"x\":[{\"equals-ignore-case\":\"tennis\"}]}", "String", "tEnNiS", true},
		{"wildcard", "{\"x\":[{\"wildcard\":\"a*b*c\"}]}", "String", "ab-middle-c", true},
		{"wildcard newline", "{\"x\":[{\"wildcard\":\"a*c\"}]}", "String", "a\nc", true},
		{"wildcard anchors", "{\"x\":[{\"wildcard\":\"a*c\"}]}", "String", "prefixabc", false},
		{"wildcard empty spans", "{\"x\":[{\"wildcard\":\"a*b*c\"}]}", "String", "abc", true},
		{"wildcard literal star", "{\"x\":[{\"wildcard\":\"file\\\\*.txt\"}]}", "String", "file*.txt", true},
		{"literal star miss", "{\"x\":[{\"wildcard\":\"file\\\\*.txt\"}]}", "String", "file123.txt", false},
		{"cidr first", "{\"x\":[{\"cidr\":\"10.0.0.0/24\"}]}", "String", "10.0.0.0", true},
		{"cidr last", "{\"x\":[{\"cidr\":\"10.0.0.0/24\"}]}", "String", "10.0.0.255", true},
		{"cidr outside", "{\"x\":[{\"cidr\":\"10.0.0.0/24\"}]}", "String", "10.0.1.0", false},
		{"cidr ipv6 ignored", "{\"x\":[{\"cidr\":\"10.0.0.0/24\"}]}", "String", "::ffff:10.0.0.1", false},
		{"numeric literal", "{\"x\":[301.5]}", "Number", "3.015e2", true},
		{"numeric exact", "{\"x\":[{\"numeric\":[\"=\",301.5]}]}", "Number", "301.50000", true},
		{"numeric range", "{\"x\":[{\"numeric\":[\">\",0,\"<=\",150]}]}", "Number", "150", true},
		{"numeric lower boundary", "{\"x\":[{\"numeric\":[\">\",0,\"<=\",150]}]}", "Number", "0", false},
		{"numeric upper boundary", "{\"x\":[{\"numeric\":[\">\",0,\"<\",150]}]}", "Number", "150", false},
		{"numeric reversed range", "{\"x\":[{\"numeric\":[\"<\",150,\">\",0]}]}", "Number", "100", true},
		{"numeric lower limit", "{\"x\":[{\"numeric\":[\">=\",-1000000000]}]}", "Number", "-1000000000", true},
		{"numeric upper limit", "{\"x\":[1000000000]}", "Number", "1000000000", true},
		{"numeric precision", "{\"x\":[0.00001]}", "Number", "1e-5", true},
		{"numeric excessive precision", "{\"x\":[{\"numeric\":[\">\",0]}]}", "Number", "0.000001", false},
		{"numeric too large", "{\"x\":[{\"numeric\":[\">\",0]}]}", "Number", "1000000001", false},
		{"numeric string mismatch", "{\"x\":[301.5]}", "String", "301.5", false},
		{"numeric malformed", "{\"x\":[{\"numeric\":[\">\",0]}]}", "Number", "1/2", false},
		{"numeric array", "{\"x\":[{\"numeric\":[\">\",100]}]}", "String.Array", "[1,150]", true},
		{"anything but string", "{\"x\":[{\"anything-but\":[\"rugby\",\"tennis\"]}]}", "String", "baseball", true},
		{"anything but excluded", "{\"x\":[{\"anything-but\":\"rugby\"}]}", "String", "rugby", false},
		{"anything but array", "{\"x\":[{\"anything-but\":[\"rugby\",\"tennis\"]}]}", "String.Array", "[\"rugby\",\"baseball\"]", true},
		{"anything but all excluded", "{\"x\":[{\"anything-but\":[\"rugby\",\"tennis\"]}]}", "String.Array", "[\"rugby\",\"tennis\"]", false},
		{"anything but numeric", "{\"x\":[{\"anything-but\":[100,500]}]}", "Number", "100.1", true},
		{"anything but numeric excluded", "{\"x\":[{\"anything-but\":[100,500]}]}", "Number", "1e2", false},
		{"anything but numeric type", "{\"x\":[{\"anything-but\":[100,500]}]}", "String", "100", false},
		{"anything but prefix", "{\"x\":[{\"anything-but\":{\"prefix\":\"order-\"}}]}", "String", "order_number", true},
		{"anything but prefix excluded", "{\"x\":[{\"anything-but\":{\"prefix\":\"order-\"}}]}", "String", "order-cancelled", false},
		{"anything but suffix", "{\"x\":[{\"anything-but\":{\"suffix\":\"ball\"}}]}", "String.Array", "[\"baseball\",\"rugby\"]", true},
		{"anything but wildcard", "{\"x\":[{\"anything-but\":{\"wildcard\":\"*ball\"}}]}", "String.Array", "[\"baseball\",\"basketball\"]", false},
		{"exists true", "{\"x\":[{\"exists\":true}]}", "String", "present", true},
		{"exists true zero", "{\"x\":[{\"exists\":true}]}", "Number", "0", true},
		{"exists true empty", "{\"x\":[{\"exists\":true}]}", "String", "", false},
		{"exists true null", "{\"x\":[{\"exists\":true}]}", "String.Array", "[null]", false},
		{"exists true empty array", "{\"x\":[{\"exists\":true}]}", "String.Array", "[]", false},
		{"exists false present", "{\"x\":[{\"exists\":false}]}", "String", "present", false},
		{"binary ignored", "{\"x\":[{\"exists\":true}]}", "Binary", "present", false},
		{"custom string type", "{\"x\":[\"rugby\"]}", "String.custom", "rugby", true},
		{"custom number type", "{\"x\":[150]}", "Number.custom", "150", true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy, err := ParseFilterPolicy(test.policy)
			require.NoError(t, err)
			attrs := map[string]MessageAttribute{"x": {DataType: test.dataType, StringValue: test.value}}
			require.Equal(t, test.want, policy.Matches(attrs))
		})
	}
}

func TestSNSFilterKeyLogicAndScope(t *testing.T) {
	policy, err := ParseFilterPolicy("{\"source\":[\"aws.cloudwatch\"],\"$or\":[{\"metricName\":[\"CPUUtilization\"]},{\"namespace\":[\"AWS/EC2\"]}]}")
	require.NoError(t, err)
	attr := func(value string) MessageAttribute { return MessageAttribute{DataType: "String", StringValue: value} }
	require.True(t, policy.Matches(map[string]MessageAttribute{"source": attr("aws.cloudwatch"), "namespace": attr("AWS/EC2")}))
	require.False(t, policy.Matches(map[string]MessageAttribute{"source": attr("wrong"), "namespace": attr("AWS/EC2")}))
	require.False(t, policy.Matches(map[string]MessageAttribute{"source": attr("aws.cloudwatch")}))
	require.True(t, policy.MatchesMessage(nil, "{\"source\":\"aws.cloudwatch\",\"metricName\":\"CPUUtilization\"}", "MessageBody"))

	literalOR, err := ParseFilterPolicy("{\"$or\":[\"one\",\"two\"]}")
	require.NoError(t, err)
	require.True(t, literalOR.Matches(map[string]MessageAttribute{"$or": attr("two")}))
	operatorOR, err := ParseFilterPolicy("{\"$or\":[{\"prefix\":\"abc\"},{\"suffix\":\"xyz\"}]}")
	require.NoError(t, err)
	require.True(t, operatorOR.Matches(map[string]MessageAttribute{"$or": attr("abc-value")}))
	require.False(t, operatorOR.Matches(map[string]MessageAttribute{"other": attr("abc-value")}))

	absent, err := ParseFilterPolicy("{\"missing\":[{\"exists\":false}]}")
	require.NoError(t, err)
	require.False(t, absent.Matches(nil))
	require.True(t, absent.Matches(map[string]MessageAttribute{"other": attr("value")}))
	require.False(t, absent.MatchesMessage(nil, "{}", "MessageBody"))
	require.True(t, absent.MatchesMessage(nil, "{\"other\":false}", "MessageBody"))
	require.False(t, absent.MatchesMessage(nil, "{\"missing\":null,\"other\":false}", "MessageBody"))

	require.NoError(t, policy.ValidateScope(""))
	require.NoError(t, policy.ValidateScope("MessageAttributes"))
	require.NoError(t, policy.ValidateScope("MessageBody"))
	require.Error(t, policy.ValidateScope("unknown"))
	require.False(t, policy.MatchesMessage(nil, "{\"source\":\"aws.cloudwatch\"}", "unknown"))
}

func TestSNSFilterNestedBodyAndArrays(t *testing.T) {
	policy, err := ParseFilterPolicy("{\"Records\":{\"eventName\":[{\"prefix\":\"ObjectCreated:\"}],\"s3\":{\"object\":{\"key\":[{\"suffix\":\".png\"}]}}}}")
	require.NoError(t, err)
	require.Error(t, policy.ValidateScope("MessageAttributes"))
	require.NoError(t, policy.ValidateScope("MessageBody"))
	require.False(t, policy.Matches(map[string]MessageAttribute{"Records": {DataType: "String", StringValue: "ignored"}}))
	require.True(t, policy.MatchesMessage(nil, "{\"Records\":[{\"eventName\":\"ObjectCreated:Put\",\"s3\":{\"object\":{\"key\":\"photo.png\"}}}]}", "MessageBody"))
	require.False(t, policy.MatchesMessage(nil, "{\"Records\":[{\"eventName\":\"ObjectRemoved:Delete\",\"s3\":{\"object\":{\"key\":\"photo.png\"}}}]}", "MessageBody"))
	require.False(t, policy.MatchesMessage(nil, "{\"Records\":[{\"eventName\":\"ObjectCreated:Put\",\"s3\":{\"object\":{\"key\":\"photo.jpg\"}}}]}", "MessageBody"))
	require.False(t, policy.MatchesMessage(nil, "{\"Records\":[]}", "MessageBody"))
	require.False(t, policy.MatchesMessage(nil, "not JSON", "MessageBody"))
	require.False(t, policy.MatchesMessage(nil, "[]", "MessageBody"))
	require.False(t, policy.MatchesMessage(nil, "null", "MessageBody"))

	scalars, err := ParseFilterPolicy("{\"flag\":[false],\"optional\":[null],\"tags\":[\"rugby\"],\"price\":[{\"numeric\":[\">\",100]}]}")
	require.NoError(t, err)
	require.True(t, scalars.MatchesMessage(nil, "{\"flag\":false,\"optional\":null,\"tags\":[\"tennis\",\"rugby\"],\"price\":[90,150]}", "MessageBody"))
	require.False(t, scalars.MatchesMessage(nil, "{\"flag\":\"false\",\"optional\":null,\"tags\":[\"rugby\"],\"price\":150}", "MessageBody"))

	intermediate, err := ParseFilterPolicy("{\"detail\":[{\"exists\":true}]}")
	require.NoError(t, err)
	require.False(t, intermediate.MatchesMessage(nil, "{\"detail\":{\"key\":\"value\"}}", "MessageBody"))
	require.True(t, intermediate.MatchesMessage(nil, "{\"detail\":[null,\"value\"]}", "MessageBody"))

	nestedOR, err := ParseFilterPolicy("{\"detail\":{\"source\":[\"app\"],\"$or\":[{\"state\":[\"ready\"]},{\"priority\":[{\"numeric\":[\">\",1]}]}]}}")
	require.NoError(t, err)
	require.True(t, nestedOR.MatchesMessage(nil, "{\"detail\":{\"source\":\"app\",\"priority\":2}}", "MessageBody"))
	require.False(t, nestedOR.MatchesMessage(nil, "{\"detail\":{\"source\":\"other\",\"priority\":2}}", "MessageBody"))
}

func TestSNSFilterEmptyPolicies(t *testing.T) {
	var absent *FilterPolicy
	require.True(t, absent.MatchesMessage(nil, "not JSON", "MessageBody"))
	empty, err := ParseFilterPolicy("{}")
	require.NoError(t, err)
	require.True(t, empty.MatchesMessage(nil, "not JSON", "MessageBody"))
	require.True(t, empty.Equal(absent))
	require.True(t, empty.Equal(&FilterPolicy{}))
}

func TestSNSFilterValidation(t *testing.T) {
	tests := []string{
		"null", "[]", "\"string\"", "{} {}", "{\"x\":\"string\"}", "{\"x\":[]}", "{\"x\":{}}",
		"{\"x\":[[]]}", "{\"x\":[{}]}", "{\"x\":[{\"unknown\":\"x\"}]}",
		"{\"x\":[{\"prefix\":\"a\",\"suffix\":\"b\"}]}", "{\"x\":[{\"prefix\":1}]}",
		"{\"x\":[{\"suffix\":[]}]}", "{\"x\":[{\"equals-ignore-case\":false}]}",
		"{\"x\":[{\"exists\":\"true\"}]}", "{\"x\":[{\"cidr\":\"not-cidr\"}]}",
		"{\"x\":[{\"cidr\":\"::/0\"}]}", "{\"x\":[{\"numeric\":[]}]}", "{\"x\":[{\"numeric\":[\"=\",\"1\"]}]}",
		"{\"x\":[{\"numeric\":[\"!=\",1]}]}", "{\"x\":[{\"numeric\":[\">\",1,\"<\"]}]}",
		"{\"x\":[{\"numeric\":[\"=\",1,\"<\",2]}]}", "{\"x\":[{\"numeric\":[\">\",1,\">\",2]}]}",
		"{\"x\":[{\"numeric\":[\">\",2,\"<\",1]}]}", "{\"x\":[{\"numeric\":[\">\",1,\"<\",1]}]}",
		"{\"x\":[0.000001]}", "{\"x\":[1000000001]}", "{\"x\":[-1000000001]}",
		"{\"x\":[1e999999999]}", "{\"x\":[1e-999999999]}",
		"{\"x\":[{\"anything-but\":[]}]}", "{\"x\":[{\"anything-but\":true}]}",
		"{\"x\":[{\"anything-but\":[\"one\",1]}]}", "{\"x\":[{\"anything-but\":{\"numeric\":[\"=\",1]}}]}",
		"{\"x\":[{\"wildcard\":\"a*b*c*d*\"}]}", "{\"x\":[{\"wildcard\":\"bad\\\\escape\"}]}",
		"{\"a\":[1],\"b\":[1],\"c\":[1],\"d\":[1],\"e\":[1],\"f\":[1]}",
	}
	for _, raw := range tests {
		t.Run(raw, func(t *testing.T) {
			policy, err := ParseFilterPolicy(raw)
			require.Error(t, err)
			require.Nil(t, policy)
		})
	}
	_, err := ParseFilterPolicy("{\"x\":[\"" + string([]byte{0xff}) + "\"]}")
	require.Error(t, err)
	_, err = ParseFilterPolicy("{\"x\":[\"" + strings.Repeat("a", snsFilterMaxBytes) + "\"]}")
	require.ErrorContains(t, err, "256 KB")
	// Zero with a very large exponent is valid and needs no large allocation.
	zero, err := ParseFilterPolicy("{\"x\":[0e999999999]}")
	require.NoError(t, err)
	require.True(t, zero.Matches(map[string]MessageAttribute{"x": {DataType: "Number", StringValue: "0"}}))
}

func TestSNSFilterComplexity(t *testing.T) {
	t.Run("documented nested depth weighting", func(t *testing.T) {
		_, err := ParseFilterPolicy("{\"key_a\":{\"key_b\":{\"key_c\":[\"one\",\"two\",\"three\",\"four\"]}},\"key_d\":{\"key_e\":[\"one\",\"two\",\"three\"]}}")
		require.NoError(t, err) // 4*3*3*2 = 72.
		_, err = ParseFilterPolicy("{\"key_a\":{\"key_b\":{\"key_c\":[\"one\",\"two\",\"three\",\"four\"]}},\"key_d\":{\"key_e\":[\"one\",\"two\",\"three\",\"four\",\"five\",\"six\",\"seven\"]}}")
		require.ErrorContains(t, err, "150 combinations") // 4*3*7*2 = 168.
	})
	t.Run("OR combination sum", func(t *testing.T) {
		_, err := ParseFilterPolicy("{\"source\":[\"aws.cloudwatch\"],\"$or\":[{\"metricName\":[\"CPUUtilization\",\"ReadLatency\"]},{\"metricType\":[\"MetricType\"],\"$or\":[{\"metricId\":[1234,4321]},{\"spaceId\":[1000,2000,3000]}]}]}")
		require.NoError(t, err) // 1*2 + 1*1*2 + 1*1*3 = 7.
	})
	t.Run("nested leaf key count", func(t *testing.T) {
		_, err := ParseFilterPolicy("{\"one\":{\"two\":{\"three\":{\"four\":{\"five\":{\"leaf\":[\"value\"]}}}}}}")
		require.NoError(t, err) // Six levels, only one leaf key.
	})
	t.Run("150 combinations boundary", func(t *testing.T) {
		policy := func(count int) string {
			return fmt.Sprintf("{\"a\":%s,\"b\":%s}", snsFilterList(10), snsFilterList(count))
		}
		_, err := ParseFilterPolicy(policy(15))
		require.NoError(t, err)
		_, err = ParseFilterPolicy(policy(16))
		require.ErrorContains(t, err, "150 combinations")
	})
	t.Run("wildcard point boundary", func(t *testing.T) {
		policy := func(count int) string {
			patterns := make([]string, count)
			for i := range patterns {
				patterns[i] = fmt.Sprintf("{\"wildcard\":\"%d*\"}", i)
			}
			return "{\"x\":[" + strings.Join(patterns, ",") + "]}"
		}
		_, err := ParseFilterPolicy(policy(10))
		require.NoError(t, err) // (10*1)*10 = 100 points.
		_, err = ParseFilterPolicy(policy(11))
		require.ErrorContains(t, err, "100 wildcard")
	})
	t.Run("multiple wildcard points", func(t *testing.T) {
		_, err := ParseFilterPolicy("{\"x\":[{\"wildcard\":\"a*b*c*\"},{\"wildcard\":\"b*c*d*\"},{\"wildcard\":\"c*d*e*\"},{\"wildcard\":\"d*e*f*\"}]}")
		require.ErrorContains(t, err, "100 wildcard") // (4*9)*4 = 144.
	})
}

func snsFilterList(count int) string {
	values := make([]int, count)
	for i := range values {
		values[i] = i
	}
	raw, _ := json.Marshal(values)
	return string(raw)
}

func TestSNSFilterEquality(t *testing.T) {
	legacy := &FilterPolicy{Attributes: map[string][]string{"event": {"b", "a", "a"}}}
	parsed, err := ParseFilterPolicy("{\"event\":[\"a\",\"b\"]}")
	require.NoError(t, err)
	require.True(t, legacy.Equal(parsed))
	require.True(t, parsed.Equal(legacy))

	pairs := [][2]string{
		{"{\"x\":[\"b\",\"a\",\"a\",{\"prefix\":\"one\"}]}", "{\"x\":[{\"prefix\":\"one\"},\"a\",\"b\"]}"},
		{"{\"x\":[{\"anything-but\":[1,2,1]}]}", "{\"x\":[{\"anything-but\":[2.0,1e0]}]}"},
		{"{\"x\":[301.5]}", "{\"x\":[{\"numeric\":[\"=\",3.015e2]}]}"},
		{"{\"x\":[{\"numeric\":[\">\",0,\"<=\",150]}]}", "{\"x\":[{\"numeric\":[\"<=\",150,\">\",0]}]}"},
		{"{\"$or\":[{\"a\":[\"one\"]},{\"b\":[\"two\"]}]}", "{\"$or\":[{\"b\":[\"two\"]},{\"a\":[\"one\"]},{\"a\":[\"one\"]}]}"},
		{"{\"a\":{\"x\":[2,1]}}", "{\"a\":{\"x\":[1,2]}}"},
	}
	for _, pair := range pairs {
		first, err := ParseFilterPolicy(pair[0])
		require.NoError(t, err)
		second, err := ParseFilterPolicy(pair[1])
		require.NoError(t, err)
		require.True(t, first.Equal(second), "%s != %s", pair[0], pair[1])
	}
	first, err := ParseFilterPolicy("{\"a\":{\"x\":[\"one\"]}}")
	require.NoError(t, err)
	second, err := ParseFilterPolicy("{\"a.x\":[\"one\"]}")
	require.NoError(t, err)
	require.False(t, first.Equal(second))
}

func TestSNSFilterJSONPreservesAdvancedPolicy(t *testing.T) {
	raw := "{\"source\":[{\"prefix\":\"aws.\"}],\"detail\":{\"price\":[{\"numeric\":[\">\",100]}]}}"
	policy, err := ParseFilterPolicy(raw)
	require.NoError(t, err)
	reparsed, err := ParseFilterPolicy(policy.JSON())
	require.NoError(t, err)
	require.True(t, policy.Equal(reparsed))
	require.True(t, reparsed.MatchesMessage(nil, "{\"source\":\"aws.s3\",\"detail\":{\"price\":150}}", "MessageBody"))
	var absent *FilterPolicy
	require.Equal(t, "{}", absent.JSON())
	require.Equal(t, "{}", (&FilterPolicy{}).JSON())
	legacy := &FilterPolicy{Attributes: map[string][]string{"event": {"one", "two"}}}
	fromLegacy, err := ParseFilterPolicy(legacy.JSON())
	require.NoError(t, err)
	require.True(t, legacy.Equal(fromLegacy))
}

func TestSNSFilterAnythingButWithoutWildcardUsesCombinationLimit(t *testing.T) {
	conditions := make([]string, 150)
	for i := range conditions {
		conditions[i] = fmt.Sprintf("{\"anything-but\":%d}", i)
	}
	// Wildcard complexity protections apply to policies using wildcard
	// operators; a plain anything-but list is governed by the 150 limit.
	policy, err := ParseFilterPolicy("{\"x\":[" + strings.Join(conditions, ",") + "]}")
	require.NoError(t, err)
	require.True(t, policy.Matches(map[string]MessageAttribute{"x": {DataType: "Number", StringValue: "200"}}))
}

func TestSNSFilterCustomArrayLabelIsLogicalString(t *testing.T) {
	policy, err := ParseFilterPolicy("{\"x\":[\"rugby\"]}")
	require.NoError(t, err)
	require.True(t, policy.Matches(map[string]MessageAttribute{"x": {DataType: "String.Array.custom", StringValue: "rugby"}}))
	require.False(t, policy.Matches(map[string]MessageAttribute{"x": {DataType: "String.Array.custom", StringValue: "[\"rugby\"]"}}))
}
