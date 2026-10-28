package gateway

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPolicyPatternExpandsVariableWildcardsAndKeepsFixedEscapes(t *testing.T) {
	for _, test := range []struct {
		pattern, value string
		facts          map[string][]string
		want, resolved bool
	}{
		{"a*/?", "abc/x", nil, true, true},
		{"a*/?", "a/x/yz", nil, false, true},
		{"${aws:PrincipalTag/team}/*", "red/child", map[string][]string{"AWS:PrincipalTag/team": {"red"}}, true, true},
		{"${aws:PrincipalTag/team}/*", "red/child", map[string][]string{"aws:PrincipalTag/team": {"*"}}, true, true},
		{"${aws:PrincipalTag/team}/*", "*/child", map[string][]string{"aws:PrincipalTag/team": {"*"}}, true, true},
		{"${*}/${?}/${$}", "*/?/$", nil, true, true},
		{"${*}/${?}", "wild/value", nil, false, true},
		{"${missing}", "", nil, false, false},
		{"${missing, 'default'}", "default", nil, true, true},
		{"${multi, 'default'}", "default", map[string][]string{"multi": {"one", "two"}}, false, false},
		{"${key}", "nested", map[string][]string{"key": {"${other}"}, "other": {"nested"}}, false, true},
		{"", "", nil, true, true},
	} {
		t.Run(test.pattern+"/"+test.value, func(t *testing.T) {
			pattern, err := compilePolicyPattern(test.pattern)
			require.NoError(t, err)
			got, resolved := pattern.matches(test.value, test.facts)
			require.Equal(t, test.want, got)
			require.Equal(t, test.resolved, resolved)
		})
	}
	for _, value := range []string{"${", "${}", "${a${b}}", "${x, unquoted}", "${x, 'a'b'}"} {
		_, err := compilePolicyPattern(value)
		require.Error(t, err, value)
	}
}

func TestPolicyPatternARNFieldsAndFullARNVariables(t *testing.T) {
	const arn = "arn:aws:iam::222222222222:role/owned/path"
	pattern, err := compilePolicyPattern("arn:aws:iam::*:role/owned/*")
	require.NoError(t, err)
	got, resolved := pattern.matchesARN(arn, nil)
	require.True(t, resolved)
	require.True(t, got)
	pattern, err = compilePolicyPattern("arn:aws:*:222222222222:role/*")
	require.NoError(t, err)
	got, resolved = pattern.matchesARN(arn, nil)
	require.True(t, resolved)
	require.False(t, got, "a wildcard cannot swallow an extra ARN field")
	pattern, err = compilePolicyPattern("${aws:PrincipalArn}")
	require.NoError(t, err)
	got, resolved = pattern.matchesARN(arn, map[string][]string{"aws:PrincipalArn": {arn}})
	require.True(t, resolved)
	require.True(t, got)
	got, resolved = pattern.matchesARN("arn:aws:iam::222222222222:role/other", map[string][]string{"aws:PrincipalArn": {arn}})
	require.True(t, resolved)
	require.False(t, got)
}
