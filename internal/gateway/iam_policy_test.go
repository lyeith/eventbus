package gateway

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

const policyTestARN = "arn:aws:execute-api:eu-west-1:111111111111:owned-api/prod/GET/items/alice"

func nativePolicyForTest(t *testing.T, statement any) invokePolicy {
	t.Helper()
	data, err := json.Marshal(map[string]any{"Version": "2012-10-17", "Statement": statement})
	require.NoError(t, err)
	policy, err := compileInvokePolicy(data)
	require.NoError(t, err)
	return policy
}

func TestInvokePolicyUsesCurrentAPIExecutionResource(t *testing.T) {
	policy := nativePolicyForTest(t, map[string]any{"Effect": "Allow", "Action": "execute-api:Invoke", "Resource": policyTestARN})
	require.True(t, policy.permits(policyTestARN, nil))
	for _, resource := range []string{
		strings.Replace(policyTestARN, "eu-west-1", "us-east-1", 1),
		strings.Replace(policyTestARN, "111111111111", "222222222222", 1),
		strings.Replace(policyTestARN, "owned-api", "other-api", 1),
		strings.Replace(policyTestARN, "/prod/", "/dev/", 1),
		strings.Replace(policyTestARN, "/GET/", "/POST/", 1),
		strings.Replace(policyTestARN, "/alice", "/Alice", 1),
		policyTestARN + "/child",
	} {
		require.False(t, policy.permits(resource, nil), resource)
	}
	// Resource identity retains encoded data within one original component.
	nested := strings.Replace(policyTestARN, "/alice", "/Type/example/history", 1)
	encoded := strings.Replace(nested, "/example/history", "/example%2Fhistory", 1)
	policy = nativePolicyForTest(t, map[string]any{"Effect": "Allow", "Action": "execute-api:Invoke", "Resource": nested})
	require.False(t, policy.permits(encoded, nil))
	policy = nativePolicyForTest(t, map[string]any{"Effect": "Allow", "Action": "execute-api:Invoke", "Resource": encoded})
	require.True(t, policy.permits(encoded, nil))
	require.False(t, policy.permits(nested, nil))
	require.False(t, policy.permits(strings.Replace(encoded, "%2F", "%252F", 1), nil))
}

func TestInvokePolicyComplementsDefaultDenyAndDenyPrecedence(t *testing.T) {
	allow := map[string]any{"Effect": "Allow", "Action": "*", "Resource": "*"}
	deny := map[string]any{"Effect": "Deny", "Action": "execute-api:Invoke", "Resource": policyTestARN}
	for _, test := range []struct {
		name      string
		statement any
		want      bool
	}{
		{"empty deny-all", []any{}, false},
		{"action and resource arrays", map[string]any{"Effect": "Allow", "Action": []string{"s3:GetObject", "ExEcUtE-aPi:InVoKe"}, "Resource": []string{"arn:aws:s3:::bucket/*", policyTestARN}}, true},
		{"implicit deny wrong action", map[string]any{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}, false},
		{"NotAction excludes unrelated operation", map[string]any{"Effect": "Allow", "NotAction": "execute-api:InvalidateCache", "Resource": policyTestARN}, true},
		{"NotAction excludes Invoke", map[string]any{"Effect": "Allow", "NotAction": []string{"s3:*", "EXECUTE-API:*"}, "Resource": "*"}, false},
		{"NotResource allows other resource", map[string]any{"Effect": "Allow", "Action": "execute-api:Invoke", "NotResource": policyTestARN + "/private"}, true},
		{"NotResource excludes current", map[string]any{"Effect": "Allow", "Action": "execute-api:Invoke", "NotResource": []string{policyTestARN, "arn:aws:s3:::bucket/*"}}, false},
		{"deny first", []any{deny, allow}, false},
		{"deny last", []any{allow, deny}, false},
		{"Deny NotAction", []any{allow, map[string]any{"Effect": "Deny", "NotAction": "iam:*", "Resource": "*"}}, false},
		{"Deny NotResource", []any{allow, map[string]any{"Effect": "Deny", "Action": "execute-api:*", "NotResource": policyTestARN + "/public"}}, false},
		{"wildcards span resource separators", map[string]any{"Effect": "Allow", "Action": "execute-api:*", "Resource": "arn:aws:execute-api:*:*:owned-api/*"}, true},
		{"incomplete identity ARN", map[string]any{"Effect": "Allow", "Action": "execute-api:Invoke", "Resource": "arn:aws:execute-api"}, true},
		{"case sensitive resource", map[string]any{"Effect": "Allow", "Action": "execute-api:Invoke", "Resource": strings.Replace(policyTestARN, "/alice", "/Alice", 1)}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, nativePolicyForTest(t, test.statement).permits(policyTestARN, nil))
		})
	}
}

func TestInvokePolicyVariablesConditionsAndOwnedCompilation(t *testing.T) {
	resource := strings.Replace(policyTestARN, "/alice", "/${aws:username}", 1)
	data, err := json.Marshal(map[string]any{
		"Version": "2012-10-17",
		"Statement": []any{
			map[string]any{"Sid": "OwnResource", "Effect": "Allow", "Action": "execute-api:Invoke", "Resource": resource,
				"Condition": map[string]any{"StringEquals": map[string]any{"aws:PrincipalAccount": "222222222222"}, "Bool": map[string]any{"aws:SecureTransport": true}}},
			map[string]any{"Effect": "Deny", "Action": "*", "Resource": "*",
				"Condition": map[string]any{"StringEquals": map[string]any{"aws:PrincipalTag/suspended": "yes"}}},
		},
	})
	require.NoError(t, err)
	policy, err := compileInvokePolicy(data)
	require.NoError(t, err)
	for index := range data {
		data[index] = 0
	}
	facts := map[string][]string{"aws:username": {"alice"}, "aws:PrincipalAccount": {"222222222222"}, "aws:SecureTransport": {"true"}}
	require.True(t, policy.permits(policyTestARN, facts))
	require.False(t, policy.permits(policyTestARN, nil), "an unresolved Resource variable cannot authorize")
	facts["aws:PrincipalTag/suspended"] = []string{"yes"}
	require.False(t, policy.permits(policyTestARN, facts), "matching conditional Deny overrides Allow")
	delete(facts, "aws:PrincipalTag/suspended")
	facts["aws:username"] = []string{"alice", "bob"}
	require.False(t, policy.permits(policyTestARN, facts), "multivalued context is not a policy variable")

	// Shared compiled policies retain no mutable per-request variable state.
	var workers sync.WaitGroup
	for index := range 32 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			current := map[string][]string{"aws:username": {"alice"}, "aws:PrincipalAccount": {"222222222222"}, "aws:SecureTransport": {"true"}}
			if index%2 != 0 {
				current["aws:username"] = []string{"bob"}
			}
			if got := policy.permits(policyTestARN, current); got != (index%2 == 0) {
				t.Errorf("request %d decision=%v", index, got)
			}
		}()
	}
	workers.Wait()
}

func TestInvokePolicyVariableDefaultsAndLanguageVersion(t *testing.T) {
	for _, version := range []string{"", "2008-10-17", "2012-10-17"} {
		t.Run(version, func(t *testing.T) {
			document := map[string]any{"Statement": map[string]any{"Effect": "Allow", "Action": "execute-api:Invoke",
				"Resource": "*", "Condition": map[string]any{"StringEquals": map[string]any{"aws:username": "${aws:PrincipalTag/owner}"}}}}
			if version != "" {
				document["Version"] = version
			}
			data, err := json.Marshal(document)
			require.NoError(t, err)
			policy, err := compileInvokePolicy(data)
			require.NoError(t, err)
			require.Equal(t, version == "2012-10-17", policy.permits(policyTestARN, map[string][]string{
				"aws:username": {"alice"}, "aws:PrincipalTag/owner": {"alice"},
			}))
			require.Equal(t, version != "2012-10-17", policy.permits(policyTestARN, map[string][]string{
				"aws:username": {"${aws:PrincipalTag/owner}"}, "aws:PrincipalTag/owner": {"alice"},
			}))
		})
	}
	policy := nativePolicyForTest(t, map[string]any{"Effect": "Allow", "Action": "*",
		"Resource": strings.Replace(policyTestARN, "/alice", "/${aws:username, 'alice'}", 1)})
	require.True(t, policy.permits(policyTestARN, nil))
	require.False(t, policy.permits(policyTestARN, map[string][]string{"aws:username": {"bob"}}))
	policy = nativePolicyForTest(t, map[string]any{"Effect": "Allow", "Action": "*",
		"NotResource": strings.Replace(policyTestARN, "/alice", "/${aws:username}", 1)})
	require.False(t, policy.permits(policyTestARN, nil), "unresolved NotResource cannot become an allow-all")
}

func TestInvokePolicyResourceVariablesExpandNativeWildcards(t *testing.T) {
	resource := strings.Replace(policyTestARN, "/alice", "/${aws:PrincipalTag/owner}", 1)
	facts := map[string][]string{"aws:PrincipalTag/owner": {"a?ic*"}}
	policy := nativePolicyForTest(t, map[string]any{"Effect": "Allow", "Action": "*", "Resource": resource})
	require.True(t, policy.permits(policyTestARN, facts))
	require.False(t, policy.permits(strings.Replace(policyTestARN, "/alice", "/bob", 1), facts))
	policy = nativePolicyForTest(t, map[string]any{"Effect": "Allow", "Action": "*", "NotResource": resource})
	require.False(t, policy.permits(policyTestARN, facts))
	require.True(t, policy.permits(strings.Replace(policyTestARN, "/alice", "/bob", 1), facts))
	policy = nativePolicyForTest(t, map[string]any{"Effect": "Allow", "Action": "*",
		"Resource": strings.Replace(policyTestARN, "/alice", "/${missing, 'a*'}", 1)})
	require.True(t, policy.permits(policyTestARN, nil))
}

func TestInvokePolicyRejectsMalformedOrUnsupportedDocuments(t *testing.T) {
	for _, document := range []string{
		"", "null", "[]", "{}", "true",
		"{" + "\"Statement\":[],\"Statement\":[]}",
		"{\"Statement\":[]}{}",
		"{\"Statement\":null}",
		"{\"Statement\":[null]}",
		"{\"Version\":null,\"Statement\":[]}",
		"{\"Version\":\"\",\"Statement\":[]}",
		"{\"Version\":\"2020-01-01\",\"Statement\":[]}",
		"{\"version\":\"2012-10-17\",\"Statement\":[]}",
		"{\"Id\":null,\"Statement\":[]}",
		"{\"Statement\":{\"Effect\":\"allow\",\"Action\":\"*\",\"Resource\":\"*\"}}",
		"{\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"*\",\"NotAction\":\"*\",\"Resource\":\"*\"}}",
		"{\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"*\",\"Resource\":\"*\",\"NotResource\":\"*\"}}",
		"{\"Statement\":{\"Effect\":\"Allow\",\"Resource\":\"*\"}}",
		"{\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"*\"}}",
		"{\"Statement\":{\"Effect\":\"Allow\",\"Action\":[],\"Resource\":\"*\"}}",
		"{\"Statement\":{\"Effect\":\"Allow\",\"Action\":[1],\"Resource\":\"*\"}}",
		"{\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"*\",\"Resource\":\"not-an-arn\"}}",
		"{\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"*\",\"Resource\":\"*\",\"Principal\":\"*\"}}",
		"{\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"*\",\"Resource\":\"*\",\"Sid\":null}}",
		"{\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"*\",\"Resource\":\"*\",\"Condition\":{\"Unknown\":{\"aws:username\":\"alice\"}}}}",
		"{\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"*\",\"Resource\":\"*\",\"Condition\":{}}}",
		"{\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"*\",\"Resource\":\"*\",\"Condition\":null}}",
		"{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"${aws:action}\",\"Resource\":\"*\"}}",
		"{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"*\",\"Resource\":\"arn:aws:execute-api:${aws:RequestedRegion}:*:api/*\"}}",
		"{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"*\",\"Resource\":\"arn:aws:execute-api:*:*:api/${unfinished\"}}",
		"{\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"*\",\"Resource\":\"*\",\"Effect\":\"Deny\"}}",
		"{\"Statement\":{\"Effect\":\"Deny\",\"effect\":\"Allow\",\"Action\":\"*\",\"Resource\":\"*\"}}",
		"{\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"s3:*\",\"action\":\"*\",\"Resource\":\"*\"}}",
		"{\"Statement\":[],\"statement\":{\"Effect\":\"Allow\",\"Action\":\"*\",\"Resource\":\"*\"}}",
	} {
		t.Run(document, func(t *testing.T) {
			_, err := compileInvokePolicy([]byte(document))
			require.Error(t, err)
		})
	}
}

func TestInvokePolicyKeepsAuthorizerResponseLimitsSeparate(t *testing.T) {
	resource := policyTestARN + strings.Repeat("x", 513)
	policy := nativePolicyForTest(t, map[string]any{"Effect": "Allow", "Action": "*", "Resource": resource})
	require.True(t, policy.permits(resource, nil))
	authorizer, err := json.Marshal(map[string]any{"principalId": "owned",
		"policyDocument": map[string]any{"Version": "2012-10-17", "Statement": map[string]any{"Effect": "Allow", "Action": "*", "Resource": resource}}})
	require.NoError(t, err)
	_, err = parseAuthorizerResponse(authorizer)
	require.Error(t, err, "native identity policies do not remove REQUEST's512-character output limit")
}
