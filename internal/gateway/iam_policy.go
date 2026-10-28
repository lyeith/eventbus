package gateway

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// compileInvokePolicy accepts an effective identity policy, not an IAM resource
// policy or control-plane resource. Unsupported semantics reject the candidate.
func compileInvokePolicy(data []byte) (invokePolicy, error) {
	if err := validateInvokePolicyJSON(data); err != nil {
		return invokePolicy{}, err
	}
	members, err := invokePolicyMembers(data, "Version", "Id", "Statement")
	if err != nil {
		return invokePolicy{}, err
	}
	for _, name := range []string{"Version", "Id"} {
		if err := optionalPolicyString(members, name); err != nil {
			return invokePolicy{}, err
		}
	}
	var document struct {
		Version   string          "json:\"Version\""
		ID        string          "json:\"Id\""
		Statement json.RawMessage "json:\"Statement\""
	}
	if err := decodeStrict(data, &document); err != nil {
		return invokePolicy{}, fmt.Errorf("invalid Invoke policy: %w", err)
	}
	if _, present := members["Version"]; present && document.Version != "2008-10-17" && document.Version != "2012-10-17" {
		return invokePolicy{}, fmt.Errorf("unsupported Invoke policy version")
	}
	variables := document.Version == "2012-10-17"
	trimmed := bytes.TrimSpace(document.Statement)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return invokePolicy{}, fmt.Errorf("Invoke policy requires Statement")
	}
	var entries []json.RawMessage
	switch trimmed[0] {
	case '{':
		entries = []json.RawMessage{trimmed}
	case '[':
		if err := json.Unmarshal(trimmed, &entries); err != nil {
			return invokePolicy{}, fmt.Errorf("invalid Invoke policy statements: %w", err)
		}
	default:
		return invokePolicy{}, fmt.Errorf("invalid Invoke policy statements")
	}
	policy := invokePolicy{statements: make([]policyStatement, 0, len(entries))}
	for _, entry := range entries {
		statement, err := compileInvokeStatement(entry, variables)
		if err != nil {
			return invokePolicy{}, err
		}
		policy.statements = append(policy.statements, statement)
	}
	return policy, nil
}

func compileInvokeStatement(data json.RawMessage, variables bool) (policyStatement, error) {
	members, err := invokePolicyMembers(data, "Sid", "Effect", "Action", "NotAction", "Resource", "NotResource", "Condition")
	if err != nil {
		return policyStatement{}, err
	}
	if err := optionalPolicyString(members, "Sid"); err != nil {
		return policyStatement{}, err
	}
	var raw struct {
		SID         string          "json:\"Sid\""
		Effect      string          "json:\"Effect\""
		Action      json.RawMessage "json:\"Action\""
		NotAction   json.RawMessage "json:\"NotAction\""
		Resource    json.RawMessage "json:\"Resource\""
		NotResource json.RawMessage "json:\"NotResource\""
		Condition   json.RawMessage "json:\"Condition\""
	}
	if err := decodeStrict(data, &raw); err != nil {
		return policyStatement{}, fmt.Errorf("invalid Invoke policy statement: %w", err)
	}
	if raw.Effect != "Allow" && raw.Effect != "Deny" {
		return policyStatement{}, fmt.Errorf("invalid Invoke policy effect")
	}
	actions, notActions, err := invokePolicyAlternatives(raw.Action, raw.NotAction, "Action")
	if err != nil {
		return policyStatement{}, err
	}
	resources, notResources, err := invokePolicyAlternatives(raw.Resource, raw.NotResource, "Resource")
	if err != nil {
		return policyStatement{}, err
	}
	statement := policyStatement{Effect: raw.Effect, notActions: notActions, notResources: notResources}
	for _, action := range actions {
		if variables && strings.Contains(action, "${") {
			return policyStatement{}, fmt.Errorf("policy variables are unsupported in Action")
		}
		compiled, err := compilePolicyGlob(strings.ToLower(action))
		if err != nil {
			return policyStatement{}, fmt.Errorf("invalid Invoke policy action pattern: %w", err)
		}
		statement.actions = append(statement.actions, compiled)
	}
	for _, resource := range resources {
		pattern, err := compileInvokeResource(resource, variables)
		if err != nil {
			return policyStatement{}, err
		}
		statement.resources = append(statement.resources, pattern)
	}
	conditionData := raw.Condition
	if !variables && len(conditionData) != 0 {
		conditionData, err = literalLegacyPolicyVariables(conditionData)
		if err != nil {
			return policyStatement{}, err
		}
	}
	statement.conditions, err = compileInvokeConditions(conditionData)
	if err != nil {
		return policyStatement{}, fmt.Errorf("invalid Invoke policy condition: %w", err)
	}
	return statement, nil
}

func invokePolicyMembers(data []byte, allowed ...string) (map[string]json.RawMessage, error) {
	var members map[string]json.RawMessage
	if err := json.Unmarshal(data, &members); err != nil || members == nil {
		return nil, fmt.Errorf("Invoke policy element must be an object")
	}
	for name := range members {
		found := false
		for _, candidate := range allowed {
			found = found || name == candidate
		}
		if !found {
			return nil, fmt.Errorf("unsupported Invoke policy element")
		}
	}
	return members, nil
}

func optionalPolicyString(members map[string]json.RawMessage, name string) error {
	if raw, present := members[name]; present {
		if trimmed := bytes.TrimSpace(raw); len(trimmed) == 0 || trimmed[0] != '"' {
			return fmt.Errorf("Invoke policy %s must be a string", name)
		}
	}
	return nil
}

func invokePolicyAlternatives(positive, negative json.RawMessage, name string) ([]string, bool, error) {
	if (len(positive) != 0) == (len(negative) != 0) {
		return nil, false, fmt.Errorf("Invoke policy requires exactly one of %s and Not%s", name, name)
	}
	not := len(negative) != 0
	if not {
		positive = negative
	}
	values, err := stringList(positive)
	if err != nil {
		return nil, false, fmt.Errorf("invalid Invoke policy %s: %w", name, err)
	}
	return values, not, nil
}

func compileInvokeResource(value string, variables bool) (policyPattern, error) {
	if value != "*" && !strings.HasPrefix(value, "arn:") {
		return policyPattern{}, fmt.Errorf("Invoke policy Resource must be an ARN or *")
	}
	if variables {
		if position := strings.Index(value, "${"); position >= 0 && strings.Count(value[:position], ":") < 5 {
			return policyPattern{}, fmt.Errorf("policy variables require the resource portion of an ARN")
		}
	} else {
		// Earlier language versions treat variable syntax literally.
		value = strings.ReplaceAll(value, "${", "${$}{")
	}
	// Identity policies complete omitted ARN fields with wildcards. REQUEST
	// response resources retain their separate native output restrictions.
	if strings.HasPrefix(value, "arn:") && !strings.Contains(value, "${") {
		for strings.Count(value, ":") < 5 {
			value += ":*"
		}
	}
	pattern, err := compilePolicyPattern(value)
	if err != nil {
		return policyPattern{}, fmt.Errorf("invalid Invoke policy resource pattern: %w", err)
	}
	return pattern, nil
}

func literalLegacyPolicyVariables(data []byte) ([]byte, error) {
	var value any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	var escape func(any) any
	escape = func(item any) any {
		switch typed := item.(type) {
		case string:
			return strings.ReplaceAll(typed, "${", "${$}{")
		case []any:
			for index := range typed {
				typed[index] = escape(typed[index])
			}
		case map[string]any:
			for key, child := range typed {
				typed[key] = escape(child)
			}
		}
		return item
	}
	return json.Marshal(escape(value))
}

// Native policy candidates reject duplicate members before publishing grants.
func validateInvokePolicyJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	first, err := decoder.Token()
	if err != nil || first != json.Delim('{') {
		return fmt.Errorf("policy must be one JSON object")
	}
	var walk func(json.Token, int) error
	walk = func(token json.Token, depth int) error {
		if depth > 64 {
			return fmt.Errorf("policy JSON nesting exceeds 64")
		}
		delimiter, container := token.(json.Delim)
		if !container {
			return nil
		}
		switch delimiter {
		case '{':
			seen := make(map[string]bool)
			for decoder.More() {
				member, err := decoder.Token()
				if err != nil {
					return err
				}
				name, ok := member.(string)
				if !ok || seen[name] {
					return fmt.Errorf("duplicate or invalid policy JSON member")
				}
				seen[name] = true
				child, err := decoder.Token()
				if err != nil {
					return err
				}
				if err := walk(child, depth+1); err != nil {
					return err
				}
			}
		case '[':
			for decoder.More() {
				child, err := decoder.Token()
				if err != nil {
					return err
				}
				if err := walk(child, depth+1); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("invalid policy JSON delimiter")
		}
		end, err := decoder.Token()
		if err != nil || delimiter == '{' && end != json.Delim('}') || delimiter == '[' && end != json.Delim(']') {
			return fmt.Errorf("invalid policy JSON container")
		}
		return nil
	}
	if err := walk(first, 0); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return fmt.Errorf("policy must contain one JSON value")
	}
	return nil
}
