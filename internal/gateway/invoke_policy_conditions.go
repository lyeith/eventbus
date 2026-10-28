package gateway

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/netip"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Each compiled condition tests one key. Statements AND these conditions; each
// key's policy values are alternatives (NOR for a negated operator).
type invokeCondition func(map[string][]string) bool

type invokeConditionOperator struct {
	name     string
	negated  bool
	set      string
	ifExists bool
}

type invokeConditionValue func(string, map[string][]string) (matched, valid bool)

func compileInvokeConditions(data json.RawMessage) ([]invokeCondition, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, nil
	}
	if err := validateInvokePolicyJSON(data); err != nil {
		return nil, fmt.Errorf("policy condition: %w", err)
	}
	operators, err := invokeConditionObject(data)
	if err != nil || len(operators) == 0 {
		return nil, fmt.Errorf("policy condition requires a nonempty operator object")
	}
	var conditions []invokeCondition
	for _, name := range sortedInvokeConditionKeys(operators) {
		operator, err := parseInvokeConditionOperator(name)
		if err != nil {
			return nil, err
		}
		keys, err := invokeConditionObject(operators[name])
		if err != nil || len(keys) == 0 {
			return nil, fmt.Errorf("condition %s requires a nonempty key/value object", name)
		}
		var seenKeys []string
		for _, key := range sortedInvokeConditionKeys(keys) {
			for _, previous := range seenKeys {
				if strings.EqualFold(key, previous) {
					return nil, fmt.Errorf("condition %s repeats a context key with different capitalization", name)
				}
			}
			seenKeys = append(seenKeys, key)
			if key == "" || strings.Contains(key, "${") {
				return nil, fmt.Errorf("condition %s has an invalid context key", name)
			}
			values, err := invokeConditionValues(keys[key])
			if err != nil {
				return nil, fmt.Errorf("condition %s key %s: %w", name, key, err)
			}
			condition, err := compileInvokeCondition(key, operator, values)
			if err != nil {
				return nil, fmt.Errorf("condition %s key %s: %w", name, key, err)
			}
			conditions = append(conditions, condition)
		}
	}
	return conditions, nil
}

func parseInvokeConditionOperator(name string) (invokeConditionOperator, error) {
	operator := invokeConditionOperator{name: name}
	if prefix, suffix, found := strings.Cut(operator.name, ":"); found {
		if prefix != "ForAllValues" && prefix != "ForAnyValue" {
			return operator, fmt.Errorf("unsupported condition qualifier %s", prefix)
		}
		operator.set, operator.name = prefix, suffix
	}
	if strings.HasSuffix(operator.name, "IfExists") {
		operator.ifExists = true
		operator.name = strings.TrimSuffix(operator.name, "IfExists")
	}
	switch operator.name {
	case "StringNotEquals", "StringNotEqualsIgnoreCase", "StringNotLike", "ArnNotEquals", "ArnNotLike", "NumericNotEquals", "DateNotEquals", "NotIpAddress":
		operator.negated = true
	case "StringEquals", "StringEqualsIgnoreCase", "StringLike", "ArnEquals", "ArnLike", "NumericEquals", "NumericLessThan", "NumericLessThanEquals", "NumericGreaterThan", "NumericGreaterThanEquals", "DateEquals", "DateLessThan", "DateLessThanEquals", "DateGreaterThan", "DateGreaterThanEquals", "Bool", "BinaryEquals", "IpAddress", "Null":
	default:
		return operator, fmt.Errorf("unsupported condition operator %s", name)
	}
	if operator.set != "" && operator.ifExists {
		return operator, fmt.Errorf("combined set and IfExists modifiers are unsupported")
	}
	if operator.name == "Null" && (operator.ifExists || operator.set != "") {
		return operator, fmt.Errorf("Null does not accept condition modifiers")
	}
	return operator, nil
}

func compileInvokeCondition(key string, operator invokeConditionOperator, values []string) (invokeCondition, error) {
	if operator.name == "Null" {
		wanted := make([]bool, len(values))
		for i, value := range values {
			parsed, ok := invokeConditionBool(value)
			if !ok {
				return nil, fmt.Errorf("Null requires true or false")
			}
			wanted[i] = parsed
		}
		return func(facts map[string][]string) bool {
			actual, present := policyFact(facts, key)
			null := !present || invokeNullDataset(actual)
			for _, value := range wanted {
				if value == null {
					return true
				}
			}
			return false
		}, nil
	}
	matchers := make([]invokeConditionValue, len(values))
	for i, value := range values {
		matcher, err := compileInvokeConditionValue(operator.name, value)
		if err != nil {
			return nil, err
		}
		matchers[i] = matcher
	}
	return func(facts map[string][]string) bool {
		actual, present := policyFact(facts, key)
		if !present && operator.ifExists {
			return true
		}
		// Empty strings are a null dataset for set operators and Null. An
		// ordinary string comparison can still explicitly match an empty string.
		if operator.set != "" && invokeNullDataset(actual) {
			actual = nil
		}
		matchesValue := func(value string) bool {
			matched := false
			for _, matcher := range matchers {
				match, valid := matcher(value, facts)
				if !valid {
					return false
				}
				matched = matched || match
			}
			return matched != operator.negated
		}
		switch operator.set {
		case "ForAllValues":
			for _, value := range actual {
				if !matchesValue(value) {
					return false
				}
			}
			return true
		case "ForAnyValue":
			for _, value := range actual {
				if matchesValue(value) {
					return true
				}
			}
			return false
		default:
			// Unqualified negation applies to the whole collection, rather
			// than accepting merely because one request value differs.
			for _, value := range actual {
				match := matchesValue(value)
				if match != operator.negated {
					return match
				}
			}
			return operator.negated
		}
	}, nil
}

func compileInvokeConditionValue(operator, value string) (invokeConditionValue, error) {
	switch operator {
	case "StringEquals", "StringNotEquals", "StringEqualsIgnoreCase", "StringNotEqualsIgnoreCase":
		if _, err := compilePolicyPattern(value); err != nil {
			return nil, err
		}
		ignoreCase := strings.HasSuffix(operator, "IgnoreCase")
		return func(actual string, facts map[string][]string) (bool, bool) {
			expected, resolved := expandPolicyVariables(value, facts)
			if !resolved {
				return false, true
			}
			if ignoreCase {
				return strings.EqualFold(actual, expected), true
			}
			return actual == expected, true
		}, nil
	case "StringLike", "StringNotLike":
		pattern, err := compilePolicyPattern(value)
		if err != nil {
			return nil, err
		}
		return func(actual string, facts map[string][]string) (bool, bool) {
			matched, resolved := pattern.matches(actual, facts)
			return matched && resolved, true
		}, nil
	case "ArnEquals", "ArnNotEquals", "ArnLike", "ArnNotLike":
		pattern, err := compilePolicyPattern(value)
		if err != nil {
			return nil, err
		}
		dynamic := false
		for _, part := range pattern.parts {
			dynamic = dynamic || part.key != ""
		}
		if !dynamic {
			// An unsupported static ARN must reject the policy, including
			// negated conditions which would otherwise match every request.
			expanded, resolved := expandPolicyVariables(value, nil)
			fields := strings.SplitN(expanded, ":", 6)
			if !resolved || len(fields) != 6 || fields[0] != "arn" || fields[1] == "" || fields[2] == "" || fields[5] == "" {
				return nil, fmt.Errorf("ARN condition requires six fields with arn, partition, service and resource")
			}
		}
		return func(actual string, facts map[string][]string) (bool, bool) {
			matched, resolved := pattern.matchesARN(actual, facts)
			return matched && resolved, true
		}, nil
	}
	if strings.Contains(value, "${") {
		return nil, fmt.Errorf("policy variables are unsupported for %s", operator)
	}
	switch {
	case strings.HasPrefix(operator, "Numeric"):
		expected, ok := invokeConditionNumber(value)
		if !ok {
			return nil, fmt.Errorf("invalid numeric condition value")
		}
		return invokeOrderedCondition(operator, expected, invokeConditionNumber), nil
	case strings.HasPrefix(operator, "Date"):
		expected, ok := invokeConditionDate(value)
		if !ok {
			return nil, fmt.Errorf("invalid date condition value")
		}
		return invokeOrderedCondition(operator, expected, invokeConditionDate), nil
	case operator == "Bool":
		expected, ok := invokeConditionBool(value)
		if !ok {
			return nil, fmt.Errorf("Bool requires true or false")
		}
		return func(actual string, _ map[string][]string) (bool, bool) {
			parsed, valid := invokeConditionBool(actual)
			return parsed == expected, valid
		}, nil
	case operator == "BinaryEquals":
		expected, err := invokeConditionBinary(value)
		if err != nil {
			return nil, fmt.Errorf("invalid base64 condition value")
		}
		return func(actual string, _ map[string][]string) (bool, bool) {
			parsed, err := invokeConditionBinary(actual)
			return bytes.Equal(parsed, expected), err == nil
		}, nil
	case operator == "IpAddress" || operator == "NotIpAddress":
		expected, err := netip.ParsePrefix(value)
		if err != nil {
			address, addressErr := netip.ParseAddr(value)
			if addressErr != nil || address.Zone() != "" {
				return nil, fmt.Errorf("invalid IP address condition value")
			}
			expected = netip.PrefixFrom(address, address.BitLen())
		}
		return func(actual string, _ map[string][]string) (bool, bool) {
			address, err := netip.ParseAddr(actual)
			valid := err == nil && address.Zone() == ""
			return valid && expected.Contains(address), valid
		}, nil
	}
	return nil, fmt.Errorf("unsupported condition operator %s", operator)
}

func invokeOrderedCondition(operator string, expected *big.Rat, parse func(string) (*big.Rat, bool)) invokeConditionValue {
	return func(actual string, _ map[string][]string) (bool, bool) {
		parsed, valid := parse(actual)
		if !valid {
			return false, false
		}
		order := parsed.Cmp(expected)
		switch {
		case strings.HasSuffix(operator, "LessThanEquals"):
			return order <= 0, true
		case strings.HasSuffix(operator, "GreaterThanEquals"):
			return order >= 0, true
		case strings.HasSuffix(operator, "LessThan"):
			return order < 0, true
		case strings.HasSuffix(operator, "GreaterThan"):
			return order > 0, true
		default:
			// Negation belongs to the collection evaluator, not each RHS.
			return order == 0, true
		}
	}
}

var invokeDecimal = regexp.MustCompile(`^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$`)

func invokeConditionNumber(value string) (*big.Rat, bool) {
	// Bound exact-number work before asking big.Rat to allocate an exponent.
	if len(value) > 256 || !invokeDecimal.MatchString(value) {
		return nil, false
	}
	if index := strings.IndexAny(value, "eE"); index >= 0 {
		exponent, err := strconv.ParseInt(value[index+1:], 10, 32)
		if err != nil || exponent < -1024 || exponent > 1024 {
			return nil, false
		}
	}
	mantissa, exponentText, hasExponent := strings.Cut(strings.ToLower(value), "e")
	exponent := int64(0)
	if hasExponent {
		exponent, _ = strconv.ParseInt(exponentText, 10, 32)
	}
	integer, fraction, _ := strings.Cut(mantissa, ".")
	digits := integer + fraction
	numerator, valid := new(big.Int).SetString(digits, 10)
	if !valid {
		return nil, false
	}
	scale := exponent - int64(len(fraction))
	power := new(big.Int).Exp(big.NewInt(10), big.NewInt(absInvokeScale(scale)), nil)
	if scale >= 0 {
		return new(big.Rat).SetInt(numerator.Mul(numerator, power)), true
	}
	return new(big.Rat).SetFrac(numerator, power), true
}

func absInvokeScale(value int64) int64 {
	if value < 0 {
		return -value
	}
	return value
}

func invokeConditionDate(value string) (*big.Rat, bool) {
	if number, valid := invokeConditionNumber(value); valid {
		return number, true
	}
	// time.Parse silently truncates finer fractions; refuse those rather than
	// making distinct date values compare equal. Epoch decimals stay exact.
	if strings.ContainsRune(value, ',') {
		return nil, false
	}
	if index := strings.IndexByte(value, '.'); index >= 0 {
		end := index + 1
		for end < len(value) && value[end] >= '0' && value[end] <= '9' {
			end++
		}
		if end-index-1 > 9 {
			return nil, false
		}
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04Z07:00", "2006-01-02"} {
		parsed, err := time.Parse(layout, value)
		if err != nil || parsed.Year() < 1 {
			continue
		}
		seconds := new(big.Rat).SetInt64(parsed.Unix())
		fraction := new(big.Rat).SetFrac64(int64(parsed.Nanosecond()), int64(time.Second))
		return seconds.Add(seconds, fraction), true
	}
	return nil, false
}

func invokeConditionBool(value string) (bool, bool) {
	switch value {
	case "true":
		return true, true
	case "false":
		return false, true
	default:
		return false, false
	}
}

func invokeConditionBinary(value string) ([]byte, error) {
	if strings.ContainsAny(value, "\r\n") {
		return nil, fmt.Errorf("base64 must not contain line breaks")
	}
	return base64.StdEncoding.Strict().DecodeString(value)
}

func invokeNullDataset(values []string) bool {
	for _, value := range values {
		if value != "" {
			return false
		}
	}
	return true
}

// policyFact preserves absence separately from an existing empty dataset. Key
// names are case-insensitive, including the caller-defined suffix of tag keys.
// Aggregate differently cased facts in sorted-key order; never let map iteration
// choose which value controls a permission decision.
func policyFact(facts map[string][]string, key string) ([]string, bool) {
	var matching []string
	for candidate := range facts {
		if strings.EqualFold(candidate, key) {
			matching = append(matching, candidate)
		}
	}
	if len(matching) == 0 {
		return nil, false
	}
	sort.Strings(matching)
	if len(matching) == 1 {
		return facts[matching[0]], true
	}
	var values []string
	for _, candidate := range matching {
		values = append(values, facts[candidate]...)
	}
	return values, true
}

func invokeConditionValues(data json.RawMessage) ([]string, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var raw any
	if err := decoder.Decode(&raw); err != nil {
		return nil, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("expected one condition value")
	}
	entries, array := raw.([]any)
	if !array {
		entries = []any{raw}
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("empty condition value array")
	}
	values := make([]string, 0, len(entries))
	for _, entry := range entries {
		switch value := entry.(type) {
		case string:
			values = append(values, value)
		case json.Number:
			values = append(values, value.String())
		case bool:
			values = append(values, strconv.FormatBool(value))
		default:
			return nil, fmt.Errorf("expected scalar or array of string, number or boolean values")
		}
	}
	return values, nil
}

func invokeConditionObject(data json.RawMessage) (map[string]json.RawMessage, error) {
	var values map[string]json.RawMessage
	if err := json.Unmarshal(data, &values); err != nil || values == nil {
		return nil, fmt.Errorf("expected condition object")
	}
	return values, nil
}

func sortedInvokeConditionKeys(values map[string]json.RawMessage) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
