package messaging

import (
	"encoding/json"
	"fmt"
	"net/netip"
	"strconv"
	"strings"
)

const snsFilterNumberScale = int64(100000)

type snsFilterMatcher struct {
	kind        string
	value       any
	comparisons []snsFilterComparison
	exclusions  []any
	negated     *snsFilterMatcher
	subnet      netip.Prefix
	wildcard    []string
}

type snsFilterComparison struct {
	op    string
	value int64
}

func parseSNSFilterMatcher(value any) (snsFilterMatcher, int, error) {
	matcher := snsFilterMatcher{kind: "literal", value: value}
	switch v := value.(type) {
	case nil, bool, string:
		return matcher, 0, nil
	case json.Number:
		if _, err := snsFilterNumber(v); err != nil {
			return matcher, 0, err
		}
		return matcher, 0, nil
	case map[string]any:
		if len(v) != 1 {
			return matcher, 0, fmt.Errorf("a match operator must contain exactly one property")
		}
		for operator, argument := range v {
			matcher.kind = operator
			matcher.value = argument
			switch operator {
			case "exists":
				if _, ok := argument.(bool); !ok {
					return matcher, 0, fmt.Errorf("exists requires a boolean")
				}
				return matcher, 0, nil
			case "prefix", "suffix", "equals-ignore-case", "wildcard", "cidr":
				text, ok := argument.(string)
				if !ok {
					return matcher, 0, fmt.Errorf("%s requires a string", operator)
				}
				if operator == "wildcard" {
					pattern, points, err := parseSNSFilterWildcard(text)
					matcher.wildcard = pattern
					return matcher, points, err
				}
				if operator == "cidr" {
					subnet, err := netip.ParsePrefix(text)
					if err != nil || !subnet.Addr().Is4() {
						return matcher, 0, fmt.Errorf("cidr requires an IPv4 CIDR")
					}
					matcher.subnet = subnet.Masked()
				}
				return matcher, 0, nil
			case "numeric":
				conditions, err := parseSNSFilterNumeric(argument)
				matcher.comparisons = conditions
				return matcher, 0, err
			case "anything-but":
				return parseSNSFilterAnythingBut(argument)
			default:
				return matcher, 0, fmt.Errorf("unknown match operator %q", operator)
			}
		}
	}
	return matcher, 0, fmt.Errorf("a match condition must be a scalar or operator object")
}

func parseSNSFilterAnythingBut(argument any) (snsFilterMatcher, int, error) {
	matcher := snsFilterMatcher{kind: "anything-but"}
	if object, ok := argument.(map[string]any); ok {
		if len(object) != 1 {
			return matcher, 0, fmt.Errorf("anything-but requires a single string operator")
		}
		for operator := range object {
			switch operator {
			case "prefix", "suffix", "wildcard":
			default:
				return matcher, 0, fmt.Errorf("unsupported anything-but operator %q", operator)
			}
		}
		negated, points, err := parseSNSFilterMatcher(object)
		matcher.negated = &negated
		return matcher, 1 + points, err
	}
	values, ok := argument.([]any)
	if !ok {
		values = []any{argument}
	}
	if len(values) == 0 {
		return matcher, 0, fmt.Errorf("anything-but list must not be empty")
	}
	kind := ""
	for _, value := range values {
		switch v := value.(type) {
		case string:
			if kind == "number" {
				return matcher, 0, fmt.Errorf("anything-but values must have the same type")
			}
			kind = "string"
		case json.Number:
			if kind == "string" {
				return matcher, 0, fmt.Errorf("anything-but values must have the same type")
			}
			if _, err := snsFilterNumber(v); err != nil {
				return matcher, 0, err
			}
			kind = "number"
		default:
			return matcher, 0, fmt.Errorf("anything-but accepts only strings or numbers")
		}
	}
	matcher.exclusions = values
	return matcher, 1, nil
}

// SNS numbers are bounded and have at most five fractional digits. Normalize
// decimal/exponent notation directly into a scaled integer, without floating
// point rounding or allocating unbounded integers for hostile exponents.
func snsFilterNumber(number json.Number) (int64, error) {
	raw := strings.ToLower(string(number))
	negative := strings.HasPrefix(raw, "-")
	if negative {
		raw = raw[1:]
	}
	parts := strings.SplitN(raw, "e", 2)
	mantissa := parts[0]
	fraction := 0
	if point := strings.IndexByte(mantissa, '.'); point >= 0 {
		fraction = len(mantissa) - point - 1
		mantissa = mantissa[:point] + mantissa[point+1:]
	}
	digits := strings.TrimLeft(mantissa, "0")
	if digits == "" {
		return 0, nil
	}
	exponent := int64(0)
	if len(parts) == 2 {
		var err error
		exponent, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("numeric exponent exceeds supported bounds")
		}
	}
	trimmed := strings.TrimRight(digits, "0")
	trailing := len(digits) - len(trimmed)
	// Bound the exponent before adding the fraction/trailing offsets.
	if exponent > int64(len(mantissa))+15 || exponent < -int64(len(mantissa))-5 {
		return 0, fmt.Errorf("numeric exponent exceeds supported bounds")
	}
	scaleExponent := exponent - int64(fraction) + 5 + int64(trailing)
	if scaleExponent < 0 {
		return 0, fmt.Errorf("numeric values require at most five decimal places")
	}
	if scaleExponent > 14 || int64(len(trimmed))+scaleExponent > 15 {
		return 0, fmt.Errorf("numeric values must be between -1 billion and 1 billion")
	}
	value, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid numeric value")
	}
	for i := int64(0); i < scaleExponent; i++ {
		value *= 10
	}
	if negative {
		value = -value
	}
	if value < -1000000000*snsFilterNumberScale || value > 1000000000*snsFilterNumberScale {
		return 0, fmt.Errorf("numeric values must be between -1 billion and 1 billion")
	}
	return value, nil
}

func parseSNSFilterNumeric(argument any) ([]snsFilterComparison, error) {
	values, ok := argument.([]any)
	if !ok || len(values) != 2 && len(values) != 4 {
		return nil, fmt.Errorf("numeric requires one comparison or a two-sided range")
	}
	conditions := make([]snsFilterComparison, 0, len(values)/2)
	for i := 0; i < len(values); i += 2 {
		operator, ok := values[i].(string)
		if !ok || operator != "=" && operator != ">" && operator != ">=" && operator != "<" && operator != "<=" {
			return nil, fmt.Errorf("invalid numeric comparison")
		}
		number, ok := values[i+1].(json.Number)
		if !ok {
			return nil, fmt.Errorf("numeric comparison requires a number")
		}
		value, err := snsFilterNumber(number)
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, snsFilterComparison{op: operator, value: value})
	}
	if len(conditions) == 2 {
		lower, upper := conditions[0], conditions[1]
		if strings.HasPrefix(lower.op, "<") {
			lower, upper = upper, lower
		}
		if !strings.HasPrefix(lower.op, ">") || !strings.HasPrefix(upper.op, "<") {
			return nil, fmt.Errorf("numeric ranges require a lower and upper bound")
		}
		if lower.value > upper.value || lower.value == upper.value && (lower.op == ">" || upper.op == "<") {
			return nil, fmt.Errorf("numeric range is empty")
		}
	}
	return conditions, nil
}

func parseSNSFilterWildcard(pattern string) ([]string, int, error) {
	var parts []string
	var part strings.Builder
	count := 0
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '\\':
			i++
			if i == len(pattern) || pattern[i] != '\\' && pattern[i] != '*' {
				return nil, 0, fmt.Errorf("wildcard escapes must precede a star or backslash")
			}
			part.WriteByte(pattern[i])
		case '*':
			count++
			parts = append(parts, part.String())
			part.Reset()
		default:
			part.WriteByte(pattern[i])
		}
	}
	parts = append(parts, part.String())
	if count > 3 {
		return nil, 0, fmt.Errorf("wildcard patterns must not contain more than three wildcards")
	}
	points := count
	if count > 1 {
		points *= 3
	}
	return parts, points, nil
}

func (matcher snsFilterMatcher) matches(value any) bool {
	switch matcher.kind {
	case "literal":
		return snsFilterScalarEqual(matcher.value, value)
	case "numeric":
		number, ok := value.(json.Number)
		if !ok {
			return false
		}
		actual, err := snsFilterNumber(number)
		if err != nil {
			return false
		}
		for _, condition := range matcher.comparisons {
			switch condition.op {
			case "=":
				if actual != condition.value {
					return false
				}
			case ">":
				if actual <= condition.value {
					return false
				}
			case ">=":
				if actual < condition.value {
					return false
				}
			case "<":
				if actual >= condition.value {
					return false
				}
			case "<=":
				if actual > condition.value {
					return false
				}
			}
		}
		return true
	case "anything-but":
		if matcher.negated != nil {
			if _, ok := value.(string); !ok {
				return false
			}
			return !matcher.negated.matches(value)
		}
		if len(matcher.exclusions) == 0 || !snsFilterSameScalarKind(matcher.exclusions[0], value) {
			return false
		}
		for _, excluded := range matcher.exclusions {
			if snsFilterScalarEqual(excluded, value) {
				return false
			}
		}
		if number, ok := value.(json.Number); ok {
			_, err := snsFilterNumber(number)
			return err == nil
		}
		return true
	}
	text, ok := value.(string)
	if !ok {
		return false
	}
	argument, _ := matcher.value.(string)
	switch matcher.kind {
	case "prefix":
		return strings.HasPrefix(text, argument)
	case "suffix":
		return strings.HasSuffix(text, argument)
	case "equals-ignore-case":
		return strings.EqualFold(text, argument)
	case "cidr":
		address, err := netip.ParseAddr(text)
		return err == nil && address.Is4() && matcher.subnet.Contains(address)
	case "wildcard":
		return snsFilterWildcardMatches(matcher.wildcard, text)
	}
	return false
}

func snsFilterSameScalarKind(a, b any) bool {
	switch a.(type) {
	case string:
		_, ok := b.(string)
		return ok
	case json.Number:
		_, ok := b.(json.Number)
		return ok
	}
	return false
}

func snsFilterScalarEqual(a, b any) bool {
	switch value := a.(type) {
	case nil:
		return b == nil
	case bool:
		actual, ok := b.(bool)
		return ok && actual == value
	case string:
		actual, ok := b.(string)
		return ok && actual == value
	case json.Number:
		actual, ok := b.(json.Number)
		if !ok {
			return false
		}
		left, leftErr := snsFilterNumber(value)
		right, rightErr := snsFilterNumber(actual)
		return leftErr == nil && rightErr == nil && left == right
	}
	return false
}

func snsFilterWildcardMatches(parts []string, value string) bool {
	if len(parts) == 1 {
		return value == parts[0]
	}
	if !strings.HasPrefix(value, parts[0]) {
		return false
	}
	value = value[len(parts[0]):]
	for _, part := range parts[1 : len(parts)-1] {
		index := strings.Index(value, part)
		if index < 0 {
			return false
		}
		value = value[index+len(part):]
	}
	return strings.HasSuffix(value, parts[len(parts)-1])
}

func (matcher snsFilterMatcher) canonical() string {
	var value any
	switch matcher.kind {
	case "literal":
		if number, ok := matcher.value.(json.Number); ok {
			scaled, _ := snsFilterNumber(number)
			value = []any{"number", scaled}
		} else {
			value = []any{"literal", matcher.value}
		}
	case "numeric":
		conditions := make([]string, 0, len(matcher.comparisons))
		for _, condition := range matcher.comparisons {
			if len(matcher.comparisons) == 1 && condition.op == "=" {
				raw, _ := json.Marshal([]any{"number", condition.value})
				return string(raw)
			}
			conditions = append(conditions, condition.op+strconv.FormatInt(condition.value, 10))
		}
		value = []any{"numeric", snsFilterSortedUnique(conditions)}
	case "anything-but":
		if matcher.negated != nil {
			value = []any{"anything-but", matcher.negated.canonical()}
		} else {
			exclusions := make([]string, 0, len(matcher.exclusions))
			for _, excluded := range matcher.exclusions {
				exclusions = append(exclusions, snsFilterMatcher{kind: "literal", value: excluded}.canonical())
			}
			value = []any{"anything-but", snsFilterSortedUnique(exclusions)}
		}
	case "cidr":
		value = []any{matcher.kind, matcher.subnet.String()}
	default:
		value = []any{matcher.kind, matcher.value}
	}
	raw, _ := json.Marshal(value)
	return string(raw)
}
