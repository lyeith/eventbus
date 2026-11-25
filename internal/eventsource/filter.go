package eventsource

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"
)

type FilterCriteria struct {
	Filters []Filter `json:"Filters,omitempty"`
}

type Filter struct {
	Pattern string `json:"Pattern"`
}

func cloneFilterCriteria(criteria *FilterCriteria) *FilterCriteria {
	if criteria == nil || len(criteria.Filters) == 0 {
		return nil
	}
	return &FilterCriteria{Filters: append([]Filter(nil), criteria.Filters...)}
}

type filterObject func(map[string]any) bool
type filterValue func(any, bool) bool

// Lambda uses EventBridge comparison rules. In particular, numeric literals
// compare their JSON spelling, while the numeric operator compares value.
func compileFilterCriteria(criteria *FilterCriteria) (func(Record) bool, error) {
	if criteria == nil || len(criteria.Filters) == 0 {
		return nil, nil
	}
	if len(criteria.Filters) > 5 {
		return nil, invalid("FilterCriteria supports up to five filters")
	}
	filters := make([]filterObject, 0, len(criteria.Filters))
	fields := make(map[string]struct{})
	for _, filter := range criteria.Filters {
		if len(filter.Pattern) > 4096 || !utf8.ValidString(filter.Pattern) {
			return nil, invalid("Filter Pattern must be valid UTF-8 and at most 4096 bytes")
		}
		value, err := decodeFilterData(filter.Pattern)
		object, ok := value.(map[string]any)
		if err != nil || !ok {
			return nil, invalid("Filter Pattern must be one JSON object")
		}
		compiled, err := compileFilterObject(object, 1)
		if err != nil {
			return nil, invalid("Invalid Filter Pattern: " + err.Error())
		}
		filters = append(filters, compiled)
		collectFilterFields(object, fields)
	}
	return func(record Record) bool {
		event := make(map[string]any, len(fields))
		for name := range fields {
			value, present := projectFilterField(record, name)
			if present {
				event[name] = value
			}
		}
		for _, matches := range filters {
			if matches(event) {
				return true
			}
		}
		return false
	}, nil
}

// Only project fields used by the pattern. A body-only filter does not clone
// metadata or allocate an encoded Lambda event for an unmatched record.
func collectFilterFields(object map[string]any, fields map[string]struct{}) {
	for name, rule := range object {
		if name == "$or" {
			for _, child := range rule.([]any) {
				collectFilterFields(child.(map[string]any), fields)
			}
		} else {
			fields[name] = struct{}{}
		}
	}
}
func projectFilterField(record Record, name string) (any, bool) {
	switch name {
	case "body":
		value, err := decodeFilterData(record.Body)
		if err != nil {
			return record.Body, true
		}
		return value, true
	case "messageId":
		return record.MessageID, true
	case "receiptHandle":
		return record.ReceiptHandle, true
	case "md5OfBody":
		return record.MD5OfBody, true
	case "eventSource":
		return record.EventSource, true
	case "eventSourceARN":
		return record.EventSourceARN, true
	case "awsRegion":
		return record.AWSRegion, true
	case "attributes":
		attributes := make(map[string]any, len(record.Attributes))
		for k, v := range record.Attributes {
			attributes[k] = v
		}
		return attributes, true
	case "messageAttributes":
		attributes := make(map[string]any, len(record.MessageAttributes))
		for k, v := range record.MessageAttributes {
			encoded, _ := json.Marshal(v)
			value, _ := decodeFilterData(string(encoded))
			attributes[k] = value
		}
		return attributes, true
	}
	return nil, false
}

func decodeFilterData(raw string) (any, error) {
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return nil, fmt.Errorf("expected one JSON value")
	}
	return value, nil
}

func compileFilterObject(pattern map[string]any, depth int) (filterObject, error) {
	if depth > 64 {
		return nil, fmt.Errorf("pattern nesting exceeds 64")
	}
	fields := make(map[string]filterValue, len(pattern))
	var branches []filterObject
	for name, patternValue := range pattern {
		if name == "$or" {
			alternatives, ok := patternValue.([]any)
			if !ok || len(alternatives) < 2 {
				return nil, fmt.Errorf("$or requires at least two objects")
			}
			for _, alternative := range alternatives {
				child, ok := alternative.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("$or requires objects")
				}
				match, err := compileFilterObject(child, depth+1)
				if err != nil {
					return nil, err
				}
				branches = append(branches, match)
			}
			continue
		}
		match, err := compileFilterField(patternValue, depth)
		if err != nil {
			return nil, err
		}
		fields[name] = match
	}
	return func(object map[string]any) bool {
		for name, match := range fields {
			value, present := object[name]
			if !match(value, present) {
				return false
			}
		}
		if len(branches) == 0 {
			return true
		}
		for _, match := range branches {
			if match(object) {
				return true
			}
		}
		return false
	}, nil
}

func compileFilterField(pattern any, depth int) (filterValue, error) {
	switch rule := pattern.(type) {
	case map[string]any:
		match, err := compileFilterObject(rule, depth+1)
		if err != nil {
			return nil, err
		}
		return func(value any, present bool) bool {
			return anyFilterValue(value, func(candidate any) bool {
				object, ok := candidate.(map[string]any)
				return present && ok && match(object)
			})
		}, nil
	case []any:
		if len(rule) == 0 {
			return nil, fmt.Errorf("comparison list cannot be empty")
		}
		tests := make([]filterValue, 0, len(rule))
		for _, operation := range rule {
			match, err := compileFilterComparison(operation)
			if err != nil {
				return nil, err
			}
			tests = append(tests, match)
		}
		return func(value any, present bool) bool {
			for _, test := range tests {
				if !present {
					if test(nil, false) {
						return true
					}
				} else if test(value, true) || anyFilterValue(value, func(candidate any) bool { return test(candidate, true) }) {
					return true
				}
			}
			return false
		}, nil
	default:
		return nil, fmt.Errorf("fields require objects or comparison lists")
	}
}

func anyFilterValue(value any, match func(any) bool) bool {
	if values, ok := value.([]any); ok {
		for _, candidate := range values {
			if match(candidate) {
				return true
			}
		}
		return false
	}
	return match(value)
}

func filterLeaf(value any) bool {
	switch value.(type) {
	case nil, bool, string, json.Number:
		return true
	default:
		return false
	}
}

func compileFilterComparison(pattern any) (filterValue, error) {
	if filterLeaf(pattern) {
		return func(value any, present bool) bool { return present && value == pattern }, nil
	}
	operators, ok := pattern.(map[string]any)
	if !ok || len(operators) != 1 {
		return nil, fmt.Errorf("expected a literal or one comparison operator")
	}
	for name, operand := range operators {
		switch name {
		case "exists":
			wanted, ok := operand.(bool)
			if !ok {
				return nil, fmt.Errorf("exists requires a boolean")
			}
			return func(value any, present bool) bool {
				if !wanted {
					return !present
				}
				return present && filterLeaf(value)
			}, nil
		case "anything-but":
			exclude, ok := operand.([]any)
			if !ok {
				exclude = []any{operand}
			}
			if len(exclude) == 0 {
				return nil, fmt.Errorf("anything-but requires values")
			}
			switch exclude[0].(type) {
			case string, json.Number:
			default:
				return nil, fmt.Errorf("anything-but requires strings or numbers")
			}
			_, stringsOnly := exclude[0].(string)
			for _, item := range exclude {
				_, isString := item.(string)
				_, isNumber := item.(json.Number)
				if (!isString && !isNumber) || stringsOnly != isString {
					return nil, fmt.Errorf("anything-but requires values of one type")
				}
			}
			return func(value any, present bool) bool {
				_, isString := value.(string)
				_, isNumber := value.(json.Number)
				if !present || (!isString && !isNumber) || stringsOnly != isString {
					return false
				}
				for _, item := range exclude {
					if value == item {
						return false
					}
				}
				return true
			}, nil
		case "numeric":
			return compileFilterNumeric(operand)
		case "prefix", "suffix", "equals-ignore-case":
			text, ok := operand.(string)
			ignoreCase := name == "equals-ignore-case"
			if nested, found := operand.(map[string]any); found && len(nested) == 1 && !ignoreCase {
				text, ok = nested["equals-ignore-case"].(string)
				ignoreCase = ok
			}
			if !ok {
				return nil, fmt.Errorf("string comparison requires text")
			}
			return func(value any, present bool) bool {
				input, ok := value.(string)
				if !present || !ok {
					return false
				}
				if name == "equals-ignore-case" {
					return strings.EqualFold(input, text)
				}
				if ignoreCase {
					input = strings.ToLower(input)
					textValue := strings.ToLower(text)
					return name == "prefix" && strings.HasPrefix(input, textValue) || name == "suffix" && strings.HasSuffix(input, textValue)
				}
				if name == "prefix" {
					return strings.HasPrefix(input, text)
				}
				return strings.HasSuffix(input, text)
			}, nil
		default:
			return nil, fmt.Errorf("unsupported comparison operator")
		}
	}
	return nil, fmt.Errorf("expected a comparison operator")
}

type numericFilterComparison struct {
	op    string
	limit float64
}

func filterNumber(value any) (float64, bool) {
	number, ok := value.(json.Number)
	if !ok {
		return 0, false
	}
	result, err := strconv.ParseFloat(string(number), 64)
	return result, err == nil && result >= -5e9 && result <= 5e9
}

func compileFilterNumeric(operand any) (filterValue, error) {
	operands, ok := operand.([]any)
	if !ok || len(operands) == 0 || len(operands)%2 != 0 {
		return nil, fmt.Errorf("numeric requires operator and number pairs")
	}
	comparisons := make([]numericFilterComparison, 0, len(operands)/2)
	for index := 0; index < len(operands); index += 2 {
		op, _ := operands[index].(string)
		switch op {
		case "=", "<", "<=", ">", ">=":
		default:
			return nil, fmt.Errorf("invalid numeric operator")
		}
		limit, ok := filterNumber(operands[index+1])
		if !ok {
			return nil, fmt.Errorf("numeric operand must be a supported number")
		}
		comparisons = append(comparisons, numericFilterComparison{op, limit})
	}
	return func(value any, present bool) bool {
		actual, ok := filterNumber(value)
		if !present || !ok {
			return false
		}
		for _, comparison := range comparisons {
			limit := comparison.limit
			switch comparison.op {
			case "=":
				if actual != limit {
					return false
				}
			case "<":
				if actual >= limit {
					return false
				}
			case "<=":
				if actual > limit {
					return false
				}
			case ">":
				if actual <= limit {
					return false
				}
			case ">=":
				if actual < limit {
					return false
				}
			}
		}
		return true
	}, nil
}
