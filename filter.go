package main

import (
	"encoding/json"
	"slices"
)

type FilterPolicy struct {
	Attributes map[string][]string // {"event_type": ["import.created"]}
}

// Matches checks if the given message attributes satisfy the filter policy.
// Each key in the policy must exist in attrs, and the attr value must match
// at least one of the allowed values (exact string match).
// A nil or empty policy matches everything.
func (fp *FilterPolicy) Matches(attrs map[string]MessageAttribute) bool {
	if fp == nil || len(fp.Attributes) == 0 {
		return true
	}

	for key, allowedValues := range fp.Attributes {
		attr, exists := attrs[key]
		if !exists {
			return false
		}
		matched := false
		for _, v := range allowedValues {
			if attr.StringValue == v {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

// Equal reports whether two policies admit the same messages: the same
// attributes, each with the same allowed values in any order. A nil policy
// equals an empty one.
func (fp *FilterPolicy) Equal(other *FilterPolicy) bool {
	var left, right map[string][]string
	if fp != nil {
		left = fp.Attributes
	}
	if other != nil {
		right = other.Attributes
	}
	if len(left) != len(right) {
		return false
	}
	for key, values := range left {
		otherValues, ok := right[key]
		if !ok {
			return false
		}
		a, b := slices.Clone(values), slices.Clone(otherValues)
		slices.Sort(a)
		slices.Sort(b)
		if !slices.Equal(slices.Compact(a), slices.Compact(b)) {
			return false
		}
	}
	return true
}

// ParseFilterPolicy parses a JSON filter policy string as used by AWS SNS.
// Format: {"attrName": ["value1", "value2"]}
func ParseFilterPolicy(raw string) (*FilterPolicy, error) {
	if raw == "" {
		return nil, nil
	}

	var parsed map[string][]string
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return nil, err
	}

	return &FilterPolicy{Attributes: parsed}, nil
}
