package messaging

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// FilterPolicy is an SNS subscription predicate. Attributes preserves the
// original string-only construction contract; parsed policies use an immutable
// predicate tree so operators and nested body policies do not lose information.
type FilterPolicy struct {
	Attributes map[string][]string
	root       *snsFilterNode
	raw        string
}

// Matches applies the default MessageAttributes scope.
func (fp *FilterPolicy) Matches(attrs map[string]MessageAttribute) bool {
	return fp.MatchesMessage(attrs, "", "MessageAttributes")
}

// MatchesMessage evaluates the selected SNS filter scope. No policy accepts
// every message, including non-JSON bodies. A body policy requires a JSON object.
func (fp *FilterPolicy) MatchesMessage(attrs map[string]MessageAttribute, body, scope string) bool {
	inputs := snsFilterInputs{attributes: attrs}
	return fp.matchesInput(&inputs, body, scope)
}

// snsFilterInputs belongs to one publication. Its decoded values are read-only;
// policies make independent decisions without re-decoding each selected body or
// the delivered attributes. Invalid/nonobject bodies are remembered as nil.
type snsFilterInputs struct {
	attributes         map[string]MessageAttribute
	attributeValues    map[string]any
	attributesPrepared bool
	bodies             map[string]map[string]any
}

func (fp *FilterPolicy) matchesInput(inputs *snsFilterInputs, body, scope string) bool {
	root := fp.filterRoot()
	if root.empty() {
		return true
	}
	if scope == "MessageBody" {
		object := inputs.bodyValues(body)
		return object != nil && root.matches([]map[string]any{object})
	}
	if scope != "" && scope != "MessageAttributes" || root.nested {
		return false
	}
	return root.matches([]map[string]any{inputs.attributesValues()})
}

func (inputs *snsFilterInputs) bodyValues(body string) map[string]any {
	if object, exists := inputs.bodies[body]; exists {
		return object
	}
	value, err := decodeSNSFilterJSON(body)
	var object map[string]any
	if err == nil {
		object, _ = value.(map[string]any)
	}
	if inputs.bodies == nil {
		inputs.bodies = make(map[string]map[string]any)
	}
	inputs.bodies[body] = object
	return object
}

func (inputs *snsFilterInputs) attributesValues() map[string]any {
	if inputs.attributesPrepared {
		return inputs.attributeValues
	}
	object := make(map[string]any, len(inputs.attributes))
	for key, attr := range inputs.attributes {
		switch {
		case attr.DataType == "String.Array":
			value, err := decodeSNSFilterJSON(attr.StringValue)
			if err == nil {
				if array, ok := value.([]any); ok {
					object[key] = array
				}
			}
		case attr.DataType == "String" || strings.HasPrefix(attr.DataType, "String."):
			object[key] = attr.StringValue
		case attr.DataType == "Number" || strings.HasPrefix(attr.DataType, "Number."):
			value, err := decodeSNSFilterJSON(attr.StringValue)
			if err == nil {
				if number, ok := value.(json.Number); ok {
					if _, err := snsFilterNumber(number); err == nil {
						object[key] = number
					}
				}
			}
		}
	}
	inputs.attributeValues = object
	inputs.attributesPrepared = true
	return object
}

// ValidateScope checks constraints that depend on the subscription's scope.
// Call this after combining FilterPolicy and FilterPolicyScope attributes.
func (fp *FilterPolicy) ValidateScope(scope string) error {
	if scope != "" && scope != "MessageAttributes" && scope != "MessageBody" {
		return fmt.Errorf("FilterPolicyScope must be MessageAttributes or MessageBody")
	}
	if scope != "MessageBody" && fp.filterRoot().nested {
		return fmt.Errorf("nested filter policies require MessageBody scope")
	}
	return nil
}

// JSON returns the complete wire policy, including operators and nested fields.
// Directly constructed string-only policies use the legacy Attributes map.
func (fp *FilterPolicy) JSON() string {
	if fp == nil {
		return "{}"
	}
	if fp.raw != "" {
		return fp.raw
	}
	if len(fp.Attributes) == 0 {
		return "{}"
	}
	raw, _ := json.Marshal(fp.Attributes)
	return string(raw)
}

// Equal compares normalized predicates, including legacy string alternatives.
// Alternative order, duplicate alternatives and numeric notation are immaterial.
func (fp *FilterPolicy) Equal(other *FilterPolicy) bool {
	return fp.filterRoot().canonical() == other.filterRoot().canonical()
}

func (fp *FilterPolicy) filterRoot() *snsFilterNode {
	if fp == nil {
		return &snsFilterNode{}
	}
	if fp.root != nil {
		return fp.root
	}
	root := &snsFilterNode{}
	keys := make([]string, 0, len(fp.Attributes))
	for key := range fp.Attributes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		field := snsFilterField{key: key}
		for _, value := range fp.Attributes[key] {
			field.matchers = append(field.matchers, snsFilterMatcher{kind: "literal", value: value})
		}
		root.fields = append(root.fields, field)
	}
	return root
}

type snsFilterNode struct {
	fields []snsFilterField
	any    []*snsFilterNode
	nested bool
}

type snsFilterField struct {
	key      string
	matchers []snsFilterMatcher
	node     *snsFilterNode
}

func (node *snsFilterNode) empty() bool {
	return len(node.fields) == 0 && len(node.any) == 0
}

func (node *snsFilterNode) matches(objects []map[string]any) bool {
	if len(objects) == 0 {
		return false
	}
	populated := false
	for _, object := range objects {
		populated = populated || len(object) > 0
	}
	for _, field := range node.fields {
		present := false
		var values []any
		for _, object := range objects {
			value, found := object[field.key]
			if found {
				present = true
				values = append(values, value)
			}
		}
		if field.node != nil {
			var children []map[string]any
			for _, value := range values {
				snsFilterObjects(value, &children)
			}
			if !field.node.matches(children) {
				return false
			}
			continue
		}
		matched := false
		for _, matcher := range field.matchers {
			if matcher.kind == "exists" {
				if matcher.value == false {
					matched = !present && populated
				} else {
					for _, value := range values {
						if snsFilterHasLeaf(value) {
							matched = true
							break
						}
					}
				}
			} else {
				for _, value := range values {
					if snsFilterMatchValue(matcher, value) {
						matched = true
						break
					}
				}
			}
			if matched {
				break
			}
		}
		if !matched {
			return false
		}
	}
	if len(node.any) > 0 {
		for _, branch := range node.any {
			if branch.matches(objects) {
				return true
			}
		}
		return false
	}
	return true
}

func snsFilterObjects(value any, objects *[]map[string]any) {
	switch v := value.(type) {
	case map[string]any:
		*objects = append(*objects, v)
	case []any:
		for _, child := range v {
			snsFilterObjects(child, objects)
		}
	}
}

func snsFilterHasLeaf(value any) bool {
	switch v := value.(type) {
	case nil, map[string]any:
		return false
	case string:
		return v != ""
	case []any:
		for _, child := range v {
			if snsFilterHasLeaf(child) {
				return true
			}
		}
		return false
	default:
		return true
	}
}

func snsFilterMatchValue(matcher snsFilterMatcher, value any) bool {
	if array, ok := value.([]any); ok {
		for _, child := range array {
			if snsFilterMatchValue(matcher, child) {
				return true
			}
		}
		return false
	}
	return matcher.matches(value)
}

func (node *snsFilterNode) canonical() string {
	fields := make(map[string]any, len(node.fields)+1)
	for _, field := range node.fields {
		if field.node != nil {
			fields[field.key] = field.node.canonical()
			continue
		}
		alternatives := make([]string, 0, len(field.matchers))
		for _, matcher := range field.matchers {
			alternatives = append(alternatives, matcher.canonical())
		}
		fields[field.key] = snsFilterSortedUnique(alternatives)
	}
	if len(node.any) > 0 {
		branches := make([]string, 0, len(node.any))
		for _, branch := range node.any {
			branches = append(branches, branch.canonical())
		}
		// An internal wrapper distinguishes an OR expression from a literal
		// field named "$or".
		raw, _ := json.Marshal([]any{fields, snsFilterSortedUnique(branches)})
		return string(raw)
	}
	raw, _ := json.Marshal(fields)
	return string(raw)
}

func snsFilterSortedUnique(values []string) []string {
	sort.Strings(values)
	result := make([]string, 0, len(values))
	for _, value := range values {
		if len(result) == 0 || value != result[len(result)-1] {
			result = append(result, value)
		}
	}
	return result
}
