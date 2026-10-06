package messaging

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"unicode/utf8"
)

const (
	snsFilterMaxBytes        = 256 * 1024
	snsFilterMaxKeys         = 5
	snsFilterMaxCombinations = 150
	snsFilterMaxWildcards    = 100
)

// ParseFilterPolicy validates the documented SNS policy syntax and shared
// limits. Scope-dependent nesting is checked separately by ValidateScope.
// Sources: AWS SNS Developer Guide, subscription-filter-policy-constraints,
// and-or-logic, string-value-matching and numeric-value-matching.
func ParseFilterPolicy(raw string) (*FilterPolicy, error) {
	if raw == "" {
		return nil, nil
	}
	if len(raw) > snsFilterMaxBytes {
		return nil, fmt.Errorf("filter policy exceeds 256 KB")
	}
	value, err := decodeSNSFilterJSON(raw)
	if err != nil {
		return nil, err
	}
	object, ok := value.(map[string]any)
	if !ok || object == nil {
		return nil, fmt.Errorf("filter policy must be a JSON object")
	}
	state := snsFilterParseState{keys: make(map[string]struct{})}
	root, combinations, err := state.parseNode(object, nil, 1)
	if err != nil {
		return nil, err
	}
	if len(state.keys) > snsFilterMaxKeys {
		return nil, fmt.Errorf("filter policy exceeds five leaf keys")
	}
	if combinations > snsFilterMaxCombinations {
		return nil, fmt.Errorf("filter policy exceeds 150 combinations")
	}
	if state.hasWildcard && state.wildcardPoints > snsFilterMaxWildcards {
		return nil, fmt.Errorf("filter policy exceeds 100 wildcard complexity points")
	}
	encoded, _ := json.Marshal(object)
	policy := &FilterPolicy{Attributes: make(map[string][]string), root: root, raw: string(encoded)}
	for _, field := range root.fields {
		if field.node != nil {
			continue
		}
		values := make([]string, 0, len(field.matchers))
		for _, matcher := range field.matchers {
			value, ok := matcher.value.(string)
			if matcher.kind != "literal" || !ok {
				break
			}
			values = append(values, value)
		}
		if len(values) == len(field.matchers) {
			policy.Attributes[field.key] = values
		}
	}
	return policy, nil
}

func decodeSNSFilterJSON(raw string) (any, error) {
	if !utf8.ValidString(raw) {
		return nil, fmt.Errorf("filter JSON must be valid UTF-8")
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			err = fmt.Errorf("multiple JSON values")
		}
		return nil, err
	}
	return value, nil
}

type snsFilterParseState struct {
	keys           map[string]struct{}
	wildcardPoints int
	hasWildcard    bool
}

func (state *snsFilterParseState) parseNode(object map[string]any, path []string, depth int) (*snsFilterNode, int, error) {
	if depth > snsFilterMaxCombinations {
		return nil, 0, fmt.Errorf("filter policy nesting exceeds combination limit")
	}
	node := &snsFilterNode{}
	combinations := 1
	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := object[key]
		if key == "$or" && snsFilterIsOR(value) {
			orCombinations := 0
			for _, branch := range value.([]any) {
				child, count, err := state.parseNode(branch.(map[string]any), path, depth)
				if err != nil {
					return nil, 0, err
				}
				node.any = append(node.any, child)
				node.nested = node.nested || child.nested
				orCombinations += count
				if orCombinations > snsFilterMaxCombinations {
					return nil, 0, fmt.Errorf("filter policy exceeds 150 combinations")
				}
			}
			combinations *= orCombinations
		} else {
			field := snsFilterField{key: key}
			fieldPath := append(append([]string(nil), path...), key)
			switch v := value.(type) {
			case map[string]any:
				if len(v) == 0 {
					return nil, 0, fmt.Errorf("nested policy for %q must not be empty", key)
				}
				child, count, err := state.parseNode(v, fieldPath, depth+1)
				if err != nil {
					return nil, 0, err
				}
				field.node = child
				node.nested = true
				combinations *= count
			case []any:
				if len(v) == 0 {
					return nil, 0, fmt.Errorf("match list for %q must not be empty", key)
				}
				fieldPoints := 0
				for _, condition := range v {
					matcher, points, err := parseSNSFilterMatcher(condition)
					if err != nil {
						return nil, 0, fmt.Errorf("invalid condition for %q: %w", key, err)
					}
					field.matchers = append(field.matchers, matcher)
					state.hasWildcard = state.hasWildcard || matcher.kind == "wildcard" || matcher.negated != nil && matcher.negated.kind == "wildcard"
					fieldPoints += points
				}
				state.wildcardPoints += fieldPoints * len(v)
				encodedPath, _ := json.Marshal(fieldPath)
				state.keys[string(encodedPath)] = struct{}{}
				combinations *= len(v) * depth
			default:
				return nil, 0, fmt.Errorf("policy property %q must be an array or nested object", key)
			}
			node.fields = append(node.fields, field)
		}
		if combinations > snsFilterMaxCombinations {
			return nil, 0, fmt.Errorf("filter policy exceeds 150 combinations")
		}
	}
	return node, combinations, nil
}

func snsFilterIsOR(value any) bool {
	branches, ok := value.([]any)
	if !ok || len(branches) < 2 {
		return false
	}
	for _, value := range branches {
		branch, ok := value.(map[string]any)
		if !ok {
			return false
		}
		for key := range branch {
			switch key {
			case "numeric", "prefix", "suffix", "anything-but", "exists", "equals-ignore-case", "cidr", "wildcard":
				return false
			}
		}
	}
	return true
}
