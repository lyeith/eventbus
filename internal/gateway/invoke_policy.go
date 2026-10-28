package gateway

import (
	"fmt"
	"regexp"
	"strings"
)

// invokePolicy is an immutable effective identity policy for execute-api:Invoke.
// REQUEST responses and credential fixtures share this evaluator.
type invokePolicy struct {
	statements []policyStatement
}

type policyStatement struct {
	Effect                   string
	actions                  []*regexp.Regexp
	resources                []policyPattern
	notActions, notResources bool
	conditions               []invokeCondition
}

func (policy invokePolicy) permits(arn string, facts map[string][]string) bool {
	allowed := false
	for _, statement := range policy.statements {
		actionMatches := matchesIAM(statement.actions, "execute-api:invoke")
		if actionMatches == statement.notActions || !statement.matchesResource(arn, facts) {
			continue
		}
		matched := true
		for _, condition := range statement.conditions {
			if !condition(facts) {
				matched = false
				break
			}
		}
		if !matched {
			continue
		}
		if statement.Effect == "Deny" {
			return false
		}
		allowed = true
	}
	return allowed
}

func (statement policyStatement) matchesResource(arn string, facts map[string][]string) bool {
	matched := false
	for _, pattern := range statement.resources {
		current, resolved := pattern.matches(arn, facts)
		if !resolved {
			// An unresolved variable must not make NotResource unrestricted.
			if statement.notResources {
				return false
			}
			continue
		}
		matched = matched || current
	}
	return matched != statement.notResources
}

func matchesIAM(patterns []*regexp.Regexp, value string) bool {
	for _, pattern := range patterns {
		if pattern != nil && pattern.MatchString(value) {
			return true
		}
	}
	return false
}

func compileIAMGlob(pattern string) *regexp.Regexp {
	// REQUEST responses historically treat compiler-limit failure as a
	// never-matching pattern. Native fixture compilation reports that error.
	compiled, _ := compilePolicyGlob(pattern)
	return compiled
}

func compilePolicyGlob(pattern string) (*regexp.Regexp, error) {
	var expression strings.Builder
	expression.WriteString("(?s)^")
	appendPolicyGlob(&expression, pattern, false, new(int))
	expression.WriteByte('$')
	return regexp.Compile(expression.String())
}

func appendPolicyGlob(expression *strings.Builder, text string, arn bool, colons *int) {
	for _, character := range text {
		switch character {
		case '*':
			if arn && *colons < 5 {
				expression.WriteString("[^:]*")
			} else {
				expression.WriteString(".*")
			}
		case '?':
			if arn && *colons < 5 {
				expression.WriteString("[^:]")
			} else {
				expression.WriteByte('.')
			}
		default:
			expression.WriteString(regexp.QuoteMeta(string(character)))
			if character == ':' {
				*colons++
			}
		}
	}
}

type policyPattern struct {
	compiled *regexp.Regexp
	parts    []policyPatternPart
}

type policyPatternPart struct {
	text     string
	key      string
	fallback *string
	literal  bool
}

func compilePolicyPattern(value string) (policyPattern, error) {
	parts, err := parsePolicyVariables(value)
	if err != nil {
		return policyPattern{}, err
	}
	pattern := policyPattern{parts: parts}
	dynamic := false
	for _, part := range parts {
		dynamic = dynamic || part.key != ""
	}
	if !dynamic {
		expression, _ := pattern.expression(nil, false)
		pattern.compiled, err = regexp.Compile(expression)
	}
	return pattern, err
}

func (pattern policyPattern) matches(value string, facts map[string][]string) (bool, bool) {
	if pattern.compiled != nil {
		return pattern.compiled.MatchString(value), true
	}
	if len(pattern.parts) == 0 {
		return false, true
	}
	expression, ok := pattern.expression(facts, false)
	if !ok {
		return false, false
	}
	compiled, err := regexp.Compile(expression)
	if err != nil {
		return false, false
	}
	return compiled.MatchString(value), true
}

// ARN conditions compare six fields, unlike Resource globs, whose wildcards
// retain IAM's ability to span delimiters.
func (pattern policyPattern) matchesARN(value string, facts map[string][]string) (bool, bool) {
	if parts := strings.SplitN(value, ":", 6); len(parts) != 6 || parts[0] != "arn" {
		return false, true
	}
	expression, ok := pattern.expression(facts, true)
	if !ok {
		return false, false
	}
	compiled, err := regexp.Compile(expression)
	if err != nil {
		return false, false
	}
	return compiled.MatchString(value), true
}

func (pattern policyPattern) expression(facts map[string][]string, arn bool) (string, bool) {
	var expression strings.Builder
	expression.WriteString("(?s)^")
	colons := 0
	for _, part := range pattern.parts {
		if part.key == "" && !part.literal {
			appendPolicyGlob(&expression, part.text, arn, &colons)
			continue
		}
		value, ok := part.resolve(facts)
		if !ok {
			return "", false
		}
		if part.literal {
			// Only the three predefined escapes quote wildcard characters.
			expression.WriteString(regexp.QuoteMeta(value))
			colons += strings.Count(value, ":")
		} else {
			// IAM substitutes ordinary context/default values before glob
			// matching; their * and ? characters remain wildcards.
			appendPolicyGlob(&expression, value, arn, &colons)
		}
	}
	expression.WriteByte('$')
	return expression.String(), true
}

func expandPolicyVariables(value string, facts map[string][]string) (string, bool) {
	parts, err := parsePolicyVariables(value)
	if err != nil {
		return "", false
	}
	var expanded strings.Builder
	for _, part := range parts {
		text, ok := part.resolve(facts)
		if !ok {
			return "", false
		}
		expanded.WriteString(text)
	}
	return expanded.String(), true
}

func (part policyPatternPart) resolve(facts map[string][]string) (string, bool) {
	if part.key == "" {
		return part.text, true
	}
	values, present := policyFact(facts, part.key)
	if present && len(values) == 1 && values[0] != "" {
		return values[0], true
	}
	if (!present || len(values) == 0 || len(values) == 1 && values[0] == "") && part.fallback != nil {
		return *part.fallback, true
	}
	return "", false
}

func parsePolicyVariables(value string) ([]policyPatternPart, error) {
	var parts []policyPatternPart
	for {
		start := strings.Index(value, "${")
		if start < 0 {
			if value != "" {
				parts = append(parts, policyPatternPart{text: value})
			}
			return parts, nil
		}
		if start > 0 {
			parts = append(parts, policyPatternPart{text: value[:start]})
		}
		value = value[start+2:]
		end := strings.IndexByte(value, '}')
		if end < 0 {
			return nil, fmt.Errorf("unterminated policy variable")
		}
		body := value[:end]
		value = value[end+1:]
		if body == "*" || body == "?" || body == "$" {
			parts = append(parts, policyPatternPart{text: body, literal: true})
			continue
		}
		key, defaultText, hasDefault := strings.Cut(body, ",")
		key = strings.TrimSpace(key)
		if key == "" || strings.ContainsAny(key, "${},'") {
			return nil, fmt.Errorf("invalid policy variable")
		}
		for _, character := range key {
			if character < ' ' || character == 127 {
				return nil, fmt.Errorf("invalid policy variable")
			}
		}
		part := policyPatternPart{key: key}
		if hasDefault {
			defaultText = strings.TrimSpace(defaultText)
			if len(defaultText) < 2 || defaultText[0] != '\'' || defaultText[len(defaultText)-1] != '\'' ||
				strings.ContainsAny(defaultText[1:len(defaultText)-1], "'{}") {
				return nil, fmt.Errorf("invalid policy variable default")
			}
			text := defaultText[1 : len(defaultText)-1]
			part.fallback = &text
		}
		parts = append(parts, part)
	}
}
