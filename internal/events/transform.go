package events

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	ruleARNVariable       = "aws.events.rule-arn"
	ruleNameVariable      = "aws.events.rule-name"
	ingestionTimeVariable = "aws.events.event.ingestion-time"
	eventVariable         = "aws.events.event"
	fullEventVariable     = "aws.events.event.json"
)

func validateTargetInput(target Target) error {
	_, _, err := prepareTargetInput(target)
	return err
}

func prepareTargetInput(target Target) ([]inputPathStep, *inputTemplate, error) {
	selected := 0
	if target.Input != nil {
		selected++
	}
	if target.InputPath != nil {
		selected++
	}
	if target.InputTransformer != nil {
		selected++
	}
	if selected > 1 {
		return nil, nil, validation("Input, InputPath and InputTransformer are mutually exclusive")
	}
	if target.Input != nil && (utf8.RuneCountInString(*target.Input) > 8192 || !json.Valid([]byte(*target.Input))) {
		return nil, nil, validation("Input must be valid JSON of at most 8192 characters")
	}
	if target.InputPath != nil {
		path, err := parseInputPath(*target.InputPath, true)
		return path, nil, err
	}
	if target.InputTransformer != nil {
		compiled, err := compileInputTransformer(target.InputTransformer)
		return nil, compiled, err
	}
	return nil, nil, nil
}

// transformTarget transforms the original event independently for each target.
// It never edits the event or the target's stored configuration.
func transformTarget(target Target, event []byte, ruleArn, ruleName string, ingestedAt time.Time) ([]byte, error) {
	path, compiled, err := prepareTargetInput(target)
	if err != nil {
		return nil, err
	}
	if target.Input != nil {
		return []byte(*target.Input), nil
	}
	if !json.Valid(event) {
		return nil, fmt.Errorf("invalid EventBridge event JSON")
	}
	if target.InputPath != nil {
		value, present := extractInputPath(event, path)
		if !present {
			return []byte("null"), nil
		}
		return value, nil
	}
	if target.InputTransformer == nil {
		return bytes.Clone(event), nil
	}
	values := make(map[string]json.RawMessage, len(compiled.paths)+5)
	for key, path := range compiled.paths {
		if value, present := extractInputPath(event, path); present {
			values[key] = value
		}
	}
	values[ruleARNVariable], _ = json.Marshal(ruleArn)
	values[ruleNameVariable], _ = json.Marshal(ruleName)
	values[ingestionTimeVariable], _ = json.Marshal(ingestedAt.UTC().Format(time.RFC3339Nano))
	values[fullEventVariable] = bytes.Clone(event)
	var eventFields map[string]json.RawMessage
	if err := json.Unmarshal(event, &eventFields); err != nil || eventFields == nil {
		return nil, fmt.Errorf("EventBridge event must be a JSON object")
	}
	delete(eventFields, "detail")
	values[eventVariable], _ = json.Marshal(eventFields)
	var rendered []string
	for _, root := range compiled.roots {
		value, present := root.render(values)
		if !present {
			value = "null"
		}
		rendered = append(rendered, value)
	}
	return []byte(strings.Join(rendered, "\n")), nil
}

type inputPathStep struct {
	key      string
	index    int
	array    bool
	wildcard bool
}

// EventBridge supports a bounded JSONPath subset. Transformer object keys use
// dot notation; InputPath additionally accepts quoted bracket object keys.
func parseInputPath(path string, bracketKeys bool) ([]inputPathStep, error) {
	bad := func() ([]inputPathStep, error) {
		return nil, validation("Input path must use $, dot keys, array indices or wildcards; quoted object keys are supported only by InputPath")
	}
	if len(path) == 0 || utf8.RuneCountInString(path) > 256 || path[0] != '$' {
		return bad()
	}
	var steps []inputPathStep
	for pos := 1; pos < len(path); {
		switch path[pos] {
		case '.':
			pos++
			start := pos
			for pos < len(path) && inputPathKeyByte(path[pos]) {
				pos++
			}
			if start == pos && pos < len(path) && path[pos] == '*' {
				steps = append(steps, inputPathStep{wildcard: true})
				pos++
			} else if start == pos {
				return bad()
			} else {
				steps = append(steps, inputPathStep{key: path[start:pos]})
			}
		case '[':
			pos++
			if pos >= len(path) {
				return bad()
			}
			if path[pos] == '*' {
				pos++
				steps = append(steps, inputPathStep{array: true, wildcard: true})
			} else if bracketKeys && (path[pos] == '\'' || path[pos] == '"') {
				quote := path[pos]
				pos++
				start := pos
				for pos < len(path) && path[pos] != quote {
					// JSONPath filters, escapes and expressions are unsupported.
					if path[pos] == '\\' || path[pos] < 0x20 {
						return bad()
					}
					pos++
				}
				if pos == len(path) {
					return bad()
				}
				steps = append(steps, inputPathStep{key: path[start:pos]})
				pos++
			} else {
				start := pos
				for pos < len(path) && path[pos] >= '0' && path[pos] <= '9' {
					pos++
				}
				if start == pos {
					return bad()
				}
				index, err := strconv.Atoi(path[start:pos])
				if err != nil {
					return bad()
				}
				steps = append(steps, inputPathStep{array: true, index: index})
			}
			if pos >= len(path) || path[pos] != ']' {
				return bad()
			}
			pos++
		default:
			return bad()
		}
	}
	return steps, nil
}

func inputPathKeyByte(b byte) bool {
	return b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '_' || b == '-' || b == '/'
}

func extractInputPath(event json.RawMessage, path []inputPathStep) (json.RawMessage, bool) {
	values := []json.RawMessage{event}
	plural := false
	for _, step := range path {
		var next []json.RawMessage
		plural = plural || step.wildcard
		for _, value := range values {
			trimmed := bytes.TrimSpace(value)
			if step.array || step.wildcard && len(trimmed) > 0 && trimmed[0] == '[' {
				var array []json.RawMessage
				if json.Unmarshal(value, &array) != nil {
					continue
				}
				if step.wildcard {
					next = append(next, array...)
				} else if step.index < len(array) {
					next = append(next, array[step.index])
				}
			} else {
				var object map[string]json.RawMessage
				if json.Unmarshal(value, &object) != nil {
					continue
				}
				if step.wildcard {
					keys := make([]string, 0, len(object))
					for key := range object {
						keys = append(keys, key)
					}
					sort.Strings(keys)
					for _, key := range keys {
						next = append(next, object[key])
					}
				} else if item, present := object[step.key]; present {
					next = append(next, item)
				}
			}
		}
		values = next
		if len(values) == 0 {
			return nil, false
		}
	}
	if plural {
		result, _ := json.Marshal(values)
		return result, true
	}
	return bytes.Clone(values[0]), true
}

type inputTemplate struct {
	paths map[string][]inputPathStep
	roots []*inputTemplateNode
}

type inputTemplateNode struct {
	kind     byte
	literal  string
	variable string
	parts    []inputTemplatePart
	fields   []inputTemplateField
	children []*inputTemplateNode
}

type inputTemplatePart struct {
	literal  string // Already JSON-escaped template text, without outer quotes.
	variable string
}

type inputTemplateField struct {
	key   string // Original JSON string token.
	value *inputTemplateNode
}

func compileInputTransformer(transformer *InputTransformer) (*inputTemplate, error) {
	if n := utf8.RuneCountInString(transformer.InputTemplate); n < 1 || n > 8192 {
		return nil, validation("InputTemplate must contain 1 to 8192 characters")
	}
	if len(transformer.InputPathsMap) > 100 {
		return nil, validation("InputPathsMap may contain at most 100 entries")
	}
	compiled := &inputTemplate{paths: make(map[string][]inputPathStep, len(transformer.InputPathsMap))}
	for key, path := range transformer.InputPathsMap {
		if len(key) < 1 || len(key) > 256 || strings.HasPrefix(key, "AWS.") {
			return nil, validation("InputPathsMap keys must contain 1 to 256 alphanumeric, underscore or hyphen characters and must not start with AWS.")
		}
		for i := range len(key) {
			if !inputPathKeyByte(key[i]) || key[i] == '/' {
				return nil, validation("InputPathsMap keys may contain only alphanumeric, underscore or hyphen characters")
			}
		}
		parsed, err := parseInputPath(path, false)
		if err != nil {
			return nil, err
		}
		compiled.paths[key] = parsed
	}
	parser := inputTemplateParser{text: transformer.InputTemplate, paths: compiled.paths}
	for {
		parser.skipSpace()
		if parser.pos == len(parser.text) {
			break
		}
		node, err := parser.value(false)
		if err != nil {
			return nil, err
		}
		compiled.roots = append(compiled.roots, node)
		separatorStart := parser.pos
		parser.skipSpace()
		if parser.pos < len(parser.text) && (node.kind != 's' || !strings.ContainsAny(parser.text[separatorStart:parser.pos], "\r\n")) {
			return nil, validation("InputTemplate must be JSON or newline-separated JSON strings")
		}
	}
	if len(compiled.roots) == 0 {
		return nil, validation("InputTemplate must be JSON or newline-separated JSON strings")
	}
	if len(compiled.roots) > 1 {
		for _, root := range compiled.roots {
			if root.kind != 's' {
				return nil, validation("Multiple InputTemplate values must all be quoted strings")
			}
		}
	}
	return compiled, nil
}

type inputTemplateParser struct {
	text  string
	pos   int
	paths map[string][]inputPathStep
}

func (p *inputTemplateParser) skipSpace() {
	for p.pos < len(p.text) && strings.ContainsRune(" \t\r\n", rune(p.text[p.pos])) {
		p.pos++
	}
}

func (p *inputTemplateParser) value(objectField bool) (*inputTemplateNode, error) {
	p.skipSpace()
	if p.pos == len(p.text) {
		return nil, validation("InputTemplate is incomplete")
	}
	switch p.text[p.pos] {
	case '"':
		token, err := p.stringToken()
		if err != nil {
			return nil, err
		}
		parts, err := p.stringParts(token)
		if err != nil {
			return nil, err
		}
		return &inputTemplateNode{kind: 's', parts: parts}, nil
	case '<':
		name, err := p.placeholder()
		if err != nil {
			return nil, err
		}
		if (name == eventVariable || name == fullEventVariable) && !objectField {
			return nil, validation("Reserved event variables must be used as a JSON object field value")
		}
		return &inputTemplateNode{kind: 'v', variable: name}, nil
	case '{', '[':
		kind := p.text[p.pos]
		p.pos++
		node := &inputTemplateNode{kind: kind}
		end := byte('}')
		if kind == '[' {
			end = ']'
		}
		p.skipSpace()
		if p.take(end) {
			return node, nil
		}
		for {
			var key string
			if kind == '{' {
				var err error
				key, err = p.stringToken()
				if err != nil {
					return nil, err
				}
				var decoded string
				_ = json.Unmarshal([]byte(key), &decoded)
				if strings.Contains(decoded, "<") {
					return nil, validation("InputTemplate placeholders cannot be used in object keys")
				}
				p.skipSpace()
				if !p.take(':') {
					return nil, validation("InputTemplate object keys require a colon")
				}
			}
			child, err := p.value(kind == '{')
			if err != nil {
				return nil, err
			}
			if kind == '{' {
				node.fields = append(node.fields, inputTemplateField{key: key, value: child})
			} else {
				node.children = append(node.children, child)
			}
			p.skipSpace()
			if p.take(end) {
				return node, nil
			}
			if !p.take(',') {
				return nil, validation("InputTemplate contains invalid JSON separators")
			}
			p.skipSpace()
		}
	default:
		start := p.pos
		for p.pos < len(p.text) && !strings.ContainsRune(",]} \t\r\n", rune(p.text[p.pos])) {
			p.pos++
		}
		literal := p.text[start:p.pos]
		if !json.Valid([]byte(literal)) {
			return nil, validation("InputTemplate must contain JSON values or declared placeholders")
		}
		return &inputTemplateNode{kind: 'l', literal: literal}, nil
	}
}

func (p *inputTemplateParser) take(b byte) bool {
	if p.pos < len(p.text) && p.text[p.pos] == b {
		p.pos++
		return true
	}
	return false
}

func (p *inputTemplateParser) stringToken() (string, error) {
	start := p.pos
	if !p.take('"') {
		return "", validation("InputTemplate object keys and strings must be quoted")
	}
	for p.pos < len(p.text) {
		b := p.text[p.pos]
		p.pos++
		if b == '\\' {
			p.pos++
		} else if b == '"' {
			token := p.text[start:p.pos]
			if !json.Valid([]byte(token)) {
				return "", validation("InputTemplate contains an invalid JSON string")
			}
			return token, nil
		}
	}
	return "", validation("InputTemplate contains an unterminated JSON string")
}

func (p *inputTemplateParser) knownVariable(name string) bool {
	if _, present := p.paths[name]; present {
		return true
	}
	return name == ruleARNVariable || name == ruleNameVariable || name == ingestionTimeVariable || name == eventVariable || name == fullEventVariable
}

func (p *inputTemplateParser) placeholder() (string, error) {
	start := p.pos + 1
	end := strings.IndexByte(p.text[start:], '>')
	if end < 0 {
		return "", validation("InputTemplate has an unterminated placeholder")
	}
	name := p.text[start : start+end]
	if !p.knownVariable(name) {
		return "", validation("InputTemplate refers to an undeclared variable")
	}
	p.pos = start + end + 1
	return name, nil
}

func (p *inputTemplateParser) stringParts(token string) ([]inputTemplatePart, error) {
	var decoded string
	_ = json.Unmarshal([]byte(token), &decoded)
	var parts []inputTemplatePart
	for {
		start := strings.IndexByte(decoded, '<')
		if start < 0 {
			encoded, _ := json.Marshal(decoded)
			parts = append(parts, inputTemplatePart{literal: string(encoded[1 : len(encoded)-1])})
			return parts, nil
		}
		end := strings.IndexByte(decoded[start:], '>')
		if end < 0 {
			return nil, validation("InputTemplate has an unterminated placeholder")
		}
		name := decoded[start+1 : start+end]
		if !p.knownVariable(name) {
			return nil, validation("InputTemplate refers to an undeclared variable")
		}
		if name == eventVariable || name == fullEventVariable {
			return nil, validation("Reserved event variables cannot be used inside strings")
		}
		encoded, _ := json.Marshal(decoded[:start])
		parts = append(parts, inputTemplatePart{literal: string(encoded[1 : len(encoded)-1])}, inputTemplatePart{variable: name})
		decoded = decoded[start+end+1:]
	}
}

func (n *inputTemplateNode) render(values map[string]json.RawMessage) (string, bool) {
	switch n.kind {
	case 'v':
		value, present := values[n.variable]
		return string(value), present
	case 's':
		var out strings.Builder
		out.WriteByte('"')
		for _, part := range n.parts {
			if part.variable == "" {
				out.WriteString(part.literal)
				continue
			}
			value, present := values[part.variable]
			if !present {
				continue
			}
			var text string
			trimmed := bytes.TrimSpace(value)
			if len(trimmed) > 0 && trimmed[0] == '"' && json.Unmarshal(value, &text) == nil {
				// AWS does not escape extracted values inside template strings.
				out.WriteString(text)
			} else {
				var compact bytes.Buffer
				_ = json.Compact(&compact, value)
				// AWS removes internal quotes when objects/arrays are embedded.
				out.WriteString(strings.ReplaceAll(compact.String(), "\"", ""))
			}
		}
		out.WriteByte('"')
		return out.String(), true
	case '{':
		fields := make([]string, 0, len(n.fields))
		for _, field := range n.fields {
			if value, present := field.value.render(values); present {
				fields = append(fields, field.key+":"+value)
			}
		}
		return "{" + strings.Join(fields, ",") + "}", true
	case '[':
		items := make([]string, 0, len(n.children))
		for _, child := range n.children {
			if value, present := child.render(values); present {
				items = append(items, value)
			}
		}
		return "[" + strings.Join(items, ",") + "]", true
	default:
		return n.literal, true
	}
}
