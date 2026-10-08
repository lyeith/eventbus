package gateway

import (
	"fmt"
	"net/url"
	"path"
	"strings"
)

type segment struct {
	literal, name string
	greedy        bool
}
type pathTemplate struct {
	original string
	segments []segment
	literal  bool
}

func parseTemplate(raw string) (pathTemplate, error) {
	result := pathTemplate{original: raw, literal: true}
	if raw == "" || !strings.HasPrefix(raw, "/") || strings.ContainsAny(raw, "\\%?#") || !canonicalSegments(raw) {
		return result, fmt.Errorf("route path must be an absolute template with canonical segments")
	}
	if raw == "/" {
		return result, nil
	}
	names := make(map[string]bool)
	parts := strings.Split(raw[1:], "/")
	for i, part := range parts {
		item := segment{literal: part}
		if strings.HasPrefix(part, "{") && strings.HasSuffix(part, "}") {
			result.literal = false
			item.name = part[1 : len(part)-1]
			item.literal = ""
			if strings.HasSuffix(item.name, "+") {
				item.greedy = true
				item.name = strings.TrimSuffix(item.name, "+")
			}
			if !identifier.MatchString(item.name) || names[item.name] || (item.greedy && i != len(parts)-1) {
				return result, fmt.Errorf("invalid route path parameter")
			}
			names[item.name] = true
		} else if strings.ContainsAny(part, "{}") {
			return result, fmt.Errorf("path parameters must occupy complete segments")
		}
		result.segments = append(result.segments, item)
	}
	return result, nil
}
func (template pathTemplate) hasParameter(name string) bool {
	for _, item := range template.segments {
		if item.name == name {
			return true
		}
	}
	return false
}

func splitRequestPath(requestPath string) []string {
	return strings.Split(strings.TrimPrefix(requestPath, "/"), "/")
}

func (template pathTemplate) match(requestPath string) (map[string]string, bool) {
	var parts []string
	if !template.literal {
		parts = splitRequestPath(requestPath)
	}
	return template.matchSegments(requestPath, parts)
}

// Gateway selection shares one request split across candidates. Other owners
// use match; both entry points preserve literal slashes and fresh captures.
func (template pathTemplate) matchSegments(requestPath string, parts []string) (map[string]string, bool) {
	if template.literal {
		if requestPath != template.original {
			return nil, false
		}
		return map[string]string{}, true
	}
	if !template.isGreedy() && len(parts) != len(template.segments) {
		return nil, false
	}
	for index, item := range template.segments {
		if index >= len(parts) {
			return nil, false
		}
		if item.greedy {
			if len(parts) == index+1 && parts[index] == "" {
				return nil, false
			}
			break
		}
		if item.name != "" {
			if parts[index] == "" {
				return nil, false
			}
		} else if item.literal != parts[index] {
			return nil, false
		}
	}
	// Failed candidates allocate no parameter map or greedy suffix.
	parameters := make(map[string]string)
	for index, item := range template.segments {
		if item.greedy {
			parameters[item.name] = strings.Join(parts[index:], "/")
		} else if item.name != "" {
			parameters[item.name] = parts[index]
		}
	}
	return parameters, true
}
func (template pathTemplate) isGreedy() bool {
	return len(template.segments) > 0 && template.segments[len(template.segments)-1].greedy
}

func moreSpecific(left, right pathTemplate) bool {
	// AWS selects full method/path matches before any greedy match. Literal
	// prefixes only decide specificity within those two route classes.
	if left.isGreedy() != right.isGreedy() {
		return !left.isGreedy()
	}
	for i := 0; i < len(left.segments) && i < len(right.segments); i++ {
		rank := func(item segment) int {
			if item.name == "" {
				return 3
			}
			if item.greedy {
				return 1
			}
			return 2
		}
		l, r := rank(left.segments[i]), rank(right.segments[i])
		if l != r {
			return l > r
		}
	}
	return len(left.segments) > len(right.segments)
}

// A single trailing slash is a literal empty terminal segment. Interior empty
// segments, dot segments and a second root slash remain ambiguous and invalid.
func canonicalSegments(raw string) bool {
	cleaned := path.Clean(raw)
	return raw == cleaned || (cleaned != "/" && raw == cleaned+"/")
}

func canonicalRequestPath(requestURL *url.URL) bool {
	if requestURL == nil || requestURL.Path == "" || !strings.HasPrefix(requestURL.Path, "/") || strings.ContainsAny(requestURL.Path, "\\\x00\r\n") {
		return false
	}
	if !canonicalSegments(requestURL.Path) {
		return false
	}
	if requestURL.RawPath != "" {
		decoded, err := url.PathUnescape(requestURL.RawPath)
		if err != nil || decoded != requestURL.Path {
			return false
		}
		for _, part := range strings.Split(requestURL.RawPath, "/") {
			value, err := url.PathUnescape(part)
			if err != nil || value == "." || value == ".." || strings.ContainsAny(value, "/\\") {
				return false
			}
		}
	}
	return true
}

// Integration placeholders may occur within path segments; mapped values are
// assigned to URL.Path rather than parsed as URLs, preventing host/query injection.
func parseIntegrationPath(raw string) ([]string, error) {
	var names []string
	for remaining := raw; strings.ContainsAny(remaining, "{}"); {
		start := strings.IndexByte(remaining, '{')
		if start < 0 || strings.Contains(remaining[:start], "}") {
			return nil, fmt.Errorf("invalid integration path placeholder")
		}
		end := strings.IndexByte(remaining[start:], '}')
		if end < 0 {
			return nil, fmt.Errorf("invalid integration path placeholder")
		}
		end += start
		name := remaining[start+1 : end]
		if !identifier.MatchString(name) {
			return nil, fmt.Errorf("invalid integration path placeholder")
		}
		names = append(names, name)
		remaining = remaining[end+1:]
	}
	return names, nil
}
