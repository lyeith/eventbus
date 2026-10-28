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
		if parts == nil {
			if requestPath != template.original {
				return nil, false
			}
		} else {
			if len(parts) != len(template.segments) {
				return nil, false
			}
			for index, item := range template.segments {
				if item.literal != parts[index] {
					return nil, false
				}
			}
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

// Request component boundaries come only from literal wire slashes. Decoding
// a component never gives an encoded separator structural routing authority.
func requestComponents(requestURL *url.URL) ([]string, bool) {
	if requestURL == nil || requestURL.Path == "" || !strings.HasPrefix(requestURL.Path, "/") {
		return nil, false
	}
	raw := requestURL.EscapedPath()
	if requestURL.RawPath != "" {
		raw = requestURL.RawPath
		decoded, err := url.PathUnescape(raw)
		if err != nil || decoded != requestURL.Path {
			return nil, false
		}
	}
	if raw == "" || !strings.HasPrefix(raw, "/") || strings.ContainsAny(raw, "\\?#") || !canonicalSegments(raw) {
		return nil, false
	}
	for _, value := range []byte(raw) {
		if value <= 0x20 || value == 0x7f {
			return nil, false
		}
	}
	if raw == "/" {
		return []string{}, true
	}
	rawParts := splitRequestPath(raw)
	parts := make([]string, len(rawParts))
	for index, part := range rawParts {
		decoded, err := url.PathUnescape(part)
		if err != nil || decoded == "." || decoded == ".." {
			return nil, false
		}
		for _, value := range []byte(decoded) {
			if value < 0x20 || value == 0x7f {
				return nil, false
			}
		}
		parts[index] = decoded
	}
	return parts, true
}

func canonicalRequestPath(requestURL *url.URL) bool {
	_, valid := requestComponents(requestURL)
	return valid
}

// requestRoutePath removes the configured mapping only across original wire
// component boundaries. Its decoded display path must never be split again.
func requestRoutePath(base string, requestURL *url.URL) (string, []string, bool) {
	parts, valid := requestComponents(requestURL)
	if !valid {
		return "", nil, false
	}
	if base != "" {
		baseParts := splitRequestPath(base)
		if len(parts) < len(baseParts) {
			return "", nil, false
		}
		for index, part := range baseParts {
			if parts[index] != part {
				return "", nil, false
			}
		}
		parts = parts[len(baseParts):]
		if len(parts) == 1 && parts[0] == "" {
			parts = []string{}
		}
	}
	return "/" + strings.Join(parts, "/"), parts, true
}

// Policy resources must not alias a component containing '/' with an actual
// child route, or interpret caller data as IAM wildcard syntax. Ordinary
// decoded spaces and Unicode retain the existing native event contract.
func routePathIdentity(parts []string) string {
	encoded := make([]string, len(parts))
	replacer := strings.NewReplacer("%", "%25", "/", "%2F", "\\", "%5C", "*", "%2A", "?", "%3F")
	for index, part := range parts {
		encoded[index] = replacer.Replace(part)
	}
	return "/" + strings.Join(encoded, "/")
}

// Captures retain their exact encoded spelling for outbound HTTP mappings.
// Route selection has already checked the template against decoded components.
func requestEscapedParameters(template pathTemplate, rawPath string) map[string]string {
	parts := splitRequestPath(rawPath)
	if rawPath == "/" {
		parts = []string{}
	}
	captures := make(map[string]string)
	for index, item := range template.segments {
		if index >= len(parts) {
			return captures
		}
		if item.greedy {
			captures[item.name] = strings.Join(parts[index:], "/")
			break
		}
		if item.name != "" {
			captures[item.name] = parts[index]
		}
	}
	return captures
}

// Integration placeholders may occur within path segments. Their substitutions
// stay in the URL path; they never acquire host, query or fragment authority.
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
