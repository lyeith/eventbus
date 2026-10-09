package gateway

import (
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strings"
)

type mappingInput struct {
	request    *http.Request
	parameters map[string]string

	authorizer authorizerResponse
	config     Config
	requestID  string
}

type mappingSourceKind uint8

const (
	mappingLiteral mappingSourceKind = iota
	mappingPath
	mappingHeader
	mappingQuery
	mappingAuthorizer
	mappingStage
	mappingRequestID
)

type mappingSource struct {
	kind mappingSourceKind
	name string
}

type requestMapping struct {
	kind, name string
	source     mappingSource
}

// Static recipe state belongs to the compiled route. mapRequest never mutates
// these URL/query/slice values; each request owns its output URL and headers.
type compiledIntegration struct {
	target           url.URL
	query            url.Values
	placeholders     []string
	parameters       []requestMapping
	removeHeaders    []string
	needsSourceQuery bool
}

func compileMappingSource(source string) (mappingSource, error) {
	if !validMappingSource(source) {
		return mappingSource{}, fmt.Errorf("unsupported integration mapping source")
	}
	if len(source) >= 2 && source[0] == '\'' && source[len(source)-1] == '\'' {
		return mappingSource{kind: mappingLiteral, name: source[1 : len(source)-1]}, nil
	}
	for _, prefix := range []struct {
		text string
		kind mappingSourceKind
	}{
		{"method.request.path.", mappingPath},
		{"method.request.header.", mappingHeader},
		{"method.request.querystring.", mappingQuery},
		{"context.authorizer.", mappingAuthorizer},
		{"stageVariables.", mappingStage},
	} {
		if strings.HasPrefix(source, prefix.text) {
			return mappingSource{kind: prefix.kind, name: strings.TrimPrefix(source, prefix.text)}, nil
		}
	}
	return mappingSource{kind: mappingRequestID}, nil
}

func (source mappingSource) resolve(input mappingInput, query url.Values) (string, bool) {
	switch source.kind {
	case mappingLiteral:
		return source.name, true
	case mappingPath:
		value, ok := input.parameters[source.name]
		return value, ok
	case mappingHeader:
		values := input.request.Header.Values(source.name)
		return strings.Join(values, ","), len(values) > 0
	case mappingQuery:
		values, ok := query[source.name]
		return strings.Join(values, ","), ok
	case mappingAuthorizer:
		if source.name == "principalId" {
			return input.authorizer.PrincipalID, input.authorizer.PrincipalID != ""
		}
		value, ok := input.authorizer.Context[source.name]
		return value, ok
	case mappingStage:
		value, ok := input.config.StageVariables[source.name]
		return value, ok
	case mappingRequestID:
		return input.requestID, true
	}
	return "", false
}

func compileIntegration(config IntegrationConfig) (*compiledIntegration, error) {
	target, err := validHTTPURL(config.URI)
	if err != nil {
		return nil, err
	}
	placeholders, err := parseIntegrationPath(target.Path)
	if err != nil {
		return nil, err
	}
	plan := &compiledIntegration{target: *target, query: target.Query(), placeholders: placeholders, removeHeaders: slices.Clone(config.RemoveHeaders)}
	destinations := make([]string, 0, len(config.RequestParameters))
	for destination := range config.RequestParameters {
		destinations = append(destinations, destination)
	}
	sort.Strings(destinations)
	for _, destination := range destinations {
		kind, name, ok := mappingDestination(destination)
		if !ok {
			return nil, fmt.Errorf("unsupported integration mapping destination")
		}
		source, err := compileMappingSource(config.RequestParameters[destination])
		if err != nil {
			return nil, err
		}
		plan.parameters = append(plan.parameters, requestMapping{kind: kind, name: name, source: source})
		plan.needsSourceQuery = plan.needsSourceQuery || source.kind == mappingQuery
	}
	return plan, nil
}

func cloneQuery(query url.Values) url.Values {
	cloned := make(url.Values, len(query))
	for name, values := range query {
		cloned[name] = slices.Clone(values)
	}
	return cloned
}

func (plan *compiledIntegration) mapRequest(input mappingInput) (*url.URL, http.Header, error) {
	target := plan.target
	header := input.request.Header.Clone()
	sanitizeProxyHeaders(header)
	header.Set("X-Request-Id", input.requestID)
	if value := input.request.Header.Get("X-Correlation-Id"); value != "" {
		header.Set("X-Correlation-Id", value)
	}
	for _, name := range plan.removeHeaders {
		header.Del(name)
	}
	pathValues := make(map[string]string, len(input.parameters))
	for key, value := range input.parameters {
		pathValues[key] = value
	}
	sourceQuery := input.request.URL.Query()
	query := sourceQuery
	if plan.needsSourceQuery {
		// Destination URI values and earlier mappings must never replace the
		// original caller query seen by subsequent source expressions.
		query = cloneQuery(sourceQuery)
	}
	for key, values := range plan.query {
		query[key] = slices.Clone(values)
	}
	for _, mapping := range plan.parameters {
		value, ok := mapping.source.resolve(input, sourceQuery)
		if !ok {
			return nil, nil, fmt.Errorf("integration mapping source is absent")
		}
		switch mapping.kind {
		case "path":
			pathValues[mapping.name] = value
		case "header":
			if strings.ContainsAny(value, "\r\n\x00") {
				return nil, nil, fmt.Errorf("integration header mapping contains invalid characters")
			}
			header.Set(mapping.name, value)
		case "querystring":
			query.Set(mapping.name, value)
		}
	}
	// Retain the recipe's sequential replacement semantics, including repeated
	// placeholders and values containing a later placeholder.
	for _, name := range plan.placeholders {
		value, ok := pathValues[name]
		if !ok || value == "" || strings.ContainsAny(value, "\\\x00\r\n") {
			return nil, nil, fmt.Errorf("integration path mapping is absent or invalid")
		}
		target.Path = strings.ReplaceAll(target.Path, "{"+name+"}", value)
	}
	if target.Path == "" {
		target.Path = "/"
	}
	target.RawPath = ""
	if !canonicalRequestPath(&target) {
		return nil, nil, fmt.Errorf("integration mapping produced an ambiguous path")
	}
	target.RawQuery = query.Encode()
	return &target, header, nil
}

// Connection can nominate arbitrary headers. Sanitize caller headers before
// applying fixture-owned mappings so a caller cannot add forwarding authority
// or remove an authorizer-minted header by naming it in Connection.
func sanitizeProxyHeaders(header http.Header) {
	for _, value := range header.Values("Connection") {
		for _, name := range strings.Split(value, ",") {
			header.Del(strings.TrimSpace(name))
		}
	}
	for _, name := range []string{"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade", "Forwarded", "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto"} {
		header.Del(name)
	}
}
