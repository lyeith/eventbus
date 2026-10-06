package gateway

import (
	"fmt"
	"net/http"
	"net/url"
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

func (input mappingInput) resolve(source string) (string, bool) {
	if len(source) >= 2 && source[0] == '\'' && source[len(source)-1] == '\'' {
		return source[1 : len(source)-1], true
	}
	if strings.HasPrefix(source, "method.request.path.") {
		value, ok := input.parameters[strings.TrimPrefix(source, "method.request.path.")]
		return value, ok
	}
	if strings.HasPrefix(source, "method.request.header.") {
		values := input.request.Header.Values(strings.TrimPrefix(source, "method.request.header."))
		return strings.Join(values, ","), len(values) > 0
	}
	if strings.HasPrefix(source, "method.request.querystring.") {
		values, ok := input.request.URL.Query()[strings.TrimPrefix(source, "method.request.querystring.")]
		return strings.Join(values, ","), ok
	}
	if strings.HasPrefix(source, "context.authorizer.") {
		name := strings.TrimPrefix(source, "context.authorizer.")
		if name == "principalId" {
			return input.authorizer.PrincipalID, input.authorizer.PrincipalID != ""
		}
		value, ok := input.authorizer.Context[name]
		return value, ok
	}
	if strings.HasPrefix(source, "stageVariables.") {
		value, ok := input.config.StageVariables[strings.TrimPrefix(source, "stageVariables.")]
		return value, ok
	}
	if source == "context.requestId" {
		return input.requestID, true
	}
	return "", false
}

func mapIntegration(config IntegrationConfig, input mappingInput) (*url.URL, http.Header, error) {
	target, err := validHTTPURL(config.URI)
	if err != nil {
		return nil, nil, err
	}
	header := input.request.Header.Clone()
	sanitizeProxyHeaders(header)
	header.Set("X-Request-Id", input.requestID)
	if value := input.request.Header.Get("X-Correlation-Id"); value != "" {
		header.Set("X-Correlation-Id", value)
	}
	for _, name := range config.RemoveHeaders {
		header.Del(name)
	}
	pathValues := make(map[string]string)
	for key, value := range input.parameters {
		pathValues[key] = value
	}
	query := input.request.URL.Query()
	for key, values := range target.Query() {
		query[key] = values
	}
	destinations := make([]string, 0, len(config.RequestParameters))
	for destination := range config.RequestParameters {
		destinations = append(destinations, destination)
	}
	sort.Strings(destinations)
	for _, destination := range destinations {
		source := config.RequestParameters[destination]
		value, ok := input.resolve(source)
		if !ok {
			return nil, nil, fmt.Errorf("integration mapping source is absent")
		}
		kind, name, _ := mappingDestination(destination)
		switch kind {
		case "path":
			pathValues[name] = value
		case "header":
			if strings.ContainsAny(value, "\r\n\x00") {
				return nil, nil, fmt.Errorf("integration header mapping contains invalid characters")
			}
			header.Set(name, value)
		case "querystring":
			query.Set(name, value)
		}
	}
	placeholders, err := parseIntegrationPath(target.Path)
	if err != nil {
		return nil, nil, err
	}
	for _, name := range placeholders {
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
	if !canonicalRequestPath(target) {
		return nil, nil, fmt.Errorf("integration mapping produced an ambiguous path")
	}
	target.RawQuery = query.Encode()
	return target, header, nil
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
