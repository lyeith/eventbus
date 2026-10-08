// Package gateway implements the local API Gateway request data plane. Application
// routes, authorizers and integration mappings are supplied as fixtures.
package gateway

import (
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"
)

// Config is the resolved local recipe used by this service. Native protocol
// fields retain AWS semantics; file loading belongs to dev_config.go.
type Config struct {
	Port                          int                         `yaml:"port"`
	Region                        string                      `yaml:"region"`
	AccountID                     string                      `yaml:"account_id"`
	APIID                         string                      `yaml:"api_id"`
	Stage                         string                      `yaml:"stage"`
	BasePath                      string                      `yaml:"base_path"`
	RetainedOwnerControlURL       string                      `yaml:"retained_owner_control_url"`       // Opt-in retained-suite harness control plane.
	RetainedOwnerContinuationPort int                         `yaml:"retained_owner_continuation_port"` // Separate opt-in listener for accepted-work continuations.
	DevHealthPath                 string                      `yaml:"dev_health_path"`                  // Local readiness reservation, not an AWS route.
	StageVariables                map[string]string           `yaml:"stage_variables"`
	Authorizers                   map[string]AuthorizerConfig `yaml:"authorizers"`
	Routes                        []RouteConfig               `yaml:"routes"`
	LogRedactions                 []string                    `yaml:"log_redactions"`
	RemoveHeaders                 []string                    `yaml:"remove_headers"`
}

type AuthorizerConfig struct {
	Type                  string        `yaml:"type"`
	PayloadFormatVersion  string        `yaml:"payload_format_version"`
	EnableSimpleResponses bool          `yaml:"enable_simple_responses"`
	InvokeURL             string        `yaml:"invoke_url"`
	TTL                   *int          `yaml:"ttl"`
	IdentitySources       []string      `yaml:"identity_sources"`
	Timeout               time.Duration `yaml:"timeout"`
}

type RouteConfig struct {
	Path        string            `yaml:"path"`
	RouteKey    string            `yaml:"route_key"`
	Method      string            `yaml:"method"`
	Authorizer  string            `yaml:"authorizer"`
	Integration IntegrationConfig `yaml:"integration"`
}

type IntegrationConfig struct {
	Type                 string            `yaml:"type"`
	URI                  string            `yaml:"uri"`
	InvokeURL            string            `yaml:"invoke_url"`
	PayloadFormatVersion string            `yaml:"payload_format_version"`
	Timeout              time.Duration     `yaml:"timeout"`
	RequestParameters    map[string]string `yaml:"request_parameters"`
	RemoveHeaders        []string          `yaml:"remove_headers"`
}

var identifier = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
var accountIdentifier = regexp.MustCompile(`^[0-9]{12}$`)

func (cfg *Config) Validate() error {
	if cfg == nil {
		return fmt.Errorf("gateway configuration is required")
	}
	if cfg.Port == 0 {
		cfg.Port = 4180
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	if cfg.AccountID == "" {
		cfg.AccountID = "000000000000"
	}
	if cfg.APIID == "" {
		cfg.APIID = "local"
	}
	if cfg.Stage == "" {
		cfg.Stage = "dev"
	}
	if cfg.Port < 1 || cfg.Port > 65535 {
		return fmt.Errorf("gateway port must be 1..65535")
	}
	if !identifier.MatchString(cfg.Region) || !accountIdentifier.MatchString(cfg.AccountID) || !identifier.MatchString(cfg.APIID) || (!identifier.MatchString(cfg.Stage) && cfg.Stage != "$default") {
		return fmt.Errorf("invalid gateway region, account_id, api_id or stage")
	}
	if cfg.BasePath != "" {
		if len(cfg.BasePath) > 300 || cfg.BasePath == "/" || strings.HasSuffix(cfg.BasePath, "/") {
			return fmt.Errorf("base_path must be a canonical absolute API mapping path up to 300 bytes")
		}
		for _, character := range cfg.BasePath {
			if !(character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || strings.ContainsRune("$-_.+!*'()/", character)) {
				return fmt.Errorf("invalid base_path character")
			}
		}
		if _, err := parseTemplate(cfg.BasePath); err != nil {
			return fmt.Errorf("invalid base_path: %w", err)
		}
	}
	if len(cfg.Routes) == 0 {
		return fmt.Errorf("gateway routes are required")
	}
	for name, authorizer := range cfg.Authorizers {
		if !identifier.MatchString(name) {
			return fmt.Errorf("invalid authorizer name %q", name)
		}
		if authorizer.Type != "REQUEST" {
			return fmt.Errorf("authorizer %q: only REQUEST is supported", name)
		}
		if !validInvokeURL(authorizer.InvokeURL) {
			return fmt.Errorf("authorizer %q: invoke_url must be a Lambda Invoke HTTP endpoint", name)
		}
		if authorizer.PayloadFormatVersion != "" && authorizer.PayloadFormatVersion != "1.0" && authorizer.PayloadFormatVersion != "2.0" {
			return fmt.Errorf("authorizer %q: payload_format_version must be 1.0 or 2.0", name)
		}
		if authorizer.EnableSimpleResponses && authorizer.PayloadFormatVersion != "2.0" {
			return fmt.Errorf("authorizer %q: simple responses require payload format 2.0", name)
		}
		if authorizer.TTL == nil {
			ttl := 300
			authorizer.TTL = &ttl
		}
		if *authorizer.TTL < 0 || *authorizer.TTL > 3600 {
			return fmt.Errorf("authorizer %q: ttl must be 0..3600", name)
		}
		if *authorizer.TTL > 0 && len(authorizer.IdentitySources) == 0 {
			return fmt.Errorf("authorizer %q: caching requires identity_sources", name)
		}
		if authorizer.Timeout == 0 {
			authorizer.Timeout = 10 * time.Second
		}
		maximum := 30 * time.Second // Preserve the existing REST authorizer recipe.
		if authorizer.PayloadFormatVersion != "" {
			maximum = 10 * time.Second
		}
		if authorizer.Timeout < time.Millisecond || authorizer.Timeout > maximum {
			return fmt.Errorf("authorizer %q: timeout must be 1ms..%s", name, maximum)
		}
		for _, source := range authorizer.IdentitySources {
			if (authorizer.PayloadFormatVersion == "" && !validIdentitySource(source)) || (authorizer.PayloadFormatVersion != "" && !validHTTPIdentitySource(source)) {
				return fmt.Errorf("authorizer %q: unsupported identity source %q", name, source)
			}
		}
		cfg.Authorizers[name] = authorizer
	}
	seen := make(map[string]bool)
	for i, route := range cfg.Routes {
		template, err := parseTemplate(route.Path)
		if err != nil {
			return fmt.Errorf("route %d: %w", i+1, err)
		}
		if route.Method == "" {
			route.Method = "ANY"
			cfg.Routes[i] = route
		}
		if !validMethod(route.Method) {
			return fmt.Errorf("route %d: invalid method", i+1)
		}
		key := route.Method + " " + route.Path
		if route.RouteKey == "" {
			route.RouteKey = key
		}
		if route.RouteKey != key && route.RouteKey != "$default" {
			return fmt.Errorf("route %d: route_key must match method/path or be $default", i+1)
		}
		if route.RouteKey == "$default" {
			if route.Method != "ANY" || route.Path != "/{proxy+}" {
				return fmt.Errorf("route %d: $default requires ANY /{proxy+}", i+1)
			}
			key = "$default"
		}
		if seen[key] {
			return fmt.Errorf("duplicate route %q", key)
		}
		seen[key] = true
		if route.Authorizer != "" {
			if _, ok := cfg.Authorizers[route.Authorizer]; !ok {
				return fmt.Errorf("route %d: unknown authorizer %q", i+1, route.Authorizer)
			}
		}
		switch route.Integration.Type {
		case "HTTP_PROXY":
			if route.Integration.InvokeURL != "" || route.Integration.PayloadFormatVersion != "" || route.Integration.Timeout != 0 {
				return fmt.Errorf("route %d: Lambda fields require AWS_PROXY", i+1)
			}
			target, err := validHTTPURL(route.Integration.URI)
			if err != nil {
				return fmt.Errorf("route %d: invalid integration URI", i+1)
			}
			if strings.ContainsAny(target.Host, "{}") || strings.ContainsAny(target.RawQuery, "{}") {
				return fmt.Errorf("route %d: URI placeholders are allowed only in the path", i+1)
			}
			if _, err := parseIntegrationPath(target.Path); err != nil {
				return fmt.Errorf("route %d: %w", i+1, err)
			}
			for destination, source := range route.Integration.RequestParameters {
				kind, name, ok := mappingDestination(destination)
				if !ok || !validMappingSource(source) {
					return fmt.Errorf("route %d: unsupported request parameter mapping", i+1)
				}
				if kind == "header" && (strings.EqualFold(name, "Host") || !validHeaderName(name)) {
					return fmt.Errorf("route %d: invalid mapped header", i+1)
				}
				if strings.HasPrefix(source, "method.request.path.") && !template.hasParameter(strings.TrimPrefix(source, "method.request.path.")) {
					return fmt.Errorf("route %d: unknown source path parameter", i+1)
				}
			}
		case "AWS_PROXY":
			if route.Integration.InvokeURL == "" {
				route.Integration.InvokeURL = route.Integration.URI
			}
			if route.Integration.URI != "" && route.Integration.URI != route.Integration.InvokeURL {
				return fmt.Errorf("route %d: choose one Lambda Invoke endpoint", i+1)
			}
			if !validInvokeURL(route.Integration.InvokeURL) {
				return fmt.Errorf("route %d: invoke_url must be a Lambda Invoke HTTP endpoint", i+1)
			}
			if route.Integration.PayloadFormatVersion != "1.0" && route.Integration.PayloadFormatVersion != "2.0" {
				return fmt.Errorf("route %d: AWS_PROXY requires payload_format_version 1.0 or 2.0", i+1)
			}
			if len(route.Integration.RequestParameters) != 0 {
				return fmt.Errorf("route %d: HTTP proxy request mappings are not supported for AWS_PROXY", i+1)
			}
			if route.Integration.Timeout == 0 {
				route.Integration.Timeout = 30 * time.Second
			}
			if route.Integration.Timeout < time.Millisecond || route.Integration.Timeout > 30*time.Second {
				return fmt.Errorf("route %d: integration timeout must be 1ms..30s", i+1)
			}
		default:
			return fmt.Errorf("route %d: integration type must be HTTP_PROXY or AWS_PROXY", i+1)
		}
		for _, name := range route.Integration.RemoveHeaders {
			if !validHeaderName(name) || strings.EqualFold(name, "Host") {
				return fmt.Errorf("route %d: invalid remove_headers entry", i+1)
			}
		}
		cfg.Routes[i] = route
	}
	for _, name := range cfg.RemoveHeaders {
		if !validHeaderName(name) || strings.EqualFold(name, "Host") {
			return fmt.Errorf("invalid remove_headers entry")
		}
	}
	for _, path := range cfg.LogRedactions {
		if _, err := parseTemplate(path); err != nil {
			return fmt.Errorf("invalid log_redactions path: %w", err)
		}
	}
	if err := cfg.validateRetainedGateway(); err != nil {
		return err
	}
	return cfg.validateDevHealth()
}

func endpointPath(endpoint *url.URL) string {
	if endpoint == nil {
		return ""
	}
	return endpoint.Path
}
func validHTTPURL(raw string) (*url.URL, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed == nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" {
		return nil, fmt.Errorf("expected absolute HTTP(S) URL without credentials or fragment")
	}
	return parsed, nil
}
func validMethod(method string) bool {
	switch method {
	case "ANY", http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodOptions, http.MethodTrace, http.MethodConnect:
		return true
	}
	return false
}
func validHeaderName(name string) bool {
	if name == "" {
		return false
	}
	for _, c := range name {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("!#$%&'*+-.^_`|~", c)) {
			return false
		}
	}
	return true
}
func validIdentitySource(source string) bool {
	for _, prefix := range []string{"method.request.header.", "method.request.querystring.", "stageVariables."} {
		if strings.HasPrefix(source, prefix) {
			return identifier.MatchString(strings.TrimPrefix(source, prefix))
		}
	}
	switch source {
	case "context.path", "context.httpMethod", "context.requestId", "context.stage", "context.apiId", "context.accountId", "context.identity.sourceIp":
		return true
	}
	return false
}
func mappingDestination(expression string) (kind, name string, ok bool) {
	for _, kind = range []string{"path", "header", "querystring"} {
		prefix := "integration.request." + kind + "."
		if strings.HasPrefix(expression, prefix) {
			name = strings.TrimPrefix(expression, prefix)
			return kind, name, name != "" && !strings.ContainsAny(name, ".{}\r\n")
		}
	}
	return "", "", false
}
func validMappingSource(source string) bool {
	if len(source) >= 2 && source[0] == '\'' && source[len(source)-1] == '\'' {
		return !strings.Contains(source[1:len(source)-1], "'")
	}
	for _, prefix := range []string{"method.request.path.", "method.request.header.", "method.request.querystring.", "context.authorizer.", "stageVariables."} {
		if strings.HasPrefix(source, prefix) {
			return identifier.MatchString(strings.TrimPrefix(source, prefix))
		}
	}
	return source == "context.requestId"
}

// clone gives the gateway ownership of the resolved configuration before
// validation applies defaults or requests read its nested maps and slices.
func (cfg Config) clone() Config {
	cloned := cfg
	cloned.StageVariables = maps.Clone(cfg.StageVariables)
	cloned.Authorizers = maps.Clone(cfg.Authorizers)
	for name, authorizer := range cloned.Authorizers {
		authorizer.IdentitySources = slices.Clone(authorizer.IdentitySources)
		if authorizer.TTL != nil {
			ttl := *authorizer.TTL
			authorizer.TTL = &ttl
		}
		cloned.Authorizers[name] = authorizer
	}
	cloned.Routes = slices.Clone(cfg.Routes)
	for index := range cloned.Routes {
		integration := &cloned.Routes[index].Integration
		integration.RequestParameters = maps.Clone(integration.RequestParameters)
		integration.RemoveHeaders = slices.Clone(integration.RemoveHeaders)
	}
	cloned.LogRedactions = slices.Clone(cfg.LogRedactions)
	cloned.RemoveHeaders = slices.Clone(cfg.RemoveHeaders)
	return cloned
}

// An Invoke endpoint is fixed by the recipe; request paths never select a function.
func validInvokeURL(raw string) bool {
	endpoint, err := validHTTPURL(raw)
	if err != nil {
		return false
	}
	path := endpoint.Path
	prefix, suffix := "/2015-03-31/functions/", "/invocations"
	if !strings.HasPrefix(path, prefix) || !strings.HasSuffix(path, suffix) {
		return false
	}
	name := strings.TrimSuffix(strings.TrimPrefix(path, prefix), suffix)
	return name != "" && !strings.ContainsAny(name, "/{}")
}
func validHTTPIdentitySource(source string) bool {
	if strings.HasPrefix(source, "$request.header.") {
		return validHeaderName(strings.TrimPrefix(source, "$request.header."))
	}
	if strings.HasPrefix(source, "$request.querystring.") {
		name := strings.TrimPrefix(source, "$request.querystring.")
		return name != "" && !strings.ContainsAny(name, "\x00\r\n")
	}
	if strings.HasPrefix(source, "$stageVariables.") {
		return identifier.MatchString(strings.TrimPrefix(source, "$stageVariables."))
	}
	switch source {
	case "$context.routeKey", "$context.path", "$context.httpMethod", "$context.requestId", "$context.stage", "$context.apiId", "$context.accountId", "$context.identity.sourceIp", "$context.http.method", "$context.http.path", "$context.http.sourceIp":
		return true
	}
	return false
}
