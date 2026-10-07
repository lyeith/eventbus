// Package gateway implements the local REST API Gateway data plane. Application
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
	Port           int                         `yaml:"port"`
	Region         string                      `yaml:"region"`
	AccountID      string                      `yaml:"account_id"`
	APIID          string                      `yaml:"api_id"`
	Stage          string                      `yaml:"stage"`
	StageVariables map[string]string           `yaml:"stage_variables"`
	Authorizers    map[string]AuthorizerConfig `yaml:"authorizers"`
	Routes         []RouteConfig               `yaml:"routes"`
	LogRedactions  []string                    `yaml:"log_redactions"`
	RemoveHeaders  []string                    `yaml:"remove_headers"`
}

type AuthorizerConfig struct {
	Type            string        `yaml:"type"`
	InvokeURL       string        `yaml:"invoke_url"`
	TTL             *int          `yaml:"ttl"`
	IdentitySources []string      `yaml:"identity_sources"`
	Timeout         time.Duration `yaml:"timeout"`
}

type RouteConfig struct {
	Path        string            `yaml:"path"`
	Method      string            `yaml:"method"`
	Authorizer  string            `yaml:"authorizer"`
	Integration IntegrationConfig `yaml:"integration"`
}

type IntegrationConfig struct {
	Type              string            `yaml:"type"`
	URI               string            `yaml:"uri"`
	RequestParameters map[string]string `yaml:"request_parameters"`
	RemoveHeaders     []string          `yaml:"remove_headers"`
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
	if !identifier.MatchString(cfg.Region) || !accountIdentifier.MatchString(cfg.AccountID) || !identifier.MatchString(cfg.APIID) || !identifier.MatchString(cfg.Stage) {
		return fmt.Errorf("invalid gateway region, account_id, api_id or stage")
	}
	if len(cfg.Routes) == 0 {
		return fmt.Errorf("gateway routes are required")
	}
	for name, authorizer := range cfg.Authorizers {
		if !identifier.MatchString(name) {
			return fmt.Errorf("invalid authorizer name %q", name)
		}
		if authorizer.Type != "REQUEST" {
			return fmt.Errorf("authorizer %q: only REST REQUEST is supported", name)
		}
		endpoint, err := validHTTPURL(authorizer.InvokeURL)
		if err != nil || !strings.HasPrefix(endpointPath(endpoint), "/2015-03-31/functions/") || !strings.HasSuffix(endpointPath(endpoint), "/invocations") || strings.TrimSuffix(strings.TrimPrefix(endpointPath(endpoint), "/2015-03-31/functions/"), "/invocations") == "" {
			return fmt.Errorf("authorizer %q: invoke_url must be a Lambda Invoke HTTP endpoint", name)
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
		if authorizer.Timeout < time.Millisecond || authorizer.Timeout > 30*time.Second {
			return fmt.Errorf("authorizer %q: timeout must be 1ms..30s", name)
		}
		for _, source := range authorizer.IdentitySources {
			if !validIdentitySource(source) {
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
		if seen[key] {
			return fmt.Errorf("duplicate route %q", key)
		}
		seen[key] = true
		if route.Authorizer != "" {
			if _, ok := cfg.Authorizers[route.Authorizer]; !ok {
				return fmt.Errorf("route %d: unknown authorizer %q", i+1, route.Authorizer)
			}
		}
		if route.Integration.Type != "HTTP_PROXY" {
			return fmt.Errorf("route %d: only HTTP_PROXY integrations are supported", i+1)
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
		for _, name := range route.Integration.RemoveHeaders {
			if !validHeaderName(name) || strings.EqualFold(name, "Host") {
				return fmt.Errorf("route %d: invalid remove_headers entry", i+1)
			}
		}
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
	return nil
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
