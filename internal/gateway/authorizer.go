package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const maxInvokePayload = 6 * 1024 * 1024
const maxAuthorizerCache = 4096

type requestEvent struct {
	routeKey                        string
	apiPath                         string
	rawPath                         string
	rawQueryString                  string
	Type                            string              `json:"type"`
	MethodARN                       string              `json:"methodArn"`
	Resource                        string              `json:"resource"`
	Path                            string              `json:"path"`
	HTTPMethod                      string              `json:"httpMethod"`
	Headers                         map[string]string   `json:"headers"`
	MultiValueHeaders               map[string][]string `json:"multiValueHeaders"`
	QueryStringParameters           map[string]string   `json:"queryStringParameters"`
	MultiValueQueryStringParameters map[string][]string `json:"multiValueQueryStringParameters"`
	PathParameters                  map[string]string   `json:"pathParameters"`
	StageVariables                  map[string]string   `json:"stageVariables"`
	RequestContext                  requestContext      `json:"requestContext"`
}
type requestContext struct {
	Path             string          `json:"path"`
	AccountID        string          `json:"accountId"`
	ResourceID       string          `json:"resourceId"`
	Stage            string          `json:"stage"`
	RequestID        string          `json:"requestId"`
	Identity         requestIdentity `json:"identity"`
	ResourcePath     string          `json:"resourcePath"`
	HTTPMethod       string          `json:"httpMethod"`
	APIID            string          `json:"apiId"`
	DomainName       string          `json:"domainName"`
	Protocol         string          `json:"protocol"`
	RequestTime      string          `json:"requestTime"`
	RequestTimeEpoch int64           `json:"requestTimeEpoch"`
}
type requestIdentity struct {
	SourceIP  string `json:"sourceIp"`
	UserAgent string `json:"userAgent"`
}
type authorizerResponse struct {
	PrincipalID      string
	NativeContext    map[string]any
	SimpleAuthorized *bool
	Context          map[string]string
	Statements       []policyStatement
}
type policyStatement struct {
	Effect             string
	Actions, Resources []string
}
type cachedAuthorization struct {
	response authorizerResponse
	expires  time.Time
}
type lambdaAuthorizer struct {
	config AuthorizerConfig
	client *http.Client
	mu     sync.Mutex
	cache  map[string]cachedAuthorization
	now    func() time.Time
}

func newRequestEvent(cfg Config, route RouteConfig, r *http.Request, parameters map[string]string, requestID string) requestEvent {
	multiHeaders := make(map[string][]string, len(r.Header))
	headers := make(map[string]string, len(r.Header))
	for name, values := range r.Header {
		multiHeaders[name] = append([]string(nil), values...)
		if len(values) > 0 {
			headers[name] = values[len(values)-1]
			if strings.EqualFold(name, "Cookie") {
				headers[name] = strings.Join(values, "; ")
			}
		}
	}
	if r.Host != "" {
		headers["Host"] = r.Host
		multiHeaders["Host"] = []string{r.Host}
	}
	query := r.URL.Query()
	singleQuery := make(map[string]string, len(query))
	for name, values := range query {
		if len(values) > 0 {
			singleQuery[name] = values[len(values)-1]
		}
	}
	sourceIP, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		sourceIP = r.RemoteAddr
	}
	routedPath, _ := apiPath(cfg.BasePath, r.URL.Path)
	rawPath := rawAPIPath(cfg.BasePath, r.URL.EscapedPath())
	arn := "arn:aws:execute-api:" + cfg.Region + ":" + cfg.AccountID + ":" + cfg.APIID + "/" + cfg.Stage + "/" + r.Method + "/" + strings.TrimPrefix(routedPath, "/")
	contextPath := "/" + cfg.Stage + r.URL.Path
	if cfg.Stage == "$default" {
		contextPath = r.URL.Path
	}
	now := time.Now().UTC()
	return requestEvent{
		apiPath: routedPath, routeKey: route.RouteKey, rawPath: rawPath, rawQueryString: r.URL.RawQuery,
		Type: "REQUEST", MethodARN: arn, Resource: route.Path, Path: r.URL.Path, HTTPMethod: r.Method,
		Headers: headers, MultiValueHeaders: multiHeaders, QueryStringParameters: singleQuery, MultiValueQueryStringParameters: query,
		PathParameters: parameters, StageVariables: cfg.StageVariables,
		RequestContext: requestContext{Path: contextPath, AccountID: cfg.AccountID, ResourceID: "local", Stage: cfg.Stage, RequestID: requestID, Identity: requestIdentity{SourceIP: sourceIP, UserAgent: r.UserAgent()}, ResourcePath: route.Path, HTTPMethod: r.Method, APIID: cfg.APIID, DomainName: r.Host, Protocol: r.Proto, RequestTime: now.Format("02/Jan/2006:15:04:05 -0700"), RequestTimeEpoch: now.UnixMilli()},
	}
}

func (authorizer *lambdaAuthorizer) authorize(ctx context.Context, event requestEvent) (authorizerResponse, int) {
	if len(event.MethodARN) > 1600 {
		return authorizerResponse{}, http.StatusRequestURITooLong
	}
	ttl := *authorizer.config.TTL
	cacheKey := ""
	identity := make([]string, len(authorizer.config.IdentitySources))
	if ttl > 0 || authorizer.config.PayloadFormatVersion != "" {
		for i, source := range authorizer.config.IdentitySources {
			var value string
			var ok bool
			if authorizer.config.PayloadFormatVersion == "" {
				value, ok = identityValue(source, event)
			} else {
				value, ok = nativeIdentityValue(source, event)
			}
			if !ok || value == "" {
				return authorizerResponse{}, http.StatusUnauthorized
			}
			identity[i] = value
		}
	}
	if ttl > 0 {
		keyJSON, _ := json.Marshal(identity)
		digest := sha256.Sum256(keyJSON)
		cacheKey = hex.EncodeToString(digest[:])
		authorizer.mu.Lock()
		cached, ok := authorizer.cache[cacheKey]
		authorizer.mu.Unlock()
		if ok && authorizer.now().Before(cached.expires) {
			return cached.response, policyStatus(cached.response, event.MethodARN)
		}
	}
	response, status := authorizer.invoke(ctx, event, identity)
	if status != 0 {
		return authorizerResponse{}, status
	}
	if ttl > 0 {
		authorizer.mu.Lock()
		now := authorizer.now()
		if len(authorizer.cache) >= maxAuthorizerCache {
			earliestKey := ""
			var earliest time.Time
			for key, entry := range authorizer.cache {
				if !now.Before(entry.expires) {
					delete(authorizer.cache, key)
					continue
				}
				if earliestKey == "" || entry.expires.Before(earliest) {
					earliestKey, earliest = key, entry.expires
				}
			}
			if len(authorizer.cache) >= maxAuthorizerCache {
				delete(authorizer.cache, earliestKey)
			}
		}
		authorizer.cache[cacheKey] = cachedAuthorization{response: response, expires: now.Add(time.Duration(ttl) * time.Second)}
		authorizer.mu.Unlock()
	}
	return response, policyStatus(response, event.MethodARN)
}

func (authorizer *lambdaAuthorizer) invoke(ctx context.Context, event requestEvent, identity []string) (authorizerResponse, int) {
	payload, err := json.Marshal(authorizerPayload(event, authorizer.config.PayloadFormatVersion, identity))
	if err != nil || len(payload) > maxInvokePayload {
		return authorizerResponse{}, http.StatusInternalServerError
	}
	invoked, failure := invokeLambda(ctx, authorizer.client, authorizer.config.InvokeURL, authorizer.config.Timeout, payload)
	if failure != invokeOK {
		return authorizerResponse{}, http.StatusInternalServerError
	}
	var functionFailure struct {
		ErrorMessage string `json:"errorMessage"`
	}
	if json.Unmarshal(invoked.payload, &functionFailure) == nil && functionFailure.ErrorMessage == "Unauthorized" && (invoked.functionError || authorizer.config.PayloadFormatVersion != "") {
		return authorizerResponse{}, http.StatusUnauthorized
	}
	if invoked.functionError {
		return authorizerResponse{}, http.StatusInternalServerError
	}
	result, err := parseConfiguredAuthorizerResponse(invoked.payload, authorizer.config)
	if err != nil {
		return authorizerResponse{}, http.StatusInternalServerError
	}
	return result, 0
}

func identityValue(source string, event requestEvent) (string, bool) {
	if strings.HasPrefix(source, "method.request.header.") {
		name := strings.TrimPrefix(source, "method.request.header.")
		for key, values := range event.MultiValueHeaders {
			if strings.EqualFold(key, name) {
				encoded, _ := json.Marshal(values)
				if len(values) == 0 || (len(values) == 1 && values[0] == "") {
					return "", false
				}
				return string(encoded), true
			}
		}
		return "", false
	}
	if strings.HasPrefix(source, "method.request.querystring.") {
		values, ok := event.MultiValueQueryStringParameters[strings.TrimPrefix(source, "method.request.querystring.")]
		if !ok || len(values) == 0 || (len(values) == 1 && values[0] == "") {
			return "", false
		}
		encoded, _ := json.Marshal(values)
		return string(encoded), true
	}
	if strings.HasPrefix(source, "stageVariables.") {
		value, ok := event.StageVariables[strings.TrimPrefix(source, "stageVariables.")]
		return value, ok
	}
	switch source {
	case "context.path":
		return event.RequestContext.Path, true
	case "context.httpMethod":
		return event.HTTPMethod, true
	case "context.requestId":
		return event.RequestContext.RequestID, true
	case "context.stage":
		return event.RequestContext.Stage, true
	case "context.apiId":
		return event.RequestContext.APIID, true
	case "context.accountId":
		return event.RequestContext.AccountID, true
	case "context.identity.sourceIp":
		return event.RequestContext.Identity.SourceIP, true
	}
	return "", false
}

func parseAuthorizerResponse(data []byte) (authorizerResponse, error) {
	return parseConfiguredAuthorizerResponse(data, AuthorizerConfig{})
}
func parseConfiguredAuthorizerResponse(data []byte, config AuthorizerConfig) (authorizerResponse, error) {
	if config.EnableSimpleResponses {
		var simple struct {
			IsAuthorized *bool                      `json:"isAuthorized"`
			Context      map[string]json.RawMessage `json:"context"`
		}
		// IAM policies remain valid when simple responses are enabled.
		var discriminator map[string]json.RawMessage
		if err := json.Unmarshal(data, &discriminator); err != nil {
			return authorizerResponse{}, err
		}
		if _, ok := discriminator["isAuthorized"]; ok {
			if err := decodeStrict(data, &simple); err != nil || simple.IsAuthorized == nil {
				return authorizerResponse{}, fmt.Errorf("invalid simple authorizer response")
			}
			result := authorizerResponse{SimpleAuthorized: simple.IsAuthorized}
			if err := parseAuthorizerContext(&result, simple.Context, true); err != nil {
				return authorizerResponse{}, err
			}
			return result, nil
		}
	}
	var raw struct {
		PrincipalID        string                     `json:"principalId"`
		PolicyDocument     json.RawMessage            `json:"policyDocument"`
		Context            map[string]json.RawMessage `json:"context"`
		UsageIdentifierKey string                     `json:"usageIdentifierKey"`
	}
	if err := decodeStrict(data, &raw); err != nil {
		return authorizerResponse{}, err
	}
	if strings.TrimSpace(raw.PrincipalID) == "" {
		return authorizerResponse{}, fmt.Errorf("missing principalId")
	}
	var document struct {
		Version   string          `json:"Version"`
		Statement json.RawMessage `json:"Statement"`
		ID        string          `json:"Id"`
	}
	if err := decodeStrict(raw.PolicyDocument, &document); err != nil || document.Version != "2012-10-17" {
		return authorizerResponse{}, fmt.Errorf("invalid policy document")
	}
	statements, err := policyStatements(document.Statement)
	if err != nil {
		return authorizerResponse{}, err
	}
	result := authorizerResponse{PrincipalID: raw.PrincipalID, Statements: statements}
	if err := parseAuthorizerContext(&result, raw.Context, config.PayloadFormatVersion == "2.0"); err != nil {
		return authorizerResponse{}, err
	}
	return result, nil
}
func parseAuthorizerContext(result *authorizerResponse, raw map[string]json.RawMessage, structured bool) error {
	result.Context = make(map[string]string, len(raw))
	result.NativeContext = make(map[string]any, len(raw))
	for key, value := range raw {
		if key == "claims" && !structured {
			return fmt.Errorf("claims is reserved in authorizer context")
		}
		decoder := json.NewDecoder(bytes.NewReader(value))
		decoder.UseNumber()
		var native any
		if err := decoder.Decode(&native); err != nil {
			return err
		}
		scalar, ok := scalarContext(native)
		if !ok && !structured {
			return fmt.Errorf("authorizer context must contain scalars")
		}
		if ok {
			result.Context[key] = scalar
		}
		result.NativeContext[key] = native
	}
	return nil
}

func policyStatements(data []byte) ([]policyStatement, error) {
	var entries []json.RawMessage
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("missing policy statements")
	}
	if trimmed[0] == '{' {
		entries = []json.RawMessage{data}
	} else if err := json.Unmarshal(data, &entries); err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("empty policy statements")
	}
	statements := make([]policyStatement, 0, len(entries))
	for _, entry := range entries {
		var raw struct {
			SID      string          `json:"Sid"`
			Effect   string          `json:"Effect"`
			Action   json.RawMessage `json:"Action"`
			Resource json.RawMessage `json:"Resource"`
		}
		if err := decodeStrict(entry, &raw); err != nil {
			return nil, err
		}
		if raw.Effect != "Allow" && raw.Effect != "Deny" {
			return nil, fmt.Errorf("invalid policy effect")
		}
		actions, err := stringList(raw.Action)
		if err != nil {
			return nil, err
		}
		resources, err := stringList(raw.Resource)
		if err != nil {
			return nil, err
		}
		for _, resource := range resources {
			if utf8.RuneCountInString(resource) > 512 || strings.Contains(resource, "${") {
				return nil, fmt.Errorf("unsupported policy resource")
			}
		}
		statements = append(statements, policyStatement{Effect: raw.Effect, Actions: actions, Resources: resources})
	}
	return statements, nil
}
func stringList(data []byte) ([]string, error) {
	var singleton string
	if json.Unmarshal(data, &singleton) == nil && singleton != "" {
		return []string{singleton}, nil
	}
	var list []string
	if err := json.Unmarshal(data, &list); err != nil || len(list) == 0 {
		return nil, fmt.Errorf("expected nonempty string or string array")
	}
	for _, value := range list {
		if value == "" {
			return nil, fmt.Errorf("empty policy action or resource")
		}
	}
	return list, nil
}
func decodeStrict(data []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return fmt.Errorf("expected one JSON value")
	}
	return nil
}
func policyStatus(response authorizerResponse, arn string) int {
	if response.SimpleAuthorized != nil {
		if *response.SimpleAuthorized {
			return 0
		}
		return http.StatusForbidden
	}
	allowed := false
	for _, statement := range response.Statements {
		actionMatches, resourceMatches := false, false
		for _, action := range statement.Actions {
			if iamGlob(strings.ToLower(action), "execute-api:invoke") {
				actionMatches = true
				break
			}
		}
		for _, resource := range statement.Resources {
			if iamGlob(resource, arn) {
				resourceMatches = true
				break
			}
		}
		if actionMatches && resourceMatches {
			if statement.Effect == "Deny" {
				return http.StatusForbidden
			}
			allowed = true
		}
	}
	if !allowed {
		return http.StatusForbidden
	}
	return 0
}
func iamGlob(pattern, value string) bool {
	var expression strings.Builder
	expression.WriteString("(?s)^")
	for _, character := range pattern {
		switch character {
		case '*':
			expression.WriteString(".*")
		case '?':
			expression.WriteByte('.')
		default:
			expression.WriteString(regexp.QuoteMeta(string(character)))
		}
	}
	expression.WriteByte('$')
	matched, err := regexp.MatchString(expression.String(), value)
	return err == nil && matched
}
