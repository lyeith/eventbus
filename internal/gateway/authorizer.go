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

// Coalescing is optional admission optimization, not an overload policy.
// Excess work keeps the synchronous request-owned path instead of retaining
// another shared goroutine or rejecting a valid native request.
const maxAuthorizerFlights = 128
const maxAuthorizerFlightWaiters = 128

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

// A parsed policy owns its immutable matchers; every request still evaluates
// the current ARN. No pattern state is shared through a global cache.
type policyStatement struct {
	Effect             string
	actions, resources []*regexp.Regexp
}
type cachedAuthorization struct {
	response authorizerResponse
	expires  time.Time
}
type authorizationFlight struct {
	done      chan struct{}
	cancel    context.CancelFunc
	waiters   int
	abandoned bool
	response  authorizerResponse
	status    int
}
type lambdaAuthorizer struct {
	config  AuthorizerConfig
	client  *http.Client
	mu      sync.Mutex
	cache   map[string]cachedAuthorization
	flights map[string]*authorizationFlight
	workers sync.WaitGroup
	closed  bool
	now     func() time.Time
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
	if ttl <= 0 {
		response, status := authorizer.invoke(ctx, event, identity)
		if status != 0 {
			return authorizerResponse{}, status
		}
		return response, policyStatus(response, event.MethodARN)
	}
	keyJSON, _ := json.Marshal(identity)
	digest := sha256.Sum256(keyJSON)
	cacheKey = hex.EncodeToString(digest[:])
	authorizer.mu.Lock()
	if authorizer.closed {
		authorizer.mu.Unlock()
		return authorizerResponse{}, http.StatusInternalServerError
	}
	cached, ok := authorizer.cache[cacheKey]
	if ok && authorizer.now().Before(cached.expires) {
		authorizer.mu.Unlock()
		return cached.response, policyStatus(cached.response, event.MethodARN)
	}
	if ctx.Err() != nil {
		authorizer.mu.Unlock()
		return authorizerResponse{}, http.StatusInternalServerError
	}
	flight := authorizer.flights[cacheKey]
	if flight != nil && !flight.abandoned && flight.waiters < maxAuthorizerFlightWaiters {
		flight.waiters++
	} else if flight == nil && len(authorizer.flights) < maxAuthorizerFlights {
		if authorizer.flights == nil {
			authorizer.flights = make(map[string]*authorizationFlight)
		}
		// The first caller supplies the native event and context values, but
		// its cancellation/deadline cannot cancel other callers' shared work.
		// Invoke still applies the configured timeout; the owner cancels when
		// no callers remain or on shutdown and joins every worker.
		invokeContext, cancel := context.WithCancel(context.WithoutCancel(ctx))
		flight = &authorizationFlight{done: make(chan struct{}), cancel: cancel, waiters: 1}
		authorizer.flights[cacheKey] = flight
		authorizer.workers.Add(1)
		go authorizer.runFlight(invokeContext, cacheKey, flight, event, identity, ttl)
	} else {
		authorizer.mu.Unlock()
		// A full/retiring flight must not retain unbounded waiting callers.
		// Overflow uses exactly the original caller-cancelled Invoke path.
		response, status := authorizer.invoke(ctx, event, identity)
		if status != 0 {
			return authorizerResponse{}, status
		}
		authorizer.mu.Lock()
		if !authorizer.closed {
			authorizer.cacheResponse(cacheKey, response, ttl)
		}
		authorizer.mu.Unlock()
		return response, policyStatus(response, event.MethodARN)
	}
	authorizer.mu.Unlock()
	select {
	case <-flight.done:
		if flight.status != 0 {
			return authorizerResponse{}, flight.status
		}
		return flight.response, policyStatus(flight.response, event.MethodARN)
	case <-ctx.Done():
		authorizer.mu.Lock()
		flight.waiters--
		last := flight.waiters == 0
		if last {
			flight.abandoned = true
			flight.cancel()
		}
		authorizer.mu.Unlock()
		if last {
			// The final caller joins cancelled work before returning. Other
			// callers may return independently while a surviving peer waits.
			<-flight.done
		}
		return authorizerResponse{}, http.StatusInternalServerError
	}
}

func (authorizer *lambdaAuthorizer) runFlight(ctx context.Context, key string, flight *authorizationFlight, event requestEvent, identity []string, ttl int) {
	defer authorizer.workers.Done()
	defer flight.cancel()
	response, status := authorizer.invoke(ctx, event, identity)
	authorizer.mu.Lock()
	if status == 0 && !flight.abandoned && !authorizer.closed {
		authorizer.cacheResponse(key, response, ttl)
	}
	flight.response, flight.status = response, status
	delete(authorizer.flights, key)
	close(flight.done)
	authorizer.mu.Unlock()
}

// cacheResponse is called with authorizer.mu held. Native successful responses
// include valid IAM Deny and simple false; invocation/parse failures never enter.
func (authorizer *lambdaAuthorizer) cacheResponse(key string, response authorizerResponse, ttl int) {
	now := authorizer.now()
	current, replacing := authorizer.cache[key]
	// An overflow request may finish after a shared peer inserted this key.
	// A live replacement needs no new slot or unrelated eviction.
	if len(authorizer.cache) >= maxAuthorizerCache && (!replacing || !now.Before(current.expires)) {
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
		if len(authorizer.cache) >= maxAuthorizerCache && !replacing {
			delete(authorizer.cache, earliestKey)
		}
	}
	authorizer.cache[key] = cachedAuthorization{response: response, expires: now.Add(time.Duration(ttl) * time.Second)}
}

func (authorizer *lambdaAuthorizer) close() {
	authorizer.mu.Lock()
	authorizer.closed = true
	for _, flight := range authorizer.flights {
		flight.abandoned = true
		flight.cancel()
	}
	authorizer.mu.Unlock()
	// Add and close are serialized by mu, so no worker can appear after Wait.
	authorizer.workers.Wait()
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
		statement := policyStatement{Effect: raw.Effect}
		for _, action := range actions {
			statement.actions = append(statement.actions, compileIAMGlob(strings.ToLower(action)))
		}
		for _, resource := range resources {
			statement.resources = append(statement.resources, compileIAMGlob(resource))
		}
		statements = append(statements, statement)
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
		if matchesIAM(statement.actions, "execute-api:invoke") && matchesIAM(statement.resources, arn) {
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
func matchesIAM(patterns []*regexp.Regexp, value string) bool {
	for _, pattern := range patterns {
		if pattern != nil && pattern.MatchString(value) {
			return true
		}
	}
	return false
}

func compileIAMGlob(pattern string) *regexp.Regexp {
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
	// A compiler limit previously made iamGlob return false. Retain that
	// never-matching pattern policy instead of rejecting the native response.
	compiled, _ := regexp.Compile(expression.String())
	return compiled
}
