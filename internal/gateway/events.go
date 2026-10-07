package gateway

import (
	"encoding/json"
	"maps"
	"strings"
)

// Native HTTP API event builders share trusted request metadata, not an
// authorizer or integration payload-version setting.
func httpHeaders(event requestEvent, multi bool) (map[string]string, map[string][]string) {
	values := make(map[string][]string, len(event.MultiValueHeaders))
	for name, items := range event.MultiValueHeaders {
		key := strings.ToLower(name)
		values[key] = append(values[key], items...)
	}
	headers := make(map[string]string, len(values))
	for name, items := range values {
		if len(items) > 0 {
			if multi {
				headers[name] = items[len(items)-1]
			} else {
				headers[name] = strings.Join(items, ",")
			}
		}
	}
	return headers, values
}

func httpV2Event(event requestEvent) map[string]any {
	headers, _ := httpHeaders(event, false)
	query := make(map[string]string, len(event.MultiValueQueryStringParameters))
	for name, items := range event.MultiValueQueryStringParameters {
		query[name] = strings.Join(items, ",")
	}
	context := event.RequestContext
	result := map[string]any{
		"version": "2.0", "routeKey": event.routeKey, "rawPath": event.rawPath,
		"rawQueryString": event.rawQueryString, "headers": headers,
		"queryStringParameters": query, "pathParameters": event.PathParameters, "stageVariables": event.StageVariables,
		"requestContext": map[string]any{
			"accountId": context.AccountID, "apiId": context.APIID, "domainName": context.DomainName,
			"domainPrefix": strings.Split(context.DomainName, ".")[0], "requestId": context.RequestID,
			"routeKey": event.routeKey, "stage": context.Stage, "time": context.RequestTime, "timeEpoch": context.RequestTimeEpoch,
			"http": map[string]string{"method": event.HTTPMethod, "path": event.apiPath, "protocol": context.Protocol, "sourceIp": context.Identity.SourceIP, "userAgent": context.Identity.UserAgent},
		},
	}
	// Native HTTP events retain each Cookie header value and its original text.
	for name, values := range event.MultiValueHeaders {
		if strings.EqualFold(name, "Cookie") && len(values) > 0 {
			result["cookies"] = values
			break
		}
	}
	return result
}

func httpV1Event(event requestEvent) map[string]any {
	headers, multiHeaders := httpHeaders(event, true)
	context := event.RequestContext
	nativeContext := map[string]any{
		"accountId": context.AccountID, "apiId": context.APIID, "domainName": context.DomainName,
		"domainPrefix": strings.Split(context.DomainName, ".")[0], "extendedRequestId": context.RequestID,
		"httpMethod": event.HTTPMethod, "identity": context.Identity, "path": event.Path, "protocol": context.Protocol,
		"requestId": context.RequestID, "requestTime": context.RequestTime, "requestTimeEpoch": context.RequestTimeEpoch,
		"resourceId": nil, "resourcePath": event.Resource, "stage": context.Stage,
	}
	return map[string]any{
		"version": "1.0", "resource": event.Resource, "path": event.Path, "httpMethod": event.HTTPMethod,
		"headers": headers, "multiValueHeaders": multiHeaders,
		"queryStringParameters": event.QueryStringParameters, "multiValueQueryStringParameters": event.MultiValueQueryStringParameters,
		"pathParameters": event.PathParameters, "stageVariables": event.StageVariables, "requestContext": nativeContext,
	}
}

func authorizerPayload(event requestEvent, version string, identity []string) any {
	switch version {
	case "2.0":
		result := httpV2Event(event)
		result["type"], result["routeArn"], result["identitySource"] = "REQUEST", event.MethodARN, identity
		return result
	case "1.0":
		result := httpV1Event(event)
		result["type"], result["methodArn"] = "REQUEST", event.MethodARN
		result["identitySource"], result["authorizationToken"] = strings.Join(identity, ","), strings.Join(identity, ",")
		return result
	default:
		return event
	}
}

func nativeIdentityValue(source string, event requestEvent) (string, bool) {
	if strings.HasPrefix(source, "$request.header.") {
		name := strings.TrimPrefix(source, "$request.header.")
		headers, _ := httpHeaders(event, false)
		value, ok := headers[strings.ToLower(name)]
		return value, ok
	}
	if strings.HasPrefix(source, "$request.querystring.") {
		values, ok := event.MultiValueQueryStringParameters[strings.TrimPrefix(source, "$request.querystring.")]
		return strings.Join(values, ","), ok
	}
	if strings.HasPrefix(source, "$stageVariables.") {
		value, ok := event.StageVariables[strings.TrimPrefix(source, "$stageVariables.")]
		return value, ok
	}
	switch source {
	case "$context.routeKey":
		return event.routeKey, true
	case "$context.http.method":
		return event.HTTPMethod, true
	case "$context.http.path", "$context.path":
		return event.apiPath, true
	case "$context.http.sourceIp":
		return event.RequestContext.Identity.SourceIP, true
	}
	return identityValue(strings.TrimPrefix(source, "$"), event)
}

// Context sent to Lambda keeps native JSON types. HTTP header mappings consume
// only the separately validated scalar projection.
func integrationAuthorizer(response authorizerResponse) map[string]any {
	result := make(map[string]any, len(response.NativeContext)+1)
	for name, value := range response.NativeContext {
		result[name] = value
	}
	if response.PrincipalID != "" {
		result["principalId"] = response.PrincipalID
	}
	return result
}

func integrationEvent(event requestEvent, version, body string, encoded bool, response authorizerResponse, authorized bool) map[string]any {
	var result map[string]any
	if version == "2.0" {
		result = httpV2Event(event)
		if authorized {
			result["requestContext"].(map[string]any)["authorizer"] = map[string]any{"lambda": integrationAuthorizer(response)}
		}
	} else {
		result = httpV1Event(event)
		if authorized {
			result["requestContext"].(map[string]any)["authorizer"] = integrationAuthorizer(response)
		}
	}
	result["body"], result["isBase64Encoded"] = body, encoded
	return result
}

func scalarContext(value any) (string, bool) {
	switch item := value.(type) {
	case string:
		return item, true
	case json.Number:
		return item.String(), true
	case bool:
		if item {
			return "true", true
		}
		return "false", true
	}
	return "", false
}

func integrationRequestEvent(event requestEvent, removeHeaders []string) requestEvent {
	if len(removeHeaders) == 0 {
		return event
	}
	event.Headers = maps.Clone(event.Headers)
	event.MultiValueHeaders = maps.Clone(event.MultiValueHeaders)
	for _, name := range removeHeaders {
		for key := range event.Headers {
			if strings.EqualFold(key, name) {
				delete(event.Headers, key)
			}
		}
		for key := range event.MultiValueHeaders {
			if strings.EqualFold(key, name) {
				delete(event.MultiValueHeaders, key)
			}
		}
	}
	return event
}

// A single configured API mapping selects this gateway. The 1.0 event retains
// the incoming mapping in path; native 2.0 rawPath and route matching omit it.
func apiPath(basePath, path string) (string, bool) {
	if basePath == "" {
		return path, true
	}
	if path == basePath {
		return "/", true
	}
	if !strings.HasPrefix(path, basePath+"/") {
		return "", false
	}
	return strings.TrimPrefix(path, basePath), true
}

// Canonical path validation rejects encoded separators. Removing mapping
// segments also handles equivalent percent-encoded spelling in the raw URL.
func rawAPIPath(basePath, path string) string {
	if basePath == "" {
		return path
	}
	segments := strings.Split(path, "/")
	count := strings.Count(basePath, "/") + 1
	if count >= len(segments) {
		return "/"
	}
	return "/" + strings.Join(segments[count:], "/")
}
