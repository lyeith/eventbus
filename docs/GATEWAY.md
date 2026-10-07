# API gateway

`eventbus-gateway` is a separate request gateway executable in this repository.
It calls authorizers and Lambda integrations through AWS Lambda Invoke HTTP,
including EventBus's [Go, Python and Node runtime](LAMBDA.md). Applications own
routes, authorizers and mappings. No application identity store or policy is
imported by the gateway.

```sh
go build -o eventbus-gateway ./cmd/gateway
./eventbus-gateway -config gateway.yaml -port 14180
curl -fsS http://localhost:14180/health
```

## Local recipe

```yaml
port: 14180
region: us-east-1
account_id: "000000000000"
api_id: local
stage: $default
base_path: /local
authorizers:
  app:
    type: REQUEST
    payload_format_version: "2.0"
    enable_simple_responses: true
    invoke_url: http://localhost:14100/2015-03-31/functions/authorize:local/invocations
    ttl: 0
    identity_sources: [$request.header.Authorization]
    timeout: 10s
routes:
  - path: /api/{proxy+}
    method: ANY
    authorizer: app
    integration:
      type: AWS_PROXY
      payload_format_version: "1.0"
      invoke_url: http://localhost:14100/2015-03-31/functions/application:local/invocations
      timeout: 10s
      remove_headers: [Authorization, Cookie]
```

Call `/local/api/...`. `base_path` is an optional local API mapping; matching
strips it. Format 1.0 preserves the original path; 2.0 rawPath omits the mapping.
Exact routes, named parameters and terminal greedy parameters are supported.
A native `$default` route is expressed as `route_key: $default`, `method: ANY`
and `path: /{proxy+}`. The `$default` stage is supported independently.

Authorizer and integration formats are independent. Omitting authorizer
`payload_format_version` preserves the existing REST REQUEST event. Selecting
`1.0` or `2.0` chooses the corresponding HTTP API authorizer contract. Integrations
select `AWS_PROXY` with explicit `1.0`/`2.0`, or existing `HTTP_PROXY` with `uri`.
The Invoke URL resolves registered function names/ARNs and aliases.

## Authorization

| Contract | Behavior |
| --- | --- |
| REST/HTTP 1.0 REQUEST | Method ARN, headers/query including repeated values, path/stage parameters and native request context; no body. |
| HTTP 2.0 REQUEST | Route ARN/key, raw path/query, identity array, cookies and HTTP context; no body. |
| IAM result | Invoke action, string/array actions/resources, wildcards, implicit deny and explicit Deny precedence. |
| Simple result | `isAuthorized` only when 2.0 plus `enable_simple_responses`; structured 2.0 context is retained. |
| Missing/empty selected HTTP API identity | 401 before Invoke, even at TTL zero. |
| Function error `Unauthorized` / valid refusal | 401 / 403. |
| Malformed policy/result or Invoke failure | 500; integration is not called. |

REST/HTTP 1.0 authorizer context is scalar. A 1.0 integration can still receive
structured context from a selected 2.0 authorizer. HTTP 2.0 supports structured context. IAM conditions,
NotAction/NotResource and policy variables fail closed. TTL defaults to 300
seconds; `ttl: 0` bypasses caching. A positive TTL requires identity sources.
Include `$context.routeKey` in HTTP API identities to scope a cache entry by
route; otherwise a cached simple result applies to the same identity across
routes, matching native behavior. Native HTTP identity syntax includes
`$request.header.NAME`, `$request.querystring.NAME`, `$stageVariables.NAME` and supported `$context` fields.
Native HTTP authorizers allow at most ten seconds; integrations at most thirty.
Invoke requests/results are capped at 6 MiB including JSON/base64 overhead.

## Integration and development controls

AWS_PROXY events include authorizer context, method/path/query/headers/cookies,
path/stage parameters and encoded bodies. Authorizer and integration share one
incoming request ID/time. Responses retain status, repeated headers/cookies and
binary bytes. Only 2.0 applies response inference. FunctionError, malformed or
oversized results produce 502, Invoke failure 500, timeout 504 and oversized
input 413; they never produce a fabricated successful application response.

HTTP_PROXY retains streaming and upgrades. `request_parameters` maps
`integration.request.path/header/querystring.NAME` from
`method.request.path/header/querystring.NAME`, `context.authorizer.NAME`,
`context.requestId`, `stageVariables.NAME` or a single-quoted literal. These
mappings apply to HTTP_PROXY; AWS_PROXY receives its native event instead.

Top-level `remove_headers` removes credentials before authorization; integration
`remove_headers` removes them before forwarding without mutating authorizer data.
These fields, file loading, frontend proxy/static hosting and `-no-auth` are
explicit local harness controls. Logs contain route templates and request IDs,
without request bodies, credentials or mapped paths.

API Gateway management/deployment APIs, VTL, TOKEN/IAM/Cognito authorizers,
usage plans and WebSocket API management remain outside this request scope.
SIGTERM drains HTTP and closes upgraded connections. Tests use real registered
Go, Python and Node handlers, both payload formats and independent combinations;
application acceptance tests still own actual policy decisions and frameworks.

AWS: [HTTP authorizers](https://docs.aws.amazon.com/apigateway/latest/developerguide/http-api-lambda-authorizer.html),
[REST authorizer output](https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-lambda-authorizer-output.html),
[proxy formats](https://docs.aws.amazon.com/apigateway/latest/developerguide/http-api-develop-integrations-lambda.html).
