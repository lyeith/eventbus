# API gateway

`eventbus-gateway` is a standalone request gateway in this repository. It uses
AWS REST API REQUEST-authorizer events, Lambda Invoke and IAM policy responses.
Applications own authorizers, routes and integration mappings. The gateway has
no application identity store or policy dependency.

```sh
go build -o eventbus-gateway ./cmd/gateway
./eventbus-gateway -config gateway.yaml -port 14180
curl -fsS http://localhost:14180/health
```

The gateway runs separately from the EventBus AWS service listener. An authorizer
can run behind any compatible Lambda Invoke endpoint, including EventBus's
[Go, Python and Node execution](LAMBDA.md). No Node/Python installation is needed
when using only an HTTP endpoint or Go binary.

## Fixture

```yaml
port: 14180
region: us-east-1
account_id: "000000000000"
api_id: local
stage: dev
authorizers:
  app:
    type: REQUEST
    invoke_url: http://localhost:14100/2015-03-31/functions/authorize/invocations
    ttl: 0
    identity_sources: []
    timeout: 10s
routes:
  - path: /api/{proxy+}
    method: ANY
    authorizer: app
    integration:
      type: HTTP_PROXY
      uri: http://localhost:18020/api/{proxy}
      remove_headers: [Authorization, Cookie]
```

Exact routes, named path parameters and terminal greedy parameters are supported.
Request bodies and repeated query/header values reach HTTP integrations.
`request_parameters` maps `integration.request.path/header/querystring.NAME`
from `method.request.path/header/querystring.NAME`, `context.authorizer.NAME`,
`context.requestId`, `stageVariables.NAME` or a single-quoted literal.
Authorizer context values are opaque scalars; the gateway never interprets them
as application permissions or exposes them in its response.

Top-level `remove_headers` runs before authorization; integration `remove_headers`
runs before forwarding. These are local fixture extensions. Logging records route
templates and generated IDs, without request bodies, credentials or mapped paths.
`-frontend-proxy URL` supports a development frontend, including upgrades;
`-frontend-dir DIR` serves a static SPA. `-no-auth` is an explicit tooling bypass.

## Contract and verification

| Case | Gateway behavior |
| --- | --- |
| REQUEST event | Concrete method ARN, method/path, headers/query including repeated values, path/stage parameters and request context; no body. |
| Valid policy | `execute-api:Invoke`, string/array actions/resources, IAM wildcards, implicit deny and explicit Deny precedence. |
| Authorizer context | String, number or boolean values; nested objects/arrays are rejected. |
| Function error `Unauthorized` | 401. |
| Valid policy refuses request | 403. |
| Invalid policy, other function error, Invoke failure | 500; backend is not called. |
| Caching | AWS default 300 seconds; explicit `ttl: 0` invokes every request, even without identity sources. |

Unsupported policy semantics fail closed. Conditions, NotAction/NotResource and
policy variables are not silently ignored. Fixtures support HTTP proxy integration;
API management/deployment operations, VTL, HTTP API v2/simple responses, TOKEN/IAM
and Cognito authorizers, usage plans and WebSocket API management are outside this
request-gateway scope. HTTP upgrade proxying is supported.

```sh
go test -race -count=1 ./internal/gateway ./internal/lambda ./internal/app ./internal/server
go vet ./internal/gateway ./internal/lambda ./cmd/gateway
```

Tests own their ports, children and fixtures. SIGTERM drains ordinary HTTP requests,
closes upgraded connections and joins owned runtime children. Consumer applications
must test their real authorizer decisions and backend mappings.

AWS references: [REQUEST events](https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-lambda-authorizer-input.html),
[authorizer output](https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-lambda-authorizer-output.html),
[Lambda Invoke](https://docs.aws.amazon.com/lambda/latest/api/API_Invoke.html).
