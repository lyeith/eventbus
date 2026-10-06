# Local Lambda invocation

EventBus runs application-owned Go, Python and Node functions behind the
[AWS Lambda Invoke API](https://docs.aws.amazon.com/lambda/latest/api/API_Invoke.html).
The gateway uses this endpoint for REST API REQUEST authorizers. Function code
and authorization decisions belong to the consuming application.

## Register functions

Pass `--lambda-functions functions.yaml --work-dir /path/to/application`.
Paths resolve against `--work-dir`; each function can override `work_dir`.
Registration validates executables and handler files at startup.

```yaml
functions:
  auth-go:
    runtime: provided
    command: [./build/authorizer]
    environment:
      AWS_REGION: us-east-1
      APP_CONFIG: fixtures/authorizer.yaml
    timeout: 10s
  auth-python:
    runtime: python
    command: [uv, run, --no-project, python]
    handler: handlers/authorizer.py.handler
  auth-node:
    runtime: node
    handler: handlers/authorizer.mjs.handler
```

`functions` maps names to immutable local fixtures. `runtime`, `command`,
`handler`, `environment`, `timeout` and `work_dir` are the only entry fields.
Timeout defaults to 10 seconds and accepts 1ms–900s. Command is an argument
array, with no implicit shell. Python/Node default to `python3`/`node`;
an explicit command can select an application virtual environment or runtime.
For example, `[uv, run, --project, ., python]` uses an application's Python project.

## Handler contracts

| Runtime | Application entry point | Returned result |
|---|---|---|
| `provided` | Go/custom binary using the AWS Runtime API | Runtime `/response` or `/error` |
| `python` | Synchronous `handler(event, context)` | A JSON-serializable value |
| `node` | ESM/CJS export; async return or `(event, context, callback)` | A JSON-serializable value |
| `command` | Executable reads event JSON on stdin | One JSON value on stdout |

Python and Node references accept `file.export` or `file#export`. Python also
accepts `package.module.handler`. Node's `index.handler` resolves an unambiguous
`index.js`, `index.mjs` or `index.cjs`; include the extension if more than one exists.
Handler logs go to captured stdout/stderr;
a private result descriptor keeps those logs out of the returned payload.
The command adapter reserves stdout for its result and uses stderr for logs.

Context provides function name/version/ARN, request ID, memory size, log names,
client context and remaining time using the language's AWS field names.
Node supports `context.done`, `succeed` and `fail`. Python async handlers fail
explicitly, matching AWS's synchronous Python handler contract.

`provided` creates a private loopback Runtime API and sets
`AWS_LAMBDA_RUNTIME_API`. Existing `aws-lambda-go/lambda.Start` binaries receive
the original event and standard request ID, deadline, ARN and client-context
headers. The supported Runtime API paths are `/invocation/next`,
`/invocation/{id}/response`, `/invocation/{id}/error` and `/init/error` under
`/2018-06-01/runtime`.

## Invoke and inspect

Invoke `POST /2015-03-31/functions/{name}/invocations` on EventBus, normally
port 4100. Function names, full ARNs, partial ARNs and `Qualifier` are accepted.
Register aliases/versions explicitly as `name:alias` or `name:version`;
`name:$LATEST` also resolves a registered unqualified name.

`RequestResponse` returns HTTP 200 with the function's raw JSON. Exceptions
return HTTP 200, `X-Amz-Function-Error: Unhandled`, and JSON `errorType`/
`errorMessage`; an authorizer's exact `Unauthorized` message is preserved.
`DryRun` validates the request and returns 204 without executing the handler.
`LogType: Tail` returns up to the last 4 KiB of captured logs as base64 in
`X-Amz-Log-Result`. Logs are otherwise retained only during the invocation.

Inputs and results are limited to 6 MiB; malformed request JSON returns 400.
Logs have a bounded 64 KiB tail. Only `PATH` is inherited from the host;
application environment variables must be declared explicitly. EventBus sets
local function metadata and supplies defaults which disable ambient AWS
credential/config file discovery.

Each invocation starts a fresh process. Success, timeout, client cancellation
and service shutdown stop its process group and join its launched child.
There is no warm-runtime cache. This executes local application code and is
not an operating-system sandbox.

Asynchronous `Event` invocation, function management/provisioning APIs,
response streaming, extensions and CloudWatch delivery are outside this scope.
See [working authorizer examples](../examples/lambda/functions.yaml), the
[gateway contract](GATEWAY.md) and the
[Runtime API reference](https://docs.aws.amazon.com/lambda/latest/dg/runtimes-api.html).
