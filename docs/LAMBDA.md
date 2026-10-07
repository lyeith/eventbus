# Local Lambda invocation

EventBus runs application-owned Go, Python and Node functions behind the
[AWS Lambda Invoke API](https://docs.aws.amazon.com/lambda/latest/api/API_Invoke.html).
The gateway uses this endpoint for authorizers and Lambda proxy integrations.
Function code and authorization decisions belong to the consuming application.

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

| `InvocationType` | Acceptance/result | Input limit |
|---|---|---|
| `RequestResponse` (default) | HTTP 200 with the function's raw JSON | 6 MiB |
| `Event` | HTTP 202 with an empty body after queue admission | 1 MiB |
| `DryRun` | HTTP 204 without handler execution | 6 MiB |

`RequestResponse` function errors return HTTP 200,
`X-Amz-Function-Error: Unhandled`, and JSON `errorType`/`errorMessage`;
an authorizer's exact `Unauthorized` message is preserved. Results are limited
to 6 MiB. `LogType: Tail` returns the last 4 KiB of captured logs as base64 in
`X-Amz-Log-Result`; logs are otherwise retained only during execution.

`Event` acceptance does not report handler completion or return its result,
function error or log tail. Accepted events outlive caller cancellation.
`ClientContext` applies only to synchronous invocation. Unknown functions or
unregistered aliases return `ResourceNotFoundException`; an unknown alias never
falls back to the base function. Oversized inputs return
`RequestTooLargeException`; malformed JSON returns `InvalidRequestContentException`.
`DryRun` checks the registered target and request options without decoding event JSON.

Logs have a bounded 64 KiB tail. Only `PATH` is inherited from the host;
application environment variables must be declared explicitly. EventBus sets
local function metadata and supplies defaults which disable ambient AWS
credential/config file discovery.

Each invocation starts a fresh process. Success, timeout, client cancellation
and service shutdown stop its process group and join its launched child.
There is no warm-runtime cache. This executes local application code and is
not an operating-system sandbox.

## Async execution evidence

Accepted events execute in an instance-owned, bounded queue. Capacity includes
running work and events waiting to retry. A full queue returns HTTP 429
`TooManyRequestsException` before acceptance. Function and runtime failures,
including timeouts, receive two retries after one and two minutes; a successful
execution settles once. Queued events older than six hours fail without execution.
Use the terminal record to distinguish business completion from HTTP 202.

The explicit development adapter controls local resources and capture:

```yaml
dev_async:
  workers: 4
  capacity: 64
  history_limit: 256
  log_path: .eventbus/lambda.jsonl
  # Isolated tests can accelerate the two native retry delays:
  # retry_delays: [0s, 0s]
```

The defaults are four workers, 64 outstanding events, 256 terminal records and
native retry delays. Limits are 32 workers, 1,024 outstanding events and 10,000
terminal records. `retry_delays` must contain two durations between 0s and 1h;
it changes timing, not the two-retry limit. `log_path` resolves against
`--work-dir`; `-` selects stdout. When omitted, metadata goes to stderr.

Records use `schema_version: eventbus.lambda.async.v1` and contain `request_id`,
`function_name`, `state`, `attempts`, timestamps and a redacted `error_type`.
States are `queued`, `running`, `retrying`, `succeeded`, `failed` and `canceled`.
Records never include event payloads, function results, error messages or handler
logs. Follow the dedicated file with `tail -f .eventbus/lambda.jsonl`.
The record's request ID identifies the handler invocation, independently of the
HTTP API request ID. Terminal in-memory history is bounded; a selected file retains
the appended metadata.

Admission requires a successful evidence append. An initial capture failure
returns HTTP 500 `ServiceException` before accepting the event. A later failure
preserves accepted execution, blocks subsequent Event admission, emits redacted
failure metadata to stderr and makes drain/close return an error.

## Embedded execution and shutdown

The typed local seam shares the HTTP runner and registered targets:

| Method | Contract |
|---|---|
| `ValidateTarget(name, qualifier)` | Resolve a registered function/alias without execution |
| `Execute(ctx, InvokeInput)` | Synchronous result and `FunctionError`, after process cleanup joins |
| `Admit(ctx, InvokeInput)` | Transfer a private payload copy; return an admission request ID |
| `AsyncSnapshot()` | Copy active and bounded terminal execution metadata |
| `DrainAsync(ctx)` | Stop Event admission and join accepted async work; keep synchronous execution available |
| `Close(ctx)` | Stop all admission, cancel synchronous work, join runtime resources and close the evidence sink |

`InvokeInput` carries `FunctionName`, optional `Qualifier`, raw JSON `Payload`,
decoded JSON `ClientContext` and `TraceID`. `InvokeError` exposes AWS `Code`,
`Status` and `Message` for failures before execution/admission. Synchronous
function failures are `InvokeOutput.FunctionError`; async failures are evidence.
Secrets rotation owns its step sequence through `Execute`; Scheduler submits
through `Admit`. Lambda owns runtime execution and retry of accepted events.

Hosts must stop/drain producers such as Scheduler and rotation first, then call
`DrainAsync` while their AWS listener and synchronous Lambda execution remain
available. Accepted handlers can make SDK calls during this drain. Afterward,
drain/stop HTTP and call `Close`. A drain deadline cancels and joins accepted
async work, records cancellation and returns a retained error; it leaves unrelated
synchronous calls available. A final close deadline also cancels and joins all
remaining runtime work. Repeated drain/close calls preserve abort and evidence
errors rather than claiming a healthy shutdown.

The queue and history are in-memory: restart recovery, durable payload storage,
exactly-once execution and cloud system/throttle retry infrastructure are not
implemented. Capture files retain evidence, not replayable events. Function
management/provisioning and event-invoke configuration APIs, cloud destinations/
DLQs, response streaming, extensions and CloudWatch delivery remain outside scope.
See [working authorizer examples](../examples/lambda/functions.yaml), the
[gateway contract](GATEWAY.md) and the
[Runtime API reference](https://docs.aws.amazon.com/lambda/latest/dg/runtimes-api.html).
Native async behavior is described in the
[AWS async error-handling guide](https://docs.aws.amazon.com/lambda/latest/dg/invocation-async-error-handling.html).
