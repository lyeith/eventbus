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

`functions` maps names to immutable execution snapshots; explicit local reload replaces a snapshot. `runtime`, `command`,
`handler`, `environment`, `timeout` and `work_dir` are the only entry fields.
Timeout defaults to 10 seconds and accepts 1ms–900s;
[phase accounting](#init-and-invoke-budgets) determines when it starts. Command
is an argument array, with no implicit shell. Python/Node default to `python3`/`node`;
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

## Init and Invoke budgets

Managed Python/Node executions begin with a bounded 10s Init phase covering
process launch, imports, static code and context preparation. Private readiness
IPC then selects the full configured Invoke budget, capped by any earlier parent
deadline. Handler remaining-time context uses that selected deadline. For
`provided`, the first valid Runtime API `GET /invocation/next` marks readiness;
its `Lambda-Runtime-Deadline-Ms` header carries the selected deadline.

If initial Init times out, EventBus joins that process group, listener and result
pipes before exactly one fresh fallback. The fallback's Init and Invoke share
one configured timeout: readiness does not reset it. The original request ID
and native Event attempt remain unchanged. This internal Init fallback is separate
from the [two async execution retries](#async-execution-evidence). Explicit Init
failure ends that attempt; cancellation or uncertain ownership never starts a
fallback. Caller/service cancellation and effective parent deadlines govern both
phases. The `command` adapter retains its whole-process configured timeout because
it has no runtime readiness boundary.

Gateway HTTP timeouts and SQS receipt visibility remain independent elapsed-time
limits and may expire during Init. Phase accounting does not automatically extend
a caller budget or queue receipt lease.

This models AWS's ordinary on-demand 10s Init and configured-timeout fallback;
AWS explicitly notes that suppressed Init can leave insufficient Invoke time.
See the [AWS lifecycle](https://docs.aws.amazon.com/lambda/latest/dg/lambda-runtime-environment.html#runtimes-lifecycle-ib),
[Init timeout rules](https://docs.aws.amazon.com/lambda/latest/dg/troubleshooting-invocation.html#troubleshooting-invocation-init-timeout)
and [Runtime API](https://docs.aws.amazon.com/lambda/latest/dg/runtimes-api.html).
The default mode remains fresh. Opt-in [warm workers](#warm-workers) reuse managed
Python/Node environments; preinitialized pools, provisioned concurrency and
SnapStart are not modeled. Phase separation or
initialization measurements alone do not attest application business-chain success.

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
`X-Amz-Log-Result`; without the opt-in [private diagnostic sink](#private-invocation-diagnostics),
logs are otherwise retained only during execution.

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

By default each invocation starts a fresh process. Success, timeout, client
cancellation and service shutdown stop its process group and join its launched
child. Warm workers retain a successful environment until explicit retirement. This executes local application code and is
not an operating-system sandbox.

## Registered GetFunction metadata

`GET /2015-03-31/functions/{name}` implements the read-only
[GetFunction API](https://docs.aws.amazon.com/lambda/latest/api/API_GetFunction.html)
for registered targets. Names, ARNs and explicit aliases/versions resolve through
the same registry as Invoke; optional `Qualifier` selects a registered entry.
Missing targets return `ResourceNotFoundException`; malformed names or qualifiers
return `InvalidParameterValueException`. Lookup never starts a handler.

The response contains only `Configuration`: `FunctionName`, `FunctionArn`,
`Runtime`, `Handler` when applicable, `Timeout`, `Version` and `State: Active`.
Runtime is the configured family (`python`, `node`, `provided` or `command`);
it does not assert an AWS runtime version. Handler is a basename/export reference,
and fractional local timeouts round up to seconds. Explicit numeric versions
are preserved; aliases report `$LATEST`. Local source paths, command arguments,
environment variables and downloadable `Code` are omitted.

This subset supports normal LocalStack S3 notification validation alongside
Invoke `DryRun` and `Event`. See [S3 notifications](S3-NOTIFICATIONS.md) for
the required mixed-provider routing and the actual upload integration proof.

## Warm workers

For a retained development stack, opt in through the same local recipe:

```yaml
dev_warm:
  max_workers: 2
```

Omitting `dev_warm` preserves fresh execution. An empty block defaults to two
workers; `max_workers` accepts 1–32. This is one global Python/Node limit across
functions, aliases and generations, including reservations, busy workers, idle
workers and retirement. Each worker handles one invocation at a time. If all
slots are busy, a call waits under its caller/service cancellation before its
Init/Invoke budget starts. Idle workers for another target can be evicted.
`provided` and `command` retain their existing fresh execution policy.

The first call imports the application's handler and uses the existing Init /
Invoke budgets. Later successful calls reuse that imported module and SDK clients
through the same Runtime API event/response/error protocol. Every call gets its
own event, request ID, client context, trace metadata, deadline and context
object. Warm calls have a configured Invoke budget and no new Init budget.
Function errors, invalid/oversized results, process exit, timeout and caller
cancellation retire the worker through actual process-group, listener and pipe
joins. A retained ownership fault fences further worker admission and resume.

Global variables, imported clients, caches, threads, timers and descendants can
survive a successful call. This is shared application state: handlers must avoid
retaining authentication or another request's data unintentionally and must
await required side work. Idle/background output is outside an invocation's log
tail. Warm reuse is local execution, not an operating-system sandbox.

A successful warm invocation joins its handler response and both output drain
boundaries. Its process remains owned by the Lambda service; response completion
allows ordinary native SQS settlement and asynchronous success. It does not prove
background work or descendants completed. When retained lifecycle observation is
configured, a separate `lambda_warm_worker` lease stays live until actual
retirement. Quiescence drains warm reuse, joins those workers and permits held
cleanup only afterward. Calls accepted during that drain execute fresh, under
the same global limit. Resume permits warm reuse again. Final `Close` joins all
worker lifetimes before closing capture sinks.

The optional one-shot `python_stacks` collector requires a fresh process.
Python functions with that option execute fresh, still counting against the warm
limit; Node functions and ordinary private diagnostics can use warm workers.

### Measured SDK calls

On SSD Linux x86_64 (Go 1.26.0, Python 3.12.11/boto3 1.40.61,
Node 22.22.1/SESv2 SDK 3.1146.0), the same module-owned SDK client performed
eight real SES sends per mode. Samples include the first call, with no artificial
Init delay, race instrumentation, cache flushing or latency assertion.

| Runtime / mode | Joined wall first / min / median / max (ms) | Init median (ms) | Invoke median (ms) |
| --- | --- | ---: | ---: |
| Python fresh | 371.626 / 198.202 / 216.078 / 371.626 | 201.048 | 9.381 |
| Python warm | 220.693 / 7.121 / 8.565 / 220.693 | 0 | 2.641 |
| Node fresh | 317.175 / 262.498 / 272.591 / 317.175 | 222.573 | 44.787 |
| Node warm | 315.632 / 8.925 / 12.150 / 315.632 | 0 | 7.124 |

Fresh mode launched eight processes; warm mode retained one. Fresh process RSS
returned to zero after each join. Warm Python idle RSS was 46,518,272–46,624,768
bytes (median 46,610,432); warm Node was 88,657,920–104,902,656 bytes
(median 97,937,408), increasing over these eight calls. These are process RSS
observations, not peak memory, steady-state limits or a leak assessment.
The first warm call still imports the SDK.

Joined wall includes native execution and private diagnostic file appends; SES
capture uses a borrowed in-memory JSONL writer. It excludes gateway transport,
queueing and consuming application work. This small fixed-order fixture does not
predict production latency or sustained-memory behavior. All 32 sends were
captured with unique native identities and the worker processes joined on Close.
Reproduce with the existing frozen SDK lanes:

```sh
EVENTBUS_PERFORMANCE_PYTHON="$PWD/.venv/bin/python" \
go test -count=1 -tags performance ./internal/lambda \
  -run '^TestPerformanceWarmNativeSDKCalls$' -v
```

### Explicit local reload

Standalone EventBus accepts:

```sh
curl -X POST http://localhost:4100/__eventbus/dev/lambda/functions/my-function/reload
```

This development route re-reads the recipe selected by `--lambda-functions` and
re-registers only the named function, including its source reference, command,
environment, timeout and work directory. Other functions and startup-level
`dev_warm`, `dev_async` and diagnostic options are unchanged. There is no file
watching, source hashing, implicit polling or AWS management API emulation.

Registration publishes a new immutable generation. New calls use it. Active old
warm calls finish and retire; already accepted Events retain their old snapshot
and execute fresh rather than reopening an old worker pool. Registration may
finish publication before its caller deadline expires while waiting for an old
worker join; a join deadline does not undo the new target.

Embedded hosts use `RegisterFunction(ctx, name, Function)` to replace a local
recipe or `ReloadFunction(ctx, name)` to reload its currently registered source.
Reload refuses a concurrent registration change instead of overwriting newer
settings. Python reload reads the selected primary source directly, preserving
package metadata; dependency modules use ordinary Python import/bytecode rules.
Explicit aliases are independent registrations; reloading an alias
reloads that entry, while `name:$LATEST` resolves the unqualified entry.

## Async execution evidence

Accepted events execute in an instance-owned, bounded queue. Capacity includes
running work and events waiting to retry. A full queue returns HTTP 429
`TooManyRequestsException` before acceptance. Function and runtime failures,
including timeouts, receive two retries after one and two minutes; a successful
execution settles once. Queued events older than six hours fail without execution.
Use the terminal record to distinguish runtime completion from HTTP 202.
Application business success requires a separate assertion.

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

## Private invocation diagnostics

To retain actual native handler logs, add this separate development option to
the function recipe passed through `--lambda-functions`:

```yaml
dev_diagnostics:
  log_path: .local/lambda-private.jsonl
```

The path resolves against `--work-dir`. There is no default diagnostic sink;
`-`, final symlinks and aliases of async/SQS/SNS/SES/Cognito capture files are
refused. The owned regular file must have permissions `0600` and belong to the
current user; missing parents are created with `0700`. Use trusted parent paths.
Handler logs can contain credentials: keep this file private, separate from
redacted captures, broker logs and public manifests.

Each admitted execution attempt emits `eventbus.lambda.invocation-diagnostic.v1`
after the invocation response/output boundary joins and any required runner,
child, pipe and Runtime API retirement. Successful warm workers remain live
under their separate worker lifetime. Records identify `request_id`,
`function_name`, `function_arn`, `runtime`, `invocation_type`, `attempt`,
`started_at`, `completed_at`, `state`, `function_error` and `ownership_confirmed`.
Synchronous attempts use `RequestResponse` and attempt 1; an async event keeps
one request ID across its numbered retry attempts. States are `succeeded`,
`failed`, `timed_out`, `canceled` and `not_started`.

`stdout` and `stderr` retain separate 64 KiB tails; `tail` retains the merged
4 KiB native log tail. On function failure, `function_diagnostic` retains up to
64 KiB. Each output object has `data`, `encoding` (`utf8` or `base64`), total
`bytes` and `truncated`; empty `data` may be omitted. Command stdout is its result
channel: `stdout_is_response: true` replaces captured `stdout`. Optional
`process_error` and `ownership_error` details are each bounded to 8 KiB with
`detail_truncated`; `context_error` identifies cancellation/deadline expiry.
The top-level `process_error` describes only the final native process launch's
OS Wait error, preserving the original single-launch meaning. It does not
determine the native outcome: a provided runtime can reply successfully, then
be deliberately killed while long-polling for another event. Native
`state`/`function_error` still describe that accepted result.
Top-level ownership covers every native launch and the optional collector;
a later success never clears earlier ownership uncertainty.

`termination_cause` distinguishes `caller_canceled`, `caller_deadline`,
`service_canceled`, `function_timeout`, `initialization_timeout` and
`runtime_protocol_error`. `elapsed_ms` measures monotonic time
through native cleanup and optional collector join; `configured_timeout_ms` is
the configured budget. Cause and native state are fixed after native cleanup.
Slow private evidence can increase terminal elapsed time past the budget without
turning an already completed function into a timeout.
`native_response_synthesized: true` identifies a generated native error response,
including the preserved legacy timeout shape. Caller/service cancellation omits
that manufactured `function_diagnostic`; ordinary function failures and actual
runtime-owned budget/protocol failures retain it. A shorter
caller cancellation does not mean the configured budget elapsed or identify the
application’s original wait.

The additive v1 `execution_phases` array has at most two records. Each contains
`init_attempt` (1 or 2), `mode` (`initial`, `fallback`, `warm` or `command`), `init_ms` and
`invoke_ms`. Optional `init_state`/`invoke_state` describe phases that occurred:
`succeeded`, `failed`, `timed_out`, `canceled` or `not_started`. Command and reused warm workers have only
an Invoke phase. Warm `init_ms` is zero. `init_attempt` is separate from the native async `attempt`.
Each launch also has `ownership_confirmed` and optional `process_error`,
`ownership_error`, `context_error`, `termination_cause` and `detail_truncated`.
Fresh/retired launch ownership covers the native process, Runtime API, result and
control pipe joins. A retained warm record covers its request/result and output
boundaries; it does not attest worker process retirement. Separate worker leases
cover retained lifecycle ownership; optional collector uncertainty remains invocation-level. Per-launch
error details share the 8 KiB bound. Truncation in any launch also sets the
top-level `detail_truncated` flag.

The additive `completion_scope` field on the invocation record and each phase is
`process` after required process retirement (also when no process started), or
`invocation` when a clean response boundary retains its warm worker. Typed
`InvocationOutcome.CompletionScope` supplies the same actual decision to
coordinators. `ownership_confirmed` reports uncertainty within that scope;
`completion_scope: invocation` cannot attest that the worker, background work or
its descendants have joined. SQS delivery evidence carries the same distinction.

A joined managed fallback can have an initial `init_state: timed_out`,
`termination_cause: initialization_timeout` and a retained process error,
followed by successful fallback Init/Invoke with no final process error.
The invocation's `process_error` is then absent, while the retired failure
remains in its phase. A final process failure belongs to the fallback phase
and the top level; a handler error can fail the native result without an OS
process error. Check these typed states/causes and confirmed ownership; do not
classify errors from their free-form text. These are additive v1 fields; older
records without per-launch facts cannot prove that attribution.

Durations are floating-point milliseconds: Init ends at host receipt of readiness,
or native join if readiness never arrived. Invoke includes readiness ACK delivery
and native process/listener/result-pipe joins, excluding optional collector joins.
Total `elapsed_ms` also includes the admission observer, fallback gap and optional
collector join; it need not equal the phase-duration sum. This evidence stays
private and does not change native response fields or redacted async records.

Correlate the native request ID from SQS delivery or SNS `DeliveryAdmission`
with diagnostics, for example:

```sh
jq -c --arg request "$request_id" \
  'select(.schema_version == "eventbus.lambda.invocation-diagnostic.v1" and .request_id == $request) | {request_id, function_arn, attempt, state, function_error, termination_cause, elapsed_ms, configured_timeout_ms, execution_phases, native_response_synthesized, ownership_confirmed, stderr, function_diagnostic, python_stack}' \
  .local/lambda-private.jsonl
```

A caught/logged business exception can coexist with `state: succeeded` and
`function_error: false`. Logs and runtime completion do not attest business
success. Require `ownership_confirmed: true` for joined ownership; unawaited
side work or forcibly closed retained pipes cannot supply that proof. Trusted
handlers must await side work and remain in the owned process group.
Diagnostic append/ownership failure is sticky development evidence failure,
reported by `DevEvidence` and retained-owner checks. Final `Close` returns retained
invocation ownership/capture uncertainty after all owned work joins. `DrainAsync`
also retains async attempt ownership uncertainty, without attributing synchronous
failures to that drain; native async admission/retry evidence remains separate.
Native Invoke outputs, Event admission, retry decisions and existing redacted
async records remain unchanged.

### Python wait snapshots

For the actual registered Python process, explicitly enable snapshots in the
same private recipe sink; omitting `python_stacks` keeps this feature off:

```yaml
dev_diagnostics:
  log_path: .local/lambda-private.jsonl
  python_stacks:
    snapshot_after: 25s
    deadline_lead: 200ms
```

One snapshot is attempted per native Python execution attempt. The collector
attaches only to its first process; Init fallback creates no second collector.
Its deadline timer follows the current Init/Invoke phase and effective parent cap.
Triggers select the earliest of admission plus `snapshot_after`, the current
deadline minus `deadline_lead`, or an explicit typed request. Omitted/zero
`snapshot_after` disables its timer; a positive value must be at most 900s.
Omitted/zero `deadline_lead` selects 200ms;
a positive value must be at most 5s. The gateway's HTTP integration budget is not
propagated as a Lambda context deadline: for a known 30s gateway budget and 60s
Lambda budget, the example requests evidence at 25s. Arbitrary cancellation
cannot wait for a snapshot.

Embedded callers can use `Service.RequestPythonSnapshot(InvocationMetadata)`
with the exact identity from `ExecuteObserved` admission. `true` means request
accepted, not captured. Let admission return so the process can start; observe
the live record before explicitly canceling when a snapshot is required. Fast or
not-yet-started processes can still yield unavailable evidence. There is no native
or HTTP management endpoint and no handler change is required.

Live `eventbus.lambda.python-stack.v1` records retain the original `request_id`,
`function_name`, `function_arn` and `attempt`, plus `threads`, `loops`, `truncated`,
`status` and optional fixed `reason`. Go owns `requested_at` (omitted without a
request) and `captured_at` (receipt/evidence publication time); thread/task views
come from nearby times rather than one atomic process snapshot.
Triggers are `explicit`, `deadline`, `snapshot_after` or `not_requested`; statuses
are `captured`, `capture_unavailable` or `capture_failed`. A fast completion before
the trigger records `not_requested`/`capture_unavailable`/`process_completed`
at join, without an actual request time. The terminal invocation record's
`python_stack` contains only `{status, reason, trigger}`. A live record does not
prove invocation join or business success.

```sh
jq -c --arg request "$request_id" \
  'select(.schema_version == "eventbus.lambda.python-stack.v1" and .request_id == $request) | {request_id, function_arn, attempt, trigger, requested_at, captured_at, status, reason, truncated, threads, loops}' \
  .local/lambda-private.jsonl
```

Collection has a 100ms Python budget and 250ms Go read budget, with at most
32 threads, 8 loops, 64 tasks, 32 frames per stack, 256-byte frame identifiers and
64 KiB JSON. Frames contain only `function`, `file` and `line`; tasks are read on
their loop thread through public APIs for supported stdlib `BaseEventLoop` loops.
Blocked GIL, custom loops or early cancellation may leave unavailable or partial
loop evidence; inspect each loop's state/reason and truncation. Locals, source
text, object reprs, task/thread names and raw exceptions are excluded. File and
function identifiers can be user-controlled: this is bounded private evidence,
not semantic redaction. Snapshots add no stdout/public output and do not extend
native deadlines, alter results/retries or confer cleanup authority.

## Embedded execution and shutdown

The typed local seam shares the HTTP runner and registered targets:

| Method | Contract |
|---|---|
| `ValidateTarget(name, qualifier)` | Resolve a registered function/alias without execution |
| `Execute(ctx, InvokeInput)` | Synchronous result and `FunctionError`, after its response boundary and required retirement join |
| `ExecuteObserved(ctx, InvokeInput, onAdmission)` | Observe the actual attempt identity and return its joined outcome |
| `RequestPythonSnapshot(InvocationMetadata)` | Accept at most one best-effort private snapshot request for an active Python attempt |
| `Admit(ctx, InvokeInput)` | Transfer a private payload copy; return an admission request ID |
| `AsyncSnapshot()` | Copy active and bounded terminal execution metadata |
| `DrainAsync(ctx)` | Stop Event admission and join accepted async work; keep synchronous execution available |
| `RegisterFunction(ctx, name, Function)` / `ReloadFunction(ctx, name)` | Explicit local snapshot/source replacement; retire the previous warm generation |
| `DevBeginWarmDrain()` / `DevResumeWarm()` | Reversible local worker retirement and reuse fence for lifecycle composition |
| `Close(ctx)` | Stop all admission, cancel synchronous work, join runtime resources and close the evidence sink |

`InvokeInput` carries `FunctionName`, optional `Qualifier`, raw JSON `Payload`,
decoded JSON `ClientContext` and `TraceID`. `InvokeError` exposes AWS `Code`,
`Status` and `Message` for failures before execution/admission. Synchronous
function failures are `InvokeOutput.FunctionError`; async failures are evidence.
Secrets rotation owns its step sequence through `Execute`; SQS mappings use
`Execute` completion before acknowledging a receipt. Scheduler and SNS submit
through `Admit`; its request ID proves acceptance, not handler completion. Lambda
owns runtime execution and retry of accepted events. `DescribeTarget` exposes
only immutable registered target identity and timeout for mapping validation.
See [SNS delivery](MESSAGING.md) and [SQS mappings](EVENT-SOURCES.md).

Hosts must cancel/join SQS mappings and stop/drain Scheduler and rotation first, then call
`DrainAsync` while their AWS listener and synchronous Lambda execution remain
available. Accepted handlers can make SDK calls during this drain. Afterward,
drain/stop HTTP and call `Close`. A drain deadline cancels and joins accepted
async work, records cancellation and returns a retained error; it leaves unrelated
synchronous calls available. A final close deadline also cancels and joins all
remaining runtime work. Repeated drain/close calls preserve abort and evidence
errors rather than claiming a healthy shutdown.

The queue and history are in-memory: restart recovery, durable payload storage,
exactly-once execution and cloud system/throttle retry infrastructure are not
implemented. Capture files retain evidence, not replayable events. Remaining function
management/provisioning and event-invoke configuration APIs, cloud destinations/
DLQs, response streaming, extensions and CloudWatch delivery remain outside scope.
See [working authorizer examples](../examples/lambda/functions.yaml), the
[gateway contract](GATEWAY.md) and the
[Runtime API reference](https://docs.aws.amazon.com/lambda/latest/dg/runtimes-api.html).
Native async behavior is described in the
[AWS async error-handling guide](https://docs.aws.amazon.com/lambda/latest/dg/invocation-async-error-handling.html).
