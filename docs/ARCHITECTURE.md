# Ownership and tests

EventBus owns the emulator and generic agent harness. Applications own their
scenarios, assertions, SDK endpoints, provisioning and consumer programs.

```text
main.go                  AWS service CLI; go build . remains supported
cmd/gateway/             Separate reusable request gateway CLI
internal/
  app/                   Flags, construction, listener and resource lifetime
  server/                AWS protocol selection, routing and /health
  awsprotocol/           Shared wire mechanics, target extraction and request IDs
  devcapture/            Harness JSONL append/sync, failure and file ownership
  devactivity/           Optional typed source/descendant ownership ports
  devquiescence/         Retained-owner fence, source leases, cleanup and resumable generations
  localexec/             Shared process groups, descendant cleanup and capped output
  gateway/               REST/HTTP API REQUEST events, IAM/simple responses and HTTP/Lambda proxy
  lambda/                App-owned multi-language execution, Invoke/Runtime API and child lifetime
  cognito/               SQLite identities, lifecycle, shared challenge state, SRP/auth and JWT/JWKS
  cognitotrigger/        Application-owned Node trigger execution and child lifetime
  messaging/             Shared registry; separate queue and topic engines/adapters
  eventsource/           Native SQS mapping state, bounded workers and completion-based ack
  sqsevent/              Shared native SQS Lambda wire types
  consumer/              Dev recipe configuration, polling, settlement and processes
  ses/                   Fixtures, sending, MIME, capture and v1/v2 adapters
  firehose/              Processing, partition buffers, GZIP and retained S3 delivery
  scheduler/             One-time schedule state, idempotency and Lambda admission
  ssm/                   Parameter state and HTTP adapter
  secrets/               Secret/version/stage state and asynchronous rotation workflow
tests/sdk/               Dispatcher proofs using pinned Python/JavaScript SDKs and JWT/SRP clients
examples/                Application-owned fixture format examples
```

## AWS core and development adapters

Service core owns native fields/defaults, validation, persisted resource settings,
state transitions and AWS protocol events/results. A setting stays core when a
fixture also supplies it: per-client Cognito token policy and gateway payload
formats are examples. Local lifecycle limits must be stated explicitly.

Put YAML/file loading and compatibility fixture extensions in `dev_*.go` files.
`cognito/dev_seed.go` and `dev_provisioning.go`, `gateway/dev_config.go` and
`lambda/dev_config.go` identify fixture loading. Gateway `dev_health.go` owns the
configurable listener readiness reservation and application collision validation.
The `consumer` package already owns harness polling/process recipes. App composition
injects local endpoints, function runners, recorders and clocks through real ports.
Retained-owner controls use `/__eventbus/dev/retained-owner`, a separate harness
namespace. `app/dev_retained_owner.go` composes source/callback listeners and the
supported profile and declared cleanup. Service `dev_*.go` seams report actual
custody/lifetimes and reversible drain; gateway shares root and optional trusted
HTTP continuation leases without changing native payloads/authentication.
The command owns a separate loopback continuation listener through accepted-work
shutdown joins. See [Retained owner](RETAINED-OWNER.md)
and [ticket ownership](ISSUE-TRIAGE.md).

## Dependency rules

- `app` constructs services and injects handlers into `server`. It owns startup,
  signals and shutdown: quiesce background SDK callers with HTTP available, then
  drain HTTP and release resources.
- `server` declares the HTTP interfaces it consumes. It selects a protocol and
  delegates operations; it imports no service package and accesses no store.
- Each service owns its state and operation adapter. Services do not import
  `app`, `server`, `consumer` or another service.
- SNS and SQS share the `messaging` registry but own separate state engines.
  `sqs_*` owns queues, typed operations and both JSON/Query adapters; `sns_*` owns
  topics, subscriptions, SMS/mobile state and Query adapters. SNS delivers to SQS
  through `SendQueueMessage`; Lambda delivery uses its `LambdaDelivery` port.
  Capture records, filtering and admission policy stay with SNS.
  SES/SNS/Cognito notification and Lambda evidence wrappers use `devcapture` for durable output mechanics.
  Publication, subscriptions and queue settlement
  need the same broker. Queue collections remain private to that owner.
- `eventsource` declares queue/function ports and owns native Create/Get/Delete
  mappings and poller lifetime. App binds the original broker queue instance and
  adapts completion-based Lambda Execute; SQS alone owns visibility/FIFO/redrive.
  Native mapping ACK delegates to messaging's receipt-settlement operation:
  current deletion or proven original-lease settlement, never stale HTTP success.
  Mapping workers enforce configured batch/concurrency limits and join as one owner.
  Messaging applies the Lambda event byte budget before leasing queue records.
  Optional `dev_delivery.go` correlates observed native Lambda identity with
  queue-owned receipt classifications; it cannot infer a join or business success.
- `sqsevent` owns shared SQS Lambda wire types. Messaging owns projection from
  immutable receive snapshots. Both native mappings and dev consumers use it.
- `consumer` declares its `QueueBroker` port and uses messaging's queue/message
  types. It owns dev subprocess recipes, batch responses, retries and dead letters.
- `cognito` declares its TriggerInvoker port and owns challenge state and decisions.
  `cognitotrigger` executes configured app handlers; it imports no Cognito package.
  `app` injects and joins the runner before releasing stores/capture.
- `gateway` consumes authorizers only through AWS Lambda Invoke HTTP. It knows no
  application policy, identity, private route format or database. Apps configure
  opaque context mappings and credential removal.
  Its development continuation ingress uses the same native handlers and auth;
  exclusive private-port ownership is an explicit harness trust assumption.
- `lambda` owns execution and runtime protocol; application handlers own policy.
  `runtime_phase.go` owns Init/readiness/Invoke deadlines and bounded Init fallback;
  all phases share caller cancellation and actual process/resource joins.
  `app` injects it into the service dispatcher and closes it after background Event drain and HTTP drain.
  `dev_diagnostics.go` owns opt-in private attempt logs after actual cleanup;
  native results/retries and redacted async metadata keep their existing contracts.
  `dev_python_stacks.go` and its Python collector own bounded live wait snapshots
  in that same sink; live evidence cannot attest invocation join or business success.
- `secrets` owns a narrow RotationInvoker port and its step/state policy;
  `scheduler` owns a narrow TargetInvoker port and admission retry/time policy.
  `app/service_invocations.go` shares redacted Execute/Admit mechanics with SNS
  and SQS mapping adapters. Service-specific delivery/retry policies stay in core;
  services do not import Lambda.
- SNS owns FirehoseDelivery; `app` injects Firehose. Filters/raw/envelopes belong
  to SNS, while extraction/buffering/retry/destination lifetime belong to Firehose.
- `awsprotocol` holds reusable wire helpers, without service state. Callers own
  protocol admission and body budgets; over-budget bodies must fail, never truncate.
  SES/Cognito/SQS retain distinct serializers, size limits and error envelopes.
- `devcapture` owns file/borrowed-writer lifetime, serialized JSONL append, fsync
  and terminal write failure. Services own schemas, timestamps and acceptance.
- `devquiescence` owns the process-exclusive source fence, counted HTTP envelopes,
  non-expiring gateway leases, declared cleanup epochs, held proof and resume.
  `devactivity` owns optional source/descendant ports, not service policy. Native
  owners retain execution, SQS custody/settlement and Firehose retry/force-flush;
  app composes the profile, gateway leases ingress before auth/body/Invoke.
  Applications own callback configuration, authentication and exact cleanup.
  No PID/grace-period proof replaces join; business success stays an app assertion.
- `localexec` owns OS process groups, cancellation, retained-pipe bounds,
  descendant cleanup and capped output buffers. Lambda, triggers and consumers
  own their protocols, results, deadlines, environment and retries; each runner
  must Wait its directly launched child.

Add an interface at a real consumer boundary; keep store implementation details
private. Application acceptance behavior belongs in the consuming application.

## Common patterns and their owners

| Pattern | Owner and boundary |
| --- | --- |
| Complete, durable capture output | `devcapture`; service wrappers select records and acceptance ordering |
| OS child/descendant lifetime and capped output | `localexec`; runners admit/join invocations, select limits and reject incomplete results |
| HTTP drain and dependency-ordered release | `app`; each service joins its own workers |
| Resumable retained-suite ownership barrier | `devquiescence` plus `devactivity` ports; app composes profile/cleanup, service seams retain custody/lifetimes, gateway leases roots/trusted continuations, consuming app owns effects |
| Operation extraction and bounded body reading | `awsprotocol`; service prefix, transport budget and native validation remain caller-owned |
| Token issuance across password/SRP/custom/MFA/refresh | Cognito `auth.go` and `client_validity.go`; every flow uses persisted client policy |
| Shared challenge continuation state | Cognito `challenge_state.go`; custom trigger decisions stay in `custom_auth.go` |
| Password policy and credential revision | Cognito `password_policy.go` and store lifecycle; keep existing distinct error formatting |
| SQS Lambda wire records and projection | `sqsevent` owns types; messaging owns snapshots, leases and settlement |
| Synchronous completion and asynchronous admission adapters | `app`; narrow consumer ports retain service policy, Lambda owns children |
| Fixture provisioning and local recipe loading | Named `dev_*.go` adapters; test-only store mutation helpers stay in `_test.go` |

Similar-looking code does not always have the same contract. Diagnostic tails,
complete function results and queue batch output have different size/error policies.
Likewise native service envelopes, attribute views and auth decisions stay with their
owners. Do not merge these policies into a generic runner or wire decoder.

## Test placement

| Change | Test owner |
| --- | --- |
| Durable JSONL concurrency/restart/write/sync/close failure | `internal/devcapture` tests; service HTTP tests cover acceptance and schema |
| OS cancellation, descendants, retained pipes and output bounds | `internal/localexec` tests; runner tests cover invocation/settlement lifetime |
| REQUEST formats, policy/simple decisions, Lambda/HTTP proxy, cache and upgrade lifetime | `internal/gateway` tests |
| Lambda Invoke, Runtime API, language handlers, child cleanup and private diagnostics | `internal/lambda` tests; actual native SDK evidence lane in `tests/sdk` |
| Store, validation, capture, filtering or operation behavior | Colocated service tests; real SQLite for Cognito |
| Node custom trigger configuration, execution, deadlines and child cleanup | `internal/cognitotrigger` tests |
| SQS mapping validation, polling, acknowledgment, correlated evidence and join barriers | `internal/eventsource` tests; queue-owned classifications in messaging; real Python SDK in `tests/sdk` |
| Consumer configuration, process execution or settlement | `internal/consumer` tests |
| Target/path selection, health or protocol fallback | `internal/server` tests with composed service handlers |
| Scheduler state/timing/idempotency/target admission | `internal/scheduler` tests; real JS SDK in `tests/sdk` |
| Worker joins, HTTP drain or independent resource closure | `internal/app` tests |
| Retained fence/leases/generation, service custody/drain and cleanup recovery | `internal/devquiescence`, owner service seam and gateway/app composition tests; opt-in retained-owner SDK lanes |
| Python/JavaScript SDK, SRP/JWT and trigger/SES interoperability, complete dispatcher and child runner | `tests/sdk`, opt-in `sdksmoke` tag |
| Firehose behavior against an owned RustFS endpoint | `internal/firehose`, opt-in `integration` tag |

Unit tests stay beside their owner so private details need no test-only exports.
Service HTTP tests run just that service; dispatcher tests verify composition.
Fixtures own their listeners, paths and IDs. No test resets a developer runtime.

See [test commands](../tests/README.md) and [SDK fixtures](../tests/sdk/README.md).
