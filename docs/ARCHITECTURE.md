# Ownership and tests

EventBus owns the emulator and generic agent harness. Applications own their
scenarios, assertions, SDK endpoints, provisioning and consumer programs.

```text
main.go                  AWS service CLI; go build . remains supported
cmd/gateway/             Separate reusable request gateway CLI
internal/
  app/                   Flags, construction, listener and resource lifetime
  server/                AWS protocol selection, routing and /health
  awsprotocol/           Shared JSON/XML envelopes and request IDs
  gateway/               REST REQUEST events, Invoke client, policy evaluation and HTTP proxy mappings
  lambda/                App-owned multi-language execution, Invoke/Runtime API and child lifetime
  cognito/               SQLite identities, lifecycle, SRP/auth, JWT/JWKS and seeds
  cognitotrigger/        Application-owned Node trigger execution and child lifetime
  messaging/             One SNS/SQS broker, filtering and HTTP adapters
  consumer/              Harness configuration, polling, settlement and processes
  ses/                   Fixtures, sending, MIME, capture and v1/v2 adapters
  firehose/              Stream state, buffered S3 delivery and HTTP adapter
  ssm/                   Parameter state and HTTP adapter
  secrets/               Secret/version state, ARN configuration and HTTP adapter
tests/sdk/               Dispatcher proofs using pinned Python/JavaScript SDKs and JWT/SRP clients
examples/                Application-owned fixture format examples
```

## Dependency rules

- `app` constructs services and injects handlers into `server`. It owns startup,
  signals and shutdown: drain HTTP, join workers, then release resources.
- `server` declares the HTTP interfaces it consumes. It selects a protocol and
  delegates operations; it imports no service package and accesses no store.
- Each service owns its state and operation adapter. Services do not import
  `app`, `server`, `consumer` or another service.
- SNS and SQS share `messaging`: publication, subscriptions and queue settlement
  need the same broker. Queue collections remain private to that owner.
- `consumer` declares its `QueueBroker` port and uses messaging's queue/message
  types. It owns subprocess policy, batch responses, retries and dead letters.
- `cognito` declares its TriggerInvoker port and owns challenge state and decisions.
  `cognitotrigger` executes configured app handlers; it imports no Cognito package.
  `app` injects and joins the runner before releasing stores/capture.
- `gateway` consumes authorizers only through AWS Lambda Invoke HTTP. It knows no
  application policy, identity, private route format or database. Apps configure
  opaque context mappings and credential removal.
- `lambda` owns execution and runtime protocol; application handlers own policy.
  `app` injects it into the service dispatcher and closes it after HTTP drain.
- `awsprotocol` holds reusable wire helpers, without service state. SES and
  Cognito retain their distinct decoders, size limits and error envelopes.

Add an interface at a real consumer boundary; keep store implementation details
private. Application acceptance behavior belongs in the consuming application.

## Test placement

| Change | Test owner |
| --- | --- |
| REQUEST events, policy evaluation, integration mapping, cache, streaming/upgrade proxy lifetime | `internal/gateway` tests |
| Lambda Invoke, Runtime API, language handlers and child cleanup | `internal/lambda` tests |
| Store, validation, capture, filtering or operation behavior | Colocated service tests; real SQLite for Cognito |
| Node custom trigger configuration, execution, deadlines and child cleanup | `internal/cognitotrigger` tests |
| Consumer configuration, process execution or settlement | `internal/consumer` tests |
| Target/path selection, health or protocol fallback | `internal/server` tests with composed service handlers |
| Worker joins, HTTP drain or independent resource closure | `internal/app` tests |
| Python/JavaScript SDK, SRP/JWT and trigger/SES interoperability, complete dispatcher and child runner | `tests/sdk`, opt-in `sdksmoke` tag |
| Firehose behavior against an owned RustFS endpoint | `internal/firehose`, opt-in `integration` tag |

Unit tests stay beside their owner so private details need no test-only exports.
Service HTTP tests run just that service; dispatcher tests verify composition.
Fixtures own their listeners, paths and IDs. No test resets a developer runtime.

See [test commands](../tests/README.md) and [SDK fixtures](../tests/sdk/README.md).
