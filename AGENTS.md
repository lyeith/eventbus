# Working on EventBus

EventBus is a standalone AWS emulator and agent development harness that
supplements LocalStack workflows. Its purpose is a shorter verification/eval
loop: run application scenarios, inspect evidence, assert outcomes and iterate.
Start with [README](README.md), [Agent workflow](docs/AGENT-HARNESS.md) and
[SES capture](docs/SES.md) and [Messaging](docs/MESSAGING.md).
[STATE.md](STATE.md) and [HANDOFF.md](HANDOFF.md) record current work, not API contracts.
For opt-in retained-suite recovery, read [Retained owner](docs/RETAINED-OWNER.md)
before configuring endpoints or cleaning fixtures.

## Ownership and contracts

- Native AWS fields/defaults, resource settings and protocol/state behavior belong
  to their service core. Per-client Cognito settings are core configuration.
- Put local recipe/file loading, deterministic overrides and agent-only controls
  in named `dev_*.go` adapters or the existing harness owners. Compose typed core
  ports in `app`; fixtures must not create separate authentication/delivery rules.
- Use [issue triage](docs/ISSUE-TRIAGE.md) for priority/dependencies. Keep authorizer
  and integration payload formats independent; unsupported AWS behavior stays a
  core capability gap. It is not a dev feature merely because execution is local.

- This repository owns the emulator, generic fixture formats and SDK tests.
  Apps own SDK configuration, seeds, resource names, provisioning and consumers.
- Preserve supported AWS wire behavior and startup flags. The README lists
  coverage and limits; source and tests are authoritative.
- [Architecture](docs/ARCHITECTURE.md) maps package ownership and test placement.
  `internal/app` constructs and closes resources; `internal/server` only routes
  through consumer-owned interfaces. Service packages own stores and operations.
- `internal/messaging` owns the shared registry and separate SQS/SNS engines;
  adapters use typed operations, SNS fanout uses SendQueueMessage, LambdaDelivery
  and FirehoseDelivery ports; external
  delivery uses versioned JSONL capture. Keep registry/entity locks independent.
- `internal/eventsource` owns native SQS mappings through queue and function ports;
  batch/concurrency limits are enforced by joined workers; only completed whole-
  batch execution permits current-receipt acknowledgment. SQS owns leases,
  FIFO and redrive; Lambda owns execution. Close mappings before the runtime.
- `internal/consumer` owns dev recipes, process execution and recipe settlement
  through QueueBroker. It does not implement native event-source mappings.
- `internal/sqsevent` owns SQS Lambda wire types; messaging projects receive
  snapshots for both consumer paths. Keep queue state private.
- `internal/cognito` owns lifecycle, SRP and persisted challenge decisions; its
  TriggerInvoker port receives an application-owned runner from `internal/app`.
- `internal/cognitotrigger` owns Node execution, deadlines and child cleanup;
  it imports no Cognito service. Apps own handler policy and dependencies.
- `internal/gateway` owns generic REQUEST-authorizer and HTTP proxy contracts;
  applications own authorizers and all private integration mappings. It imports
  no consuming application package or store.
- `internal/lambda` owns Invoke, language execution and child lifetime. App
  handlers are external fixtures; keep their logs separate from function results.
- Shared mechanics have one owner: `devcapture` for durable JSONL sinks,
  `localexec` for process groups/descendants and capped output, `awsprotocol`
  for wire mechanics.
  Services keep schemas, admission, result policies and native validation.
- `internal/devquiescence` owns the development retained-owner fence, live counts
  and source leases/generations; `devactivity` supplies optional typed ports.
  Services own native execution, mapped-message custody and reversible drain hooks;
  app composes listeners/profile/declared cleanup, gateway leases root ingress.
  Require safe held proof before cleanup and explicitly re-quiesce after declared
  cleanup before fixture assertions/resume. Preserve callback peers and sentinels;
  no unrelated concurrent suite/callback caller is supported. Keep auth/payloads
  native and actual application cleanup external.
- `internal/ses` owns fixtures, capture records, MIME and both sending adapters.
- Preserve SES JSONL schema/version and exact request/binary capture. A send
  succeeds only after capture; closure follows HTTP drain.
- Do not reset a developer identity database or interrupt an application stack.
  Tests own their state paths, listeners and resources.

## Verify and finish

Run scoped Go tests, then race/vet checks appropriate to the change.
[README](README.md#verify-and-build-releases) lists commands;
[SDK verification](tests/sdk/README.md) explains the frozen Python/JavaScript lanes.
The opt-in Firehose integration suite requires an explicitly owned loopback
RustFS endpoint.

On SSD, wrap builds with `ssd-dev run --purpose build -- <command>` and tests
with `ssd-dev operation --purpose test -- <command>`. Save complete output
before filtering it. Run Python through `uv run`.

Preserve unrelated changes. Commit, push and publish only when authorized.
Release artifacts come from `scripts/build-release.sh` and are excluded from
Git. Stop owned processes and remove temporary probes/environments when done.
Update STATE/HANDOFF when work materially changes; keep each under 80 lines.
