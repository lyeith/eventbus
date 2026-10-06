# EventBus state

- Public repository: https://github.com/lyeith/eventbus; MIT, David Wong, 2026.
- Canonical source: /home/spite/Projects/eventbus on SSD, branch main.
- Organization complete on main; Git records its revision and publication.
- No new binary release; v0.1.0 binaries predate SES.
- No task-owned runtime, test process, virtualenv or CLI artifact remains.

EventBus is a standalone AWS emulator and agent development harness, supplementing
LocalStack and other local AWS stacks. Applications own scenarios and assertions.

The former root package is separated by ownership:
- internal/app: configuration, construction, listener and resource lifetime.
- internal/server: protocol selection, health and consumer-owned HTTP ports.
- internal/awsprotocol: shared JSON/XML envelopes and request IDs.
- internal/cognito, messaging, ses, firehose, ssm and secrets: service-owned state
  and HTTP adapters. SNS/SQS share one broker; queue collections are private.
- internal/consumer: harness configuration, polling/settlement and processes,
  using its QueueBroker port.
- Unit/service tests are colocated; tests/sdk owns Python SDK/JWT proofs/models.
- examples owns seed and SES fixture samples.

Supported AWS behavior, CLI flags, root go build . and SES capture contracts
remain intact. SES supports all six v1 and three v2 sending operations, using
synchronous JSONL capture. Management APIs remain low-priority backlog work.

Verified 2026-10-06: full Go race suite with real SDK lane, tagged vet, five
Python contracts, routing/health/fallback checks, native CLI build and
startup/fixtures/JWKS/v1-v2 capture/SIGTERM, integration-tag compilation.
RustFS integration execution and LocalStack coexistence were not exercised.

docs/ARCHITECTURE.md maps seams and test placement; README, AGENTS, examples,
SDK paths, attribution links and manual CI discovery match the new layout.
No dependencies or lockfiles changed.

Next: publish an
SES-capable binary release when requested; other SES operations remain backlogged.
