# Handoff

Tickets #13–#15 are implemented, reviewed, closed and published in v0.6.0:
https://github.com/lyeith/eventbus/releases/tag/v0.6.0
Release source/tag: 32142a3f23c312e3eb972b5fe9cc1235d5e4d39c.
Canonical checkout: SSD /home/spite/Projects/eventbus, main.
Public/latest release: v0.6.0, 2026-10-07, nine assets.
No developer stack, Plans pin or identity database changed.

#13: SNS uses its LambdaDelivery port, app adapts the registered Lambda Admit
operation, and native Records preserve message/topic/subscription/time/attributes.
Filtering precedes lookup; eligible unavailable targets and admission pressure
produce explicit delivery failure/DLQ evidence. Admitted execution/retries belong
solely to Lambda. SNS admission request IDs correlate terminal Lambda evidence.

#14: eventsource owns native Create/Get/Delete and one serial poller per mapping.
Explicit batch size one, zero batching window, whole-batch success only. Unknown
selected settings/default ten fail explicitly. Target timeout cannot exceed queue
visibility. Broker queue handles retain instance ownership; visibility/FIFO/redrive
stay native. Successful Execute completion settles only a current receipt. Shared
sqsevent types/messaging projection also serve existing dev recipe consumers.
Deletion/shutdown cancel and join actual work before runtime/resources close.
First Close context governs one published terminal outcome for every closer.

#15: explicit literal trailing-slash templates and full-before-greedy precedence;
original paths remain unchanged in authorizer and integration events. Native
request contracts and dev readiness validation retain distinct owners. Real
unchanged Express/Swagger works through registered Node Lambdas in both formats.
The public ingress readiness guard is an application-owned fixture, not deployment.

Verification logs under SSD /tmp, governed by existing finite retention:
- eventbus-delivery-final-core-race.log: app/server/consumer/messaging/lambda/
  gateway/CLI all pass; includes real Swagger and startup/shutdown proofs.
- eventbus-delivery-final-sdk-race.log: six actual SDK scenarios pass (28.889s),
  including SNS/SQS mappings, Lambda Event, Secrets rotation and Scheduler.
- eventbus-mappings-final-core-race.log: final mappings pass (1.062s).
- eventbus-mappings-final-sdk-race.log: final actual mapping proof PASS (11.909s);
  ssd-dev finalization hit transient EAGAIN after the successful child exit. Exact
  run b6139df94ec046ecb90dcbc6615b8c64 inspected inactive/quiescent; its empty
  owned scratch was removed. The test output and resource barrier are verified.
- eventbus-eventsource-agent-concurrent-close-race.log: full suite x50 (2.612s).
- eventbus-delivery-final-vet.log and eventbus-delivery-sdk-fixtures.log pass.
- npm ci passed with exact approved test-only Express/Swagger locks.
Read-only final ownership review found no remaining issues.

Release verification completed:
- scripts/build-release.sh built eight CGO-free artifacts from clean v0.6.0.
  All embed the tag version, source revision and correct target metadata.
- Packaged Linux amd64 and macOS arm64 passed actual SNS alias/SQS mapping
  side effects and acknowledgment, SQLite create/describe, gateway 1.0/2.0
  slash/query/assets/binary/private routing/readiness and collision refusal.
  Other targets were cross-built, not executed. Simple artifact fixtures supplement
  the unchanged Express/Swagger source proof, without a live AWS parity claim.
- Downloaded all nine GitHub assets, verified SHA256SUMS and exact manifest before
  publication. Public/latest, non-draft, nine assets and source/tag verified.
- Source/main pushed; GitHub has no open issues. Release/download/laptop staging,
  temporary smoke scripts and owned fixture state/processes removed. No hold.
Logs: eventbus-v0.6.0-{build,artifact-smoke,macos-artifact-smoke,metadata}.log
and eventbus-v0.6.0-publication.json, governed by finite SSD /tmp retention.
Next: consuming-application acceptance; guides state exact remaining limits.
