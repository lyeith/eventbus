# Handoff

Tickets #13–#15 are implemented and reviewed; preparing v0.6.0.
Canonical checkout: SSD /home/spite/Projects/eventbus, main.
Latest public release remains v0.5.1 until publication verification finishes.
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
  run b6139df94ec046ecb90dcbc6615b8c64 inspected inactive/quiescent; owned cleanup
  is pending release finishing. No rerun is needed to reinterpret the test result.
- eventbus-eventsource-agent-concurrent-close-race.log: full suite x50 (2.612s).
- eventbus-delivery-final-vet.log and eventbus-delivery-sdk-fixtures.log pass.
- npm ci passed with exact approved test-only Express/Swagger locks.
Read-only final ownership review found no remaining issues.

Next: publish both service/gateway for Linux/macOS amd64/arm64 + SHA256SUMS;
verify native Linux/macOS artifacts and downloaded manifest before making release
latest, clean staging and record actual source/tag/publication.
Other targets will be cross-built, not executed. Service guides state all limits.
