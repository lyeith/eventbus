# Handoff

Canonical SSD checkout: /home/spite/Projects/eventbus, main.
Latest public release: v0.7.0. No Plans pins/stacks/databases changed.
#19 committed/pushed9b8f56f; #18 committed/pushed973e5f1. #20 accepted, committing.

#18 ownership is typed and optional: devquiescence fences/generations/remote
leases; services own native work, SQS custody, reversible drains and actual child
joins; app composes exact cleanup/namespace policy; gateway leases ingress before
body/auth/Invoke and reconciles ACK without replay. Cleanup revokes held safety
until explicit re-quiesce. Original resources persist through Resume.

#20 adds sqs_receipts.go as the canonical receipt owner. Existing issued history
has a settlement bit set only by current/unexpired deletion. Mapping ACK uses the
original bound queue and succeeds on current or proven previous settlement;
stale HTTP no-op, expiry, purge/redrive, replacement and cancellation do not prove
settlement. App delegates; eventsource retains joined whole-batch execution.

Verification (complete logs under finite SSD /tmp retention):
- #18 all affected owner race/vet suites and combined read-only review PASS.
- eventbus-retained-full-stack-app-race-20261008.log: app PASS19.587s.
- eventbus-retained-stack-sdk-race-final-20261008.log: SDK/RustFS PASS23.832s.
- #19 eventbus-sqs-uri-{core-race,core-vet,sdk-race,sdk-vet}.log: PASS unchanged
  current and isolated approved boto3/botocore1.39.4 batch/retry/teardown proofs.
- #20 eventbus-sqs-manual-receipt-*-20261008.log: MQ focused/full race + vet PASS.
- eventbus-sqs-ack-eventsource-{race,vet}.log: PASS.
- eventbus-sqs-manual-settlement-app-{race,vet}-20261008.log: production SDK PASS.
- eventbus-sqs-manual-ack-sdk-normal-fixed-20261008.log: PASS79.177s.
- eventbus-sqs-manual-ack-sdk-race-20261008.log: PASS80.299s; tagged vet PASS.
  Fixture correction preserved real60s QueueDeletedRecently contract; failed
  initial fixture log retained. All fixture children/listeners/data were joined.
- eventbus-pre-evidence-combined-race-20261008.log: final combined run in flight.
SDK verifies generic authenticated cleanup; unavailable production Identity
business handler is not claimed verified. Trusted process-group boundary applies.

#21/#22 newly open; bounded owners are designing SQS source/invocation/join lineage
and opt-in private bounded logs at the native Lambda runner, preserving redacted
public capture and AWS responses. No source edits granted yet. Parent coordinates
app adapters/config and native SDK acceptance; one serial Go test/build lane.

Finish evidence/diagnostics, final regressions, commit/push then clean-source
v0.8.0 build/publication (eight binaries + SHA256SUMS). Packaged native Linux/macOS
proof and downloaded checksums precede publish. No release staging/tag yet.
Retain legacy env /tmp/eventbus-legacy-sdk.UAIkso only for final SDK, then remove;
portable smoke /tmp/eventbus-v0.8.0-native-smoke.py only for packaged proof.
Keep reusable frozen caches and unrelated user resources.
