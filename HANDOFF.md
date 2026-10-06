# Handoff

All 23 SQS and 42 SNS actions are implemented for the local emulator/harness.
SQS JSON and Query share a typed engine; SNS topic/subscription, SMS and mobile
state have separate owners. SNS fanout uses direct SQS sending, not duplicate
queue mutation. No dependencies were added.

SQS adds direct/batch send, String/Number/Binary attributes and AWS MD5s,
visibility batches, renewed receipts, cancellation-aware long polling, FIFO
ordering/dedup/receive attempts, retroactive FIFO delay, tags/policies, DLQ
source listing and real rate-controlled/cancellable message-move tasks.

SNS adds topic/subscription attributes, confirmation/unsubscribe tokens,
advanced attribute/body filters, raw/protocol delivery, batch/FIFO publishing,
permissions/tags/data policies, all SMS/mobile operations and bounded FIFO
archive/replay. DLQ policies validate account/region/FIFO identity atomically.
SMS sandbox OTPs and subscription tokens are available as local capture evidence.

--sns-log emits append-only eventbus.sns.capture.v1 JSONL. Initial capture
precedes acceptance/commit/fanout; write failures are terminal. Later outcome
capture failures preserve accepted publication success and report to stderr.
Shutdown joins workers/drains HTTP before independently closing SNS/SES capture.
Consumer records now preserve AWS SQS metadata, attributes and binary values.

Verification on SSD:
- Full go test -race ./... passed, including Cognito/SES/gateway/Lambda.
- Final scoped messaging/server races passed after contract and XML fixes.
- Consumer/app races passed; independent capture closure is covered.
- Real Go AWS SDKs cover all operation families through service/dispatcher tests.
- Frozen Python messaging and all SES sending SDK checks passed; runner controls
  passed. Python tooling's five tests and go vet ./... passed.
- AWS's documented SQS MD5 example independently verifies checksums.
- Tests own listeners/state; no live AWS comparison or provider delivery is claimed.

MESSAGING.md lists all operations, JSONL parsing, archive limits and exclusions.
IAM/SigV4 enforcement, actual KMS, cloud feedback/inspection and provider retries
are outside the harness. SNS envelopes are unsigned. Archives are in-memory,
synchronous snapshots capped at 64 MiB serialized requests plus metadata/topic.

Gateway/Lambda v0.3.0 is already public and adopted by Plans. Generic gateway
knows no Trust policy/store; Go/Python/Node use synchronous Lambda Invoke.
Messaging release v0.4.0 clean builds, native CLI proof and publication are next.
No developer application stack has been restarted or its state reset.
Temporary staging/model copies are removed; native probe script is task-owned
and will be removed after acceptance. /tmp test receipts use existing 24-hour TTL.
