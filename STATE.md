# EventBus state

Canonical source: /home/spite/Projects/eventbus on SSD; branch main.
Public repository: https://github.com/lyeith/eventbus; MIT, David Wong, 2026.
Published release: v0.4.0 from clean b60caa1; all eight hashes match GitHub digests.

Completed: all 23 SQS and 42 SNS operations for the local harness.
SNS includes SMS/mobile APIs, with capture rather than external provider delivery.
Implementation is complete. Full Go race suite, final affected race tests,
frozen Python messaging/SES SDK checks, Python tooling tests and vet pass.
No dependencies added. The shared application stack has not been reset/restarted.

Messaging ownership: shared broker registry, separate sqs_* and sns_* engines.
SQS JSON and Query share typed operations; SNS fanout uses SendQueueMessage.
SNS capture uses --sns-log and schema eventbus.sns.capture.v1; initial writes
precede acceptance. Queue/custom/system metadata propagates to consumer events.
FIFO replay uses an in-memory bounded archive; production IAM/KMS/cloud metrics
and real provider delivery remain outside the harness.

Gateway/Lambda work is already pushed and published. eventbus-gateway is separate
from the AWS listener and imports no consuming application policy/store.
Go/custom Runtime API, Python and Node authorizers use AWS Lambda Invoke.
Plans pins service v0.4.0 and gateway v0.3.0. Native auth/route proof passed;
new service public download/checksum and native CLI capture/restart proof passed.

Cognito lifecycle/SRP/custom Node triggers and SES sending capture remain supported.
SQLite preserves identities/signing keys; do not reset developer data.
All messaging/resource state is in memory and reprovisioned after restart.
Docs: MESSAGING.md, GATEWAY.md, LAMBDA.md, ARCHITECTURE.md, AGENT-HARNESS.md.
API Gateway management, Lambda async/warm behavior and SES management are separate.
Test fixtures own listeners/stores; task build/probe files are removed.
Test receipts use existing /tmp 24-hour TTL; normal artifact caches are retained.
