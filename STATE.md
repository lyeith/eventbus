# EventBus state

Canonical SSD checkout: /home/spite/Projects/eventbus, main.

Latest release [v0.22.0](https://github.com/lyeith/eventbus/releases/tag/v0.22.0)
was published on 2026-11-25 from 616f89a8f2cc8f910d1be0a4387452c5fb4574ee.
It includes native SQS ReportBatchItemFailures and the gateway follow-up
previously on main. CI 45553342666 passed. All eight Linux/macOS service and
gateway downloads match SHA256SUMS. Packaged Linux boto3 verified Create/Get,
per-record delivery evidence, selective retry and settlement. Owned release
outputs and downloads are removed.

The local coverage follow-up adds SQS mapping List, Update and tags on main.
List filters and paginates detached snapshots. Update stops intake, joins
accepted work through receipt processing, and keeps UUID and original queue.
An invalid candidate leaves the current worker generation in place. Tag writes
during drainage survive the update. The latest binaries remain v0.22.0.

Remaining local coverage is tracked in docs/BACKLOG.md: SQS filters, batching
windows and larger batches, plus SNS admission retry, Lambda async destinations,
and local HTTP/S confirmation/delivery with effective policies. SNS uses push
subscriptions; FIFO SNS reaches Lambda through SQS. CloudWatch, IAM/KMS and
AWS-managed scaling are outside the requested follow-up.
