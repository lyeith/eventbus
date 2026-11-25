# EventBus state

Canonical SSD checkout /home/spite/Projects/eventbus, main.

[v0.23.0](https://github.com/lyeith/eventbus/releases/tag/v0.23.0) is public and
latest. Source 2a8b22bc2334590b22e4bf977719beebc2ea3604 passed CI 45557958852.
All eight Linux/macOS service and gateway binaries match SHA256SUMS from the
public release downloads. Temporary release and test resources are removed.

Local delivery coverage:
- SQS mapping Create/Get/List/Update/Delete and tags; filter-based removal,
  partial batch responses, 0–300s batching windows and batches up to 10,000.
  Exact 6 MiB admission, FIFO group bounds, original receipts and mapping/queue
  identity remain owned and joined. Provisioned polling uses fixed local workers.
- SNS admission retries for SQS/Lambda, subscription DLQ, stable IDs and FIFO
  retry order; opt-in loopback HTTP/S confirmation, delivery and effective policy.
- Lambda native async config for function/version/alias, age and retry limits,
  terminal records to local SQS/SNS/Lambda, selected compatible EventBridge, and
  S3 on failure. Owner drain joins accepted deliveries before resource teardown.

The local scope excludes AWS-managed scaling, CloudWatch, IAM/KMS and managed
signatures. HTTP/S endpoints require explicit loopback fixture selection.
EventBridge destinations use an explicitly configured compatible peer.
