# Handoff

Released v0.23.0 on 2026-11-25:
https://github.com/lyeith/eventbus/releases/tag/v0.23.0
Exact tag source 2a8b22bc2334590b22e4bf977719beebc2ea3604.
https://github.com/lyeith/eventbus/actions/runs/45557958852

Changes:
- 2ed625b adds SQS mapping List, Update and tags, released here.
- aefc67d adds compiled Lambda filter criteria and windowed batch selection.
  Messaging deletes filtered records and preserves pre-admitted payloads,
  original leases, FIFO group caps and handler timeout headroom. Update keeps
  the original bound source and joins the prior generation.
- 35a5863 adds SNS retries and local webhooks through the existing SNS
  publication/capture owner, plus Lambda async policy and terminal destinations
  through the existing Lambda worker. App composes local fixture HTTP/S and
  destination adapters plus SNS/Lambda shutdown. Shared signed S3 PUT transport
  retains Firehose behavior and supplies Lambda failure object writes.
- 2a8b22b registers the S3 transport in the architecture guard. The draft was
  rebuilt and verified from this final commit before publication.

Verification:
- Full CI Go race/vet, frozen SDKs and Express/Swagger passed.
- Combined local SDK batch, partial failure, SNS webhook/async, and filter
  proofs are in /tmp/eventbus-v023-local-sdk.log and
  /tmp/eventbus-v023-filter-sdk.log. The first filter run had a fixture PASS
  marker mismatch, fixed before the final run. Scoped race/vet and final
  compatibility corrections also passed.
- Actual packaged Linux amd64 CLI passed real boto3 HTTP/S confirmations,
  stable retry IDs, effective policy, raw delivery, DLQ, Lambda async
  configuration/destinations and pending retry drain.
  /tmp/eventbus-v023-final-linux-package.log.
- Shared signed S3 path tested with VersityGW and boto3.
  /tmp/eventbus-s3delivery-versity-race.log.
- macOS arm64 service/gateway help passed on native hardware. Remaining
  two targets passed cross-build/format checks. All eight embed the clean
  source revision and CGO disabled. All nine public assets were downloaded
  and checked against the built manifest.
  /tmp/eventbus-v023-final-artifact-audit.log and
  /tmp/eventbus-v023-public-verification.log.

Owned release outputs, downloads, worktree, temporary .venv and test processes
removed. Host log retention applies. No live stack or cloud resources changed.
See docs/BACKLOG.md for the local boundary and managed AWS exclusions.
