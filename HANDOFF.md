# Handoff

Canonical SSD /home/spite/Projects/eventbus, main; public MIT EventBus.
Published v0.12.0: https://github.com/lyeith/eventbus/releases/tag/v0.12.0
Clean tagged source 953ca65; Go 1.26.0, CGO_ENABLED=0, module v0.12.0.
Eight service/gateway binaries + SHA256SUMS; all downloaded assets match builds.
All new tickets #30/#31/#32 implemented, pushed and closed; board is clear.

Owner commits:
- 9f508f6: registered GetFunction metadata and validated LocalStack S3 proof.
- 4f33bd7: bounded warm Python/Node workers, explicit local reload, completion
  scopes, worker leases, native messaging proof and measured SDK latency/RSS.
- b6f94cb: six v1 SES configuration-set/destination operations and native Send/
  explicit local Open/Bounce through the existing SNS delivery owner.
- 953ca65: agent contracts, common ownership and verification guides.

Warm success joins response/log/per-request RPC boundaries; a separate lease
owns the worker until error/timeout/reload/drain/Close joins process and children.
Global capacity counts retired generations and fresh fallbacks. Registry
snapshots are immutable; source reload refuses a concurrent registration change.
Exact Python primary-source loading preserves package import/re-export semantics
and avoids stale same-timestamp bytecode. Dependencies import normally.

SES separates management from durable send capture, snapshots Send routing and
retains bounded metadata-only correlations. Concurrent updates preserve resource
identity; delete/recreate is fenced. Accepted Send survives caller disconnect.
Downstream SNS admission failure cannot undo captured SES acceptance.
GetFunction shares Invoke selection; private paths/environment/code are omitted.

Independent integrated review accepted all ownership, contract and lifecycle
fixes: cancellation before launch, stale bytecode, exited-worker reuse, ordered
logs, stalled Runtime API body, truthful completion scope, SES cancellation,
bounded correlation expiry and concurrent destination updates.

Verification:
- Release-source CI passed both jobs; full unit race/vet, frozen Python/JS SDK
  checks and unchanged Express/Swagger:
  https://github.com/lyeith/eventbus/actions/runs/37941175215
  CI Lambda 103.356s / SES 11.294s / SDK 231.840s / Swagger 4.603s.
- Local full SDK race 277.526s, including actual warm SNS/SQS delivery/ACK/drain.
- Linux allWarm race 9.077s; macOS arm64 allWarm 5.206s.
- Local app 32.762s / eventsource 2.301s / architecture 1.115s races passed.
- Initial full Lambda run failed Node fallback fixtures before module markers;
  realistic deliberate phase margins/actual shared-budget assertions fixed the
  fixture only. Targeted fallback 17.145s and full CI Lambda now pass.
- SES core 13.721s; cancellation 1.018s; accepted bulk/concurrent updates 1.025s;
  unchanged SES/SNS/Lambda SDK proof 2.081s.
- Actual LocalStack 3.8.1 S3 race 13.134s: validation enabled, original Put/Copy/
  multipart/version/filter records, native retries, pending graceful drain.
- Vet with sdksmoke/integration/performance tags; five Python contracts.
- Actual packaged Linux amd64/macOS arm64 metadata, warm/reload, native SES/
  SNS/SQS Send/Open/Bounce and process shutdown. Other targets not executed.
- Performance fixture: 32 accepted SDK sends; eight calls per runtime/mode,
  including first. Python fresh/warm medians 216.078/8.565ms; Node 272.591/12.150ms.
  Idle RSS medians 46,610,432/97,937,408 bytes; no sustained stability claim.

Cleanup: exact S3 resources/containers and introduced 636MB image removed;
macOS archive/binary, release stage, probes and downloaded duplicates removed.
No owned process, temporary worktree, dependency or application/default DB change.
Task logs retained under receipt 4eaefff6, quiescent/unpinned, expire Oct10
13:43:54 UTC; S3 fixture failures 779b0793/20a07398 expire 13:26:28/13:27:22 UTC.
Successful SSD scratch removes automatically. Host launcher work is separate.
Preserve unrelated laptop Plans edits; no remaining work for these tickets.
