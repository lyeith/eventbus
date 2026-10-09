# Handoff

Canonical SSD /home/spite/Projects/eventbus, main; public MIT EventBus.
Published release remains v0.11.4 while the v0.12.0 candidate is prepared.
All new tickets #30/#31/#32 implemented; no unrelated Plans changes touched.

Owner commits:
- 9f508f6: registered GetFunction metadata and genuine validated LocalStack S3 proof.
- 4f33bd7: bounded warm Python/Node workers, local reload, completion scopes,
  worker-lifetime leases, native messaging proof and measured SDK latency/RSS.
- b6f94cb: six v1 configuration-set/destination operations and native Send/
  explicit local Open/Bounce through the existing SNS delivery owner.

Shared execution uses native Runtime API events/context/result policies.
Warm success joins response/log/per-request RPC boundaries, retaining the worker
under a separate lease; error/timeout/reload/drain/Close retires and joins it.
Global capacity includes retired generations and fresh fallbacks. Explicit recipe
reload is local; registry snapshots are immutable and source reload is CAS-bound.
Exact Python primary-source loading preserves normal package import/re-export
semantics and avoids stale same-timestamp bytecode. Dependencies import normally.

SES separates management from durable send capture, snapshots Send routing and
retains bounded metadata-only correlations. Concurrent updates preserve resource
identity; delete/recreate is fenced. Accepted events survive caller disconnect.
Downstream SNS admission failure cannot undo a captured SES acceptance.

Final independent review accepted ownership/seams/contracts; findings fixed:
cancellation before launch, stale bytecode, exited-worker reuse, ordered logs,
stalled admitted Runtime API body, truthful completion scope, SES acceptance
cancellation, bounded expiry and concurrent destination updates.

Saved /tmp/eventbus-tickets-*-20261009.log:
- Linux allWarm race9.077s; macOS arm64 allWarm5.206s.
- Affected first Lambda/app/server/architecture race passed; latest app32.762s,
  eventsource2.301s, architecture1.115s passed.
- Latest full Lambda failed only managed fallback timing fixtures under load;
  corrected deliberate phase margins/actual-budget assertions pass17.145s.
  Production deadlines unchanged. Unknown-scope ACK refusal race1.046s passed.
- SES full13.721s; cancellation1.018s; accepted bulk IDs/concurrent updates1.025s.
- Real SES/SNS/Lambda SDK2.081s; full SDK race277.526s, warm native messaging included.
- Real LocalStack3.8.1 S3 race13.134s: normal validation and original upstream
  Put/Copy/multipart/version/filter records, native retries, graceful drain.
- Vet with sdksmoke/integration/performance tags and five Python contracts passed.
- Performance fixture4.806s: 32 accepted SDK sends; eight calls per mode.
  Python fresh/warm median216.078/8.565ms; Node272.591/12.150ms.
  Warm idle RSS medians46,610,432/97,937,408 bytes; no long-run stability claim.

Next steps: eight clean-source binary builds; actual packaged Linux amd64/macOS
arm64 proof; push and CI; publish v0.12.0/check uploaded assets; prune owned scratch.
Other two targets receive cross-build metadata/checksum verification only.

Actual S3 fixture containers and introduced636MB image removed; exact buckets/
objects/multipart/notifications cleaned. No dependency/default DB/live stack
changes or temporary worktrees. macOS scratch is retained for packaged checks.
Failed evidence779b0793/20a07398/4eaefff6 quiescent/unpinned, 24h expiry.
Successful SSD operations remove scratch automatically. Host launcher optimization
is separate from this runtime increment. Preserve unrelated laptop Plans edits.
