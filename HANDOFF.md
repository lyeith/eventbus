# Handoff

Canonical SSD main; current runtime/fixture source5186da3. Public MIT EventBus.
Latest public v0.11.2; audited v0.11.3 binaries to build/publish next.
No Plans/application data or default Cognito DB touched; no dependencies added.

Owner commits:
- ffb9ad6 shared opt-in performance reporting; earlier reducers consolidated.
- 63f41cb Cognito bootstrap owns one unchanged-SQL transaction; seed omits only
  redundant bcrypt comparison after successful create. Legacy rollback/reopen,
  credential changes/concurrent creators and persisted RSA keys covered.
- 4bd6113 SQS sorts after actual visibility returns; receive compacts owned slice,
  retains delayed/rejected fronts and detached snapshots. FIFO/fairness/receipt/
  transfer/attempt/redrive and mapping custody contracts preserved.
- 17db939 SNS publication-local immutable envelopes keyed by protocol text.
  Filters/raw/attrs/capture/FIFO/replay and per-subscription Lambda wrappers retained.
- 810473d log byte counts avoid discarded merged-tail copies; stream tails preserved.
- 8efc82a asyncTransitionMu→Service.mu; Append/Sync holds only transition lock.
  Reservations count capacity/activity before capture but stay private/non-executable.
  Healthy Close honors prior reservation; abort captures canceled terminal/no child.
  Workers, pending admissions, native children and terminal capture all join.
  Capture interruption/failure remains dirty; global fence/cancel stays responsive.
- 5186da3 gateway literal fastpath/shared split, immutable parsed IAM matchers,
  current-ARN/Deny checks; same-key concurrent insertion avoids unrelated eviction.

docs/PERFORMANCE-AUDIT.md owns distributions, native scopes, reproduction and limits.
Local medians: empty bootstrap153→34ms; async DescribeTarget11ms→4µs;
FIFO10k native receive1.27→0.16ms; SNS100queues disabled1.16→0.61ms;
gateway1000route miss142→14µs; full-cache exact IAM hit27→1.4µs.
Seed numbers include random RSA; async durable throughput remains similar.
Crypto/key caches, queue dedup indexes, cache expiry/routing structures and runtime
reuse remain separate priorities, not required unfinished implementation.

PASS saved SSD /tmp/eventbus-audit-*-20261008.log, existing24h lifetime:
- Cognito fullnon42.474s; focused bootstrap/seed/legacy/persistence race35.022s.
- Lambda fullrace96.269s /devcapture1.202s /localexec2.076s.
- Messaging fullrace5.594s; gateway fullrace20.301s.
- Eventsource1.593s /app19.444s /server2.732s /devquiescence1.052s races.
- Ten native Python/Node SDK + retained-stack cases118.583s, no selected skips:
  provision/restart/JWT, SQS/SNS/Event, batches/FIFO/retries/settlement, evidence,
  retained resume, authenticated gateway callbacks and actual owned RustFS cleanup.
- Vet all packages with sdksmoke,integration,performance tags; matrices and diff checks.
Full Cognito race timed out4m during bcrypt; current test had run1s.
No race/assertion failure was reported before timeout; full package race is a gap.
Initial gateway mixed fixture expected greedy over full parameter route; corrected
before production edits and reran only failed case; other baseline data retained.

No owned environments/worktrees or active processes remain. Successful fixtures
close/join before temp deletion. Failed owned operations72b1f2fa/ae026ad1 verified
quiescent/unpinned, GC expiry Oct9 14:15:15/14:22:51UTC under host24h policy.
Raw logs share finite24h retention; no new retention/migration/cleanup machinery.
Prior empty failed-tooling scratch receipts keep Oct9 13:25/13:29UTC expiry.
