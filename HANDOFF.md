# Handoff

Canonical SSD main; runtime/fixture source through 5186da3. Public MIT EventBus.
Published v0.11.3: clean tagged 0ac08e2, Go 1.26.0, CGO_ENABLED=0.
Eight EventBus/gateway binaries + SHA256SUMS; all downloaded hashes verified.
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
Local medians: empty bootstrap 153→34ms; async DescribeTarget 11ms→4µs;
FIFO 10k native receive 1.27→0.16ms; SNS 100 queues disabled 1.16→0.61ms;
gateway 1000-route miss 142→14µs; full-cache exact IAM hit 27→1.4µs.
Seed includes random RSA; durable throughput remains similar. Larger crypto/key
cache, queue dedup index, routing/cache structure and runtime reuse designs are
separate priorities, not required unfinished work. Board was empty at publication.

PASS saved SSD /tmp/eventbus-audit-*-20261008.log, existing 24h lifetime:
- Cognito full non-race 42.474s; focused bootstrap/seed/legacy/persistence race 35.022s.
- Lambda full race 96.269s / devcapture 1.202s / localexec 2.076s.
- Messaging full race 5.594s; gateway full race 20.301s.
- Eventsource 1.593s / app 19.444s / server 2.732s / devquiescence 1.052s races.
- Ten native Python/Node SDK + retained-stack cases 118.583s, no selected skips:
  provision/restart/JWT, SQS/SNS/Event, batches/FIFO/retries/settlement, evidence,
  retained resume, authenticated gateway callbacks and actual owned RustFS cleanup.
- Vet all packages with sdksmoke,integration,performance tags; matrices/diff checks.
Full Cognito race timed out at 4m during bcrypt; current test had run 1s.
No warning/assertion before timeout; complete package race remains a gap.
Initial mixed gateway fixture expected greedy over full parameter route; corrected
before production edits; only failed case reran, other baseline data retained.

Actual packaged Linux amd64/macOS arm64 seed/reopen/JWKS, two-queue SNS, five
Events/native execution IDs/terminal capture/actual child absence, 1000-route
gateway and joined shutdown passed. Other targets cross-built, not executed.
Initial packaged fixture wrongly equated HTTP request IDs with execution IDs;
corrected correlation to actual child IDs without changing service contracts.
Release source/metadata/checksum/package logs: /tmp/eventbus-v0.11.3-*-20261008.log.

No owned environments/worktrees/processes remain; fixtures closed/joined before
temp deletion. Both release stages/probes/downloaded duplicates were removed.
Failed operations 72b1f2fa/ae026ad1/7aee0bfe verified quiescent/unpinned, normal GC
expiry Oct9 14:15:15/14:22:51/14:57:04 UTC under host 24h policy.
Prior empty tooling scratch receipts retain Oct9 13:25/13:29 UTC expiry.

Oct9 read-only review at58d27a1; source-backed follow-ups in PERFORMANCE-AUDIT.md:
- Cognito can validate pending S1 then promote concurrently replaced S2.
- Consumer five-second polling lacks context; cleanup failures are retried and
  omitted from Wait, so app cannot retain ownership uncertainty.
- SQS expiry/replay/transfer shorten slices without clearing removed payload roots.
- Add import-boundary/PR checks; reuse Lambda preparation; move Scheduler refusal.
- Measure FIFO expiry scans, Firehose prepared jq/GZIP admission contention and
  real cold import chains; avoid unused Lambda receive snapshots.
No new measurements/regression executions/runtime changes; all findings need
focused verification when implemented. Existing common owners are sound.
