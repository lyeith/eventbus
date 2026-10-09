# Performance audit: startup, capture, messaging and routing

For the remaining targets on v0.12.0, read the [second audit round](PERFORMANCE-AUDIT-ROUND-2.md).

8 October 2026. This continues the [runtime and launcher review](PERFORMANCE-REVIEW.md).
EventBus baseline was `c977806`, plus the opt-in measurement fixtures.
After measurements used the same fixtures with the package fixes described here.
Final integrated source is recorded with verification below.
The fixes retain native API payloads, cryptographic costs, capture durability,
queue ordering, acceptance and actual process joins. No dependencies were added.

## Measured changes

All times below are milliseconds, except the allocation row. These are local
fixture medians; they do not establish production percentiles or application
business outcomes.

| Path | Before | After | Responsible change |
| --- | ---: | ---: | --- |
| Cognito empty bootstrap | 152.772 | 33.553 | One transaction over the unchanged startup SQL |
| First tiny Cognito seed | 204.766 | 135.574 | Skip the second bcrypt comparison after successful creation |
| Lambda DescribeTarget during durable async capture, four producers | 11.120 | 0.003692 | Capture transitions have a separate owner from shared service state |
| FIFO native receive, 10,000 waiting messages | 1.266 | 0.158 | Sort after actual visibility returns; compact receive state in place |
| FIFO Lambda receive, same queue | 1.546 | 0.349 | Same queue owner; admission and wire projection unchanged |
| FIFO native ten-delete batch, same queue | 5.452 | 1.184 | Each delete no longer sorts an already ordered waiting queue |
| SNS publish to 100 queues, capture disabled | 1.162 | 0.609 | Encode a notification once per selected protocol message |
| SNS publish to 100 queues, durable capture | 6.172 | 4.949 | Same encoding change; per-record Sync preserved |
| Lambda log projection, private capture disabled / 64KiB merged tail | 65,536 bytes/op, one allocation | 0 bytes/op, zero allocations | Read the byte count without copying the tail |

Fresh seed timings include real random RSA generation, so the observed difference
does not isolate bcrypt. The component fixture measured bcrypt generation at
55.278ms and comparison at 58.017ms before the fix; the fresh-create branch now
omits only the latter. Existing-user comparison remains required.

[Gateway routing](../internal/gateway/routes.go) now compares literal paths
directly, splits a parameterized request once across route candidates, and
allocates captures only after the full match succeeds. Native literal-slash,
method/ANY, full-before-greedy, default and base-path precedence remain unchanged.

[Parsed authorizer policies](../internal/gateway/authorizer.go) own immutable IAM
matchers; compilation happens once at response parse instead of every cache hit.
Every hit still evaluates the current method ARN and explicit Deny. Action case,
resource case, Unicode question marks, anchored literals and wildcard grammar
remain unchanged. The cache also avoids evicting an unrelated identity when two
concurrent misses publish the same new key.

| Gateway path, median ms | Before | After |
| --- | ---: | ---: |
| Last literal among 1,000 routes | 0.225444 | 0.064969 |
| Miss among 1,000 routes | 0.142260 | 0.014339 |
| Exact-policy hit with 4,096 cached identities | 0.026808 | 0.001418 |
| Wildcard-policy hit with 4,096 cached identities | 0.024467 | 0.001477 |

First-literal median at 1,000 routes was 0.034449ms before and 0.036924ms after:
ordinary handler/mapping bookkeeping remains. Full-cache new-identity misses
still include expiry scanning and native invoke/parse; no cache-index speedup
is claimed.

## Owners and preserved contracts

[Cognito bootstrap](../internal/cognito/store.go) owns one transaction through
CREATE/ALTER statements, [identity repair](../internal/cognito/lifecycle_store.go)
and [verification table setup](../internal/cognito/verification_store.go).
It publishes the store only after commit and rolls back on failure. SQL,
duplicate-column tolerance, five-second startup context, WAL/durability settings
and the single connection are unchanged. Real legacy-fixture tests force a late
unique-index failure, verify original schema/rows/key bytes, repair the owned
fixture and reopen successfully twice.

The separate [seed adapter](../internal/cognito/dev_seed_store.go) treats successful
creation as proof that the requested bcrypt/SRP credentials were persisted.
Concurrent-create losers still load and compare the existing credential.
Existing unchanged passwords preserve lifecycle/version/grants and backfill
missing SRP credentials through the existing guarded update. Changed passwords
still pass through the shared password owner and invalidate challenge sessions.

[Lambda async](../internal/lambda/async.go) owns serialized capture transitions,
with lock order `asyncTransitionMu` then `Service.mu`. Append/Sync does not hold
the shared mutex. Pending admission reserves capacity and retained activity but
cannot execute, appear as a queued snapshot or return native acceptance until
queued capture succeeds. A reservation before healthy Close remains accepted;
irreversible abort captures cancellation and never launches that reservation.
Workers, pending admissions, actual children and terminal capture join before
drain/Close completes. Capture failure remains sticky and prevents healthy
retained proof. Shutdown can fence and cancel other native children while a
writer remains blocked.

The async change improves unrelated service responsiveness, not capture
throughput. The four-producer durable batch of 20 events took 251.653ms before
and 263.462ms after; admission median was 23.662ms before and 29.997ms after.
Both retained three durable records per successful Event. The shared sink still
serializes each write and Sync. Controlled queued/running/terminal writer gates
changed shared-mutex availability from false to true in all three cases; retained
quiescence still refused safe hold/resume until actual completion.

[Lambda log projection](../internal/lambda/process.go) uses a locked count accessor.
The existing merged native tail and optional stdout/stderr snapshots keep their
limits and counts. After 1MiB output, private capture enabled / 64KiB merged tail
fell from 131,073 to 65,536 bytes/op, and two to one allocations. Private capture
enabled / 4KiB merged tail fell from 69,632 to 65,536 bytes/op. Its measured CPU
time varied; the allocation reduction is the supported conclusion.

[SQS](../internal/messaging/sqs_queue.go) owns waiting order, visibility, leases and
detached receive snapshots. Sends/transfers assign increasing destination-owned
sequence numbers; actual visibility returns require sorting older in-flight
entries back into order. Receive compacts the queue-owned slice under its mutex,
retains delayed/byte-rejected fronts, stops scanning when the batch is full and
clears unused backing pointers. Standard fair-group ordering, FIFO group blocking,
attempt replay, redrive, stale receipt rejection and mapping custody remain native
owner behavior. The regressions cover numeric order restoration, old receipts,
fairness, transfers and detached snapshots.

[SNS](../internal/messaging/sns_delivery.go) caches immutable envelope strings only
within one publication, keyed by final protocol-selected message text.
Eligibility/filter gates precede encoding; raw delivery uses the original text.
Publication-wide attributes/IDs/time/subject/sequence/replay fields remain shared.
Lambda still builds its own subscription-specific event wrapper; protocol JSON
still strips delivered attributes, while request capture retains the originals.
Topic serialization, durable intent, archive/FIFO/dedup state, delivery admissions,
shared delivery deadline and redrive remain unchanged.

Gateway route/policy ownership is described in the measured routing section
above. No routing index, expiry heap, unbounded regex cache or runtime reuse was
introduced.

## Method and limits

The parent ran one serial SSD command lane: `GOMAXPROCS=4`, `GOFLAGS=-p=2`,
Go `-p 2`, and existing `ssd-dev operation --purpose test` ownership.
Measurements were non-race; race correctness checks were separate.
The host/interpreters are recorded in the [earlier method](PERFORMANCE-REVIEW.md#method-and-environment).
Private fixture files were on the owned XFS `/var/lib/ssd-dev/data` path.
No default/application DB, live stack or application seed was opened.

Cases ran before then after in fixed order, without randomized repetitions,
cache flushing or a saturation workload. First is the first indexed sample,
including producer/worker input order for concurrent cases. It is not necessarily
the earliest completion or a cold sample; some fixtures verify/setup outside timing.
Counts include that sample. Five fresh DBs were used for each startup
phase; RSA generation, bcrypt, SRP and new-key cases use five samples;
warm PEM/key/JWT/JWKS cases use 20. Timing variation and random RSA work
limit causal speedup claims beyond the source-backed mechanisms and controlled
lock/allocation evidence.

- [Cognito fixture](../internal/cognito/performance_review_test.go): empty/reopened
  private stores; one-pool/client/user YAML; first/unchanged seed; real bcrypt,
  SRP, RSA, PEM, persisted key SELECT, JWKS, sign/verify. Fixed 40-operation
  one-/four-worker cases report actual DB waits and joins. Startup was remeasured;
  unchanged crypto/JWT components were not rerun.
- [Async fixture](../internal/lambda/performance_async_test.go): 20 real command
  Events per capture/producer case, four workers; original native RequestIDs/PIDs,
  every actual child/group join and native history are verified. Durable cases
  also verify three ordered captured records per successful Event.
  The DescribeTarget sampler stops at batch completion: durable four-producer baseline
  has 19 metadata samples, after has 20. Controlled gates are single observations;
  their old approximately 20ms delay is the configured quiescence wait.
- [Log benchmark](../internal/lambda/performance_log_benchmark_test.go):
  joined-writer projection after 1MiB stdout, 4/64KiB merged tails, private capture
  on/off; 100ms benchmark duration, setup excluded, result escapes.
  This isolates allocation/projection, not process launch or fsync.
- [Messaging fixture](../internal/messaging/performance_review_test.go):
  100/1,000/10,000 waiting messages, standard/FIFO, 100 FIFO groups, 1KiB bodies,
  ten attributes including binary. Twenty native and twenty Lambda ten-message
  receives per case; real sends restore exact depth outside receive timing.
  Native receive times ReceiveMessagesContext without HTTP or event projection;
  Lambda receive includes byte admission and event projection. Native projection
  plus marshal and Lambda marshal are distinct separately timed scopes. Settlement
  times ten strict Broker.DeleteMessage calls or ten mapping receipt operations,
  not a single HTTP DeleteMessageBatch. Maintenance is timed separately.
  Receipt history grows by 400 per case. SNS has 20 publishes at 1/10/100 actual
  queue destinations, plus joined 40-publish four-worker same/separate-topic
  cases. Capture-disabled cases leave SNS capture unconfigured; durable cases
  verify the actual file. Real destination outcomes are verified in both.
- [Gateway fixture](../internal/gateway/performance_review_test.go):
  real ServeHTTP at 10/100/1,000 routes, literal first/middle/last/miss plus
  parameter/greedy/default precedence. Twenty ten-request batches. Route and
  cache-hit medians are medians of normalized batch averages (ten calls / ten),
  not individual request-latency medians. Cache miss cases use one call per sample.
  Cache tests cover exact/wildcard policies, zero/full occupancy, hit/current-ARN
  policy refusal, new/expired identity and all-expired state. Real owner validation,
  body closure, cap/expiry and cancellation apply; only downstream transport I/O
  is replaced. These are not HTTP-network or authorizer runtime benchmarks.
- [Shared report owner](../internal/testperf/report.go): all performance fixtures
  emit count/first/min/median/max without timing assertions. Ordinary verification
  does not execute performance-tag fixtures.

Selected distributions, all in milliseconds:

| Case | Run | Count | First | Min | Median | Max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Cognito empty bootstrap | before | 5 | 156.524 | 146.418 | 152.772 | 166.890 |
| Cognito empty bootstrap | after | 5 | 36.195 | 28.614 | 33.553 | 36.195 |
| Cognito first tiny seed | before | 5 | 180.970 | 167.510 | 204.766 | 398.435 |
| Cognito first tiny seed | after | 5 | 270.734 | 127.648 | 135.574 | 270.734 |
| Lambda DescribeTarget, durable capture / 4 producers | before | 19 | 16.663 | 3.121 | 11.120 | 24.091 |
| Lambda DescribeTarget, durable capture / 4 producers | after | 20 | 0.005 | 0.003 | 0.004 | 0.005 |
| SQS FIFO depth 10,000 / native receive | before | 20 | 1.217 | 1.217 | 1.266 | 1.379 |
| SQS FIFO depth 10,000 / native receive | after | 20 | 0.161 | 0.137 | 0.158 | 0.944 |
| SQS FIFO depth 10,000 / Lambda receive | before | 20 | 1.421 | 1.421 | 1.546 | 1.766 |
| SQS FIFO depth 10,000 / Lambda receive | after | 20 | 0.356 | 0.316 | 0.349 | 0.485 |
| SQS FIFO depth 10,000 / native ten-delete batch | before | 20 | 5.559 | 5.066 | 5.452 | 10.855 |
| SQS FIFO depth 10,000 / native ten-delete batch | after | 20 | 1.141 | 1.092 | 1.184 | 1.854 |
| SNS 100 subscribers / capture disabled | before | 20 | 1.144 | 1.033 | 1.162 | 1.385 |
| SNS 100 subscribers / capture disabled | after | 20 | 0.507 | 0.485 | 0.609 | 1.412 |
| SNS 100 subscribers / durable capture | before | 20 | 7.280 | 5.073 | 6.172 | 8.755 |
| SNS 100 subscribers / durable capture | after | 20 | 5.624 | 4.371 | 4.949 | 5.704 |
| Gateway 1,000 routes / last literal | before | 20 | 0.251289 | 0.144764 | 0.225444 | 0.587462 |
| Gateway 1,000 routes / last literal | after | 20 | 0.097247 | 0.023472 | 0.064969 | 0.099220 |
| Gateway 1,000 routes / miss | before | 20 | 0.124296 | 0.100931 | 0.142260 | 0.213531 |
| Gateway 1,000 routes / miss | after | 20 | 0.016578 | 0.012990 | 0.014339 | 0.047112 |
| Authorizer exact-policy hit / full cache | before | 20 | 0.028059 | 0.022170 | 0.026808 | 0.112173 |
| Authorizer exact-policy hit / full cache | after | 20 | 0.007852 | 0.001082 | 0.001418 | 0.007852 |
| Authorizer wildcard-policy hit / full cache | before | 20 | 0.028005 | 0.021974 | 0.024467 | 0.069654 |
| Authorizer wildcard-policy hit / full cache | after | 20 | 0.001663 | 0.001069 | 0.001477 | 0.005370 |


## Verification

PASS in the same serial SSD lane:

- Cognito full non-race suite: 42.474s. Focused bootstrap/seed/legacy identity,
  persisted key/credentials and workflow verification race suite: 35.022s.
- Full Lambda race: 96.269s; capture: 1.202s; localexec: 2.076s.
- Full messaging race: 5.594s; gateway race: 20.301s.
- Affected consumer race suites: eventsource 1.593s, app 19.444s,
  server 2.732s, devquiescence 1.052s.
- Ten real Python/Node SDK and retained-stack cases, `sdksmoke,integration`,
  with no selected-case skips: 118.583s. These cover empty Cognito provision/
  restart/token admission; SQS/SNS; Event acceptance/capture; native queue batches,
  FIFO, visibility/retry/settlement; retained proof/resume; authenticated gateway
  continuations; and actual owned RustFS delivery/cleanup.
- Vet across all packages with `sdksmoke,integration,performance` tags: PASS.
- Before/after performance matrices, log allocation benchmark and diff checks: PASS.

Final integrated runtime/fixture source: [5186da3](https://github.com/lyeith/eventbus/commit/5186da354e8053e6e2b4f867b45cb4e987432ef9).
The preceding seven commits separate reporting and each affected service owner.

The first full Cognito race attempt used an insufficient four-minute package
budget. It timed out in `TestChangePassword_AttemptLimit` after that test had
run for one second; its stack was computing bcrypt. No race warning or assertion
failure was reported before timeout. The complete non-race Cognito suite and
focused race startup/seed/legacy/restart/key/verification checks subsequently
passed. The entire Cognito race suite remains uncompleted in this audit.

The initial gateway mixed-route fixture incorrectly expected greedy selection
for `/docs/assets/` despite the full `/docs/{id}/` route. The fixture expectation
was corrected to existing native full-route precedence before production edits.
Only the failed mixed case was rerun; the unchanged literal/cache baseline was
retained.

Published [v0.11.3](https://github.com/lyeith/eventbus/releases/tag/v0.11.3)
contains eight binaries from clean tagged
[0ac08e2](https://github.com/lyeith/eventbus/commit/0ac08e2a53f88d58262e1b3b9bc94724fec530a5),
Go 1.26.0, CGO_ENABLED=0 and module version v0.11.3. Every uploaded binary was
downloaded and verified against the build's SHA256SUMS.

The actual packaged Linux amd64 and macOS arm64 EventBus/gateway binaries passed
private Cognito seed/reopen and persisted JWKS; two-queue SNS envelope fanout;
five async Event executions correlated through native IDs, three transition
records per execution and actual child absence; a 1,000-literal-route gateway;
and joined server shutdown. Linux arm64 and macOS amd64 were cross-built and
metadata-checked but not executed.

The initial packaged fixture incorrectly equated the HTTP API request ID with
the native execution ID. The corrected fixture separately validates HTTP IDs and
correlates capture to the IDs recorded by actual children. The service contract
was unchanged.

## Reproduce

Run each command serially, saving full output before filtering. Frozen SDK setup
and required environment variables are in [SDK verification](../tests/sdk/README.md).
No new test dependency is required.

```sh
set -o pipefail
env GOMAXPROCS=4 GOFLAGS=-p=2 ssd-dev operation --purpose test -- \
  go test -p 2 -count=1 -tags performance -run '^TestPerformanceReviewCognito' -v ./internal/cognito \
  2>&1 | tee /tmp/eventbus-audit-cognito.log | tail -30
env GOMAXPROCS=4 GOFLAGS=-p=2 ssd-dev operation --purpose test -- \
  go test -p 2 -count=1 -tags performance -run '^TestPerformanceAuditLambdaAsync' -v ./internal/lambda \
  2>&1 | tee /tmp/eventbus-audit-async.log | tail -30
env GOMAXPROCS=4 GOFLAGS=-p=2 ssd-dev operation --purpose test -- \
  go test -p 2 -count=1 -tags performance -run '^$' -bench '^BenchmarkPerformanceLogDiagnostics$' -benchmem -benchtime 100ms ./internal/lambda \
  2>&1 | tee /tmp/eventbus-audit-log.log | tail -15
env GOMAXPROCS=4 GOFLAGS=-p=2 ssd-dev operation --purpose test -- \
  go test -p 2 -count=1 -tags performance -run '^TestPerformanceReview(SQSDepth|SNSFanout|SNSContention)$' -v ./internal/messaging \
  2>&1 | tee /tmp/eventbus-audit-messaging.log | tail -30
env GOMAXPROCS=4 GOFLAGS=-p=2 ssd-dev operation --purpose test -- \
  go test -p 2 -count=1 -tags performance -run '^TestPerformanceReviewGateway' -v ./internal/gateway \
  2>&1 | tee /tmp/eventbus-audit-gateway.log | tail -30
```

Saved SSD evidence is `/tmp/eventbus-audit-*-20261008.log` and
`/tmp/eventbus-v0.11.3-*-20261008.log`, including corrected fixture baselines,
failed attempts and packaged/release checksum results. Existing evidence retention
is 24 hours. Successful fixtures close services, databases, writers, listeners
and actual native child groups before removing owned temporary state. Release
stages, probes and downloaded duplicates were removed on both hosts. Failed
operations are verified quiescent/unpinned and retain diagnostic scratch under
the same finite host policy; their expiry is recorded in STATE/HANDOFF.

## Follow-up fixes and measurements

9 October 2026. The bounded ownership/performance follow-ups start at
`b69f543`. Before measurements use a temporary Git archive of that source;
the same frozen opt-in fixtures run against the final implementation. All
measurements use one serial SSD lane, GOMAXPROCS=4 and GOFLAGS=-p=2, without
the race detector. Setup is excluded unless stated. These are local fixture
observations, not application/cloud throughput guarantees.

### Owners and corrected behavior

- Cognito `mfa_store.go` owns atomic enrollment and preference transitions.
  Promotion requires the exact verified pending secret, current account revision,
  enabled self-service user and unrevoked grant. Replacement, consumption,
  deletion and authorization changes refuse without publishing another factor.
- Consumer uses the contextual queue port; ordinary handler failures retain
  retries/dead letters. Cleanup uncertainty fences new launches, cancels peers,
  joins actual children and stays an error through Wait. App joins after caller
  deadlines and retains SQLite/capture dependencies on uncertainty.
- `localexec.TrackedOutput` owns output-copy evidence independently of Go's
  command exit error. Nonzero exits/cancellation can mask ErrWaitDelay; all
  consumer/Lambda/trigger runners now retain that uncertainty while preserving
  their native result and deadline policies. Failed Start creates no copy evidence.
- SQS clears removed backing slots during retention, replay restoration and
  transfers. Shared dedup insertion tracks a conservative earliest expiry; current
  receipt deletion avoids unrelated scans while native receive/maintenance owns
  expiry/redrive. Shared fields/MD5 stay in messaging, with distinct HTTP selection
  and Lambda encoding. Lambda receive counts leases without discarded snapshots.
- Firehose streams own prepared jq with fresh per-record execution. The existing
  flush slot owns immutable buffer snapshots and retry-object construction outside
  the stream lock. Captured records remain quota-counted during construction;
  publication moves only the captured prefix, preserving concurrent appends.
  Counters decrease only after successful destination body cleanup.
- Gateway owns immutable compiled redactions/mapping plans and fresh request
  output. SecretsStore indexes exact canonical ARNs and names to one state;
  rotation binds that ARN before external validation, so delete/recreate cannot
  mutate a replacement generation.
- Lambda HTTP reuses native invocation preparation/version helpers. Scheduler's
  adapter owns unsupported group-management refusal; server forwards through its
  HTTP port.
- `tests/architecture_test.go` enforces production import classes across every
  platform/build tag. PR/main CI runs it with unit races/vet; SDK checks run their
  own package plus the unchanged Express/Swagger handler proof. Python and Node
  are explicitly provisioned in both appropriate lanes.

### Fixed-workload results

Times are milliseconds. Each cell is first / minimum / median / maximum.
Counts apply separately to before and after.

| Fixture | Count | Before | After |
| --- | ---: | --- | --- |
| FIFO 10k: unique send | 20 | 0.154595 / 0.136450 / 0.146274 / 0.176948 | 0.018936 / 0.013976 / 0.016211 / 0.068270 |
| FIFO 10k: duplicate send | 20 | 0.017764 / 0.012564 / 0.014006 / 0.020449 | 0.014207 / 0.011381 / 0.013316 / 0.015650 |
| FIFO 10k: ten strict deletes | 20 | 1.536042 / 0.997233 / 1.096823 / 1.588361 | 0.004228 / 0.002004 / 0.002540 / 0.005300 |
| FIFO 10k: native ten-delete batch | 20 | 1.257299 / 1.027190 / 1.115309 / 1.691868 | 0.009247 / 0.005921 / 0.007333 / 0.020549 |
| FIFO 10k: unexpired prune | 20 | 0.162400 / 0.113085 / 0.128817 / 0.274194 | 0.157260 / 0.089571 / 0.101133 / 0.157260 |
| Lambda receive: ten 64KiB binary attributes | 20 | 4.047804 / 1.647864 / 3.343621 / 5.987327 | 3.354090 / 1.350707 / 3.347132 / 6.244785 |
| Firehose 500 records: simple metadata | 20 | 8.866518 / 5.349311 / 6.728909 / 8.866518 | 6.694474 / 4.396920 / 5.033840 / 6.694474 |
| Firehose 500 records: compound metadata | 20 | 18.651842 / 15.365219 / 17.818973 / 24.628236 | 7.844780 / 6.495887 / 7.810820 / 9.945820 |
| Firehose 500 admissions: no pending objects | 20 | 6.098317 / 4.479678 / 6.068991 / 7.899705 | 3.706584 / 2.957013 / 4.122812 / 5.407581 |
| Firehose 500 admissions: 10k pending objects | 20 | 11.968468 / 11.189662 / 11.798554 / 14.841492 | 3.866839 / 2.908068 / 3.663330 / 5.843241 |
| Firehose admission during 32MiB GZIP | 5 | 599.451652 / 531.111169 / 553.900935 / 599.451652 | 0.023014 / 0.023014 / 0.032993 / 0.042191 |
| Firehose 32MiB build + bounded transport read | 5 | 641.225604 / 571.888018 / 594.447704 / 641.225604 | 634.355235 / 598.284826 / 634.355235 / 712.655153 |
| Gateway 100 redactions / 12 mappings | 20 | 0.105638 / 0.105638 / 0.127608 / 0.206012 | 0.055586 / 0.046065 / 0.058293 / 0.129470 |
| Secrets 10k: canonical ARN GetValue | 20 | 0.130255 / 0.098890 / 0.116544 / 0.133276 | 0.001820 / 0.000672 / 0.001143 / 0.002026 |
| Secrets 10k: missing canonical ARN | 20 | 0.111741 / 0.111178 / 0.114979 / 0.123843 | 0.000095 / 0.000078 / 0.000098 / 0.000626 |

FIFO 10k setup, including enqueue work, was one observation: 917.421→231.092ms.
The due dedup sweep remains linear (0.714→0.650ms, one observation); retention
pruning remains linear too. Duplicate send behavior is similar. Current deletion
improves responsiveness without claiming unrelated expired records already moved.

Lambda binary projection has no demonstrated wall-time improvement in this run:
3.344→3.347ms median. Observed allocations fell from 988 to 886.5 per batch;
bytes from 6,848,784 to 6,174,472. Runtime MemStats/JSON pools/GC vary between
samples; these are observations, not a zero-allocation or exact heap claim.
The removed second snapshot is covered separately by lease/detachment regressions.

Gateway 100-redaction allocation observations fell from 636 to 128 per operation
and 85,448 to 44,448 bytes. Twenty samples each normalize ten full ServeHTTP calls
with 12 mappings and bounded transport; correctness/borrowed-state assertions
run outside the timed loop. Static route precedence/privacy remains intact.

Secrets samples normalize 100 GetValue operations, excluding resource creation.
The 10k name lookup remains constant-time (0.000604→0.001694ms median in this
run); existing snapshot allocations remain 720B/five allocations per successful
lookup. Only canonical ARN resolution changes from a state-wide scan to an index.

GZIP no longer blocks admission, but build/read time remains about 0.6 seconds
and did not improve. Records remain fully charged during the unlocked build,
with cancellation/failure rollback, concurrent same-group admission, 100k quota
through body Close, retry replacement, deletion and retained-abort join tests.

### Representative cold application chains

Frozen fixtures use the existing Python 3.12.11 virtual environment,
boto3 1.40.61/botocore 1.40.76 and Node 22.22.1 with SDK 3.1146.0.
Interpreter aliases must resolve to the same executable and virtual-environment
prefix. No dependency installation, warm pool or production interpreter change
is part of these observations.

Before consumer: five cold batches of five native SQS records, actual boto3
sends, SQLite effects/commit, receipt settlement and child join. Median total
1524.894ms (1276.460..1940.912); launch-to-module 1222.209ms,
SDK import 136.954ms, client preparation 68.161ms, handler work 54.984ms.
Phase medians do not add to total median.

Before registered trigger chain: five actual five-invocation
Define/Define/Create/Verify/Define chains with SES capture.
Median joined chain 1239.375ms (959.951..1472.490);
25 native invocations median 225.447ms (129.775..563.466).
Create imports the actual SES SDK; the other real policy modules are smaller.
Every timed operation checks actual direct child reaping and closes its runner.

Before native SDK: three unchanged SRP/email/token chains each make three
Cognito HTTP calls and five registered trigger invocations, capture an email
and independently verify JWTs. Median native HTTP 1003.959ms
(993.229..1066.043); full auth flow 1041.152ms
(1027.124..1100.786), excluding separate token-validation timing.
One whole driver/provisioning/import/three-flow/join observation 3764.681ms.

After runs use the same frozen fixtures and preserve interpreter/environment
identity. Consumer total median 1202.372ms (1046.374..1340.767),
five-trigger chain 895.680ms (884.609..907.463), and native auth HTTP
819.230ms (812.020..856.509); full auth flow 851.585ms
(843.228..890.315). These changes are not attributed to a runtime speedup:
fresh-process policy is unchanged, phase sampling and OS/host load differ.

An additional attribution run selects installed native uv inside the same owned
operation. The five-record workload, interpreter, virtual-environment prefix,
uv frozen/offline/no-sync options, effects and joins remain identical. Median
joined batch is 247.286ms (240.120..257.175); launch-to-module 25.994ms
(25.312..26.524), versus managed after-run launch median 935.482ms.
PATH selects the launcher and is also inherited by the child, so this is a
launcher attribution comparison, not a byte-identical environment benchmark
or a production bypass. Native uv must not replace host ownership checks.

The managed project-mode launcher performs repeated workspace/version/digest
and ownership proofs, including hashing its 60 MB uv binary several times.
That responsibility belongs to ssd-dev-tools; consolidating those proofs needs
its own correctness checks. No EventBus child/environment selection changed.

Cold measurements identify fresh interpreter/import work, not an established
bounded EventBus algorithm defect. SSD's project-mode uv launcher also performs
workspace/version/digest and repeated ownership checks; that tooling owner needs
separate attribution before changing its safety/selection behavior.
Warm native worker issue [#30](https://github.com/lyeith/eventbus/issues/30)
is a separate runtime-lifetime/state-sharing feature.

### Verification and reproduction

Full Cognito race passed 295.735s, closing the earlier 4m audit gap.
Scoped initial races passed consumer/app, eventsource, gateway, Lambda,
Scheduler and dispatcher; final messaging/Firehose/architecture/Secrets races
passed 6.012/7.070/1.045/1.409s. Both native Firehose integration proofs passed
against a fresh owned RustFS, including SNS filtering/partition/GZIP/errors/
recovery/final drain; RustFS child/group joined and private data was removed.
Full SDK/retained-stack race passed 278.385s; unchanged Express/Swagger passed
9.179s. The optional older-SDK lane initially skipped without an explicit
environment, then passed separately in 25.023s with unmodified
boto3/botocore 1.39.4, including batch/concurrency/retry/teardown proofs.
That isolated environment was removed by its successful owner operation.
After-cold/attribution fixtures all passed. Final full runner races passed:
localexec 6.175s, consumer 12.222s, Cognito triggers 8.866s, Lambda 100.112s,
app 19.970s, devquiescence 1.039s and architecture 1.041s. All-package vet
with sdksmoke/integration/performance tags passed. macOS arm64 localexec race
passed 6.873s, including real retained pipes and merged stdout/stderr ordering.
Linux-only retained-descriptor fault injection is not executed on macOS.

Reproduce after frozen dependencies are provisioned, using the managed SSD lane:

```sh
env GOMAXPROCS=4 GOFLAGS=-p=2 ssd-dev operation --purpose test -- \
  go test -p 2 -count=1 -tags performance -run '^TestPerformanceSQSFollowup' -v ./internal/messaging
# Same lane, serially:
go test -p 2 -count=1 -tags performance -run '^TestPerformanceFollowupFirehose' -v ./internal/firehose
go test -p 2 -count=1 -tags performance -run '^TestPerformanceReviewGatewayStaticPlans$' -v ./internal/gateway
go test -p 2 -count=1 -tags performance -run '^TestPerformanceReviewSecretsCanonicalLookup$' -v ./internal/secrets
EVENTBUS_SMOKE_PYTHON="$PWD/.venv/bin/python" go test -p 2 -count=1 -tags performance \
  -run '^TestPerformance' -v ./internal/consumer ./internal/cognitotrigger
go test -p 2 -count=1 -tags sdksmoke,performance \
  -run '^TestPerformanceCognitoNativeSDKAuth$' -v ./tests/sdk
```

Save output before filtering; wrap every SSD command in its owned test operation.
Logs are SSD /tmp/eventbus-followup-*-20261009.log, existing 24h lifetime.
The initial SQS fixture compile/invalid dedup input failures were corrected
without relaxing native validation. The first cold command selected no test
packages; it contributed no samples. Consumer interpreter alias assertion failed
before its first observation; executable+environment identity checks replaced
string equality. Successful trigger observations were retained without repetition.


### Published artifacts

[v0.11.4](https://github.com/lyeith/eventbus/releases/tag/v0.11.4) contains eight
CGO-free EventBus/gateway binaries and SHA256SUMS from clean tagged source
4bac246, Go1.26.0, module v0.11.4. Downloaded assets match local checksums.
[Release-source CI](https://github.com/lyeith/eventbus/actions/runs/37908234098)
passed both complete unit-race/vet and scoped SDK/Swagger jobs.

Actual Linux amd64 and macOS arm64 binaries passed Cognito enrollment/MFA,
three native Node results with sequential merged log tails and reaped children,
Lambda REQUEST authorization, compiled HTTP mappings and healthy owner shutdown.
Other targets cross-built with verified metadata/checksums; not executed.
The packaged gateway probe initially used an unquoted YAML path. Its registered
owner was stopped and verified quiescent; corrected fixture/backend cleanup
passed without source changes. Private state and release/download duplicates
were removed. Subsequent #30–#32 runtime and native event work shipped in
[v0.12.0](https://github.com/lyeith/eventbus/releases/tag/v0.12.0); the
[warm SDK comparison](LAMBDA.md#measured-sdk-calls) records latency and retained RSS.
