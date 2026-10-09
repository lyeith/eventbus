# EventBus performance review

The subsequent [warm-worker comparison](LAMBDA.md#measured-sdk-calls) measures
real Python/Node SDK reuse, latency and retained memory for #30.

The [general audit](PERFORMANCE-AUDIT.md) continues the candidates below with
measured Cognito, Lambda async/log, SQS/SNS and gateway fixes and current priorities.

The [launcher follow-up](#launcher-fix-and-follow-up-8-october-2026) records the
SSD tooling fix: managed startup median 1154ms → 349ms; native UV Init 1161ms → 389ms.

2026-10-08. The controlled no-op fixtures measured median Init of **29.675ms**
for direct Python and **1160.549ms** for the managed UV recipe using the same
frozen interpreter, version and environment prefix. The difference occurred
before handler readiness. This identifies the configured launcher path as the
first measured investigation target; it does not isolate host policy from
native UV work or explain a particular application's imports.

Both non-race packages passed: Lambda **47.402s**, capture **0.168s**.
There were 180 successful Lambda invocations and 80 successful capture appends.
Every Lambda child/launcher group and provided-runtime listener passed its
joined closure checks. All 20 enabled snapshots captured the actual delayed
Python handler. No runtime optimization, application configuration or data
change was made.

This review is separate from [#28 Init/Invoke accounting](https://github.com/lyeith/eventbus/issues/28).
Source investigation began at `a1a3aad`; measurements used base `6e53ceb`
plus the frozen #28 working-tree implementation, before the final owner-only
JSON-schema extraction. Final source projects those unchanged phase facts
into a private schema using at most two records; those structural edits were
not remeasured. Measurements also predate the later Darwin-only EPERM
cleanup reconciliation; that fault path was not benchmarked. The phase implementation gives
ordinary Init 10s, starts the configured Invoke budget at readiness, and permits
one fresh-process fallback with a shared configured Init/Invoke budget.
AWS documents these distinct phases and the retried shared budget in its
[lifecycle specification](https://docs.aws.amazon.com/lambda/latest/dg/lambda-runtime-environment.html).
Correct accounting does not make initialization faster.

## Method and environment

The parent granted one serial lane, with `GOMAXPROCS=4`, `GOFLAGS=-p=2`,
Go `-p 2`, and the existing `ssd-dev` test resource owner. Packages ran
sequentially without race instrumentation. Each case has **20 total samples,
including index 0**; “First” below is that same sample, not an excluded warm-up.
Cases ran in a fixed order without cache flushing or randomized repetitions.
These observations do not support a production p99 or a causal speedup claim.

Measured host: Linux 7.0.0-31-generic x86_64, Microsoft virtual machine,
AMD Ryzen 7 7730U, 12 logical / 6 physical cores. Go 1.26.0, Node 22.22.1,
Python 3.12.11, UV 0.11.17. The frozen Python path was
`/home/spite/Projects/eventbus/.venv/bin/python`.
Within both owned test operations, `findmnt -T "$TMPDIR"` reported **XFS**
at `/var/lib/ssd-dev/data` with `noatime,prjquota`; source checkout ext4
is a different filesystem and is not the capture measurement filesystem.

[Lambda fixtures](../internal/lambda/performance_review_test.go) call the
private native invoke engine and retain actual RequestID, PID and phase data.
Init is process launch through readiness. Invoke includes execution plus native
process/result-reader/listener joins, not isolated application CPU time.
Joined wall surrounds the native invoke call, including optional diagnostic
work; it excludes HTTP/gateway transport and admission queue time.
Command has no readiness phase, so its entire joined process lifetime is Invoke.

Each call uses a fresh process. Python imports its real fixture module; Node
uses its managed wrapper and dynamic import. Provided and command fixtures are
small Go test-binary children, so their costs are not language or production
runtime comparisons. The provided fixture uses the native loopback Runtime API;
the command fixture writes its response to stdout without that API.

## Runtime measurements

| Case | Metric (ms) | First | Min | Median | Max |
| --- | --- | ---: | ---: | ---: | ---: |
| python_direct | Init | 29.459 | 28.042 | 29.675 | 32.908 |
| python_direct | Invoke | 1.629 | 1.531 | 1.633 | 1.876 |
| python_direct | Joined wall | 31.110 | 29.690 | 31.300 | 34.568 |
| python_managed_uv | Init | 1169.999 | 1123.391 | 1160.549 | 1242.363 |
| python_managed_uv | Invoke | 3.827 | 3.777 | 4.105 | 4.558 |
| python_managed_uv | Joined wall | 1173.847 | 1127.196 | 1164.572 | 1246.827 |
| node | Init | 104.147 | 102.611 | 106.700 | 117.838 |
| node | Invoke | 12.012 | 11.234 | 11.917 | 13.937 |
| node | Joined wall | 116.173 | 114.144 | 118.420 | 130.506 |
| provided | Init | 5.481 | 4.443 | 4.943 | 6.858 |
| provided | Invoke | 1.420 | 0.896 | 1.077 | 1.515 |
| provided | Joined wall | 6.913 | 5.462 | 5.974 | 8.056 |
| command | Init | 0.000 | 0.000 | 0.000 | 0.000 |
| command | Invoke | 4.592 | 3.942 | 4.208 | 4.592 |
| command | Joined wall | 4.606 | 3.949 | 4.216 | 4.606 |

Direct and UV samples returned the same canonical Python executable,
`sys.version` and `sys.prefix`. Resolved UV argv was:

```text
/home/spite/.local/lib/ssd-dev/entrypoints/uv run --offline --no-project --python /home/spite/Projects/eventbus/.venv/bin/python python
```

That entry point dispatches through
`ssd-dev-tools/tools/shell_entrypoints.py`, checks resource/environment/cache
bindings, then executes native UV. The direct fixture still runs inside the
approved test owner. The measured difference covers the complete additional
launcher path; it is not a separate measurement of individual controller checks,
UV resolution or interpreter imports. Do not bypass the resource owner to
reduce the number.

## Private diagnostic measurements

All three direct-Python cases ran the same 300ms handler sleep and emitted
1KiB each on stdout and stderr. Terminal mode wrote an owned private JSONL file.
Stack mode also requested a snapshot 200ms after process admission; every sample
contained its original RequestID/attempt and the actual handler frame.

| Case | Metric (ms) | First | Min | Median | Max |
| --- | --- | ---: | ---: | ---: | ---: |
| disabled | Init | 32.129 | 27.241 | 28.775 | 36.841 |
| disabled | Invoke | 302.387 | 301.837 | 302.168 | 302.920 |
| disabled | Joined wall | 334.529 | 329.607 | 330.945 | 339.083 |
| terminal | Init | 29.349 | 27.815 | 29.309 | 33.480 |
| terminal | Invoke | 301.836 | 301.766 | 301.956 | 302.464 |
| terminal | Joined wall | 340.769 | 333.927 | 335.338 | 340.769 |
| python_stack | Init | 49.282 | 49.282 | 52.150 | 57.217 |
| python_stack | Invoke | 302.431 | 302.230 | 302.547 | 303.122 |
| python_stack | Joined wall | 355.138 | 355.138 | 358.058 | 363.039 |

Median joined wall was 330.945ms disabled, 335.338ms terminal, and 358.058ms
with stacks. The terminal case's median per-sample wall minus Init minus Invoke
was 3.898ms; that remainder includes terminal construction/append and enclosing
bookkeeping. It is not a separately timed fsync. Stack work can overlap the
handler; subtracting case medians does not isolate every collector operation.

[Sink measurements](../internal/devcapture/performance_review_test.go) timed
JSON encoding, write and per-record Sync in `Append`, excluding open/close.
The owned files retained exactly 20 ordered records and mode 0600.
Borrowed `io.Discard` omits durability deliberately.

| Sink / data bytes | First (ms) | Min | Median | Max |
| --- | ---: | ---: | ---: | ---: |
| discard / 1024 | 0.038714 | 0.001282 | 0.001533 | 0.038714 |
| private_file / 1024 | 6.333426 | 2.902349 | 3.354266 | 6.333426 |
| discard / 65536 | 0.164484 | 0.100352 | 0.113527 | 0.283340 |
| private_file / 65536 | 4.524564 | 3.132007 | 3.916273 | 4.700548 |

This confirms millisecond durable-record cost on this measured XFS path.
Other disks, host load, record schemas and concurrent writers may differ.
Private capture is optional; its durability and joined evidence contract must
be preserved in any later optimization.

## Small concurrency measurement

Twenty direct-Python no-op invocations on one service with four workers
completed as a joined batch in **225.225ms**. Sample order below remains input
index order; index 0 was concurrent with the next three samples.

| Case | Metric (ms) | First | Min | Median | Max |
| --- | --- | ---: | ---: | ---: | ---: |
| python_concurrency_4 | Init | 37.827 | 37.505 | 40.681 | 52.314 |
| python_concurrency_4 | Invoke | 1.809 | 1.535 | 1.803 | 2.787 |
| python_concurrency_4 | Joined wall | 39.650 | 39.619 | 42.303 | 54.326 |

Per-call wall increased versus the sequential case while calls overlapped.
This is a small fixture observation, not an async queue or saturation benchmark.

## Ranked owner actions

The first two rows have measurements above. The remaining rows are source
candidates, **unmeasured** in this review.

| Priority | Evidence and responsible owner | Bounded next action |
| --- | --- | --- |
| 1 | Lambda launches/imports on every call; the same-interpreter UV recipe has much larger pre-readiness cost here. Lambda owns launch/readiness, the function owner owns its command/imports, and SSD tooling owns controller checks. | Attribute the configured launcher path with owned timing boundaries; trace representative imports only when the application owner authorizes it. Warm runtime reuse is a separate lifecycle/state-isolation design. |
| 2 | [Sink.Append](../internal/devcapture/sink.go) serializes write/Sync. Private terminal/stack modes have measured joined costs. Capture owns durability; Lambda owns where evidence blocks completion. | Measure representative record sizes and concurrent writers before changing scheduling. Do not substitute discard, batching or unjoined background writes for accepted durable evidence. |
| 3 | [Lambda async](../internal/lambda/async.go) `Admit`/`recordAsyncLocked` append under `Service.mu`; a slow sink can delay admissions, registration/retirement and Close. | Measure blocked-writer lock delay and async queue wait. If material, separate state-lock and capture admission/finalization ownership while retaining acceptance atomicity and actual joins. |
| 4 | [Cognito store](../internal/cognito/store.go) has one SQL connection. [Signing keys](../internal/cognito/jwt.go) load/decode PEM under the store mutex; first-use RSA generation also holds it. | Measure DB wait stats and JWT/JWKS separately from bcrypt/SRP. Decoded-key reuse needs pool deletion/recreation invalidation; connection changes need SQLite correctness evidence. |
| 5 | [SNS PublishSNS](../internal/messaging/sns_topic.go) retains topic `publishMu` across durable intent, FIFO/archive mutation and sequential [delivery admission](../internal/messaging/sns_delivery.go) sharing a 10s budget. | Measure subscriber count and same/separate-topic concurrency with owned destinations. Preserve FIFO, deduplication, redrive and accepted-outcome policy. |
| 6 | [SQS pruning](../internal/messaging/sqs_queue.go) scans under the queue lock; FIFO sorts waiting messages, and event-size JSON work also runs under that lock. [Gateway](../internal/gateway/gateway.go) scans routes; its [authorizer cache](../internal/gateway/authorizer.go) scans up to 4096 entries when full. | Measure queue depth/attributes and route/cache matrices before adding indexes/heaps. Preserve receipt, FIFO and route-precedence semantics. |

[App startup](../internal/app/run.go) opens Cognito SQLite before serving.
Store bootstrap attempts idempotent migrations and legacy identity UPDATEs
each open; optional seed applies signing-key and credential work before
readiness. A useful follow-up compares empty, reopened and explicitly seeded
private DBs, never the default DB. Server JSON/form decoding and large payload
allocations also need a size matrix; no-op process measurements do not cover them.

A small unmeasured allocation candidate in
[invocationLogs.diagnostics](../internal/lambda/process.go) copies merged tail
bytes merely to read their count after the native result has copied its logs.
Measure log-volume sensitivity before prioritizing a count accessor.
[Terminal diagnostics](../internal/lambda/dev_diagnostics.go) append after
runner/collector joins; private `elapsed_ms` stops before its own terminal
append/Sync and is not HTTP wall time. [Python collectors](../internal/lambda/dev_python_stacks.go)
must join even when their snapshot overlaps handler execution.

The one-second retained-pipe deadlines in Lambda and
[localexec](../internal/localexec/process_unix.go) bound fault cleanup.
They are not healthy-path sleeps. Completing before a pipe/process join would
conceal uncertainty rather than improve performance.

## Reproduction and limits

The `performance` build tag enables these fixtures only on Linux/Darwin.
Normal verification does not run them. Missing frozen-Python configuration skips
Lambda measurements; a configured missing executable or interpreter/environment
mismatch fails. Node and managed UV are required. Run serially in an allocated
lane from the canonical checkout:

```sh
set -o pipefail
env GOMAXPROCS=4 GOFLAGS=-p=2 EVENTBUS_PERFORMANCE_PYTHON="$PWD/.venv/bin/python" \
  ssd-dev operation --purpose test -- bash -c \
  'findmnt -T "$TMPDIR" -o TARGET,FSTYPE,OPTIONS -n; go test -p 2 -count=1 -timeout 4m -tags performance -run "^TestPerformanceReview" -v ./internal/lambda' \
  2>&1 | tee /tmp/eventbus-performance-lambda-20261008.log | tail -30
env GOMAXPROCS=4 GOFLAGS=-p=2 \
  ssd-dev operation --purpose test -- bash -c \
  'findmnt -T "$TMPDIR" -o TARGET,FSTYPE,OPTIONS -n; go test -p 2 -count=1 -timeout 1m -tags performance -run "^TestPerformanceReview" -v ./internal/devcapture' \
  2>&1 | tee /tmp/eventbus-performance-capture-20261008.log | tail -30
```

The raw logs above retain every sample, actual RequestID/PID, resolved launcher
and summary. Aggregates were recomputed from those saved logs without rerunning
passing cases. Test owners joined services, child groups, listeners, collectors
and sinks before deleting their private temporary fixtures. Logs remain subject
to the existing finite evidence retention policy.

This bounded review did not execute an application/default DB, consumer business
chain, broader startup/SQL/routing benchmarks, or import tracing. The reported
consumer 6.245s and 10.359s intervals combine unknown launcher, runtime, imports
and application work; these fixture results do not attribute those intervals,
claim a measured optimization, or establish the business chain's outcome.

## Launcher fix and follow-up, 8 October 2026

The measured launcher overhead was fixed in
[SSD tooling 796c1dd](https://github.com/lyeith/ssd-dev-tools/commit/796c1dd).
Its [separate owner report](https://github.com/lyeith/ssd-dev-tools/blob/main/docs/performance-startup-20261008.md)
records the implementation, native timing boundaries and suite limits.

An already-owned managed shell unnecessarily entered another operation
controller. The wrapper also validated four policy roots and eight cache targets
through nine separate storage proofs. Before profiles counted 11 broker calls
inside the wrapper (767.570ms broker time) and 2 in controller reuse (139.475ms).
They are separate instrumented stages, so their totals are not an exact additive
breakdown of command latency. Each broker request starts a fresh privileged query
worker; neither native UV resolution nor application imports explains these calls.

The wrapper now performs the same exact receipt/cgroup verification directly
for existing owners and uses the established storage owner to pin all policy
roots/cache targets together for one fresh proof before directory creation.
The actual owned outer wrapper profile counted **3 queries** (240.860ms broker
time). Partial/stale markers still refuse; markers themselves grant no authority.
Project/cwd/argv/environment, private physical paths and current mount, backing,
project inheritance and quota checks remain. No cached proof, dependency,
root broker/config update or Lambda runtime-policy change was introduced.
Installed source symlinks activate the fix for new invocations without a restart.

Ten actual managed-UV launches before and ten after, in serial owned operations,
used the exact no-project argv and existing Python 3.12.11 environment above.
Each emitted monotonic readiness plus executable/version/prefix and was joined.

| Joined managed startup, ms | First | Min | Median | Max |
| --- | ---: | ---: | ---: | ---: |
| Before | 1155.083 | 1128.627 | 1154.143 | 1187.268 |
| After | 354.803 | 322.171 | 348.860 | 452.141 |

Median joined launcher startup fell **69.8%**. Samples include index zero and
run before then after without randomized ordering or cache flushing. Broker
profiles use cProfile; this ten-sample comparison is uninstrumented.

The native Lambda runtime fixture also passed another 100 fresh invocations:
20 each for direct Python, managed UV, Node, provided and command.
All native RequestID/PID, same-interpreter/version/prefix and actual process
group/listener closure assertions passed. Source was EventBus base `5a18876`
plus the frozen #29 attribution changes; tooling candidate is `796c1dd`.

| Python native metric, ms | Earlier median | Follow-up median |
| --- | ---: | ---: |
| Direct Init | 29.675 | 41.873 |
| Managed UV Init | 1160.549 | 388.942 |
| Managed UV joined wall | 1164.572 | 393.150 |

This follow-up ran only `TestPerformanceReviewLambdaRuntimes`, non-race,
and passed in 11.872s. Existing capture/concurrency measurements were not rerun
because their implementations were unchanged. Direct-Python variation shows
host timing noise; these ordered fixtures do not establish a production p99
or attribute a particular application's import/business-flow intervals.

The tooling run passed all 12 Policy and 29 shell tests, including owned/unowned
dispatch, invalid owner refusal, fresh batched proof and all cache targets.
Operation, UV bootstrap and storage cases also passed. Its complete suite had
18 failures/3 import errors; a clean `26f8468` source copy under the same
owner/interpreter reproduced exactly those failures. Existing GC fixture
fields, umask/NoNewPrivs assumptions and unavailable pytest account for them.
The full tooling suite remains nongreen; no test dependency was installed.

Saved SSD logs: `/tmp/eventbus-overhead-*-20261008.log` and
`/tmp/eventbus-performance-runtimes-after-20261008.log`, with the existing
24-hour evidence lifetime. Temporary profiling/source stages are removed after
acceptance. Broader startup/SQL/routing actions above remain unmeasured follow-ups.
