# Performance improvements: second round

9 October 2026. Implements every target in the
[second audit](PERFORMANCE-AUDIT-ROUND-2.md), including its capture, SQS and
async-history follow-ups. Baseline source is `febe604`; that audit-only commit
has the same runtime as `1a842d1`. No dependency changes.

## Ownership and behavior

| Target | Implementation and preserved contract |
| --- | --- |
| SSM pagination | Store-owned sorted raw-name index, updated atomically with versions; prefix ranges and exclusive live cursors preserve hierarchy, order, decryption and signed-token binding. Create/delete now pay ordered-slice mutation cost. |
| SNS filters | Publication-owned lazy body/attribute decoding; selected protocol bodies, stripped attributes, UseNumber and each subscription's decision remain native. |
| Firehose timezone | Stream owns its validated immutable location; admission, grouping, error prefixes, object construction and retries share it, including DST. |
| Lambda serialization | Python and warm Node serialize success once inside the existing failure boundary; stateful serialization cannot produce different validation/transport values. Native Node callback and fresh toJSON key behavior remain supported. |
| Bounded bodies | `awsprotocol.ReadBoundedBody` prepares capacity from a bounded length hint; actual bytes, oversize sentinel, JSON validation and read failures still determine acceptance. Lambda Runtime API and shared JSON adapters use it. |
| SNS FIFO dedup | Topic owns its conservative earliest-expiry bound; common SNS/SQS map mechanics live in `messaging/dedup_expiry.go`. Due expiry still sweeps, native ID/sequence/capture policy stays with each service. |
| SES raw MIME | Line scanning removes the line-slice index while retaining byte limits and existing CRLF/final-line accounting for both sending APIs. |
| Gateway misses | Cache-enabled same-identity requests share a joined flight, bounded to 128 identities and 128 waiters each. Caller ARN decisions remain independent; last cancellation cancels/joins, Close joins flights, TTL-zero/overflow use the original direct path. Valid native Deny/false responses remain cacheable; execution/parse failures do not. |
| Cognito triggers | Consumer-owned event/response/deadline port in `cognitotrigger`; app composes a private instance of Lambda's managed runtime. Duplicate Node wrapper/process code is removed. Fresh is default; optional warm reuse has its own cap, per-pool state, strict validation before reuse and retained drain/resume. |
| Cognito signing keys | Store lends immutable decoded keys internally; exported accessors detach RSA state. Real first-use generation runs outside the store mutex, coalesces per pool and joins on Close. Committed deletion invalidates keys/generations; rollback and final account/revocation checks remain intact. |
| Capture | Opportunistic synchronous group commit, bounded to 64 pending records/1MiB, with a single oversized record allowed under service-owned admission. No timer/background worker. Every successful owned-file Append waits for real covering Sync; order, sticky failure, Err/Close joins and panic/Goexit ownership remain intact. |
| SQS batches | Queue owner encodes candidates once before exact byte/receipt admission. `sqsevent.Batch` carries detached records and immutable JSON; eventsource dispatches those bytes. Native leases, FIFO, custody, retry and original-receipt ACK policy are unchanged. Event-only dev consumers remain supported. |
| Lambda history | Bounded terminal ring removes repeated shifts; snapshots copy under the mutex and sort after unlocking, preserving retained records and native ordering. |

Trigger diagnostics now remain private and are discarded even under `--debug`;
the previous debug forwarding is removed. Events/results stay capped at 1MiB,
combined diagnostics at 64KiB, deadlines at at most five seconds. Private targets
have no public Invoke route. See [trigger configuration](CUSTOM-TRIGGERS.md#warm-trigger-execution)
for worker caps, module state and source-edit behavior.

## Measurements

These are local observations, not AWS throughput or universal speedups.
Measurements use one serial non-race SSD lane, Linux amd64 / Ryzen 7 7730U,
Go 1.26.0, GOMAXPROCS=4, Node 22.22.1 and frozen Python 3.12.11. Fixed case
order, shared host caches and no randomized repetitions limit attribution.
Go allocation includes the measured boundary, not child-runtime allocation.
MB denotes decimal bytes; MiB/KiB denote binary sizes.

| Workload | Before | After | Observations |
| --- | ---: | ---: | --- |
| SSM 10k-name full traversal, core | 3338.296ms | 29.157ms | 3 traversals; native cursor/value checks |
| SSM 10k-name full traversal, unchanged Go SDK/HTTP | 4396.619ms | 1423.997ms | 3 traversals; SDK/signing/wire overhead retained |
| SSM sparse ten-name core first page in 10k registry | 0.637696ms | 0.001618ms | 20 samples |
| SNS 100 subscribers / 64KiB, all body-filtered | 124.020ms / 40.446MB | 21.654ms / 8.127MB | 20 native publications |
| SNS same input, no filters | 31.310ms / 7.796MB | 22.095ms / 7.796MB | Control also varies with host state; body filtering is now close to its allocation floor |
| SNS same input, half body-filtered | 103.802ms / 36.736MB | 12.499ms / 4.418MB | 20 publications |
| SNS FIFO, 10k unexpired IDs, unique publication | 0.900011ms | 0.003477ms | 20 samples |
| SNS FIFO, same history, duplicate publication | 0.917753ms | 0.002224ms | 20 samples; same native result |
| SQS ten records / 64KiB binary attributes, Lambda receive/dispatch encoding | 3.347132ms / 6.174MB | 2.872301ms / 3.577MB | 20 samples; prior baseline reused; exact wire equivalence checked outside timing |

SSM's core traversal after range is 19.525–36.910ms; SDK range is
1345.367–1493.239ms. Unexpired SNS FIFO setup of 10k native IDs fell from
1019.740 to 87.664ms (one observation); a deliberately due sweep remains linear,
1.553 to 0.883ms (one observation). The SQS boundary still includes the one
mutable byte copy needed by invocation; observed allocations fell from 886.5 to
567 per batch.

| Further workload | Before | After | Observations |
| --- | ---: | ---: | --- |
| Firehose 500 records, UTC | 1.545ms / 0.190MB | 0.839ms / 0.190MB | 20 samples; control varies with host state |
| Firehose 500 records, Singapore | 6.630ms / 0.714MB | 0.771ms / 0.193MB | 20 samples |
| Firehose 500 records, New York | 10.214ms / 4.513MB | 0.703ms / 0.190MB | 20 samples; prepared location removes repeated loads |
| SES 16MiB PDF MIME validation | 152.588ms / 30.055MB | 89.627ms / 22.986MB | 5 samples; serialized message 22,959,201 bytes |
| Warm Lambda 4MiB Python result, diagnostics off | 44.925ms / ~10MB | 28.792ms / 4.220MB | 20 calls after untimed warmup |
| Warm Lambda 4MiB Node result, diagnostics off | 66.859ms / ~10MB | 37.409ms / 4.221MB | 20 calls after untimed warmup |
| Warm Lambda 4MiB Python / Node, diagnostics on | 51.292 / 89.897ms | 32.773 / 49.890ms | 20 calls each; real private file capture retained |
| Runtime API 4MiB response benchmark | 29.912ms / 10,009,442B / 54 allocations | 20.194ms / 4,209,000B / 22 allocations | 100ms-target benchmark; four before/six after operations |
| Gateway sixteen forced misses, fresh authorizer | 719.401ms / 16 Invokes | 153.637ms / 1 Invoke | 3 bursts; per-caller native decisions checked |
| Gateway same workload, warm cap one | 70.697ms / 16 Invokes | 5.226ms / 1 Invoke | 3 bursts after warmup |
| Cognito internal persisted-key load / cached borrow | 0.170911ms | 0.000050ms | 20 samples; native callers use immutable borrow |
| Cognito JWKS generation | 0.259730ms | 0.001482ms | 20 samples; prior baseline reused |
| Cognito native access-token verification | 0.293756ms | 0.063977ms | 20 samples; current account/revocation checks retained |
| Cognito native access-token signing | 1.493330ms | 1.226536ms | 20 samples; real RSA work retained |
| Async terminal completion, 256 records | 3.337µs / 1360B / 8 allocations | 2.933µs / 1416B / 10 allocations | Same real capture/state/wake transition on baseline archive and current source |
| Async terminal completion, 10k records | 183.203µs / 1364B / 8 allocations | 3.436µs / 1416B / 10 allocations | Removed whole-history shift; added capture group bookkeeping |
| Async 10k-record snapshot, paired repeated benchmark | 0.926ms / 1,843,464B | 0.991ms / 1,843,464B | Median of three 250ms-target trials; four allocations each |

The original 100ms snapshot run varied more (0.889→1.659ms), so paired trials
were used to resolve that concern. Their ranges overlap: 0.910–0.973ms before,
0.924–0.995ms after. No snapshot wall-time speedup is claimed; sorting now
releases Service.mu before its work. Completion improves most at large history.

Exported mutable signing-key snapshots rebuild detached RSA precomputation:
Load/Ensure medians were 0.682/0.648ms, above the old 0.171ms load. That cost
is explicit; native JWT/JWKS/challenge paths borrow immutable store-owned keys.
First-use real RSA generation still costs CPU, but no longer holds the store
mutex or SQLite connection and is joined before Close.

### Durable capture

Each case completes 64 actual file records, verifies private permissions,
parses every JSONL record and joins closure. Fixed totals are one observation;
Append medians use 64 observations. Successful file appends remain synchronous.

| Payload / producers | Total before → after ms | Append median before → after ms | Real Sync calls before → after |
| --- | --- | --- | --- |
| 1KiB / 1 | 294.262 → 236.018 | 4.133 → 3.311 | 64 → 64 |
| 1KiB / 4 | 272.802 → 113.730 | 16.297 → 6.557 | 64 → 30 |
| 1KiB / 16 | 289.300 → 28.208 | 70.509 → 6.914 | 64 → 8 |
| 64KiB / 1 | 328.359 → 248.240 | 4.768 → 3.614 | 64 → 64 |
| 64KiB / 4 | 323.367 → 130.982 | 19.051 → 7.520 | 64 → 32 |
| 64KiB / 16 | 311.955 → 37.090 | 74.455 → 8.663 | 64 → 8 |

Single-producer Sync count is unchanged; its wall-time variation is filesystem/
host behavior. Concurrent savings have structural evidence: fewer actual Syncs,
each covering every acknowledged record in its group.

### Registered Cognito execution

The real Define/Define/Create/Verify/Define chain and SES SDK/capture are unchanged.
The first new fresh measurement varied from 919.024–1323.605ms (median
1214.117ms). A nearby baseline archive/current-source pair resolved that concern:
baseline fresh 925.891–1009.509ms, median 935.885; current fresh
922.166–965.297ms, median 929.974. No cold-mode speedup is claimed.

Warm mode runs five chains, retains three actual Node PIDs and joins their
retirement. First chain includes initialization: 890.474ms. The remaining four
are 33.738–40.036ms; all-five median is 37.035ms. Fresh mode launches 25 PIDs.

The unchanged real SDK driver also provisions resources and performs three
SRP/email/token flows, each with three native HTTP calls and five triggers.
Current fresh native HTTP median is 1265.418ms (1235.328–1348.496).
Warm native HTTP is 1534.249ms for initial imports, then 93.716 and 80.078ms;
all-three median 93.716ms. Whole repeated warm flows, including client SRP and
capture lookup, are 136.999 and 123.778ms. JWT verification remains independent.
Earlier 819.230ms fresh native HTTP is historical; these observations do not
establish a cold-flow regression or gain. Warm benefits apply after initialization.

## Verification and reproduction

Final service race coverage passed: the full `go test -race ./...` run passed
every owner except three new fixture assumptions; corrected app suite and
affected Lambda tests passed on rerun. The fixtures now gate actual handler
startup/retained initialization and distinguish native callback completion from
a Promise resolving undefined. A private synchronous-undefined regression was
also found by independent review, corrected and covered in fresh/warm mode.

- Full SDK race suite: 302.197s, including default fresh and opt-in warm Cognito.
- Full-profile retained SDK/RustFS recovery: 28.085s.
- Unchanged Express/Swagger native gateway proof: 10.527s.
- Frozen Python fixture contracts: five tests passed.
- Eight Linux/Darwin amd64/arm64 binaries built, SHA256 checks passed; both native
  help commands passed. Build outputs stayed in disposable owned scratch.
- All-tag vet, including SDK/performance/integration sources, passed.
- Independent integrated reviews covered runtime privacy/lifetimes, gateway
  cancellation, capture failures, key generations, SSM order and SQS custody.

Use existing pinned dependencies and explicit frozen Python selection:

```sh
export GOMAXPROCS=4 GOFLAGS=-p=2
export EVENTBUS_SMOKE_PYTHON="$PWD/.venv/bin/python"
export EVENTBUS_PERFORMANCE_PYTHON="$EVENTBUS_SMOKE_PYTHON"
go test -race -count=1 -timeout 15m ./...
go test -race -count=1 -timeout 15m -tags sdksmoke ./tests/sdk
go vet -tags 'sdksmoke performance integration' ./...
go test -p 1 -count=1 -timeout 5m -tags performance \
  -run '^TestPerformanceRound2|^TestPerformanceSigningKeyReuse|^TestPerformanceSQSFollowupBinaryProjection|^TestPerformanceCognitoRegisteredTriggerChain$' -v \
  ./internal/ssm ./internal/messaging ./internal/firehose ./internal/ses \
  ./internal/lambda ./internal/devcapture ./internal/gateway ./internal/cognito ./internal/app
go test -p 1 -count=1 -tags performance -run '^$' \
  -bench '^BenchmarkPerformanceRound2RuntimeResponse|^BenchmarkPerformanceAsyncHistory' -benchtime=100ms -benchmem ./internal/lambda
go test -p 1 -count=1 -tags 'sdksmoke performance' \
  -run '^TestPerformanceCognitoNativeSDKAuth$' -v ./tests/sdk
```

Wrap SSD tests/builds with the documented ssd-dev owners and save complete output
before filtering. Raw logs use `/tmp/eventbus-opt-round2-*-20261009.log` under
the existing 24h policy. Baseline archives and their read-only dependency links
were temporary owned scratch, removed automatically after successful runs.
Two SSD wrapper finalization errors followed successful Go runs; their trees
were verified quiescent and empty scratch removed. Test results remain in logs.
No application/default DB, live stack, dependency or public release was changed.
