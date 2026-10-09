# Performance audit: second round

Implementation and comparison: [second-round improvements](PERFORMANCE-IMPROVEMENTS-ROUND-2.md).
The measurements and recommendations below describe the pre-change baseline.

9 October 2026. Runtime baseline: `main 1a842d1` (published v0.12.0 runtime).
This round adds opt-in measurements and ranks work; it changes no production
behavior, dependencies or release binaries. The [previous audit](PERFORMANCE-AUDIT.md)
records optimizations already implemented. Source inspection confirms the
Cognito key decoding and registered-trigger cold starts below remain present.

## Recommended work

Prioritize the first three bounded changes. Large-result Lambda work is next
when application workloads return sizeable JSON. Authorizer coalescing has
higher cancellation/contract risk. Cognito trigger reuse is a separate,
larger runtime-composition change with substantial per-authentication potential.

| Target | Observed cost | Owner and proposed change |
| --- | --- | --- |
| SSM pagination | 10k matching parameters: core 3.338s, SDK 4.397s per full traversal | SSMStore maintains ordered names; select prefix/cursor ranges instead of scanning/sorting every ten-item page |
| SNS body filtering | 100 destinations, 64KiB payload: 124.020ms / 40.446MB allocated; unfiltered 31.310ms / 7.796MB | SNS prepares immutable filter inputs once per publication and selected body |
| Firehose named timezones | 500 records: UTC 1.545ms, Singapore 6.630ms, New York 10.214ms | Stream configuration owns its prepared immutable location, used for grouping and object keys |
| Lambda large results | Warm 4MiB result: Python 44.925ms, Node 66.859ms; about 10MB Go allocation each | Lambda wrappers encode once; Runtime API uses bounded capacity preparation for known lengths |
| SNS FIFO dedup | At 10k unexpired IDs: unique 0.900ms, duplicate 0.918ms, versus about 0.003ms near empty | Topic publication owner tracks conservative earliest expiry and skips unnecessary sweeps |
| SES MIME allocation | 16MiB PDF: validation 152.588ms / 30.055MB allocated | SES checks line lengths by scanning rather than constructing every line slice |
| Gateway authorizer misses | Sixteen same-identity misses cause sixteen actual invocations; fresh/warm-cap-one burst 719.401/70.697ms | Gateway can coalesce bounded cache-enabled in-flight identities, with each caller evaluating its own ARN |
| Cognito trigger cold starts | Prior actual five-trigger chain 895.680ms; Lambda warm mode does not cover this runner | App composes a typed trigger execution port backed by the existing managed runtime owner |
| Cognito signing key decoding | Prior key load 0.171ms, token verification 0.294ms; private PEM decode 0.122ms | Cognito store owns immutable decoded keys with pool-generation invalidation |

Times/allocations in this table are local medians. Rows are different workloads,
not comparable service throughput figures. No optimization speedup is claimed:
this round measures current behavior only. MB denotes decimal bytes; MiB/KiB
denote binary sizes. Prior Cognito observations were reused rather than rerun.

## Measurements and required contracts

### SSM: repeated whole-registry work

[Fixture](../internal/ssm/performance_round2_test.go) uses the unchanged Go SDK
against an actual loopback HTTP handler, plus the real store path. Each registry
also contains ten parameters on a separate sparse path. Setup is excluded.
First pages have twenty observations; full traversals have three each.

| Registry / matching names | Core first-page median ms | Core full traversal min / median / max ms | SDK full traversal min / median / max ms |
| --- | ---: | --- | --- |
| 110 / 100 | 0.020920 | 0.265 / 0.344 / 0.345 | 6.514 / 7.467 / 7.704 |
| 1,010 / 1,000 | 0.243194 | 24.310 / 26.110 / 27.617 | 88.957 / 98.441 / 99.256 |
| 10,010 / 10,000 | 2.899187 | 2884.721 / 3338.296 / 3435.062 | 3915.508 / 4396.619 / 4914.209 |
| 110 / 10 sparse | 0.007595 | 0.011 / 0.017 / 0.025 | 0.645 / 0.653 / 0.691 |
| 1,010 / 10 sparse | 0.056829 | 0.061 / 0.079 / 0.109 | 0.644 / 0.663 / 0.700 |
| 10,010 / 10 sparse | 0.637696 | 0.703 / 0.835 / 1.067 | 1.564 / 1.663 / 3.060 |

`pathNames` scans all names, sorts matches and holds the store RLock throughout
each page. With a fixed ten-item page size, dense traversal repeats the
O(N log N) preparation about N/10 times; sparse paths still inspect unrelated
names. The SDK includes native signing, wire encoding/decoding, loopback and
ordinary handler debug logging. Core timing removes SDK/HTTP overhead and
exposes native store pagination cost; snapshots/decryption, signed cursors and
fixture ordering/value checks remain inside its traversal boundary.

An ordered name index belongs to the store and must change atomically with
create/delete. Preserve lexicographic raw-name order, bare/slash names, recursive
path boundaries, signed parameter-bound cursors and decryption. Do not cache
entire pagination snapshots or change cursor semantics merely to optimize this.
Fixtures verify complete counts, pages, ordering, path boundaries and plaintext
values. A post-measurement failure-only page bound makes cursor regressions fail
promptly; successful measured page sequences remain unchanged.

### SNS: decode once for all applicable filters

[Fixture](../internal/messaging/performance_round2_test.go) publishes through the
native broker into real SQS queues. Every admitted message is received, its SNS
envelope checked and its receipt strictly settled outside timing. Queue depth
returns to zero after each sample. Capture is disabled to isolate core CPU work.

| Payload / destinations | Unfiltered median ms | All body filters median ms | Half body filters median ms | All attribute filters median ms |
| --- | ---: | ---: | ---: | ---: |
| 1KiB / 10 | 0.156 | 0.283 | 0.230 | 0.143 |
| 1KiB / 100 | 0.876 | 2.502 | 2.254 | 1.524 |
| 64KiB / 10 | 3.373 | 11.398 | 9.937 | 3.423 |
| 64KiB / 100 | 31.310 | 124.020 | 103.802 | 29.050 |

Each case has twenty observations. Payload size describes its JSON padding
field; native body bytes include JSON metadata. At 64KiB/100 destinations,
all-body filtering ranged 99.671–161.597ms; first was 104.161ms.
Allocation medians were 40,445,608 bytes for all-body versus 7,795,888 unfiltered.
Rejecting half the destinations still allocates 36,736,048 bytes.

`MatchesMessage` reparses the body for each subscription. Attribute values also
repeat String.Array/Number conversion. SNS already reuses encoded envelopes;
that previous optimization does not own filter decoding. Prepare per-publication
inputs keyed by the actual protocol-selected body. Preserve UseNumber, malformed
or nonobject rejection, empty-policy behavior, nested arrays, numeric semantics,
and protocol-JSON attribute stripping. Keep each subscription's policy decision
independent. There is no reason for an unbounded cross-publication JSON cache.

### Firehose: prepare configured locations

[Fixture](../internal/firehose/performance_round2_test.go) admits twenty actual
500-record batches per zone, then explicitly flushes and verifies all final
bytes, buffered counts and the native configured timestamp prefix. Destination
transport is bounded in-memory I/O; this is not a storage durability benchmark.
No jq or gzip work is included.

| Zone | First / min / median / max admission ms | Median allocated bytes |
| --- | --- | ---: |
| UTC | 1.234 / 0.900 / 1.545 / 7.553 | 190,480 |
| Asia/Singapore | 6.521 / 5.052 / 6.630 / 15.516 | 714,384 |
| America/New_York | 10.765 / 7.813 / 10.214 / 15.584 | 4,513,196 |

Record preparation and object-key construction each call `time.LoadLocation`.
Installed Go 1.26 source confirms named locations reread/parse zone data; UTC is
special-cased. Prepare the location with immutable stream configuration, beside
prepared jq. Preserve native CustomTimeZone readback, DST interpretation and
the same rules for both grouping and final object keys. Avoid a global location
cache when stream ownership already provides bounded lifetime.

### Lambda: separate encoding, buffer growth and validation

[Fixture](../internal/lambda/performance_round2_test.go) executes actual managed
Python/Node warm workers. Each size has one untimed warmup, then twenty calls.
The handler caches padding once per size; setup/first launch are excluded.
Diagnostics-enabled cases use actual private per-record durable capture.

| Runtime / data size | Diagnostics off min / median / max ms | Diagnostics on min / median / max ms |
| --- | --- | --- |
| Python / 1KiB | 0.458 / 0.547 / 1.160 | 3.753 / 4.252 / 10.034 |
| Python / 256KiB | 3.059 / 3.576 / 8.533 | 6.619 / 7.879 / 13.212 |
| Python / 4MiB | 39.531 / 44.925 / 55.744 | 42.879 / 51.292 / 70.258 |
| Node / 1KiB | 1.427 / 2.399 / 8.562 | 5.611 / 7.383 / 14.553 |
| Node / 256KiB | 4.189 / 5.453 / 10.693 | 9.571 / 10.817 / 26.496 |
| Node / 4MiB | 55.730 / 66.859 / 83.926 | 62.829 / 89.897 / 106.701 |

These are joined native invocation boundaries, not serialization-only timings.
Fixtures check exact bytes, module counter, retained PID, native unique request
IDs, truthful completion scope, exact optional capture and actual exit on Close.
Observed 4MiB Go allocation medians are about 10.0MB in all cases.

Both warm wrapper response paths serialize successful results twice, once for
validation and again for transport. Fresh Node result transport encodes once. Carry immutable encoded bytes from the same handler-error
boundary to transport. In warm Node this also fixes a semantic split: a stateful
`toJSON` can change or throw on the second pass outside that boundary.
Preserve error classification, result limits and native JSON behavior.

The isolated real Runtime API admission benchmark measures ReadAll plus JSON
validation, request/recorder setup and result ownership; it excludes network,
child runtime and handler encoding. Non-profiled 100ms-target observations:
1KiB 12.987µs / 8,443 bytes/op; 256KiB 1.617ms / 644,292 bytes/op;
4MiB 29.912ms / 10,009,442 bytes/op. The 4MiB case had only four iterations,
so its time is indicative rather than a stable distribution.

A separate one-second 4MiB attribution run had twenty iterations and profiling
overhead (50.413ms/op). Allocation sampling attributes 90.71% of total profiled
allocation space to `io.ReadAll`. CPU sampling attributes 73.28% cumulatively
to required JSON validation and 12.93% to ReadAll. These samples support bounded
initial capacity to reduce allocation/copy work; they do not support removing
JSON validation or claiming it removes most response CPU. Preserve known/unknown
length handling, truncation, maxPayload+1 admission, cancellation and first-result
claim semantics. Shared awsprotocol has a similar bounded-read pattern, but its
impact has not been separately measured.

Actual child CPU ticks were recorded without assuming USER_HZ: 4MiB medians
Python three, Node five with diagnostics off. These cover the whole child
boundary. RSS includes runtime and deliberately retained handler caches; twenty
calls per size cannot establish a leak or steady-state memory plateau.
For 4MiB/off, observed Python RSS stayed at 28.9MB; Node ranged 96.4–171.8MB.

### SNS FIFO: skip healthy dedup sweeps

The native FIFO fixture seeds 0/1k/10k IDs, then measures twenty unique/duplicate
pairs without subscribers or capture. No queue transport cost is included.

| Seeded history | Unique median ms | Duplicate median ms | Setup ms, one observation | Due sweep ms, one observation |
| --- | ---: | ---: | ---: | ---: |
| 0 | 0.003456 | 0.002930 | 0.000160 | 0.004659 |
| 1,000 | 0.044846 | 0.041254 | 28.411 | 0.982 |
| 10,000 | 0.900011 | 0.917753 | 1019.740 | 1.553 |

Every publish scans all unexpired dedup entries while holding topic publication
ownership. SQS already uses conservative earliest-expiry bookkeeping. Apply
that established pattern to the SNS owner; real due/unknown sweeps remain linear.
Keep five-minute expiry, acceptance-before-dedup mutation, duplicate capture,
group scope and native stable message IDs/sequence numbers. Forced expiry changes
only timestamps of native-issued test entries under the topic lock.

### SES: remove the line index, retain MIME validation

[Fixture](../internal/ses/performance_round2_test.go) validates native SES v2 raw
MIME with folded PDF attachments. It independently verifies attachment length,
SHA256, filename and type outside timing. Setup/capture are excluded; one setup
GC precedes the five measured calls.

| PDF comment payload | Actual MIME bytes | Native validation min / median / max ms | Median allocated bytes |
| --- | ---: | --- | ---: |
| 1MiB | 1,435,795 | 7.560 / 10.997 / 13.134 | 1,899,448 |
| 16MiB | 22,959,201 | 141.307 / 152.588 / 177.046 | 30,054,856 |

`bytes.Split` constructs every line slice solely to enforce the 1,000-character
limit. At 76-character base64 folding, the larger case has roughly 294k attachment
lines; their slice headers alone are about 7MB on amd64. This is a source-based
allocation estimate, not a measured isolated time or speedup. Scan using
`bytes.Cut` instead. Preserve CRLF accounting, malformed base64/MIME rejection,
recursive attachment checks and exact original request capture.

### Gateway: concurrent cached-identity misses

[Fixture](../internal/gateway/performance_round2_test.go) uses the actual Node
authorizer through actual Lambda Invoke HTTP/Runtime API and an HTTP proxy
with bounded in-memory backend transport. Backend network time is excluded.
A downstream start barrier deliberately guarantees every caller initially misses
the same cache key. Each size has three bursts. It is structural evidence,
not a natural-traffic percentile; trace-file writes are included.

| Producers | Real authorizer calls per burst | Fresh burst min / median / max ms | Warm cap-one burst min / median / max ms |
| --- | ---: | --- | --- |
| 1 | 1 | 174.311 / 181.834 / 189.458 | 6.658 / 8.886 / 12.131 |
| 8 | 8 | 315.933 / 330.451 / 344.462 | 25.763 / 42.037 / 59.408 |
| 16 | 16 | 717.360 / 719.401 / 759.883 | 52.758 / 70.697 / 81.585 |

Warm execution retains one actual PID; fresh bursts launch separate workers.
Later identical requests use cache; the cached exact-resource policy refuses a
different current ARN. In-flight coalescing belongs to each gateway authorizer
and only applies with caching enabled. Bound identities/waiters, define ownership
when the first caller cancels, keep invocation/parse failures uncached, join on
shutdown and evaluate every caller's current ARN/explicit Deny after the shared
response. Valid Allow/Deny or simple authorization responses retain native cache
behavior. The barrier
fixture must be adapted after such a fix; waiting for sixteen downstream invokes
would then be an invalid test. This is not permission for a second runtime pool.

### Capture: storage durability limits throughput

[Fixture](../internal/devcapture/performance_round2_test.go) keeps work fixed at
64 records with 1/4/16 producers. Every private append still performs a real
file Sync; the instrumentation only times it. Files are 0600, all JSONL records
and per-producer order are verified, and writer/producers join before cleanup.

| Payload / producers | Whole 64-record wall ms, one observation | Append median ms, 64 observations | Real Sync median ms, 64 observations |
| --- | ---: | ---: | ---: |
| 1KiB / 1 | 294.262 | 4.133 | 4.101 |
| 1KiB / 4 | 272.802 | 16.297 | 4.002 |
| 1KiB / 16 | 289.300 | 70.509 | 4.312 |
| 64KiB / 1 | 328.359 | 4.768 | 4.619 |
| 64KiB / 4 | 323.367 | 19.051 | 4.621 |
| 64KiB / 16 | 311.955 | 74.455 | 4.591 |

Actual durable throughput was about 195–235 records/s on this owned XFS host.
More producers mainly add lock waiting. Non-durable io.Discard fixed-work
comparisons took 0.211–0.439ms at 1KiB and 3.106–12.085ms at 64KiB; they are
explicitly not replacement policies. Serializing JSON already happens outside
the sink mutex. The earlier async fix protects unrelated Service.mu operations,
but cannot remove physical Sync cost.

A future group-commit design is larger work: each accepted append must await a
Sync covering its record, sticky failure must reach every affected waiter, and
ordering/drain/Close/retained evidence must remain truthful. This audit proposes
no durability weakening or background fire-and-forget logging.

## Larger seams and secondary findings

Registered Cognito triggers still launch Node per step. Reuse prior
[representative cold-chain evidence](PERFORMANCE-AUDIT.md#representative-cold-application-chains):
five native triggers median 895.680ms (884.609–907.463); native auth HTTP
819.230ms (812.020–856.509). The implementation remains cold in v0.12.0.
An app-composed consumer-owned execution port can reuse the Lambda lifecycle
owner without peer-package imports. Keep Cognito challenge policy and the trigger
adapter's strict full-event response validation, privacy-safe errors, Node/module/
environment isolation, deadlines, capped output, retained activity and actual
joins. Native Lambda error/log semantics cannot simply replace trigger privacy
rules. A separate app-owned private service instance may isolate registrations
while reusing the same execution implementation.

Cognito verification/JWKS still loads and decodes private/public PEM under the
store mutex. Prior twenty-observation medians: private decode 0.122368ms,
public decode 0.002480ms, SQL SELECT 0.021541ms, full load 0.170911ms,
JWKS 0.259730ms, verify 0.293756ms, sign 1.493330ms. Real new RSA generation
had median 30.113ms/max 118.522ms, under the same mutex. Cache immutable decoded
pool keys or provide a public-key-only path; deletion commit, failed-delete
rollback and reseed/generation invalidation belong to the store. Preserve final
current-user/revocation checks and real cryptography. Do not increase SQL
connections as a substitute for correcting repeated decode ownership.

Two lower-priority source-backed targets remain unmeasured in this round:

- SQS Lambda admission marshals prospective records for the exact 6MiB gate,
  then eventsource marshals the admitted batch again. The prior ten-record
  64KiB-binary-attribute median remains about 3.35ms. A consumer-owned batch
  port could carry detached records plus exact encoded wire bytes; messaging
  retains receipt/byte admission, eventsource retains dispatch/ACK ownership.
- Lambda async history shifts up to 10k retained records at completion and
  sorts an already detached snapshot while holding Service.mu. Unlock before
  sorting is bounded; ring retention needs measurement at configured limits.

Ordinary event-source receive already uses 20-second native long polling and
notifications. Warm connections are reused. New SES correlation already has
bounded metadata-only oldest-first eviction. Source leases intentionally retain
settlement proof until safe resume with a hard cap. No high-value redesign or
generic polling/retention tuning was established for those paths.

## Reproduce and verify

One serial SSD measurement lane, Linux amd64 / Ryzen 7 7730U, Go 1.26.0,
GOMAXPROCS=4 and GOFLAGS=-p=2. Node 22.22.1 and the frozen Python 3.12.11
interpreter were used. Native children belong to private fixture services.
Private files use existing owned XFS scratch. Measurements run without race;
race correctness checks run separately. Fixed case order, shared host caches
and no randomized repetitions limit causal comparisons and traffic inference.
MemStats describes observed Go-process allocation across the boundary, not
child-runtime allocation. First indexed samples are included unless warmup is
explicitly excluded; concurrent indexing need not follow completion order.

Run each package separately to avoid concurrent measurement interference:

```sh
# Execute in /home/spite/Projects/eventbus on SSD; repeat for each owner.
set -o pipefail
env GOMAXPROCS=4 GOFLAGS=-p=2 \
  ssd-dev operation --purpose test -- \
  go test -p 2 -count=1 -timeout 5m -tags performance \
  -run '^TestPerformanceRound2' -v ./internal/messaging \
  2>&1 | tee /tmp/eventbus-round2-messaging.log | tail -30
# Other owners: ssm, firehose, gateway, ses, devcapture.
# Lambda additionally selects the existing frozen interpreter and benchmark:
env GOMAXPROCS=4 GOFLAGS=-p=2 EVENTBUS_PERFORMANCE_PYTHON="$PWD/.venv/bin/python" \
  ssd-dev operation --purpose test -- \
  go test -p 2 -count=1 -timeout 5m -tags performance \
  -run '^TestPerformanceRound2' -bench '^BenchmarkPerformanceRound2RuntimeResponse$' \
  -benchmem -benchtime 100ms -v ./internal/lambda \
  2>&1 | tee /tmp/eventbus-round2-lambda.log | tail -30
```

For allocation attribution, run only the 4MiB benchmark with a one-second target,
`-o "$TMPDIR/runtime-response.test"`, `-cpuprofile "$TMPDIR/runtime-response.cpu"`
and `-memprofile "$TMPDIR/runtime-response.mem"` inside an owned test operation.
Read both profiles with go tool pprof before the operation exits and removes its
scratch. Profiling is not part of the headline invocation timings.

Verification and cleanup are recorded in STATE/HANDOFF. Full raw output was
saved before filtering under /tmp/eventbus-audit-round2-*-20261009.log;
host /tmp retention is 24 hours. Profiles, fixture files and owned children are
disposable; durable findings and reproductions live here and in Git. No default
identity DB, application stack, new dependency or temporary worktree was used.
