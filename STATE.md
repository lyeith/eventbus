# EventBus state

Canonical SSD /home/spite/Projects/eventbus, main; public MIT standalone AWS
emulator and agent harness. Plans/application state remains untouched.
Latest public release: https://github.com/lyeith/eventbus/releases/tag/v0.11.3
Eight binaries + SHA256SUMS; downloaded assets verified against local builds.
Clean tagged source 0ac08e2; Go 1.26.0, CGO_ENABLED=0, module v0.11.3.

General audit: docs/PERFORMANCE-AUDIT.md; prior runtime/launcher evidence is
docs/PERFORMANCE-REVIEW.md. Seven owner commits through 5186da3: reporting;
Cognito atomic bootstrap/seed; SQS waiting order/compaction; SNS envelope reuse;
Lambda count-only logs; Lambda async capture ownership; gateway matching/cache.
No API/schema/dependency/crypto/durability changes. Open issue board is empty.

Local medians, fixed before→after fixtures (ms):
- Empty Cognito bootstrap 152.772→33.553; first tiny seed 204.766→135.574.
  RSA randomness limits seed attribution; existing bcrypt comparison remains.
- Lambda DescribeTarget during durable Event capture 11.120→0.003692.
  Accepted batches still serialize durable records; no throughput claim.
- FIFO depth 10k native receive 1.266→0.158; Lambda receive 1.546→0.349.
  Native receive excludes projection; Lambda includes byte admission/projection.
- SNS 100-queue publish: capture disabled 1.162→0.609; durable 6.172→4.949.
- Gateway 1000-route miss 0.142260→0.014339; cached IAM hit 0.026808→0.001418.
  Gateway values are normalized ten-call batch observations.
- Disabled private log projection / 64KiB merged tail: 65536→0 bytes/op.

PASS: full Cognito non-race 42.474s, focused startup/seed/legacy/persistence
race 35.022s; Lambda full race 96.269s, capture 1.202s/localexec 2.076s;
messaging race 5.594s/gateway race 20.301s; affected consumer race suites.
Ten native Python/Node SDK + retained-stack cases 118.583s, no selected skips;
owned RustFS delivery/cleanup and authenticated gateway continuations included.
All-package vet with sdksmoke/integration/performance tags PASS.
Full Cognito race hit its 4m budget in bcrypt; no warning/assertion before timeout.
Complete package-wide Cognito race remains unverified; scoped race/full non pass.

Actual packaged Linux amd64 and macOS arm64: private Cognito seed/reopen/JWKS,
SNS fanout, async native IDs/terminal capture/actual child absence, 1000-route
gateway and joined shutdown PASS. Other two targets cross-built, not executed.

One serial SSD lane, GOMAXPROCS=4/GOFLAGS=-p=2; owned private fixtures only.
No default DB/live stack/new environment/worktree/owned process remains.
Release stages, downloaded duplicates and probes removed on both hosts.
Audit/release logs in SSD /tmp have existing 24h TTL. Failed operations are
quiescent/unpinned: gateway baseline, Cognito race and initial packaged fixture;
GC expiry Oct9 14:15:15 / 14:22:51 / 14:57:04 UTC respectively.
Prior empty tooling scratch receipts retain Oct9 13:25/13:29 UTC TTL.
