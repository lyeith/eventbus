# EventBus state

Canonical SSD /home/spite/Projects/eventbus, main; public MIT standalone AWS
emulator and agent harness. Plans/application state remains untouched.
Latest public release: https://github.com/lyeith/eventbus/releases/tag/v0.11.2
Performance fixes integrated through 5186da3; v0.11.3 build/publication next.

General audit: docs/PERFORMANCE-AUDIT.md; prior runtime/launcher evidence is
docs/PERFORMANCE-REVIEW.md. Seven owner commits: shared performance reporting;
Cognito atomic bootstrap/seed; SQS waiting order/compaction; SNS envelope reuse;
Lambda count-only logs; Lambda async capture ownership; gateway matching/cache.
No API/schema/dependency/crypto/durability changes.

Local medians, fixed before→after fixtures (ms):
- Empty Cognito bootstrap152.772→33.553; first tiny seed204.766→135.574.
  RSA randomness limits seed attribution; existing bcrypt comparison remains.
- Lambda DescribeTarget during durable Event capture11.120→0.003692.
  Accepted event batches still serialize durable records; no throughput claim.
- FIFO depth10k native receive1.266→0.158; Lambda receive1.546→0.349.
  Native receive excludes projection; Lambda includes byte admission/projection.
- SNS100queue publish: capture disabled1.162→0.609; durable6.172→4.949.
- Gateway1000route miss0.142260→0.014339; cached exact IAM hit0.026808→0.001418.
  Gateway values are normalized ten-call batch observations.
- Disabled private log projection64KiB merged tail:65536→0bytes/op.

PASS: full Cognito non-race42.474s, focused startup/seed/legacy/persistence
race35.022s; Lambda fullrace96.269s, capture1.202s/localexec2.076s;
messagingrace5.594s/gatewayrace20.301s; eventsource/app/server/devquiescence races.
Ten native Python/Node SDK + retained-stack cases118.583s, no selected skips;
owned RustFS delivery/cleanup and authenticated gateway continuations included.
All-package vet with sdksmoke/integration/performance tags PASS.
Full Cognito race hit4m budget in bcrypt; no race/assertion failure before timeout.
Complete package-wide Cognito race remains unverified; scoped race/fullnon pass.

One serial SSD lane, GOMAXPROCS4/GOFLAGS=-p=2. Private owned fixtures only;
no default DB, live application stack, new environment or worktree.
Successful owners joined children/listeners/DBs/sinks and deleted their temp state.
Logs /tmp/eventbus-audit-*-20261008.log have existing24h TTL.
Failed gateway baseline and Cognito-race owners verified quiescent/unpinned;
normal GC expiry Oct9 14:15:15 /14:22:51UTC. No custom cleanup system.
Prior two empty tooling scratch receipts retain documented Oct9 13:25/13:29UTC TTL.
