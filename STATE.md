# EventBus state

Canonical SSD /home/spite/Projects/eventbus, main; public MIT AWS emulator
and agent development harness. Published runtime remains v0.12.0:
https://github.com/lyeith/eventbus/releases/tag/v0.12.0
Tagged runtime source 953ca65; eight binaries + SHA256SUMS were verified.
Plans/application data remains untouched.

Second performance audit completed against main 1a842d1:
docs/PERFORMANCE-AUDIT-ROUND-2.md. This round adds seven owner-local opt-in
fixture files and documentation; no runtime, dependency or release change.

Recommended bounded work:
- SSM ordered-name pagination: 10k traversal core/SDK medians 3.338/4.397s.
- SNS per-publication filter decoding: 100 destinations/64KiB body
  filtering 124.020ms/40.446MB allocated versus unfiltered 31.310ms/7.796MB.
- Firehose prepared timezone: 500 records UTC/Singapore/New York medians
  1.545/6.630/10.214ms.
- Lambda warm result encode-once and bounded Runtime API response capacity:
  4MiB Python/Node 44.925/66.859ms, about 10MB Go allocation.
- SNS FIFO earliest-expiry bookkeeping and SES streaming MIME line checks.
- Gateway bounded same-identity authorizer coalescing needs cancellation/
  cache ownership; 16 forced simultaneous misses cause 16 native invocations.
Larger seam: app-composed Cognito trigger execution using existing Lambda
runtime owner. Registered triggers remain cold; prior five-step median 0.896s.
Cognito decoded-key reuse remains a bounded hot-auth candidate.
Capture actual Sync dominates; group commit is separate design work.

Non-race native measurements passed in one serial SSD lane, including real
Go SDK SSM HTTP, Python/Node workers, SNS/SQS delivery, Firehose and SES.
Runtime API CPU/allocation attribution completed; no validation removal proposed.
All seven fixture owners passed scoped race checks; performance-tag vet passed.
SSM uses one full traversal per case under -short correctness checks; measured
non-race runs retain three. Initial 30s race deadline failed, corrected to 2m.
Fixture review accepted after bounded cursors/unlocked fatal assertions.
Independent report review accepted the metrics and owner/contract proposals.
No full SDK/release rebuild was needed for production-unchanged audit fixtures.

Owned children/listeners/files/profiles joined and removed; no temporary worktree.
Raw audit logs: /tmp/eventbus-audit-round2-*-20261009.log, existing 24h retention.
Initial race failure receipt 52df5492 is quiescent/unpinned, expires Oct10
14:36:28 UTC. Previous release evidence 4eaefff6 expires Oct10 13:43:54 UTC;
S3 fixture failures 779b0793/20a07398 expire 13:26:28/13:27:22 UTC.
Successful SSD scratch removes automatically; shared caches use host policy.
Preserve unrelated laptop Plans edits. Optimization implementation is next work.
