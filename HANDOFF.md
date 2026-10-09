# Handoff

Canonical SSD /home/spite/Projects/eventbus, main. Published runtime remains
v0.12.0 (tagged source 953ca65); no release change in this audit round.
Previous tickets #30/#31/#32 remain implemented; see Git/release documentation.

Completed second performance audit against main 1a842d1:
docs/PERFORMANCE-AUDIT-ROUND-2.md leads with ranked remaining work and gives
native measurements, exact owner boundaries, required contracts and reproduction.
README links current and historical reports. Seven performance_round2_test.go
files belong to devcapture, firehose, gateway, lambda, messaging, ses and ssm.
Production source and dependencies are unchanged.

Strongest bounded targets:
1. SSM ordered-name index/prefix-cursor selection: 10k SDK traversal 4.397s,
   core 3.338s. Each current ten-item page rescans/sorts all names.
2. SNS immutable filter inputs per publication: 100 destinations/64KiB body
   124.020ms and 40.446MB allocated vs 31.310ms/7.796MB without filters.
3. Firehose stream-owned prepared location: 500 records UTC 1.545ms,
   Singapore 6.630ms, New York 10.214ms.
Then: Lambda warm encode-once/bounded response capacity; SNS FIFO expiry;
SES MIME line-index removal; gateway cached-identity in-flight coalescing.
Gateway 16 forced misses caused 16 actual authorizer invocations; fresh/warm-
cap-one bursts 719.401/70.697ms. This is controlled structural evidence.
Do not equate separate workload timings or claim unmeasured speedups.

Larger seam: Cognito triggers still launch Node each step (prior five-step
median 895.680ms). App should compose a consumer-owned execution port into
the trigger adapter backed by the existing Lambda managed execution owner.
Preserve trigger-specific privacy, strict event validation, deadlines/output
caps and actual joins; avoid a second pool or peer-package imports.
Cognito key decoding remains uncached; immutable pool keys need deletion/
generation invalidation and final account/revocation checks.
Capture throughput 195–235 durable records/s on owned XFS is Sync-limited;
more producers mostly add waiting. Any group commit must await actual durable
completion and retain sticky failures, order, drain/Close and evidence.

Verification:
- Non-race owner measurements: messaging 14.342s, Firehose 0.531s,
  Lambda 10.632s incl isolated benchmarks, SSM 23.398s, gateway 5.800s,
  capture 1.991s, SES 1.514s. All native assertions passed.
- Runtime API 4MiB attribution: ReadAll 90.71% sampled allocation space;
  required JSON validation 73.28% cumulative CPU. Profiles removed.
- Scoped new-fixture races: capture 4.422s, Firehose 3.137s, gateway 9.296s,
  Lambda 60.725s, messaging 110.069s, SES 20.455s.
- Initial SSM race 30s SDK deadline failed; instrumentation magnified known
  store cost. -short now runs one full traversal, bounded 2m context and exact
  page-count guards; all sizes/paths passed in 51.332s.
- Final FIFO fixture race 19.067s after moving fatal assertions outside locks.
- Performance-tag vet across all seven owners and formatting/diff checks passed.
- Independent fixture/report review accepted after bounded failure paths and
  precise warm-serialization/store-timing/native-deny-cache wording.
- Full SDK suite/release rebuild not rerun: production behavior unchanged.

Cleanup: private fixtures, actual children/listeners and profiles joined/removed;
no temporary worktree, dependency, default DB or live stack change.
Raw logs in /tmp/eventbus-audit-round2-*-20261009.log have existing 24h retention.
Failure receipt 52df5492 verified quiescent/unpinned; expiry Oct10 14:36:28 UTC.
Previous release receipt 4eaefff6 expires13:43:54 UTC; S3 failures 779b0793/
20a07398 expire 13:26:28/13:27:22 UTC on Oct10. Success scratch auto-removes.
Preserve unrelated laptop Plans changes. Remaining optimizations are proposals,
not implemented behavior; this audit's requested work is complete.
