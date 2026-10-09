# EventBus state

Canonical SSD /home/spite/Projects/eventbus, main; public MIT standalone AWS
emulator and agent harness. Plans/application data remains untouched.
Current public release: https://github.com/lyeith/eventbus/releases/tag/v0.11.4
Eight CGO-free binaries + SHA256SUMS; downloaded assets match local builds.
Clean tagged source 4bac246; Go 1.26.0, module v0.11.4.
All Oct9 bounded performance/ownership fixes are shipped; main adds this handoff.

Owners now enforce:
- Cognito atomic exact-secret MFA promotion with current authorization admission.
- Contextual consumer polling; sticky cleanup faults fence launches and retain
  app SQLite/capture dependencies after actual shutdown joins.
- localexec output-copy evidence independent of exit/cancellation errors;
  merged pipes and native Lambda/trigger result/retry policies preserved.
- SQS removed-reference clearing, scoped dedup expiry, shared current deletion
  and field/MD5 projection; Lambda avoids discarded receive snapshots.
- Firehose prepared jq and immutable flush snapshots; quotas count construction,
  retries and pending delivery; compression runs outside admission locks.
- Gateway immutable mapping/redaction plans; Secrets exact ARN index and
  rotation generation binding before external validation.
- Lambda shared HTTP preparation; Scheduler owns group capability refusal.
- Production import-boundary guard; PR/main CI with explicit Python/Node setup
  and SDK/Swagger checks scoped to their owner.

Fixed before/after fixture medians (local milliseconds):
- Firehose admission during 32MiB GZIP: 553.901 -> 0.032993.
  Build/read remains about 0.6s; no compression throughput gain claimed.
- FIFO 10k unique send: 0.146274 -> 0.016211;
  ten strict deletes: 1.096823 -> 0.002540.
- Gateway 100 redactions/12 mappings: 0.127608 -> 0.058293;
  allocations 636 -> 128/op.
- Secrets 10k canonical ARN GetValue: 0.116544 -> 0.001143.
- Lambda binary projection: similar wall time, fewer detached copies/allocations.
Full distributions, workloads and caveats: docs/PERFORMANCE-AUDIT.md.

PASS: full Cognito race 295.735s (prior gap closed); messaging/Firehose,
gateway/Secrets, Scheduler/dispatcher, final full localexec/consumer/trigger/
Lambda/app/quiescence/architecture races. Tagged all-package vet passed.
Full SDK/retained stack race 278.385s; unchanged Swagger 9.179s;
older unmodified boto3/botocore 1.39.4 mapping proof 25.023s.
Both owned RustFS Firehose integrations passed; native process/group joined.
macOS arm64 localexec race passed 6.873s; Linux FD fault tests are Linux-only.
Release-source GitHub CI passed: actions/runs/37908234098.
Actual Linux amd64/macOS arm64 packaged Cognito MFA, Node merged logs/results/
child joins, Lambda REQUEST authorizer and compiled HTTP mappings passed.
Linux arm64/macOS amd64 cross-build metadata/checksums passed; not executed.

Cold native consumer/trigger/SRP-email-token fixtures passed with fixed
interpreter/environment, actual effects and process joins. No warm pool added.
Managed consumer total median 1.20s vs native-uv attribution 0.25s;
repeated project-mode ownership/workspace/digest work belongs to ssd-dev-tools.
#30 opt-in warm Lambda workers remains a separate runtime design.
New #31 SES configuration-set SNS events is native core functionality;
explicit local outcomes belong in a dev adapter. Triage records both follow-ups.

No default DB/live stack/new retained environment/worktree touched.
Successful private archives/legacy SDK environment/RustFS data were removed;
macOS scratch, release stages, probes and download duplicates removed. Failed runs verified quiescent/unpinned, existing 24h TTL:
47266c55 / a396cb0b / 044356a5 expire Oct10 08:16:37 / 08:31:29 / 08:35:11 UTC.
One serial SSD test/build lane; no owned active test process remains.
