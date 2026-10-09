# Handoff

Canonical SSD /home/spite/Projects/eventbus, main; public MIT EventBus.
Published v0.11.4: https://github.com/lyeith/eventbus/releases/tag/v0.11.4
Clean tagged source 4bac246; Go 1.26.0, CGO_ENABLED=0, module v0.11.4.
Eight binaries + SHA256SUMS; every downloaded asset matched the local build.
All agreed Oct9 audit targets completed and shipped.
No Plans/application data/default Cognito DB touched or dependency files changed.

Owner commits:
- 3778034 Cognito atomic MFA authorization and exact-secret promotion.
- 6ddeacf SQS references, dedup expiry, native deletion and projection owners.
- 99cf493 Firehose prepared jq, retained counts and unlocked snapshot construction.
- 1cf7e63 gateway immutable static mapping/redaction plans.
- 4b68dc9 Secrets canonical index and rotation-generation admission.
- f6f0cd9 Lambda HTTP preparation/version parity.
- 8618ba2 Scheduler capability refusal moved from dispatcher to adapter.
- ee0cc9e production import guard, automatic/scoped CI and verification docs.
- 2f45911 shared output-copy evidence; native result/error/retry and merged pipes.
- 105a441 consumer cancellation, sticky fences and app dependency retention.
- f92de66 frozen real cold-start/native-auth fixtures and measured audit.

Independent reviews accepted Cognito/consumer flows, gateway/Secrets generations,
Firehose counters/snapshot/retry publication, shared output-copy joins and CI.
Reviews found and fixed masked pipe errors, writer-identity ordering/equality,
deadline-first app retention and Secrets delete/recreate during validation.
docs/ARCHITECTURE.md maps common owners; docs/PERFORMANCE-AUDIT.md is evidence.

PASS, saved complete /tmp/eventbus-followup-*-20261009.log:
- Cognito full race 295.735s; earlier package-wide timeout gap is closed.
- Messaging 6.012s / Firehose 7.070s / Secrets 1.409s / architecture 1.045s races.
- Gateway 20.955s / Scheduler 1.592s / dispatcher 2.152s initial races.
- Final localexec 6.175s / consumer 12.222s / triggers 8.866s / Lambda 100.112s /
  app 19.970s / quiescence 1.039s / architecture 1.041s races.
- Full native SDK/retained-stack race 278.385s; Swagger 9.179s.
  Optional older SDK initially skipped, then explicit 1.39.4 proof passed 25.023s.
- Both fresh owned RustFS native Firehose proofs passed; process/group/data joined.
- Vet all packages with sdksmoke/integration/performance tags.
- macOS arm64 localexec race 6.873s; Unix exits/pipes/order executed, Linux-only
  retained FD fault injection excluded by platform.
- Frozen before/after fixtures and actual consumer/trigger/SRP-email-token chains.
- Release-source GitHub CI: actions/runs/37908234098 passed both jobs.
- Actual packaged Linux amd64/macOS arm64: Cognito enrollment/MFA, Node native
  result/merged-log/child joins, REQUEST authorizer and compiled HTTP mappings.
  Other targets checked for clean metadata/checksums, not executed.

Local medians: GZIP admission554ms ->33us, FIFO10k ten deletes1.10ms ->2.54us,
Secrets10k ARN117us ->1.14us, gateway100redactions128us ->58us/636 ->128allocs.
GZIP build/read remains~0.6s; binary projection wall time similar despite fewer
snapshots/allocations. Distributions and limitations are retained in the audit.

Source uses fresh processes; no interpreter/SDK selection, durability or native
management surface change. #30 warm workers is a separate design.
New #31 SES configuration-set SNS events belongs in core; explicit local
outcome injection belongs in a named dev adapter. See ISSUE-TRIAGE.
Native-uv consumer attribution 0.25s vs managed1.20s identifies repeated SSD
project-mode wrapper proofs/digest/workspace work; host tooling follow-up is
separate. PATH differs in that attribution; no production bypass introduced.

Successful test archives, temporary older-SDK environment and private RustFS
data were removed; macOS scratch, release stages/probes/download duplicates removed. No owned active process/worktree/
environment remains. Failed diagnostics verified quiescent/unpinned with 24h TTL:
47266c55 / a396cb0b / 044356a5 expire Oct10 08:16:37 / 08:31:29 / 08:35:11 UTC.
Initial packaged gateway fixture had an unquoted YAML path; corrected fixture
and robust backend cleanup, no source change. Its registered owner was stopped/
verified quiescent and scratch removed. All packaged checks then passed.
Preserve unrelated laptop Plans edits; next work is separately triaged #30/#31
and SSD project-mode launcher optimization, with current native contracts intact.
