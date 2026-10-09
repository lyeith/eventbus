# EventBus state

Canonical SSD /home/spite/Projects/eventbus, main; public MIT AWS emulator
and agent development harness. All second-round performance targets implemented.
Current report: docs/PERFORMANCE-IMPROVEMENTS-ROUND-2.md.
Published binaries remain v0.12.0 (tagged runtime 953ca65); no new release published.

Owners now retain ordered SSM names, publication-local SNS filter inputs,
prepared Firehose timezones, shared SNS/SQS dedup expiry, scanned SES MIME lines,
single result serialization/bounded bodies, gateway joined authorizer flights,
immutable Cognito keys, synchronous capture groups, encoded SQS batches and
bounded async history. Duplicate Cognito Node execution code is removed.

Cognito triggers use the app-composed private Lambda runtime.
Fresh is default; optional trigger dev_warm has separate bounded capacity,
per-pool imports, strict response/privacy policy and retained drain/resume.
Private targets have no public Invoke route; diagnostics are discarded even
under --debug. See docs/CUSTOM-TRIGGERS.md for configuration/source edits.

Observed local results:
- SSM 10k traversal core 3338→29ms, unchanged SDK/HTTP 4397→1424ms.
- SNS 100 body filters/64KiB 124→22ms, allocation 40.4→8.1MB.
- Firehose 500 New York records 10.2→0.70ms.
- Warm Lambda 4MiB Python/Node 44.9/66.9→28.8/37.4ms; allocation ~10→4.2MB.
- Gateway sixteen misses: sixteen→one actual Invoke.
- Capture sixteen producers/64KiB: 312→37ms for 64 records; Sync 64→8.
- Cognito warm five-step median 37ms; first 890ms; paired fresh 936→930ms.
- Async 10k terminal completion 183→3.4µs; snapshot wall time similar.

Final race coverage passed after correcting new fixture startup/undefined
assumptions; private sync-undefined contract regression fixed and covered.
Full SDK race 302.197s, retained SDK/RustFS 28.085s, real Swagger 10.527s.
All-tag vet, five frozen Python fixture tests, formatting/architecture passed.
Eight Linux/Darwin amd64/arm64 binaries built and hashes checked; native help
checks passed. Build outputs removed with owned successful scratch.

No pending implementation target from this audit. No dependencies, application/
default DB, live stack, branch or worktree change. Source commits are on main.
Raw /tmp/eventbus-opt-round2-*-20261009.log uses existing 24h retention.
Failed test receipts 3d134fe1/e5ed6c12/3c325bb6/30b9ae5a are quiescent/unpinned,
expire Oct10 15:01/15:06/15:18/15:22 UTC.
Wrapper finalization failures a33970dd/31f8660f/e257c705 are quiescent; owned
scratch removed. Metadata awaits existing janitor orphan reconciliation after
24h, then failure retention 24h. Successful archives/probes/builds auto-removed.
Shared caches retain host bounds. Unrelated laptop Plans edits preserved.
