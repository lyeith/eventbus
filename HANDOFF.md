# Handoff

Canonical SSD /home/spite/Projects/eventbus, main. All second-round performance
targets implemented; report docs/PERFORMANCE-IMPROVEMENTS-ROUND-2.md.
Published v0.13.0 as latest; tag points to verified clean source b97996f.
https://github.com/lyeith/eventbus/releases/tag/v0.13.0

Core changes:
- SSM sorted index and cursor/prefix selection with hierarchy/mutation tests.
- SNS lazy publication filter decoding and earliest FIFO expiry; expiry
  mechanics shared with SQS, native acceptance/sequence rules remain separate.
- Stream-owned Firehose timezone used by admission/build/error/retry paths.
- SES scanning preserves line accounting without allocating a line index.
- Lambda successful results encoded once; awsprotocol owns bounded length-hint
  reads with oversize/read-error checks. Native callback behavior preserved.
- Cognito decoded immutable keys, detached exports, generation outside mutex,
  cancellation retry/deletion rollback/generation fencing/Close joins.
- Gateway bounded same-identity flights; each current ARN evaluated, last
  cancellation and shutdown join; native TTL-zero/overflow path retained.

Execution/evidence seams:
- Cognito event/response/deadline port composed by app into private Lambda
  instance. Duplicate Node wrapper/process owner deleted. Fresh default,
  separately capped warm option, private error/log limits, per-pool imports,
  strict validation before reuse, retained drain/resume. Debug logs suppressed.
- Capture bounded synchronous group commit: complete writes+covering Sync before
  owned-file success, prefix Err/draining Close, sticky partial/Sync failures,
  panic/Goexit cleanup. No background worker or delay.
- SQS exact candidate encoding reused through immutable Batch payload; all app,
  eventsource and tagged SDK consumers migrated. Original custody/ACK intact.
- Async terminal ring; detached snapshots sort outside Service.mu.

Verification:
- Full Go race run passed all unchanged owners including Cognito 342.741s,
  gateway 17.813s, messaging 6.608s and architecture. New app deadline fixtures
  were corrected to actual startup/warm gates; full app rerun 38.740s passed.
- Lambda new serialization fixture corrected to Promise-resolved undefined;
  final affected suite 14.167s passed; full remaining Lambda tests passed earlier.
- Independent review caught private sync-undefined becoming timeout; shared
  private hook restored direct-return/Promise contract with fresh/warm proof.
- Full SDK race 302.197s incl fresh/warm auth and unchanged handlers.
- Full retained SDK/RustFS 28.085s; unchanged Express/Swagger 10.527s.
- All-tag vet passed; Python fixture contracts: five tests passed; gofmt/diff clean.
- Serial non-race measurements passed. Initial Python perf omission was
  explicitly rerun with frozen interpreter; earlier Node/bench results retained.
- Baseline archives resolved snapshot/cold timing variance. Snapshot totals
  comparable; cold trigger paired 936→930ms, warm steady 34–40ms.
- Release rebuild: eight CGO-free binaries, clean b97996f metadata, Go 1.26.0.
- Packaged Linux amd64/macOS arm64: warm Lambda serialization/reuse, private
  warm Cognito SRP/custom auth, SES capture and joined shutdown passed.
- Initial disposable smoke assumed non-SRP custom auth; corrected to the native
  SRP contract using the unchanged SDK arithmetic helper. No source fix needed.
- All nine uploaded assets downloaded; SHA256SUMS and all binaries match.
- GitHub race/vet and SDK/Swagger CI 37955422569 passed.

Report includes caveats: first warm imports, separate capacity, unchanged
single-producer Sync counts, host timing variation, detached RSA export cost
0.65–0.68ms while native crypto paths borrow cached immutable keys.

Cleanup: owned children/listeners/archives/build artifacts removed; no worktree,
dependency, default DB or live-stack change. Raw logs have existing 24h expiry.
Failure receipts 3d134fe1/e5ed6c12/3c325bb6/30b9ae5a quiescent/unpinned,
expire Oct10 15:01/15:06/15:18/15:22 UTC.
Host wrapper finalization errors a33970dd/31f8660f/e257c705 followed completed
Go/doc work; quiescent trees and disposable scratch removed, orphan metadata
reconciles under existing 24h+24h policy. No host framework changes.
No pending audit implementation. Preserve unrelated laptop Plans edits.

Release build/downloads and laptop copies removed; owned package processes
joined. Release logs use /tmp/eventbus-release-v0.13.0-*-20261010.log (24h).
