# Handoff

Canonical SSD main. Published v0.11.2 from clean 9e1b652:
https://github.com/lyeith/eventbus/releases/tag/v0.11.2
Eight Go 1.26.0/CGO0 binaries and SHA256SUMS verified after upload/download.
#29 closed; issues board empty at final check. No dependencies added.
Plans/application state and shared frozen SDK caches preserved.

Source c36ce1c: top process detail belongs to final launch; bounded per-launch
facts retain retired errors, native ownership and raw frozen cancellation pairs.
One adapter classifies causes. Top ownership is cumulative over native launches
and optional collector. No native policy/identity/outcome change.
docs/LAMBDA.md defines additive v1 fields and older-record limitations.
Real managed success/final process+handler failure, Event identity, cancellation,
dirty first cleanup/no retry, truncation and late cancellation are covered.
Provided success followed by deliberate long-poll shutdown preserves its raw
OS Wait detail; consumers use native state/ownership, not free-form error text.

PASS SSD /tmp logs, existing 24h lifetime:
- eventbus-29-race-20261008: Lambda 84.689s/localexec 2.077s/devcapture 1.169s.
- eventbus-29-native-consumers-20261008: native Event/evidence/retained-owner
  SDK 34.994s; actual configured cleanup/gateway continuation 1.530s.
- eventbus-29-vet-20261008: all packages, sdksmoke/integration/performance tags.
- eventbus-v0.11.2-{build,metadata,local-checksums,downloaded-checksums}-20261008.
- eventbus-v0.11.2-{linux,macos}-packaged-20261008: both real Python/Node
  default 10s Init timeout → successful fallback; exact native request/attempt,
  private phase facts, actual process/group absence and joined server shutdown.
Linux arm64/macOS amd64 cross-built/metadata-checked, not executed.
The consumer selection ran no eventsource tests; no claim for that package.

Proper overhead owner: ssd-dev-tools 796c1dd, pushed and immediately active through
installed symlinks. Three fresh storage queries replace 13; exact owner/root/cache
checks retained, no proof cache or root broker/config/dependency/restart.
Launcher median 1154ms → 349ms over 10 samples per side. Native UV Init median
388.942ms over 20, prior 1160.549ms. Runtime fixture passed 100 joined invocations
in 11.872s. Performance report cf292c8 records frozen interpreter, timing noise,
fixed ordering and limitations; no app import/business-flow or p99 claim.
Tooling 41 Policy/shell tests and storage/operation/bootstrap cases pass.
Full suite 898 tests: 18 failures/3 import errors/23 skips. Clean 26f8468
reproduces all 18+3; fixture fields, umask/NoNewPrivs assumptions and absent pytest
documented. No unrelated tooling fixes or added dependency.

Cleanup: owned profiling/source/packaged helpers, binaries/download stages and
duplicate laptop evidence removed. Native children, servers and all operations
joined. No owned environment/worktree retained. Saved SSD logs have 24h TTL.
Two failed tooling receipts are quiescent/unpinned with empty 0KiB scratch; the
existing owner has no per-run purge. GC eligible Oct 9 13:25/13:29 UTC.
No application/default DB or running stack was exercised or reset.
