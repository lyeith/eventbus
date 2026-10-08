# EventBus state

Canonical SSD /home/spite/Projects/eventbus, main; public MIT standalone AWS
emulator and agent harness. Plans/application state remains untouched.
Latest: https://github.com/lyeith/eventbus/releases/tag/v0.11.2
Published eight binaries and SHA256SUMS from clean tagged revision 9e1b652.
Uploaded/downloaded hashes match. #29 closed; issues board empty at final check.

#29: top process_error describes the final native launch. Retired Init errors,
native ownership and frozen typed causes are bounded per-launch facts.
Ownership remains cumulative over all launches and the optional collector.
Native outcomes, IDs/Event attempts, Init/Invoke budgets and retries unchanged.
Provided runtimes may reply successfully then be stopped while long-polling;
raw OS Wait detail does not determine native success. See docs/LAMBDA.md.

Linux full race: Lambda 84.689s / localexec 2.077s / devcapture 1.169s PASS.
Native Event/evidence/retained-owner SDK race 34.994s and actual app cleanup
continuation race 1.530s PASS; tagged vet across all packages PASS.
Packaged Linux amd64/macOS arm64: Python/Node default Init timeout → successful
fallback, original request/attempt, private capture and actual group/server joins
PASS. Linux arm64/macOS amd64 cross-built and metadata-checked, not executed.

Overhead fixed/pushed in SSD tooling 796c1dd, active through installed symlinks.
Existing owners reuse exact receipt/cgroup checks and batch fresh storage proofs;
13 broker calls → 3. No root broker/config, restart, dependency or proof cache.
Same-interpreter launcher median 1154ms → 349ms; native UV Init 1161ms → 389ms.
docs/PERFORMANCE-REVIEW.md records source/method, measurements and limits.
All 41 Policy/shell tests passed. Full tooling 18 failures/3 import errors
reproduce at unchanged 26f8468; existing fixture/environment blockers documented.

Owned profiling, packaged-proof, binary and download stages removed on both
hosts; no owned process, environment or worktree retained. SSD evidence has
24h TTL. Two failed tooling receipts have only empty 0KiB scratch directories,
verified quiescent/unpinned; existing GC expiry is Oct 9 13:25/13:29 UTC.
One serial SSD lane used GOMAXPROCS=4/GOFLAGS=-p=2. No application/default DB used.
