# EventBus state

Canonical SSD /home/spite/Projects/eventbus, main. Public MIT standalone emulator
and agent harness. Plans and consuming application state remain untouched.
Latest published: https://github.com/lyeith/eventbus/releases/tag/v0.11.1
All 8 binaries and SHA256SUMS verified after upload/download. #27/#28 closed;
GitHub issue board empty. No dependency changes or current blockers.

Both CLIs refuse positionals before resource construction. Native Python, Node
and provided Init is capped at 10s; readiness starts configured Invoke. One
cleanly joined fallback shares its configured Init+Invoke budget. Command keeps
whole-process timeout. Caller/service/gateway deadlines and SQS leases remain
independent; native request IDs/Event attempts unchanged. Core owns deadlines
and facts; development adapters own the private phase schema.
Darwin group-cleanup EPERM triggers absence probes for at most 1s; persistent
denial stays dirty. Actual native process/resource joins remain required.

Final owner race passed: Lambda 106.864s, localexec 2.067s, cognitotrigger 7.838s,
consumer 11.036s, gateway 40.141s; all tagged vet passed. App/eventsource/native
SDK and retained-stack race checks passed. Native macOS old-owner regression
fails; corrected process-owner race/vet passed. Packaged Linux amd64 and three
complete macOS arm64 proofs passed CLI, cold starts, timeouts, cancellation,
privacy and actual joins. Linux arm64/macOS amd64 cross-built and metadata-
checked only. Release binaries have clean tagged revision 416e309.
The failed v0.11.0 draft was removed; its immutable source tag remains and was
never a published release.

Separate docs/PERFORMANCE-REVIEW.md measured 180 invocations and 80 appends.
Same-interpreter median Python Init: 29.675ms direct, 1160.549ms managed UV.
Durable XFS append median: 3.35–3.92ms. Report records method/source boundaries
and ranked owner investigations; application-chain behavior was outside scope.
Owned probes, source staging, binaries/downloads and native test copy removed.
No owned processes, environments or worktrees retained; SSD evidence has 24h TTL.
