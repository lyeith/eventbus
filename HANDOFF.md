# Handoff

Published https://github.com/lyeith/eventbus/releases/tag/v0.11.1 from clean
416e309 source. All 8 CGO0 binaries and SHA256SUMS verified after upload/download.
#27/#28 closed; issue board empty. Canonical SSD main; no dependencies added.
Plans/application state and shared frozen SDK caches remain untouched.

Both CLIs reject positional input before runtime effects. Native Init is bounded
separately from configured Invoke. A cleanly joined fallback shares configured
Init+Invoke budget and keeps the original RequestID/Event attempt. Caller/service
cancellation and dirty ownership prevent retry; command keeps whole-process
budget. Managed fd6/fd7 READY/ACK remain distinct from native result fd3 and
optional stack fd4/fd5. Provided /next selects the effective deadline. Core owns
causes/facts; development adapters own the private schema. Python collector
follows the current phase and joins before terminal evidence.

Shared localexec fixes Darwin negative-group SIGKILL EPERM for unreaped exited
children. Signal0 probes accept only ESRCH within 1s; persistent denial or a live
group stays dirty. No extra kill signals or skipped Wait joins. The native
regression fails with old code and passes the correction. Failed v0.11.0 draft
and assets removed; immutable source tag preserved, never publicly released.

PASS logs on SSD /tmp under existing 24h TTL (eventbus- prefix):
- issue27-cli-race: parser/startup guard, app 2.497s/gateway 2.128s; issue27-vet.
- issue28-lambda-race: 101.852s; phase-channel-race: 3.522s, protocol/dirty joins.
- issue27-28-consumers-race: app 24.922s/gateway 39.356s/eventsource 1.431s.
- issue28-native-sdk-final-race: Lambda 23.812s/native SDK and retained 40.624s.
- issue27-28-owners-final-race: Lambda 106.864s/localexec 2.067s/
  cognitotrigger 7.838s/consumer 11.036s/gateway 40.141s; owners-final-vet all tags.
- macos-group-native-old-regression: expected EPERM failure; native-race 2.614s
  and native-vet pass; localexec-linux-race 2.075s and vet pass.
- v0.11.1-build/metadata-20261008: Go 1.26/CGO0, four platforms, clean tag.
- v0.11.1-linux-packaged-20261008: CLI, three cold runtimes, actual timeouts,
  caller cancellation, private evidence, gateway, child/listener joins and SIGINT.
- v0.11.1-macos-packaged-20261008: three complete native arm64 runs passed.
- v0.11.1-downloaded-checksums-20261008: eight hashes and matching manifest pass.
Linux arm64/macOS amd64 were cross-built and metadata-checked, not executed.

Separate performance review: 180 invocations/80 appends; non-race Lambda 47.402s,
capture 0.168s. Groups/listeners/sinks joined; 20 actual handler snapshots.
Report records first/min/median/max, same interpreter, managed XFS, source
boundary and ranked unmeasured investigations. README links the review.
Application-chain behavior was outside this audit; no optimization was applied.

Cleanup complete: owned source/test probes, packaged scripts, failed/corrected
binary/download/metadata staging and duplicate laptop logs removed. No owned
process, environment or worktree remains; shared SDK caches preserved. Native
failure and final proofs retained only under SSD's existing 24h evidence TTL.
