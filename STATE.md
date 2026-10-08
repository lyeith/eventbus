# EventBus state

Canonical SSD /home/spite/Projects/eventbus, main. Public MIT standalone emulator
and agent harness; Plans and consuming application state are untouched.
Latest public release: https://github.com/lyeith/eventbus/releases/tag/v0.10.0
Clean tagged source:06a55d87bb003cf5a9694e12302e7a90812a3c0b.
#25/#26 are published and closed; the final open issue board is empty.

#25 private diagnostics distinguish caller cancel/deadline, service stop and own
function timeout. Original native wire/state/retries remain compatible; early
external cancellation omits misleading legacy function_diagnostic. Cause freezes
at native cleanup; terminal elapsed includes the joined private collector.
#26 default-off Python wait snapshots use separate bounded JSONL descriptors,
private0600 sink and original request/attempt. Explicit/scheduled pre-stop capture
retains only frame locations and safe thread/task state. Actual kill never waits
for evidence. Missing evidence is explicit; pipe/sink ownership failure is dirty.
Docs give earlier-gateway snapshot_after setup and supported/unavailable cases.

PASS: full Go race; final Lambda race75.424s after sole optional-join fix;
actual native SDK/retained consumer race69.998s; full sdksmoke-tagged vet.
Gated capture proves native success survives subsequent budget/service cancel
while its private join still holds ownership. Linuxamd64/macOSarm64 packaged
ASGI/privacy/native timeout/caller cancel/fast result/privatepermissions/join PASS.
Other platforms cross-built/metadata-inspected. All8 Go1.26/CGO0 binaries have
v0.10.0/expected platform/clean tagged revision. All9 uploaded assets downloaded;
identical SHA256SUMS and every binary checksum verified before publication.

No dependencies changed. No task environment/worktree/branch created.
Owned processes/listeners joined; release/download/laptop staging and probes
removed. Shared caches retained; SSD evidence follows existing24h expiry.
No active test/build lane or unfinished task remains.
