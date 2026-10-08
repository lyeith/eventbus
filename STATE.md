# EventBus state

Canonical SSD /home/spite/Projects/eventbus, main. Public MIT standalone emulator
and agent harness; Plans/application stacks/databases are untouched.
Latest public release: https://github.com/lyeith/eventbus/releases/tag/v0.9.0
Clean tagged source: c563e37d9b185b5f3e467b673d5581cff25568bf.
#24 is published and closed; final open issue board is empty.
Earlier #18–#23 shipped in v0.8.0.

Optional retained_owner_continuation_port / --retained-owner-continuation-port
adds a separate exclusively trusted 127.0.0.1 gateway listener.
Public requests remain roots. Native REQUEST auth and Invoke are unchanged.
Shared leases atomically require actual accepted work, owner/generation and
healthy evidence before body read/native routing. Idle open/held, transitions,
stale generation and uncertainty refuse. No public header changes the lane.
Kind-bound replay shares the bounded no-expiry ledger. Only normal handler return
confirms completion; panic/Goexit remain dirty. Accepted shutdown chains keep
private peers alive through join or a bounded sticky failure.
Apps configure existing HTTP and callback issuer/JWKS bindings; no Trust import.

PASS: full affected-owner race (coordinator1.048s/gateway39.652s/app23.523s/
cmd1.066s), actual native SDK/JWT/cold JWKS race5.635s, scoped tagged vet.
Production configured cleanup→authenticated gateway→native integration PASS.
Packaged Linuxamd64/macOSarm64 private/prebody/fence/explicit0 proofs PASS.
Other two platforms cross-built/metadata-inspected. All8 Go1.26/CGO0 assets carry
v0.9.0/expected platform/clean tagged source. All9 uploaded assets downloaded and
verified against identical SHA256SUMS before publication.

No dependencies changed. Owned processes/listeners joined; release/download/
laptop staging and probe scripts removed. No task environments/worktrees/branches.
Shared caches retained; SSD complete logs expire under existing24h policy.
No work or test/build lane remains active.
