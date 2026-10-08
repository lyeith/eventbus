# Handoff

Published latest v0.9.0: https://github.com/lyeith/eventbus/releases/tag/v0.9.0
Tagged clean source c563e37d9b185b5f3e467b673d5581cff25568bf, pushed to main.
Public assets:8 EventBus/gateway Linux/macOS amd64/arm64 binaries+SHA256SUMS.
All9 uploaded assets downloaded; checksum file identical and all8 hashes PASS.
#24 closed; final open board empty. Plans/application state untouched.

#24 adds explicit trusted loopback gateway continuations with normal native
routes/REQUEST auth/Invoke. Public roots remain fenced. Atomic ledger admission
requires actual accepted work in all states, preventing parent-finish races.
Counts precede body read and cover handler return; frozen owner/gen, kind-bound
replay and shared ACK reconciliation keep uncertainty strict.
Idle open/held, transition-only/resuming, stale/foreign/dirty requests refuse.
Native accepted shutdown descendants remain live until counts/cleanup join;
monitor starts its bounded sticky deadline only after observing Shutdown.
Normal return confirms completion; Goexit/panic stays dirty.
CLI binds all peers first, rolls back partial binding and drains both listeners
before shared close. Explicit continuation flag0 disables a YAML-selected port.
Docs state endpoint roles/exclusive trust and callback issuer/JWKS adoption.

PASS logs SSD /tmp, existing24h retention:
- eventbus-24-app-focused-20261008.log: actual declared cleanup0.584s.
- eventbus-24-owners-race-20261008.log: coordinator1.048s/gateway39.652s/
  app23.523s/cmd1.066s; includes all existing affected-owner suites.
- eventbus-24-sdk-race-20261008.log:5.635s; actual registered A/typed cleanup/
  shutdown→native IAM authorizer→B, real Cognito token/cold callback JWKS,
  forged signature/wrong signed audience refusal and final child joins.
- eventbus-24-vet-20261008.log: scoped tagged owners+SDK PASS.
- eventbus-v0.9.0-{linux,macos}-packaged-20261008.log: private listener/prebody
  lease/public fence/native error join/explicit0 overrides, clean SIGTERM joins.
- eventbus-v0.9.0-tagged-build-20261008.log and
  eventbus-v0.9.0-final-metadata-20261008.log: all8 Go1.26/CGO0 expected
  platforms, modulev0.9.0, tagged SHA and vcs.modified=false.
SDK fixture owns no app declaration policy; separate production run proof uses
the actual configured declaration and authenticated native integration.
Initial focused parent-finish regression failed before the final atomic guard;
the final full race passed. First pretag build had a pseudoversion; discarded
and rebuilt under v0.9.0 before final proofs/upload. No runtime code changed.

Cleanup complete: native fixtures/listeners/children joined, release/download/
laptop binary staging and both probe copies removed. No dependencies or task
environments/branches/worktrees created; shared SDK/tool caches preserved.
Disposable macOS evidence removed after copying proof to SSD24h logs.
Source/artifacts durable in GitHub. No active work or test/build lane.
