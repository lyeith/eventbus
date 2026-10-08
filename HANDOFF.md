# Handoff

Latest published v0.8.0: https://github.com/lyeith/eventbus/releases/tag/v0.8.0
Tickets18–23 closed; #24 implemented/verified, publication pending.

#24: optional private loopback gateway continuation listener, shared native
auth/routes and remote source ledger. Source roots remain fenced.
Coordinator atomically requires actual accepted work in every permitted state;
a parent finishing after precheck can no longer grant its continuation.
Owner/gen frozen before RPC, kind-bound replay, existing ACK reconciliation.
Idle open/held, transition-only/resuming, stale/foreign/dirty requests refuse.
Native accepted descendants remain available during shutdown; monitor starts a
bounded sticky join-failure deadline only after observing Shutdown.
Normal handler return alone confirms completion; Goexit/panic stays dirty.
CLI binds both listeners before serving, rolls back partial binding and drains/
joins both before shared owner close; explicit continuation flag0 disables YAML.

PASS logs SSD /tmp under existing24h retention:
- eventbus-24-coordinator-focused-20261008.log (initial policy0.012s).
- eventbus-24-cli-focused-20261008.log (0.049s).
- eventbus-24-app-focused-20261008.log (actual declaration0.584s).
- eventbus-24-gateway-focused-20261008.log: initial race reproduced parent-finish
  grant; fixed by authoritative core guard, then full race passed.
- eventbus-24-owners-race-20261008.log: coordinator1.048s/gateway39.652s/
  app23.523s/cmd1.066s.
- eventbus-24-sdk-race-20261008.log:5.635s; actual registered A/typed cleanup/
  shutdown→native REQUEST IAM authorizer→B, genuine Cognito token/cold callback
  JWKS, forged signature/wrong signed audience refusal, final child joins.
SDK fixture adds no declaration policy; production app proof uses the actual
configured cleanup declaration plus authenticated native integration.
Source/static diff, bounded ownership review and scoped tagged vet PASS.

Next: commit/push, build clean-source v0.9.0 eight-platform-pair assets,
run Linuxamd64/macOSarm64 packaged CLI private/prebody/public-fence/explicit0 proof,
verify uploaded checksums, publish and close24. Update final state/triage then
remove owned probe/staging. No deps/temporary branches/worktrees/env added.
Disposable probe /tmp/eventbus-24-packaged-probe.py awaits release binaries.
No test lane remains active; release work is next. Plans and application state untouched.
