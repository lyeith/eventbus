# Handoff

Preparing v0.11.0 on canonical SSD main; latest published v0.10.0.
#27/#28 implementation/verification complete; packaging/publication pending.
Plans/application state untouched; no dependency/environment/worktree added.

Commits:6e53ceb rejects CLI positionals before DB/capture/listener construction;
24de500 owns native Init/readiness/Invoke budgets and strict joined fallback;
e50418d keeps the independent performance audit/opt-in measurements separate.
Init10s, configured Invoke after readiness, one fallback sharing configured
Init+Invoke timeout; original request/native Event attempt stable. Command
whole-process budget retained. Parent/caller/service cancellation stays final;
dirty cleanup prevents fallback. Core owns causes/facts, dev owns JSON schema.
Managed fd6/fd7 READY/ACK preserve resultfd3 and optional stackfd4/fd5.
Provided first /next selects deadline; Init errors fenced before delivery.
Snapshot deadline follows current phase, one collector per native attempt.

PASS logs SSD /tmp, existing24h TTL:
- eventbus-issue27-cli-race.log:app2.497s/gateway2.128s; issue27-vet.log.
- eventbus-issue28-integration-focused.log:Lambda38.535s before review fixes.
- eventbus-issue28-lambda-race.log:101.852s all Lambda.
- eventbus-issue28-phase-channel-race.log:3.522s, forged result/protocol faults,
  live retained-ready framing, original import error and dirty no-retry proof.
- eventbus-issue27-28-consumers-race.log:app24.922s,gateway39.356s,eventsource1.431s.
- eventbus-issue28-native-sdk-final-race.log:Lambda23.812s after final private
  schema owner extraction; native SDK/retained consumers40.624s.
- eventbus-issue27-28-vet-final.log:all sdksmoke+performance-tagged packages.
- eventbus-performance-lambda-20261008.log:non-race47.402s,180 invocations.
- eventbus-performance-capture-20261008.log:non-race0.168s,80 durable/discard appends.
Separate report records source/measurement boundary, exact first/min/median/max,
XFS, same-interpreter attribution and unmeasured owner follow-ups; no speedup
or consumer-chain success claimed. No runtime optimization expanded into task.

Next: tag/build all8 artifacts; inspect clean source/platform/version metadata;
run owned packaged proof Linuxamd64/macOSarm64; upload draft/download/checksums;
publish/close #27/#28, update state/triage, prune owned outputs/script/staging.
Owned temporary authoring:/tmp/eventbus-issue28-root (laptop); packaged script
/tmp/eventbus-issue28-packaged-owned (both hosts). No owned processes left.
