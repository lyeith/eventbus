# Handoff

#25/#26 implementation accepted on canonical SSD EventBus main.
Latest public release remains v0.9.0; next step is v0.10.0 packaging/publication.
Plans/application state untouched. No dependency, environment or worktree added.

Private termination evidence separates caller cancel/deadline/service stop from
own function timeout, preserves native wire/state/retries and original identity.
Cause freezes after actual native cleanup; terminal elapsed includes optional
collector join. Gated collector latency cannot change already joined success.
Python capture is default-off, safe thread locations + loop-owned native task
await chains, separately framed fd4/fd5 JSONL; fd3/native outputs stay unchanged.
UV retains duplicate writer descriptors, so live capture must use newline framing
rather than EOF. Collector100ms/read250ms bounds;32threads/8loops/64tasks/32frames,
256UTF8-byte identifiers and64KiB wire. Unknown early cancel/GIL/custom loops can
leave explicit unavailable evidence. Actual kill and ownership joins stay strict.

PASS evidence SSD /tmp, existing24h retention:
- eventbus-25-26-focused-final-20261008.log:39.442s.
- eventbus-25-26-all-race-20261008.log: all Go owners PASS, Lambda73.572s,
  Cognito277.731s/app23.075s/gateway41.539s; initial join policy then corrected.
- eventbus-25-26-lambda-final-race-20261008.log:75.424s, sole optional join and
  gated post-native-budget/service-close success regression included.
- eventbus-25-26-sdk-race-20261008.log:69.998s actual Lambda/SNS/SQS/Secrets,
  private native evidence, retained barrier and authenticated gateway callbacks.
- eventbus-25-26-vet-20261008.log: full sdksmoke-tagged vet PASS.
- eventbus-python-stack-live-framing-probe-20261008.log: actual UV retainedfd5,
  live safe frames + unchanged native output, all scratch/processes cleaned.
Earlier focused logs show corrected fixture timing/header errors, decoder default
reason and EOF framing failures; final focused/race/SDK evidence supersedes them.

Parent owns serial SSD Go lane, GOMAXPROCS4/-p2; currently idle.
Parent staging /tmp/eventbus-25-26-parent has disposable source copies and the
stdlib packaged proof. Build tag first so binaries carry actual semver and clean
source metadata. Verify Linuxamd64/macOSarm64 and download all public assets.
After publication close issues, rewrite state/triage and remove owned staging.
