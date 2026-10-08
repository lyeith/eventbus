# Handoff

Published latest v0.10.0: https://github.com/lyeith/eventbus/releases/tag/v0.10.0
Tagged clean source06a55d87bb003cf5a9694e12302e7a90812a3c0b, pushed to main.
Eight EventBus/gateway Linux/macOS amd64/arm64 binaries+SHA256SUMS.
All9 uploaded assets downloaded; identical checksum file and all8 hashes PASS.
#25/#26 closed; final open board empty. Plans/application state untouched.

Private cause evidence separates caller cancel/deadline/service stop from own
function timeout, preserving native wire/state/retries and actual identity.
Cause freezes after native cleanup; terminal elapsed includes optional collector
join. Slow capture cannot change native success after process completion.
Default-off Python safe thread frames + loop-owned task await chains use separate
fd4/fd5 JSONL; native fd3 untouched. UV retains duplicate writer descriptors, so
live capture is newline-framed rather than EOF-delimited.100ms helper/250ms read,
32threads/8loops/64tasks/32frames/256UTF8 bytes and64KiB wire. Unknown early cancel,
GIL/custom loops can leave explicit unavailable evidence. Hard kill/join strict.
Docs cover earlier known gateway budgets and private identity/schema filtering.

PASS logs SSD /tmp, existing24h retention:
- eventbus-25-26-focused-final-20261008.log:39.442s.
- eventbus-25-26-all-race-20261008.log: all owners PASS; Lambda73.572s,
  Cognito277.731s/app23.075s/gateway41.539s before final optional-join correction.
- eventbus-25-26-lambda-final-race-20261008.log:75.424s after correction;
  actual gated native success despite later budget/service-close cancellation.
- eventbus-25-26-sdk-race-20261008.log:69.998s actual Lambda/SNS/SQS/Secrets,
  private native evidence, retained barrier and authenticated gateway callbacks.
- eventbus-25-26-vet-20261008.log: full sdksmoke-tagged vet PASS.
- eventbus-python-stack-live-framing-probe-20261008.log: actual UV writer proof.
- eventbus-v0.10.0-{linux,macos}-packaged-20261008.log: live registered ASGI
  receive frames/privacy, native timeout, caller cancel, fast payload,0600/join.
- eventbus-v0.10.0-build-20261008.log and metadata-20261008.log: all8 Go1.26/
  CGO0, v0.10.0, expectedplatform/clean source. Other two platforms cross-built;
  only Linuxamd64/macOSarm64 executed natively.
- eventbus-v0.10.0-download-verify-20261008.log: all8 uploadedbinary hashes PASS.
Earlier focused logs retain corrected fixture timing/header, stale decoder reason
and EOF framing failures. Final focused/race/SDK/package evidence supersedes them.

Cleanup complete: native test/probe listeners and children joined; release/
download/laptop staging and all probe sources removed. No dependencies, task
venvs/worktrees/branches added. Shared SDK/tool caches preserved.
Disposable macOS proof removed after copying to SSD24h evidence.
Source/artifacts durable on GitHub; no active test/build lane or unfinished work.
