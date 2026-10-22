# EventBus state

Canonical SSD /home/spite/Projects/eventbus, main; public MIT AWS emulator
and agent development harness. #43 and #44 source changes are complete.
Latest published binaries remain v0.18.0:
https://github.com/lyeith/eventbus/releases/tag/v0.18.0
The new flags below require the updated source; no binary release in this pass.

#43 final shutdown:
- Positive --shutdown-timeout controls ordinary and retained final joins;
  default 30s and graceful/discard-pending behavior are preserved.
- Lambda dev metadata freezes before permanent abort, at most16 native IDs
  with state/attempt/known retry time and outstanding/reservation/omitted counts.
  App logs it only for a final async timeout, without payload/env/error/output.
- Retry timings and per-request HTTP deadlines are unchanged. Final timeout
  still cancels/joins work and exits nonzero; pending Events cannot survive restart.
  Retained /quiesce remains resumable and preserves accepted work on timeout.

#44 Firehose:
- --s3-access-key/--s3-secret-key/--s3-session-token are independent of endpoint.
  Defaults remain test/testtest123/empty; empty access or secret fails parsing.
- App selects an immutable credential value. Existing AWS SigV4 signs real
  object bytes and optional session token; no provider chain or role assumption.
- Owned HTTP client refuses redirects. Failed PUTs retain the prepared object
  for retry at the original endpoint without forwarding credentials or body.
- Existing static constructor is preserved. No dependency added.

Verification:
- Full affected race: Lambda188.171s, Firehose9.307s, app154.283s,
  gateway29.725s, eventsource1.940s, messaging7.599s, devquiescence1.076s,
  architecture1.188s. All-tag integration/sdksmoke vet and diff checks passed.
- Real ordinary CLI third native attempt starts >30s after SIGTERM, with the
  original request/payload, overlapping active work and live SDK/JWKS callbacks.
- Retained final timeout fails with exact pre-abort metadata/ServiceShutdown;
  separate /quiesce timeout recovers to held before successful final stop.
  Existing disposable/business-failed/default lifecycle cases remain in full gates.
- Independent HMAC authenticates actual S3 bytes/custom/session/legacy settings;
  redirect301/302/303/307/308 never forward and retained same-key retry succeeds.
- Real CLI SDK ingestion flushes to a held S3 receiver on SIGTERM; exact selected
  secret authenticates Authorization; no secret/token in health/readback/logs.
- Independent ownership, privacy and docs review clean.

Successful managed source owners quiescent and scratch automatically removed.
Corrected fixture failures retain raw evidence under existing24h unpinned policy;
disposable failed scratch removed after inspection. No task worktrees, branches,
environments, application/default-state changes or new dependencies. Laptop
Plans untouched. Packaged binaries and full frozen SDK lane were not run locally;
CI covers the complete SDK lane on push.
