# EventBus state

Canonical SSD /home/spite/Projects/eventbus, main; public MIT AWS emulator
and agent development harness. #39 is released in v0.16.0.
https://github.com/lyeith/eventbus/releases/tag/v0.16.0
Eight Linux/macOS amd64/arm64 service/gateway binaries plus SHA256SUMS are public.
Annotated tag points to clean source92dd784506b99af24189c0db88b7e87628981f35.

#39 exact worker crash:
- Public recipe dev_worker_crash.enabled defaults false.
- GET /__eventbus/dev/lambda/invocations exposes redacted actual active identities
  and existing async event snapshots. PUT/DELETE /invocations/<id>/worker-crash
  select registered function/alias, existing native request ID and exact attempt.
- Pending selection waits for actual launch/leased worker; active selection kills
  only its runtime through existing localexec cancellation/cleanup ownership.
- Native response/exit and hard deadline boundaries serialize selection.
  Completed workers cannot be reopened through diagnostic or warm reuse gaps.
- Nonlaunch throttle retains the same upcoming task arm; pending clear works
  even while the old diagnostic lifetime is still active. Duplicate/raced/stale/
  foreign/terminal selections are refused. No synthetic admission or PID surface.
- Runtime.ExitError uses existing native retry/redelivery/ACK budgets.
  Ordinary async/private diagnostic records add injected_worker_crash;
  successful actual joins retain clean ownership. Prior external writes survive.
- SNS retains its async event/request identity and numbered attempts.
  SQS selects an admitted active Lambda ID at attempt1; native message identity/
  receive count link redelivery to its new Lambda ID. Whole-batch ACK remains.
- Cognito private runtimes cannot opt in and are never wired to this public port.
  Completion loss remains a separate capability. Fresh defaults unchanged.

Verification:
- Release-source CI38532431406 passed full Go race/vet, frozen fixture contracts,
  complete real SDK suite and unchanged Express/Swagger handlers.
- Actual packaged Linux amd64/macOS arm64 fresh/prewarmed SNS/SQS proofs passed:
  selected worker deaths/retries, broker/Cognito availability, held quiescence0
  work, joined shutdown and native service/gateway help.
- All eight binaries have CGO=0/clean source metadata. All nine uploads were
  downloaded; every original byte/checksum matches. Public latest/tag verified.
- Full affected race: Lambda135.343s, app40.849s, eventsource1.419s,
  server2.153s, ownership1.042s. Final control-only fix has focused race4.864s.
- Focused lifecycle covers four fresh/two retained runtimes, active/prelaunch
  crashes, retained reuse, overlapping peers, actual process joins, delayed
  deadline/first cause, native exit with held cleanup, and saturated-pool
  pending clear during held throttle diagnostics followed by healthy attempt1.
- Final real SNS/SQS frozen boto3 race16.726s (fresh8.25s/warm7.42s):
  each mode has two selected worker deaths and real producer retries, prior
  durable writes, healthy overlap/broker/Cognito, queued clear and clean quiescence.
- Related completion-loss, warm messaging/tuning and warm Node Cognito SDK
  cases ran; an initial new-proof terminal-readiness race was corrected by
  awaiting native snapshot publication. Final new-proof rerun passed.
- All-tag vet and diff checks pass. No dependencies or supported AWS wire changes.

Docs cover settings, identities, explicit SQS boundary and code ownership.
No new worktrees/branches/environments or application-stack/default-state changes.
Owned tests/processes joined; successful scratch removed by the existing owner.
Release dist/download/archive/notes and Mac source/binaries/test environment removed.
Failed scratch and raw /tmp/eventbus-ticket39-* evidence remain unpinned with
the existing24h expiry. Unrelated laptop Plans changes preserved.
