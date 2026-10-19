# EventBus state

Canonical SSD /home/spite/Projects/eventbus, main; public MIT AWS emulator
and agent development harness. #41 source is complete and published in v0.17.0.
https://github.com/lyeith/eventbus/releases/tag/v0.17.0

Release v0.17.0 (2026-10-19):
- Annotated tag/source8d6a8bd8d4466cd23979767b15bfa7646cc46c49.
  Includes #40 native admission diagnostics and #41 disposable shutdown.
- Latest public release: eight Linux/macOS amd64/arm64 service/gateway binaries
  plus SHA256SUMS. Downloaded uploaded assets and verified every checksum.
- Full source CI39214678215 passed Go race/vet and complete SDK/Swagger lanes.
- All binaries CGO-free with matching clean VCS metadata and target formats.
- Actual Linux amd64/macOS arm64 packages passed help and native failed-handler
  retry cancellation in ordinary/retained modes; handlers/listeners joined.
- Release build, downloads, platform copies and temporary probes removed.
  Inspected build/test owners quiescent; their scratch auto-removed.

#41 disposable final shutdown:
- New --shutdown-policy graceful|discard-pending; graceful remains default.
- Named app/dev_shutdown adapter selects the permanent policy after retained
  source fencing, before the retained join. Resumable controls are unchanged.
- Lambda dev_async_shutdown activates the existing async queue/terminal owner:
  pending Events and prospective retries cancel as Dev.ShutdownDiscarded.
  Claimed attempts may launch and must finish/join; actual successes and
  exhausted final failures keep their outcomes. Native throttle accounting stays.
- Cancellation capture advances on existing workers. Final operational summary
  counts published cancellations, including retries from finishing attempts.
- Scheduler/Events dev adapters cancel unaccepted admission retries through
  existing contexts/finalizers, so their source leases cannot strand shutdown.
  App logs their final outcomes separately from accepted Lambda cancellations.
- Active callbacks/stores stay available through joins; capture/ownership/
  cleanup/deadline failures remain failures. No new coordinator/dependency.
- Agent/Lambda/retained/ownership docs describe policy, evidence and limits.

Verification:
- Real CLI race4.275s: ordinary/retained modes, native minute retry beyond
  shutdown budget, queued Event, overlapping success/failure, real SNS SDK and
  Cognito JWKS callbacks, cancellation count3, joined PIDs/listeners/store.
- Composed app regression saturates actual Lambda pending capacity, then
  cancels a Scheduler hour-retry/source lease before the retained join.
- Full affected race: app45.286s, Events1.102s, Scheduler1.591s,
  devquiescence1.043s, architecture1.047s. Final full Lambda race149.652s.
- Focused Lambda race3.052s covers admission/claim/retry/capture races, native
  throttle attempts, default deadlines and sticky capture/cleanup errors.
- First full run found an existing PID-test observation race. Original async
  implementation reproduced2/100: /proc was absent before fallback kill.
  Linux test helper now treats disappeared PIDs as exited; 100 race runs pass
  in22.552s. Production process cleanup was unchanged.
- All-tag vet and diff checks pass. Independent ownership/docs review is clean.

Owned tests/processes joined; successful inspected test scratch removed.
Raw /tmp/eventbus-ticket41-* logs and unpinned failed/probe scratch use existing
24h retention; a quiescent UV tool scratch may await metadata maintenance.
No worktrees, branches, new environments or application/default-state changes.
Unrelated laptop Plans changes preserved. Release smoke/build logs use existing
24h retention under /tmp/eventbus-v0.17.0-*.
