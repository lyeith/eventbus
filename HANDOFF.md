# Handoff

Canonical SSD /home/spite/Projects/eventbus, main. #41 source is complete.
Published v0.17.0 as latest on2026-10-19:
https://github.com/lyeith/eventbus/releases/tag/v0.17.0

Release:
- Annotated tag points to8d6a8bd8d4466cd23979767b15bfa7646cc46c49.
  Includes #40 admission diagnostics and #41 shutdown policy.
- Eight CGO-free Linux/macOS amd64/arm64 service/gateway binaries + SHA256SUMS.
  All match clean source metadata; all uploaded downloads passed checksums.
- Source CI39214678215 passed full Go race/vet and SDK/Swagger checks.
- Packaged Linux amd64 and macOS arm64 passed help and actual native Python
  failure/minute-retry cancellation in normal and retained-owner modes.
  Explicit cancellation, exit0, handler joins and listener release verified.
- Inspected successful build/test receipts quiescent; scratch auto-removed.
  Removed release dist, downloaded assets, platform copies and probe directories.
  Raw /tmp/eventbus-v0.17.0-* build/metadata/smoke logs use existing24h retention.

#41 fixes pointless native retry waits during disposable stack teardown:
- Add --shutdown-policy discard-pending to the existing launch command.
  graceful is the default; the final budget remains30s.
- app/dev_shutdown.go composes explicit Lambda and producer dev ports after
  the permanent retained source fence, before waiting for retained leases.
- Lambda dev_async_shutdown.go only fences admission and activates cancellation.
  Existing async.go owns claims/retries/terminal capture, queue removal and
  lease release. One consolidated terminal owner publishes/counts cancellation.
- Pending and prospective retry Events become canceled/Dev.ShutdownDiscarded.
  Claimed attempts join; real success and exhausted final failure remain native.
  Throttles preserve native attempt refunds. Count is read after final joins.
- Events/Scheduler dev_shutdown.go cancel existing contexts to stop unaccepted
  target admission retries. Their original Close/finalizers still join callbacks
  and preserve successful transfer to Lambda. Native resource state is untouched.
- App logs final producer outcomes and the Lambda canceled_events summary.
  Active SDK/JWKS peers/stores remain usable. Intentional cancellation can exit0;
  evidence/cleanup/deadline failure still withholds release/reports failure.
- Resumable quiesce/resume and default graceful semantics are unchanged.
  No dependencies, application prerequisite logic, replay or new coordinator.

Verification:
- /tmp/eventbus-ticket41-disposable-cli.log: real CLI race4.275s, ordinary and
  retained modes. Native1m retry exceeds30s budget; three canceled Events,
  real active success/failure, SNS SDK/JWKS callbacks, joined children and
  listener/store release. Diagnostics preserve actual failed handler attempts.
- /tmp/eventbus-ticket41-affected-race.log: app45.286s (includes composed actual
  native429 capacity rejection plus Scheduler source lease/hour retry),
  Events1.102s, Scheduler1.591s, devquiescence1.043s, architecture1.047s pass.
- /tmp/eventbus-ticket41-lambda-final-race.log: full Lambda race149.652s pass.
  Focused owner/capture/race tests also passed3.052s; all-tag vet passed.
- Initial full run hit a pre-existing test-helper liveness race. Baseline async
  overlay probe reproduced2/100 with PID already absent before fallback kill.
  process_unix_test.go now handles Linux /proc disappearance between syscalls;
  100 race repetitions pass22.552s. Production process cleanup is unchanged.
- Docs and independent source/composition/ownership review are clean.

Inspected successful test receipts quiescent; scratch auto-removed.
Raw logs/failed probe evidence have existing24h expiry and no pins. A quiescent
UV tool scratch may await metadata maintenance under the same bounded policy.
No task worktrees/branches/new environments/live-stack changes or dependencies.
Unrelated laptop Plans changes preserved. No remaining release work.
