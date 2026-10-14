# Handoff

Canonical SSD /home/spite/Projects/eventbus, main. #39 released in v0.16.0.
https://github.com/lyeith/eventbus/releases/tag/v0.16.0
Annotated tag source92dd784506b99af24189c0db88b7e87628981f35, clean CGO=0 builds.
Eight Linux/macOS amd64/arm64 service/gateway binaries plus SHA256SUMS are public.

#39 adds opt-in exact native-attempt worker death:
- lambda/dev_worker_crash*.go owns config, target selection, one-shot decision,
  redacted snapshots and strict HTTP validation. Native runtime boundaries
  synchronize launch/response/exit; localexec still owns process groups/joins.
- Separate Server.LambdaWorkerDev is routed/wired only to public functions.
  Private Cognito rejects opt-in. Retained held controls stay fenced before body.
- Select existing function/alias + request ID + attempt. No new event admission,
  future function-only selector, PID surface or replacement application handler.
- Existing queued/current SNS async attempts and admitted active SQS execution
  are supported. SQS delivery attempt1 has a new Lambda ID on each redelivery;
  the native message ID/receive count preserve producer lineage.
- Fired attempts fail as Runtime.ExitError with injected_worker_crash in
  ordinary async/private diagnostics. Prior writes remain; native retry/
  visibility/redrive/whole-batch ACK and completion-loss behavior stay owned.
- Deadline/earlier cancellation takes precedence. Accepted response or observed
  natural process exit closes selection before cleanup/diagnostics can linger.
- Joined nonlaunch throttle keeps upcoming-task arms clearable through held
  diagnostic activity. Atomic publication prevents arm loss or resurrection.

Verification:
- Release-source CI38532431406 passed full Go race/vet, frozen fixture contracts,
  complete real SDK suite and unchanged Express/Swagger handlers.
  https://github.com/lyeith/eventbus/actions/runs/38532431406
- Actual packaged Linux amd64/macOS arm64 fresh/prewarmed SNS/SQS proof passed:
  native deaths/retries and prior writes, healthy broker/Cognito, clean native
  held quiescence0 work, joined shutdown, service/gateway help.
- All eight binaries have clean source/CGO=0 metadata; downloaded all nine assets
  and verified SHA256SUMS plus every original byte. Public latest/tag verified.
- Full affected package race passed: Lambda135.343s, app40.849s,
  eventsource1.419s, server2.153s, architecture1.042s.
- Final focused Lambda owner/HTTP/real lifecycle race4.864s covers all fresh
  runtimes, warm Node/Python, overlap/reuse/actual PID joins, real prelaunch
  faults, hard deadlines/first cause, held natural-exit cleanup and saturated
  nonlaunch diagnostics pending-clear200 followed by healthy nativeattempt1.
- Final genuine SNS/SQS frozen SDK race16.726s, fresh8.25s/warm7.42s.
  Both prove preserved writes, native retry/redelivery/ACK, replacement worker
  import, healthy peer invocations, broker/Cognito and clean retained quiescence.
- Related completion-loss, warm-native messaging, warm tuning and warm Node
  Cognito SDK cases ran. The only initial SDK failure was test readiness:
  a capture line preceded native terminal publication. The fixture now waits
  for the native snapshot before requiring404; final new-proof rerun passed.
- All-tag vet, Lambda vet and diff checks passed; independent ownership/race
  review reconciled the deadline, natural-exit and pending-clear boundaries.

Evidence: /tmp/eventbus-ticket39-affected-race.log,
worker-sdk-final.log, related-sdk.log, composition.log and vet.log (same prefix);
agent logs have crash-* suffixes. Existing SSD24h retention, no pins.
Owned test processes joined, successful scratch removed; release dist/downloads/
archive/notes and Mac source/binaries/test environment removed. Failed scratch expires
under the same policy. No worktrees/branches/environments/live-stack changes.
Unrelated laptop Plans edits preserved. No dependencies added.

Packaged/build/metadata logs: /tmp/eventbus-release-v0.16.0-* under existing24h
retention, no pins. No required work remains for this release.
