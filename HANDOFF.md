# Handoff

Canonical SSD /home/spite/Projects/eventbus, main. #37/#38 released in v0.15.0.
https://github.com/lyeith/eventbus/releases/tag/v0.15.0
Release source 66648bec82acdec70c50d0ebed2675002880155c is clean; annotated tag.
Eight EventBus/gateway Linux/macOS amd64/arm64 binaries plus SHA256SUMS are public.

0807872 live public warm tuning:
- lambda/dev_warm_config*.go owns the explicit dev port and validation;
  the existing pool atomically publishes limits and joins excess idle retirement.
- Separate Server.LambdaWarmDev exact route is wired only to public functions.
- Bounded dev JSON decoding is shared with completion-loss controls.
- Active/queued native calls and SDK-created SNS/SQS resources survive.
  Fresh defaults/private Cognito budgets remain intact; canceled waits do not undo.

1f99460 native recurring EventBridge Rules:
- events owns nine default-bus rate rule/target operations, detached readback,
  scoped pagination, scheduled identity and supported input transformation.
- app asyncLambdaTargetInvoker serves Events/Scheduler consumer ports; separate
  resources and native retry/time policies remain with each service.
- Mutation gates fence later first calls. Source leases, per-target retries and
  release callbacks join before Lambda drains accepted work.
- Capacity parks due work. Expired permanent Close cancels unaccepted retries,
  joins real callbacks and retains uncertainty; resumable source fencing does not.
- Held callback cleanup allows only root AWSEvents Disable/Remove/Delete.
  Producing calls/tuning remain fenced. Real coordinator tests cover these seams.
- Explicit gaps and AWS-unverified missing-path/wildcard choices are documented.

66648be shared reload publication:
- A retired signal preceded pool removal; CI caught the still-counted child.
- joinRetiredWorkers now uses existing warm.retire after that signal, outside
  locks, to publish removal/fold errors before returning. Existing assertions stay.
- Deterministic healthy/uncertain proof fails before fix; real native/public/
  private reload, nested calls, tuning and retained resume proofs pass.

Verification:
- Clean release-source CI38510551698 full Go race/vet and frozen SDK/Swagger passed.
  https://github.com/lyeith/eventbus/actions/runs/38510551698
- Local full pinned SDK race561.502s includes two genuine minute-rate firings,
  transformed/original/constant/path calls, producer fencing and actual PID joins.
- Lambda full race140.603s; Events race1.094s; affected composition race and all-tag vet.
- Reload correction race6.289/2.216/3.843s; independent owner reviews clean.
- Actual packaged SDK Linux5.467s/macOS4.972s, native help and CGO=0 clean metadata.
- Uploaded nine assets downloaded; SHA256SUMS and every original byte match.
  Public latest v0.15.0 and immutable source tag verified; #37/#38 closed.

Only new dependency is test Events SDK3.1146.0 in the frozen npm lockfile.
CI enables genuine recurrence; agent-facing contracts/examples/owners are linked.

Cleanup complete: owned dist/downloads/archive/notes/macOS source+deps removed,
all owned processes joined. No worktrees/branches/environments/live-stack changes.
Raw /tmp/eventbus-ticket37-*, ticket38-*, tickets37-38-*, warmreload-* and
release-v0.15.0 logs plus CI evidence use existing24h retention.
Failed/busy unpinned scratch has finite expiry; warm SDK ed0e2667 awaits native UV
metadata maintenance. Other inspected successful scratch removed; caches bounded.
Unrelated laptop Plans edits preserved. No required work remains for this release.
