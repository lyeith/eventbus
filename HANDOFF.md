# Handoff

Canonical SSD /home/spite/Projects/eventbus, main. Source fixes #37 and #38.
Public latest remains v0.14.0; user authorized pushing and publishing v0.15.0.
Next: clean eight-target build, packaged Linux/macOS checks, upload/download
verification, then public latest release and final state update.

0807872 feat(lambda): tune public warm budgets without restarting
- lambda/dev_warm_config*.go owns optional dev port, explicit ranges,
  atomic same-pool publication and joined excess idle retirement.
- Server LambdaWarmDev exact route is wired only to public functions.
- Existing lambda dev decoder is shared with completion-loss HTTP.
- Real held calls, queued Events, SDK-created SNS/SQS resources survive.
  Private Cognito budgets/default fresh execution remain unchanged.

1f99460 feat(events): support recurring rules and native Lambda targets
- events owns default-bus rate rules, nine native operations, target snapshots,
  scoped pagination, scheduled identity and input transformation.
- app's asyncLambdaTargetInvoker serves Events and Scheduler consumer ports;
  neither core imports Lambda or shares resource state.
- Native mutation gates fence later first calls. Accepted retry lifetimes,
  per-target callbacks and source release join before Lambda drains.
- Pending capacity parks rather than drops due work. Final shutdown expiry
  cancels unaccepted retries and retains uncertainty through real joins.
- Source leases preserve native rule state; real coordinator tests prove
  hold/resume/wakes, capacity, interrupted delivery and publication boundaries.
- Held callback cleanup allows only root AWSEvents Disable/Remove/Delete.
  Native payload/mode stay intact; work-producing operations stay fenced.

Verification:
- Lambda full race140.603s; Events final race1.094s; owner vets passed.
- Full affected app/server/protocol/coordinator/architecture race passed:
  52.397/3.421/1.027/1.070/1.086s.
- Full unchanged frozen SDK race including real minute recurrence passed561.502s.
  Two real firings and12 transformed/original/constant/path native invocations;
  disabled/removed/deleted producers stop, active handler completes and PIDs join.
- Latest warm SDK proof3.867s also checks actual runtime request-ID correlation.
- All-tag vet, formatting/diff checks and independent frozen review passed.
- Logs: /tmp/eventbus-tickets37-38-final-{composition-race,sdk,vet}-20261014.log
  and /tmp/eventbus-ticket38-events-final-review-{race,vet}-20261014.log.

Only new dependency: test-only Events SDK3.1146.0 pinned in npm lockfile.
CI now enables the ~3minute genuine recurrence flag. Contract docs linked in
README, AGENTS, architecture, triage, Lambda/Events and SDK/harness workflows.
AWS-unverified missing-path array/root and wildcard ordering are disclosed.

All task tests/processes joined; no worktrees/branches/environments/live-stack
changes. Temporary laptop doc copy removed. Raw logs and unpinned failed/busy
scratch have existing24h retention; shared caches remain bounded.
Unrelated laptop Plans edits preserved.
