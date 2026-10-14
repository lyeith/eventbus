# EventBus state

Canonical SSD /home/spite/Projects/eventbus, main; public MIT AWS emulator
and agent development harness. Source implements #37 and #38.
Latest published binaries remain v0.14.0; v0.15.0 publication is authorized
and is the next step after clean-source build and artifact verification.

#37 0807872: POST /__eventbus/dev/lambda/warm updates the existing public pool.
Both max_workers (1..32) and max_concurrency (1..1024) are explicit positive
integers. Limits publish together; active calls finish, queued Events and
SDK-provisioned resources survive, excess oldest idle workers join retirement.
Canceled waits preserve published limits. Startup opt-in/private Cognito budgets
stay unchanged. Shared dev HTTP decoding has one owner.

#38 1f99460: native AWSEvents default-bus rate Rules and registered Lambda targets.
Nine operations cover rule/target CRUD/readback, enable/disable and pagination.
Input, InputPath, InputTransformer and reserved scheduled-event variables work.
Events owns cadence, occurrence identity, transformation and API admission retry;
app reuses Lambda's existing async adapter. Scheduler remains separate.
Native mutation gates stop later first admissions; accepted retry/source work joins.
Capacity parks due work, and expired final Close aborts unaccepted retries with
sticky uncertainty and actual callback joins. Retained cleanup permits only
root POST DisableRule/RemoveTargets/DeleteRule. Producing controls stay fenced.
Cron/patterns/other buses/targets and unverified transform choices are documented.

Shared reload correction: joinRetiredWorkers now uses the existing pool owner
after worker.retired, so reload releases counted retirement reservations before
returning. Deterministic regression reproduced the earlier CI failure in both
healthy/uncertain cases; existing native nested/Node/Python/private reload checks
passed. The first source CI failed only this previously existing publication
gap; new clean-source CI is required before v0.15.0 publication.

Final local verification:
- Full Lambda race 140.603s; final Events race 1.094s; scoped owner vet passed.
- Affected full race: app52.397s, server3.421s, protocol1.027s,
  coordinator1.070s, architecture1.086s; production CLI wiring included.
- Full pinned SDK race with genuine recurrence enabled passed561.502s.
  Earlier isolated recurrence proved2 firings,12 native calls,60000ms cadence;
  final SDK lane reran it against final production source.
- Reload correction race: nested/tuning6.289s, Python/Node2.216s, private/app3.843s;
  scoped vet passed.
- All-tag vet (sdksmoke,performance,integration), formatting/diff and independent
  ownership review passed. Added only pinned test Events SDK3.1146.0.

No application-stack/default-state changes, new worktrees/branches or runtime
dependencies. Temporary documentation copy removed. Owned tests/processes joined.
Raw /tmp/eventbus-ticket37-*, ticket38-* and tickets37-38-* logs use SSD24h policy.
Successful scratch auto-removes; busy UV metadata retains unpinned finite-lifetime
scratch for existing maintenance. Shared caches/dependency environments stay bounded.
