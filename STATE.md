# EventBus state

Canonical SSD /home/spite/Projects/eventbus, main; public MIT AWS emulator
and agent development harness. #37/#38 and shared reload correction are released.
Published latest v0.15.0; annotated tag points to clean source
66648bec82acdec70c50d0ebed2675002880155c.
https://github.com/lyeith/eventbus/releases/tag/v0.15.0
Eight Linux/macOS amd64/arm64 service/gateway binaries plus SHA256SUMS are public.

#37 0807872: POST /__eventbus/dev/lambda/warm atomically updates the public pool.
Both max_workers (1..32) and max_concurrency (1..1024) are explicit positive
integers. Active calls finish, queued Events and SDK-created resources survive;
excess oldest idle workers join retirement. Canceled waits preserve publication.
Startup opt-in and private Cognito budgets stay unchanged. Shared dev HTTP
decoding has one owner; synchronize recipes yourself for future restarts.

#38 1f99460: nine native AWSEvents rule/target operations on the default bus.
Real rate scheduling, registered Lambda targets, Input/InputPath/InputTransformer
and reserved scheduled-event variables are supported. Events owns cadence,
identity, transformation and admission retry; app reuses Lambda's async adapter.
Scheduler resources remain separate. Mutation gates fence later first calls;
accepted source/retry work joins. Capacity parks due work without a new scheduler.
Expired final Close aborts unaccepted retries with sticky uncertainty and joins.
Held cleanup permits root POST DisableRule/RemoveTargets/DeleteRule only.
Cron/patterns/other buses/targets and unverified transform choices are documented.

66648be fixes shared reload publication: after worker.retired, the existing pool
owner removes the reservation before reload returns. The deterministic regression
reproduced the concrete nested-worker CI failure; existing assertions remain.
No second retirement owner or active-call cancellation was introduced.

Verification:
- Release-source CI38510551698 passed full Go race/vet and frozen SDK/Swagger;
  CI includes genuine minute-rate recurrence. Local full SDK race passed561.502s.
- Local Lambda full race140.603s, Events final race1.094s; affected full
  app/server/protocol/coordinator/architecture race and all-tag vet passed.
- Reload correction races: nested/tuning6.289s, Python/Node2.216s, private/app3.843s.
- Actual packaged SDK proof: Linux amd64 5.467s and macOS arm64 4.972s;
  native service/gateway help passed; all observed workers joined.
- All eight binaries have clean source/CGO=0 metadata. All nine uploads were
  downloaded; checksums and original bytes match. Public latest and tag verified.

Cleanup complete: dist/downloads/source archive/notes and macOS source/dependency
copy removed; owned tests/processes joined. No new worktrees/branches or live
stack/default-state changes. Unrelated laptop Plans edits preserved.
Raw ticket/release/CI logs use existing SSD24h policy. Failed/busy scratch remains
unpinned with finite expiry (successful warm SDK ed0e2667 held for UV metadata
maintenance); other successful inspected scratch removed. Shared caches bounded.
