# Handoff

## Current maintenance work — Plans row 887

The isolated sp1/r887-cognito-json-20261117 branch starts at product pin 0cd0c486.
Only the Cognito receiving owner, its tests and directly related docs change.
JSON admission reads at most the 1 MiB budget plus one sentinel byte; it refuses
overflow/read failure before effects and retains typed envelopes.
Test-only a308328 and job_12251ab6 reproduced the old reader's excess read and
pool creation. Retain that baseline and the initial final-source 10-minute timeout
(job_3e1230d9); the current README already specifies a 15-minute race budget.
On unchanged 76537d0, corrected job_cdfcc65d completed the Cognito race package
and remaining scoped vet/frozen SDK stages. Combined owner proof: 613 leaf plus
61 container passes, including all 20 admission controls; Python helpers 5/5;
all six Cognito SDK lanes passed (10 leaves, three containers), without skips.
documented-budget-original-review.json retains full output, hashes and clean source.
Next: build eight binaries and SHA256SUMS with scripts/build-release.sh, then run
the documented Linux packaged trigger-reload SDK proof using EVENTBUS_SMOKE_BINARY.
Recheck v0.14.1 absence; publish only after artifact review, without moving latest.
Verify downloads before both Plans pins. Keep shared runtimes and history below.

## Historical v0.14.0 preparation handoff

Canonical SSD /home/spite/Projects/eventbus, main. #33–#36 source work complete.
Preparing v0.14.0; latest public binaries remain v0.13.0 until publication.

#36 f9fd81b:
- cognitotrigger/dev_reload*.go owns optional DevTriggerReloader, reload admission,
  cancellation/Close joins and static HTTP errors. Server.CognitoDev is separate
  from native routing; app composes the configured private runner.
- App maps pool/trigger to its existing immutable hidden target. Lambda's existing
  ReloadFunction publishes and retires that shared generation; no new coordinator.
- Active old warm calls finish while subsequent invocations use replacement code.
  Matching Define/Create/Verify entries change together. Other pool/generation/
  public workers, recipe settings and native Cognito state remain intact.
- HTTP 200 confirms retired warm workers joined; canceled wait 504 leaves published
  replacement active. Bad source 503 retains previous usable generation.
- Real regression checks actual old/new PIDs, shared counts, original env/timeout,
  independent pool/public imports, interrupted waits and validation refusal.
- Maintained SDK scenario proves real SRP+SES, completing an old signed challenge
  under new handlers, old access/refresh tokens, JWKS/client/user preservation.
  EVENTBUS_SMOKE_BINARY selects the same proof through an owned packaged CLI.
  SIGTERM/Wait also verifies every observed private/public worker PID exited.

Shared fix b367656:
- A full affected run exposed Resume after Quiesce rejecting a worker whose
  activity lease completed before its pool-removal publication.
- DevResumeWarm joins retiring workers through the existing owner outside mu,
  then checks closure/sticky errors/new work before reopening. Live workers
  still refuse resume; capacity stays held through actual retirement.
- Deterministic Activity-port gate reproduces that exact publication gap.
  Actual child is already gone, resume waits, evidence remains readable, then
  gate release yields healthy Open/new worker and joined cleanup.

Validation:
- Focused reload/router race passed; full affected rerun passed app (71.539s),
  cognitotrigger (1.387s), server (4.875s) and full Lambda (151.239s).
- Real reload SDK passed (8.940s); all-tag vet passed; gofmt/diff clean.
- Initial old resume failure led to owner fix; twenty isolated pre-fix iterations
  passed but did not establish causality. New gated regression does.
- #33–#35 prior full Go/SDK/retained/Swagger and CI38212732667 passed.
- Next: clean source commit/push, eight release builds, Linux/macOS packaged proof,
  successful release-source CI, tag/publication and download/hash verification.

Release notes must include #33 max_workers→idle/max_concurrency defaults 64 and
#34 unsigned ListUsers cursor restart. Native fresh/completion-loss defaults stay.
Contracts: docs/CUSTOM-TRIGGERS.md, docs/LAMBDA.md, docs/COGNITO.md, SDK README.

Cleanup uses standard task owners; no dependency or live-state change.
Failed receipt d9c24ade* is unpinned with 24h expiry; raw ticket36 logs use 24h.
Successful scratch auto-removes; no manual disposable fixture is retained.
Preserve unrelated laptop Plans edits. Shared caches keep existing host bounds.
