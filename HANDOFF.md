# Handoff

Canonical SSD /home/spite/Projects/eventbus, main. #33–#36 released in v0.14.0.
https://github.com/lyeith/eventbus/releases/tag/v0.14.0
Release tag source 0cd0c4867f3720de87f8d7479bd17228936a84e4 is clean.
Eight EventBus/gateway Linux/macOS amd64/arm64 binaries plus SHA256SUMS are public.

#36 f9fd81b:
- cognitotrigger/dev_reload*.go owns the optional DevTriggerReloader port,
  reload admission/cancellation/Close joins and static HTTP errors.
  Server.CognitoDev is separate from AWS routing; app composes its private runner.
- App resolves its immutable pool/trigger target and calls Lambda.ReloadFunction.
  Coalesced entries share replacement imports; active old warm calls finish.
  Other generations/pools/public functions, recipe settings and native Cognito
  identity/session state, tokens and signing keys remain intact.
- A canceled wait 504 leaves published source active. Source validation failure 503
  retains the usable prior generation. HTTP 200 confirms old warm workers joined.
- Real worker regression covers aliases, old/new PIDs, preserved env/timeouts,
  peers, interrupted waits and validation refusal.
- Maintained SDK proof uses real SRP+SES, finishes a pre-reload signed challenge,
  and validates old access/refresh tokens, stable JWKS/client/user metadata.
  EVENTBUS_SMOKE_BINARY reuses that proof through an owned packaged CLI.
  SIGTERM/Wait checks every observed private/public worker PID is ESRCH.

Shared retirement fix b367656:
- An existing retained test failed on Resume after clean Quiesce. Worker activity
  release preceded pool-removal publication, so resume rejected a joined worker
  and falsely made the coordinator's evidence permanently dirty.
- DevResumeWarm joins already-retiring workers through the same owner outside mu,
  then checks closure/errors/new work before reopening. Live workers still refuse.
- Deterministic Activity-port gate holds the exact publication gap after actual
  process cleanup; resume waits, evidence remains readable, then opens cleanly.

Validation:
- Full affected race passed app (71.539s), cognitotrigger (1.387s), server (4.875s),
  full Lambda (151.239s); reload SDK (8.940s); all-tag vet and formatting/diff checks.
- Release-source CI 38217102424 passed full Go race/vet and frozen SDK/Swagger.
- Packaged SDK proof Linux amd64 8.265s and macOS arm64 3.473s passed, including
  real private/public worker shutdown; native EventBus/gateway help passed.
- All eight build metadata records show source 0cd0c48, modified=false, CGO=0.
  All nine uploaded assets downloaded; SHA256SUMS and original binaries match.
- Public latest release verified; #36 closed automatically on source push.

Release includes prior #33–#35. Upgrade notes: max_workers now means idle
retention; max_concurrency defaults to 64. Old unsigned ListUsers cursors restart.
Fresh execution/completion-loss disabled remain defaults.
Contracts: custom-trigger docs, Lambda/Cognito docs, architecture and SDK README.

Cleanup complete: owned dist/downloads/archive/macOS source+dependency copy and
package processes removed/joined. No new dependency, worktree/branch or live state.
Unrelated laptop Plans edits preserved. Failed receipt d9c24ade* is unpinned with
24h expiry; ticket/release raw logs use existing 24h policy. Shared caches bounded.
