# EventBus state

Canonical SSD /home/spite/Projects/eventbus, main; public MIT AWS emulator
and agent development harness. Tickets #33–#36 implemented and released.
Published latest v0.14.0; annotated tag points to clean source
0cd0c4867f3720de87f8d7479bd17228936a84e4.
https://github.com/lyeith/eventbus/releases/tag/v0.14.0
Eight Linux/macOS amd64/arm64 EventBus/gateway binaries plus SHA256SUMS are public.

#36 f9fd81b adds source-only private Cognito trigger reload:
POST /__eventbus/dev/cognito/pools/{pool_id}/triggers/{trigger_name}/reload
with no body. The named dev port tracks admission/Close and maps the selected
immutable hidden target into Lambda's existing generation owner.
Matching entries within a pool share replacement imports; active old warm calls
finish and retire. Config/environment/timeouts, native identity/session state,
tokens/signing keys and unrelated private/public functions remain intact.
Interrupted waits do not roll back published code or cancel old handler calls.
See docs/CUSTOM-TRIGGERS.md#development-source-reload.

b367656 fixes warm resume observing an activity release before pool-removal
publication. Resume joins existing retirements outside the pool lock, then
rechecks state before reopening. Live workers/ownership failures still refuse.
The deterministic real-worker test holds that precise publication boundary.

v0.14.0 also includes #33 independent idle/execution limits and native throttling,
#34 Cognito pool/client inventory with authenticated pagination, and #35 scoped
one-shot async completion loss with real effects/native retries.
Upgrade: max_workers now limits idle retention; max_concurrency defaults to 64.
Old unsigned ListUsers tokens require a fresh scan. Fresh execution and disabled
completion-loss injection remain defaults. See the service contracts/issue triage.

Verification:
- Affected full race: app (71.539s), triggers (1.387s), server (4.875s), Lambda (151.239s).
- Real reload Cognito/SES SDK (8.940s); all-tag vet, formatting and diff checks passed.
- Release-source CI 38217102424 passed full Go race/vet and frozen SDK/Swagger.
- Same packaged reload proof: Linux amd64 8.265s; macOS arm64 3.473s.
  Both assert actual observed private/public workers exited after SIGTERM/join.
- All eight binaries have clean VCS revision/CGO=0 metadata. Native help passed.
  All nine uploads were downloaded; checksums and local binary bytes match.

Owned build outputs, downloaded assets, source archive, macOS test/dependency
copy and package processes are removed/joined. No new dependencies, worktrees,
branches, live-stack/default-state changes. Unrelated laptop Plans edits preserved.
Failed test receipt d9c24ade* remains unpinned under 24h retention.
Raw /tmp/eventbus-ticket36-*-20261012.log and release-v0.14.0 logs use existing 24h
retention. Successful SSD scratch auto-removes; shared caches keep host bounds.
