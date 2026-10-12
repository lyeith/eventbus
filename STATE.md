# EventBus state

Canonical SSD /home/spite/Projects/eventbus, main; public MIT AWS emulator
and agent development harness. Tickets #33–#36 implemented.
Release v0.14.0 is being prepared; published latest remains v0.13.0 at b97996f.
No application/default DB or live stack is changed by release verification.

#36 f9fd81b provides selected private Cognito trigger source reload:
POST /__eventbus/dev/cognito/pools/{pool_id}/triggers/{trigger_name}/reload
with no body. The named Cognito dev port tracks admission/Close and delegates
its immutable hidden target to Lambda.ReloadFunction. Matching entries within
a pool share replacement imports; active old warm calls finish and retire.
Source-only reload preserves config/environment/timeouts, pools/clients/users,
signing keys, tokens and pending challenge/session state. Peers stay unchanged.
Canceled waits do not undo published code or cancel old handler calls.
See docs/CUSTOM-TRIGGERS.md#development-source-reload.

b367656 fixes the shared warm retirement publication gap: resume joins existing
retirements outside the pool lock before reopening reuse. Genuine live workers
and ownership failures still block resume. The deterministic regression holds
actual worker activity release after process cleanup and observes real resume.

The release also includes previous source-only changes:
- #33 c0ef7a7: independent idle/execution limits, joined LRU/retirement and
  native prelaunch throttling without spending accepted Event handler retries.
- #34 c7b86d4: native Cognito pool/client inventory, persisted metadata and
  one authenticated cursor owner for all lists.
- #35 f480dd9: opt-in one-shot async completion loss for an exact event/attempt,
  preserving real writes, payload/identity and normal retry policy.
Upgrade notes: max_workers now limits idle retention; max_concurrency defaults 64.
Old unsigned ListUsers tokens require a fresh scan. Fresh execution and
completion-loss disabled remain defaults. See service docs/ISSUE-TRIAGE.

Verification:
- Affected full race: app (71.539s), triggers (1.387s), server (4.875s), Lambda (151.239s).
- New real Cognito/SES SDK reload proof (8.940s); all-tag vet passed.
- Prior full Go/SDK/retained/Swagger gates and CI38212732667 passed for #33–#35.
- Packaged Linux/macOS proof, release checksums and new CI are next.

Owned fixtures join native processes/listeners and remove their temporary data.
No new dependencies/environments/worktrees/branches were added.
Successful SSD scratch auto-removes; failed test metadata is unpinned under 24h
retention. Raw /tmp/eventbus-ticket36-*-20261012.log uses existing 24h policy.
Unrelated laptop Plans edits are preserved; shared caches retain host bounds.
