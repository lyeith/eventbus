# Handoff

Canonical SSD /home/spite/Projects/eventbus, main. Tickets #33/#34/#35 implemented.
Published latest remains https://github.com/lyeith/eventbus/releases/tag/v0.13.0
at b97996f; this pass does not publish binaries.

Changes and ownership:
- #34 c7b86d4: Cognito core ListUserPools/ListUserPoolClients, summary metadata,
  native errors, stable pagination, persistence and independent-instance proofs.
  Shared pagination.go now owns all Cognito list cursors and persisted HMAC keys.
  Pool incarnations reject stale delete/recreate tokens; summaries omit secrets.
- #33 c0ef7a7: Lambda warm.go separates bounded execution from idle retention.
  Defaults: max_workers=2 idle, max_concurrency=64 execution. All runtimes use
  execution reservations when warm mode is enabled. Native saturation is 429;
  joined LRU/retirement keeps capacity until actual cleanup.
  The existing async queue requeues accepted prelaunch throttles with backoff,
  without consuming function-error retries. No second scheduler was introduced.
  A rejected Init fallback preserves the real failed launch/PID/logs/attempt;
  caller cancellation stops pending fallback while retaining its initial phase.
  App forwards both limits into the separate private Cognito runtime.
- #35 f480dd9: dev_async_completion_loss*.go owns opt-in exact-event faults and
  a separate server LambdaDev HTTP port. Atomic admission covers fast unchanged
  handlers; PUT/DELETE control an already-owned event/attempt. Completion is
  decided after real handler join, before private diagnostic append.
  Effects, native payload/request identity and normal two-retry policy remain.
  Explicit injection markers distinguish deliberate loss from owner uncertainty.
- aa46d62: gateway timeout fixture retains real auth with its ordinary independent
  budget and the same tested integration deadline; failures report joined phases.
- 495f419: retained fixture holds its actual first transport error before rethrow,
  joins the independent child, then observes native retry immediately. Its 4s
  retry delay is unchanged; actual child activity/PID and gate cleanup are checked.

Contracts and upgrades:
- max_workers now means retained idle capacity. Set max_concurrency explicitly
  when an existing recipe needs a chosen active bound; counts are not RSS caps.
- Old unsigned ListUsers tokens are rejected; restart the scan after upgrade.
  New tokens survive store restart and remain bound to operation/query/store/pool.
- Fresh execution and completion-loss disabled remain defaults.
  See docs/LAMBDA.md, docs/COGNITO.md, docs/CUSTOM-TRIGGERS.md,
  docs/ARCHITECTURE.md and tests/sdk/README.md.

Verification:
- Full ./... Go race passed including architecture and all existing service owners.
  After final fallback cancellation correction, full Lambda race passed (162.249s);
  focused real Init/cancel/ownership and already-completed-success tests also passed.
- Full pinned SDK race passed (527.046s): unchanged native consumers, Python/Node
  Cognito inventory, fresh/warm SNS completion loss and ungated atomic admission.
- Owned retained SDK/RustFS passed (36.845s); unchanged Express/Swagger (18.452s).
- All-tag vet passed. Five frozen Python contract tests passed; gofmt/diff clean.
  Initial fixture failures were corrected at their responsible test boundaries:
  unrelated cold authorizer deadline and setup consuming a transient retry window.

Cleanup:
No new dependency/environment/worktree/branch or live-stack/default-state change.
Owned test processes/listeners/RustFS data join and temporary files are removed.
Successful SSD scratch auto-removes. Failed test receipts 259234*,02e500*,a98ffb*
are unpinned and use existing 24h expiry; raw ticket logs use the same policy.
No disposable task resource is retained manually. Shared caches keep host bounds.
Unrelated laptop Plans edits preserved. Binary release is separate from this pass.
