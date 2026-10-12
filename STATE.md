# EventBus state

Canonical SSD /home/spite/Projects/eventbus, main; public MIT AWS emulator
and agent development harness. Tickets #33, #34 and #35 are implemented.
Published binaries remain v0.13.0 (source b97996f); this pass changes source,
with no new binary release. See docs/ISSUE-TRIAGE.md for feature ownership.

Source commits:
- c7b86d4: native Cognito pool/client inventory (#34).
- c0ef7a7: independent warm retention/execution budgets (#33).
- f480dd9: opt-in exact-event async completion loss (#35).
- aa46d62 / 495f419: causal gateway and retained-retry fixture checks.

Lambda warm mode now defaults to two retained idle Python/Node environments
and 64 execution reservations. max_workers limits idle retention;
max_concurrency limits reserved/executing/retiring execution across all runtimes.
Saturation returns native 429; no parent/child slot wait. Joined LRU eviction
and retirement preserve the sum-of-budgets bound, not an RSS limit.
Accepted Events retain their original native queue, identity and retry budget
when execution is throttled before launch. Actual failed Init attempts retain
their charge and phase evidence if fallback cannot reserve capacity.
Private Cognito execution receives its own separate limits through app composition.

Cognito owns ListUserPools and ListUserPoolClients, native validation/errors,
page-sized summary queries, persisted metadata and no-secret projection.
pagination.go owns authenticated cursors for all three Cognito lists.
Store keys, query scope and pool incarnation bind cursors across restart.
Upgrade: old unsigned ListUsers tokens are invalid; start a fresh scan.

Completion loss defaults to disabled. Named dev adapters and a distinct HTTP
port target one native event/attempt, preserve real handler writes, and reuse
ordinary async retries. Atomic admission/arming also covers fast handlers.
Redacted async and private diagnostics identify injected loss explicitly.
Docs: docs/LAMBDA.md, docs/COGNITO.md and docs/ARCHITECTURE.md.

Verification:
- Full Go race passed; final Lambda correction rerun passed (162.249s).
- Full frozen SDK race passed (527.046s), including fresh/warm faults and both
  Cognito inventory SDKs; five Python fixture contract tests passed.
- Owned retained SDK/RustFS passed (36.845s); real Express/Swagger (18.452s).
- All-tag vet, formatting and diff checks passed.

No dependency, worktree, temporary branch, application/default DB or live-stack
change. Fixtures own and join their listeners, children, files and RustFS data.
Successful SSD scratch auto-removes; failed receipts are unpinned under 24h
retention. Raw /tmp/eventbus-tickets33-35-*-20261012.log uses the existing
24h retention policy. Shared caches retain host bounds.
Unrelated laptop Plans edits are preserved. Next release is separate work.
