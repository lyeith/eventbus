# EventBus state

Canonical source: /home/spite/Projects/eventbus on SSD; branch main.
Public repository: https://github.com/lyeith/eventbus; MIT, David Wong, 2026.
Published service release: v0.4.0 from b60caa1; gateway release: v0.3.0.
Plans pins remain service v0.4.0 and gateway v0.3.0. This cleanup has no new release.

Current work: ten GitHub tickets triaged and shared ownership cleanup completed.
All #2–#11 are AWS core gaps: four P1, six P2, six crosscutting. None is closed.
Per-client settings belong to Cognito's persisted app-client policy, not a global
harness client abstraction. Dependencies/acceptance: docs/ISSUE-TRIAGE.md.

Shared mechanics now have concrete owners:
- devcapture: serialized JSONL append/fsync, terminal failure and file lifetime.
- localexec: process groups, descendant cleanup, retained pipes and capped output.
- awsprotocol: target extraction and caller-budgeted JSON body reading.
Services retain schemas, validation, admission, results and execution policy.
Consumer invocations now clean up descendants and reject truncated output.
SSM/Secrets/Firehose reject oversized bodies instead of accepting a JSON prefix;
their existing 1 MiB budgets remain explicit pending native protocol review.

Cognito separates shared challenge state, lifecycle/password policy, dev seeding
and test-only mutation helpers. Legacy fixture behavior is preserved explicitly.
Gateway owns a typed configuration copy without invoking the YAML adapter;
gateway/Lambda file loaders are dev_config.go. App owns project-root discovery.
Architecture and test placement: docs/ARCHITECTURE.md.

Two independent reviews found no cleanup blockers. Existing follow-ups remain
in docs/BACKLOG.md: consumer Go/Python execution duplication and Cognito DB()
test access. Native protocol, SSM and Firehose gaps remain separate core work.

Verification: every Go package passed race checks across the full run and final
consumer rerun; go vet ./... passed. The initial Python fixture launcher failure
was corrected with explicit test-owned environment; production inheritance is
unchanged. Darwin/Windows runner branches compile; target execution was not run.
No new dependencies, database migrations or live application stack changes.
No task child processes/worktrees remain. Failed managed test allocations are
quiescent/unpinned and expire under existing SSD GC; logs use /tmp retention.

Supported: local SQS/SNS operation families and capture-only external delivery,
SES sending capture, Cognito lifecycle/SRP/custom triggers and generic request
gateway with Go/Python/Node authorizers through synchronous Lambda Invoke.
Missing native capability tickets remain required; see README/docs for limits.
SQLite retains developer identities/signing keys; do not reset its data.
Messaging/resource state is in memory and reprovisioned after restart.

Next: #3 secret-stage correctness and #9 Cognito metadata/readback independently;
then #10 token validity and incremental #11 settings enforcement. #7 async Lambda
precedes #8 Scheduler; #5/#6 gateway formats coordinate independently; #4 follows
#3 and #2 delivery is independent. SES management remains low priority.
