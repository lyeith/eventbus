# EventBus state

- Public repository: https://github.com/lyeith/eventbus; MIT, David Wong, 2026.
- Canonical source: /home/spite/Projects/eventbus on SSD, branch main.
- Issue #1 is completed and closed; implementation commit 1df3ee8.
- Published release: https://github.com/lyeith/eventbus/releases/tag/v0.2.0
- Tag v0.2.0 points to 1df3ee8; all five uploaded sizes/hashes were verified.
- No task-owned runtime, environment, dependency cache or build artifact remains.

EventBus is a standalone AWS emulator and agent development harness that
supplements LocalStack. Applications own scenarios, assertions and handlers.

Cognito separates Username/email/sub and supports the required lifecycle,
listing, temporary/permanent password, admin password/refresh and SRP flows.
Custom authentication executes configured application-owned Node handlers,
persists challenge history/private parameters and preserves enrolled real TOTP.
Signed access/ID/refresh grants bind account revisions; disable, administrative
reset and global sign-out invalidate old grants and pending challenges.

SQLite migration preserves identity, lifecycle, signing keys and legacy grants.
Legacy bcrypt-only users gain SRP by plaintext seeding or password setup;
do not reset developer identity data. Node is required only for configured triggers.
Ownership: cognito consumes TriggerInvoker; cognitotrigger owns execution and
child cleanup; app constructs, injects and joins the runner before closing stores.

Verified 2026-10-06: full Go race suite, tagged vet, Python contracts, all SDK
scenarios, real Node email-MFA/SES capture, two isolated runs, restart/error/
process cleanup and native CLI lifecycle/custom/pending-restart/SIGTERM.
Release binaries were rebuilt from a clean commit with exact Git provenance.
Linux amd64 executed; macOS/other Linux targets were cross-built.
Live RustFS and actual consuming-app handlers were not exercised.

Temporary .venv, SDK node_modules, npm cache, release builds, probe data/scripts,
bytecode and the merged task branch were removed. No developer stack was reset.
Receipts /tmp/eventbus-issue1-*.log and release metadata follow existing 24-hour
retention; failed managed test runs use that policy too.
See docs/COGNITO.md, CUSTOM-TRIGGERS.md and tests/sdk/README.md for contracts/setup.

Next work is separate: direct SQS SendMessage for a concrete consumer and
low-priority SES management/rich rendering from docs/BACKLOG.md.
