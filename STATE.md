# EventBus state

- Public repository: https://github.com/lyeith/eventbus; MIT, David Wong, 2026.
- Canonical source: /home/spite/Projects/eventbus on SSD.
- Branch: feat/cognito-integration-workflows; issue #1 implemented and verified.
- Latest published binary: v0.1.0; v0.2.0 candidate verified, publication pending.
- No developer stack or database was reset.

EventBus is a standalone AWS emulator and agent development harness that
supplements LocalStack. Applications own scenarios, assertions and handlers.

Cognito now separates Username/email/sub and supports the required lifecycle,
listing, temporary/permanent password, admin password/refresh and SRP flows.
Custom authentication executes configured application-owned Node handlers,
persists challenge history/private parameters and preserves enrolled real TOTP.
Signed access/ID/refresh grants bind the account revision; disable, administrative
reset and global sign-out invalidate old grants and pending challenges.

SQLite migration preserves identity, lifecycle, signing keys and legacy grants.
Legacy bcrypt-only users gain SRP by plaintext seeding or password setup;
do not reset developer identity data. Node is required only for configured triggers.

Ownership: cognito owns state/auth and consumes TriggerInvoker; cognitotrigger
owns execution/deadlines/children; app constructs, injects and joins the runner.
SDK fixtures own complete-dispatcher Python/JavaScript and local Node/SES proofs.
JavaScript dev SDKs are pinned to 3.1146.0 with an exact transitive lockfile.

Verified 2026-10-06: full Go race suite, tagged vet, Python contracts, all SDK
scenarios (one legacy Python assertion corrected and rechecked), real Node
email-MFA/SES capture, two isolated runs, restart/error/process cleanup and
native CLI lifecycle/custom/pending-restart/SIGTERM. Four release targets build
and checksums verify. Linux amd64 executed; other targets were cross-built.
Live RustFS and consuming-app handlers were not exercised.

Next: commit/push, rebuild from the clean commit and publish v0.2.0 with checksums.
Then remove task .venv, SDK node_modules, npm cache and temporary release/probe files.
Logs use /tmp/eventbus-issue1-*.log under the existing 24-hour retention.
See docs/COGNITO.md and docs/CUSTOM-TRIGGERS.md for supported contracts.
Direct SQS SendMessage remains separate follow-up work.
