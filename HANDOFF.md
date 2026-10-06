# Handoff

Issue #1 is complete on public main; implementation commit 1df3ee8.
v0.2.0 is published with four CGO-free binaries and SHA256SUMS:
https://github.com/lyeith/eventbus/releases/tag/v0.2.0
The tag points to the implementation commit. Uploaded sizes and SHA-256 hashes
for all five assets match the locally verified release artifacts.

Cognito owns separate Username/email/sub, lifecycle and paginated email lookup,
policy/client settings, admin password/refresh, real SRP and persisted challenges.
Password proof, exact account revisions, one-use sessions and enrolled real
TOTP protect custom auth. Temporary passwords require replacement; Define is
invoked at that transition before custom policy continues.
Signed access and ID tokens preserve canonical identity and protected claims.

Generic Node execution is separate under cognitotrigger. App supplies it through
Cognito's TriggerInvoker port using --cognito-triggers and --work-dir.
It executes app-owned ESM/CJS Define/Create/Verify handlers with typed failures,
bounded events/results, deadlines, isolated environment and joined child cleanup.
No Node prerequisite exists when triggers are unconfigured.

Migration and seed reapplication preserve identities, signing keys and lifecycle.
Plaintext seed or password setup backfills legacy SRP credentials; bcrypt alone
cannot supply a verifier. No developer stack/database was touched.

Verified on SSD, 2026-10-06:
- go test -race -count=1 -timeout=15m ./...: all packages passed.
- go vet -tags sdksmoke,integration ./...: passed.
- Python fixture contracts: 5 passed.
- SDK race: both isolated JS lifecycle/custom/restart/error scenarios, Python
  auth/client and SES scenarios passed. The sole old forced-email-verification
  assertion was corrected; its Python SDK subtest then passed with race.
- Native Linux amd64 release CLI: lifecycle, real Node SRP/email handlers, SES
  capture, pending challenge/refresh restart, SIGTERM and child cleanup passed,
  including the final clean-commit binary.
- Four platform builds, local checksums and uploaded asset hashes passed.
- Full scoped review findings resolved; local Markdown links/diff checks passed.

Other release targets were cross-built. Live RustFS and actual consuming-app
handlers were not exercised. Applications own scenarios and handler policy.
docs/COGNITO.md, CUSTOM-TRIGGERS.md and SDK guide document supported contracts.

Cleanup complete: task .venv, node_modules/npm cache, bytecode, native probe
resources/scripts, release outputs and merged task branch removed.
No owned processes remain. Receipts /tmp/eventbus-issue1-*.log, release metadata
and failed managed test runs expire under the existing 24-hour policy.
Separate follow-up: direct SQS sending and low-priority SES management/rendering.
