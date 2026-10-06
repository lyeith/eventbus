# Handoff

Issue #1 is implemented and verified in the canonical SSD checkout on
feat/cognito-integration-workflows, based on public main e81f2b9.
Source commit/push and v0.2.0 publication are the remaining steps.

Cognito owns separate Username/email/sub, lifecycle and paginated email lookup,
policy/client settings, admin password/refresh, real SRP and persisted challenges.
Password proof, exact account revisions, one-use sessions and real enrolled
TOTP protect custom auth. Temporary passwords require replacement; Define is
invoked at that transition before custom policy continues.
Signed access and ID tokens preserve canonical identity and protected claims.

Generic Node execution is separate under cognitotrigger. App supplies it through
Cognito's TriggerInvoker port using --cognito-triggers and --work-dir.
It supports actual app-owned ESM/CJS Define/Create/Verify handlers, typed failures,
bounded events/results, deadlines, environment isolation and joined child cleanup.
No Node prerequisite exists when triggers are unconfigured.

Schema migration and seed reapplication preserve existing identities/keys/state.
Plaintext seed or password setup backfills legacy SRP credentials; bcrypt alone
cannot supply an SRP verifier. No developer stack/database was touched.

Verified on SSD, 2026-10-06:
- go test -race -count=1 -timeout=15m ./...: all packages passed.
- go vet -tags sdksmoke,integration ./...: passed.
- Python fixture contracts: 5 passed.
- SDK race: both isolated JS lifecycle/custom/restart/error scenarios, Python
  auth/client and SES scenarios passed. The sole old forced-email-verification
  assertion was corrected; its Python SDK subtest then passed with race.
- Native Linux amd64 CLI: lifecycle, actual Node SRP/email handlers, live SES,
  pending challenge/refresh restart, SIGTERM and process cleanup passed.
- Four CGO-free Linux/macOS amd64/arm64 builds and SHA256SUMS checks passed.
- Scoped controller boundaries and read-only review findings resolved.
- Local Markdown links and git diff --check passed.

Other release targets were cross-built; live RustFS and actual consuming-app
handlers were not exercised. Applications own scenario assertions/handler logic.
docs/COGNITO.md, CUSTOM-TRIGGERS.md and SDK guide document contracts and setup.

Next: commit/push source, rebuild from clean Git provenance, publish v0.2.0,
verify uploaded hashes, remove owned .venv/node_modules/npm cache/probe/release files,
then record final release/cleanup. Root owns the serialized operation window.
Receipts /tmp/eventbus-issue1-*.log expire under the existing 24-hour policy.
