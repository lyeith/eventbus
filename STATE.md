# EventBus state

Canonical source: SSD /home/spite/Projects/eventbus, main.
Public MIT: https://github.com/lyeith/eventbus. Latest public binaries: v0.7.0.
Plans, application stacks and developer databases are untouched.

Accepted implementation for v0.8.0:
- #18 973e5f1: retained native owners, exact declared cleanup, same-resource
  resume, gateway root leases and strict shutdown.
- #19 9b8f56f: exact current/older mapping-create URIs, unchanged SDK proofs.
- #20 a6b48d5: actual original-receipt settlement, stale/replaced-lease safety.
- Shared capture072a180, queue outcomes4de060a and eventsource91e2678.
- #21/#22 Lambda83a9b6c and app/native SDK b8c4628: actual invocation/message
  lineage, post-join SQS terminals and bounded opt-in private diagnostics.
  Native outputs, Event admission and retry policy remain unchanged.
- #23 SES5480973: MIME raw configuration-set selection/validation with verified
  API-field precedence and exact original submitted capture bytes.

--sqs-delivery-log requires --lambda-functions; private Lambda diagnostics use
recipe dev_diagnostics.log_path. Captures require owned0600 regular files and
diagnostics cannot alias other captures. Runtime success is not business success.
Strict Close retains ownership uncertainty; DrainAsync retains async uncertainty
separately. Trusted handlers await side work in owned OS groups; escaped/
unawaited pipe holders cannot attest healthy joins. No process-reaper expansion.

Owner race/vet and independent review accepted. Final selected13 SDK race
PASS208.846s including current/older SDK, retained full stack and real60s queue
recreation; app23.067s, gateway45.455s/cmd1.043s and tagged vet PASS.
SES14focused cases/full race11.274s/vet and actual boto3 sending race1.747s PASS.
No runtime dependencies/frozen locks changed. Approved isolated older-SDK env
removed after proof (28MiB). Task-generated SDK bytecode removed; caches preserved.
Unavailable consuming production Identity business handler is not claimed verified.
Completed fixtures/listeners/children joined; logs use finite SSD /tmp retention.

Next authorized work: commit/push docs, clean v0.8.0 tag, build eight binaries and
SHA256SUMS. Physical Linuxamd64/macOSarm64 smoke; inspect all cross-build metadata,
draft/upload/download/check/publish assets, close18-23 and clean task staging.
Release staging: /tmp/eventbus-v0.8.0-release.w467OX (task-owned notes).
Portable567line /tmp/eventbus-v0.8.0-native-smoke.py on SSD/laptop held for binary
proofs, then remove. No tag or published release exists yet.
