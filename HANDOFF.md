# Handoff

Canonical SSD main. Latest public EventBusv0.11.1; v0.11.2 preparation for #29.
Plans/application state and shared frozen SDK caches preserved; no dependencies.

#29 sourcec36ce1c: top process detail belongs to final launch; bounded per-launch
facts retain retired errors, native ownership and raw frozen cancellation pairs.
One adapter classifies causes. Top ownership is cumulative over native launches
and optional collector. No service-policy/native identity/outcome changes.
docs/LAMBDA.md defines additive v1 fields and older-record limitations.
Real managed success/final process+handler failure, Event identity, cancellation,
dirty first cleanup/no retry, truncation and late cancellation tests are covered.
Provided success followed by deliberate long-poll shutdown preserves its raw
OS Wait detail; consumers must use native state/ownership, not free-form text.

PASS SSD logs (/tmp, existing24h lifetime):
- eventbus-29-race-20261008: full Lambda84.689s/localexec2.077s/devcapture1.169s.
- eventbus-29-native-consumers-20261008: native Event, evidence and retained owner
  SDK34.994s; actual configured cleanup/gateway continuation1.530s.
- eventbus-29-vet-20261008: all packages sdksmoke,integration,performance.
The consumers selection ran no eventsource tests; no eventsource verification
claim is made for this turn.

Proper overhead owner: ssd-dev-tools796c1dd, pushed. Installed symlinks activate
new launches immediately.3 fresh storage queries replace13; no proof caching,
root broker/config/dependency/restart. Launcher median1154ms→349ms over10 samples
per side; actual native UV Init median388.942ms over20, prior1160.549ms.
Native runtime fixture passed100 joined invocations in11.872s. Direct variation,
fixed ordering and exact frozen interpreter are documented in performance review
cf292c8. No application import/business-flow attribution or production p99.
Tooling41 affected Policy/shell tests and storage/operation/bootstrap cases pass.
Full suite898 tests:18 failures/3 import errors/23 skips. Clean26f8468 reproduces
all18+3; fixture fields, umask/NoNewPrivs assumptions and absent pytest documented.
No unrelated tooling fixes or new dependency.

Next: clean tagged eight-binary build, packaged Linux/macOS #29 fallback proof,
upload/download checksums, publish/close #29, prune owned probe/release stages.
Source tags/history and application/default DBs remain untouched.
