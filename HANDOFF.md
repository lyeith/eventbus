# Handoff

Canonical SSD /home/spite/Projects/eventbus, main. User authorized completion,
push and publication of #48/#49 as v0.21.0. Source fixes and final local checks
are complete; release verification/publication is next.

Ownership:
- routes.go owns once-decoded wire component routing and safe policy identity.
  mapping.go retains exact raw-capture provenance without recursive replacement;
  trusted recipe mappings retain their existing multi-segment behavior.
- dev_health.go owns the shared reserved-readiness boundary, including retained
  ingress/continuations. Escaped separators cannot bypass held admission.
- iam.go owns native SigV4 canonical request verification against original bytes.
  dev_iam.go owns explicit local static key/token/principal fixtures.
- gateway.go composes selected authentication, preserving a detached integration
  view. Verified request bytes are reused by HTTP proxy and Lambda event owners.
  events.go projects v2 IAM / v1 identity; API owner remains a separate account.
- No consuming application dependency, new dependency or alternate verifier.

Verification:
- /tmp/eventbus-v0.21.0-final-race.log: gateway15.515s, cmd/gateway2.201s,
  tests1.096s, all passed with race detector.
- /tmp/eventbus-v0.21.0-final-vet.log: integration/sdksmoke-tagged vet passed.
- /tmp/eventbus-v0.21.0-final-swagger.log: unchanged Express/Swagger smoke10.419s.
- Native real SDK HTTP -> provided Go process proof covers v1/v2 identity, raw
  components/base paths/query bytes, unsigned/tampered rejection before invoke,
  token credentials, actor spoofing controls and NoAuth/REQUEST separation.
- Optional EVENTBUS_SMOKE_GATEWAY_BINARY reuses that maintained proof against the
  actual CLI package, with private0600 YAML, child readiness, SIGTERM/join/rebind.
- Independent frozen botocore control and HTTP_PROXY body-reuse proof pass.
- Independent source review found no blocker; readiness gap fixed and tested
  with real REQUEST fixture before body read, authorization and invocation.
- Pre-reboot temporary logs disappeared; above final evidence was saved fresh.

Qualification:
- Live AWS encoded-route normalization and noncanonical raw-URL query parity are
  unverified. Docs state component-safe local behavior and strict SDK query form.
- No presign/SigV4a/streaming/unsigned-payload/STS/refresh/IAM policy evaluation.
  Every configured key is admitted to this gateway's IAM routes.

Next: push commits, await source CI, tag/build all eight CGO-free targets and
SHA256SUMS; actual Linux amd64/Mac arm64 packaged gateway SDK acceptance; publish
and verify all public downloads; remove owned output and update these documents.
No live/default-stack changes, task worktrees/environments or dependencies added.
Managed success scratch auto-removes; failed-run scratch has existing6h expiry.
