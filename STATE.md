# EventBus state

Canonical source: /home/spite/Projects/eventbus on SSD, main.
Public MIT repository: https://github.com/lyeith/eventbus.
Latest public release: v0.5.1 (2026-10-07), service and gateway for Linux/macOS
amd64/arm64 plus SHA256SUMS. Release source/tag: 8998f424c853af4b6c8ca9eeec2a6d4c426d8e02.
All nine GitHub assets verified; downloaded binaries match checksums and embed
clean v0.5.1 source/target metadata. Plans pins and live stacks are unchanged.

#12 is implemented, verified, published and closed:
- dev_health_path is an explicit development control, default /health.
- dev_health.go owns readiness validation/response; native events remain untouched.
- Canonical requests reserve only GET at the selected absolute listener path,
  independent of base_path; closed gateways report 503.
- Startup refuses collisions with GET/ANY literal/non-greedy parameter routes.
  Greedy routes and $default intentionally reserve the selected readiness GET.
- Protected application /health reaches its real REQUEST authorizer/integration;
  missing/invalid credentials fail before integration. AWS_PROXY 1.0/2.0 retain
  original application paths, without a harness prefix.

Verified: focused race regressions, full gateway/CLI race suites (40.747s/1.057s),
real Go/Python/Node handlers and scoped vet. Read-only ownership review passed.
Guides document collision rules, same-listener unauthenticated readiness and agent
configuration. No developer stack/database was changed; tests own their resources.

All earlier #2–#11 scopes and ownership findings shipped in v0.5.0. See service
guides for exact capabilities and limits. Native AWS behavior stays in service
core; local recipes, clocks, evidence and compatibility profiles have dev owners.
Approved dependencies remain gojq v0.12.19/timefmt-go v0.1.8 and test-only JS
Scheduler SDK 3.1146.0. No new dependency for readiness.

Packaged Linux amd64 and macOS arm64 passed service health/native SQLite plus
gateway readiness, credential refusals, original paths in both proxy formats,
collision refusal and shutdown. Other targets were cross-built, not executed.
Owned release/download/laptop staging and smoke state/processes are cleaned.
Evidence uses existing finite SSD /tmp retention.

Next: consuming-app acceptance. SES management remains low priority. Gateway
management, full Scheduler APIs, production IAM/KMS/provider
delivery and durable async/schedule recovery remain outside the agreed subset.
