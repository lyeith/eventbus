# EventBus state

Canonical source: /home/spite/Projects/eventbus on SSD, main.
Public MIT repository: https://github.com/lyeith/eventbus.
Latest public release: v0.5.0 (2026-10-07), service and gateway for Linux/macOS
amd64/arm64 plus SHA256SUMS. Plans pins and live stacks are unchanged.
Preparing v0.5.1 to ship gateway readiness ticket #12 using the existing release
script and asset set; no deployment or new dependencies.

#12 is implemented and verified:
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

Next: build/smoke/checksum v0.5.1, verify uploaded GitHub assets, publish and clean
owned release scratch. Consuming-app acceptance follows. SES management remains
low priority; gateway management, full Scheduler APIs, production IAM/KMS/provider
delivery and durable async/schedule recovery remain outside the agreed subset.
