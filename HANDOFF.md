# Handoff

Gateway ticket #12 is implemented on SSD main; preparing v0.5.1 publication.
Latest published release is v0.5.0 until upload verification completes. Plans pins
and running stacks are unchanged. Prior #2–#11 scopes shipped in v0.5.0; its
pinned HANDOFF and service guides retain detailed verification/capability limits.

Cause: gateway hard-coded GET /health returned before canonical validation,
route matching and REQUEST authorization, shadowing application health routes.
Config.DevHealthPath / YAML dev_health_path now selects the absolute listener
readiness path, default /health. gateway/dev_health.go owns its validation and
response. Gateway routing calls that dev adapter after canonical path validation;
native authorizer/integration event builders and API mappings are unchanged.

GET/ANY literal and non-greedy parameter-route collisions fail startup after
applying base_path. Greedy routes (including prefixed greedy routes) and $default
reserve only the selected readiness GET. Queries do not affect matching; other
methods/trailing slashes follow routes. A private name is an unauthenticated
endpoint on the same listener, not a separate management listener.
Select dev_health_path: /.eventbus/ready to keep application /health protected
and preserve its original AWS_PROXY 1.0/2.0 path. Parameter routes like /{id}
that overlap default readiness must choose a deeper path. No dependencies added.

Verification, full logs retained under /tmp through existing finite SSD GC:
- go test -race -count=1 ./internal/gateway -run '^TestDevHealth': passed 1.128s;
  default/query/method/canonical/closed behavior, YAML/ownership/idempotency,
  mapping/collision validation and 3 authorizer × 2 integration × explicit/default
  protected-health scenarios; refused requests never invoke integration.
- go test -race -count=1 -timeout 5m ./internal/gateway ./cmd/gateway: passed
  40.747s / 1.057s, including real Go/Python/Node Lambda handlers, both proxy
  formats, configured readiness/protected /health, HTTP/upgraded/drain lifetime.
- go vet ./internal/gateway ./cmd/gateway: passed.
- Read-only review found no implementation/acceptance/ownership gaps. Corrected
  guide startup curl to use its configured readiness path. Diffcheck passed.
Logs: eventbus-issue-12-{focused,gateway-race,gateway-vet}.log.
Tests own state/listeners and join children; no live stack/database reset.

Next: build all eight v0.5.1 service/gateway assets with build-release.sh, smoke
packaged gateway readiness/routing, verify source/architecture/checksums and
uploaded assets, then publish, remove owned scratch and record release state.
