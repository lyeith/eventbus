# Handoff

Generic API gateway and multi-language Lambda authorizer execution are implemented.
EventBus owns the reusable gateway and execution; Plans retains Trust, authorizer
policy, private backend paths, route configuration and OpenAPI/AppGraph ownership.

cmd/gateway is a separate executable. Its boundary is AWS REST REQUEST events,
IAM policy responses and synchronous Lambda Invoke. Unsupported policy semantics
fail closed; authorizer context is an opaque scalar mapping, never client output.
Authorizer TTL defaults to AWS 300 seconds; Plans explicitly sets zero.

internal/lambda supports provided Go/custom Runtime API, Python sync handlers,
Node ESM/CJS async/callback handlers and JSON command adapters. Outputs/logs are
separate and bounded. Declared environment avoids inheriting host credentials;
process groups and runtime listeners close/join on success, timeout or shutdown.
app injects the optional service into the dispatcher and closes after HTTP drain.

Verification on SSD:
- Affected gateway/lambda/app/server race aggregate passed.
- Final Lambda races and partial-ARN/Runtime API contract tests passed.
- Scoped gateway/Lambda/app/server/CLI/example vet passed.
- Plans' tagged authorizer races/vet passed, including actual gateway binary,
  shipped-route fixture, real JWT validation and 15 auth/binding/privacy cases.
- Oversized result blocking, child-output inheritance and endpoint mapping
  regressions found during review are corrected and covered.

Published v0.3.0 from clean c379a24: four-platform EventBus/gateway builds,
SHA256SUMS and matching GitHub digests. Plans pins both executables.
Actual Plans aws-lambda-go binary + frozen boto3 SDK proof passed: real Cognito
JWT, real Trust fail-closed read, Go/Python/Node Invoke, Tail, DryRun and
gateway allow/401/403/private mapping/credential stripping.
No dependencies added; API management, async and warm runtimes are separate scope.
No developer identities or application state reset; tests own their fixtures.
Task binaries/fixtures/probe copies are removed; /tmp receipts expire
under the existing 24-hour policy. Final useful implementation is canonical SSD.
