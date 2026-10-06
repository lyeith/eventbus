# EventBus state

Canonical source: /home/spite/Projects/eventbus on SSD; branch main.
Public repository: https://github.com/lyeith/eventbus; MIT, David Wong, 2026.
Current work: extract the generic API gateway and support multi-language authorizers.

The AWS service CLI now accepts --lambda-functions and --work-dir. Registered
Go/custom runtime binaries use Lambda Runtime API; Python and Node use standard
handler events/context. Invoke supports RequestResponse and DryRun, raw JSON,
function errors and bounded Tail logs. Environment is explicitly declared.
Each invocation is cold; process groups stop/join on completion, timeout or close.

cmd/gateway builds eventbus-gateway, independently of the AWS listener.
It implements REST REQUEST-authorizer events, Lambda Invoke HTTP, strict IAM
Allow/Deny evaluation, TTL/identity caching, HTTP proxy mappings and configurable
header removal. It owns neither application policy nor an identity datastore.
Applications own route definitions, authorizers and private integration context.
Frontend static/Vite proxy, streaming and upgrade closure remain supported.

Verified: affected gateway/lambda/app/server race tests; final Lambda fixes and
contract races; scoped vet. Tests execute real Go/custom, Python and Node.
Plans' separate authorizer passed native gateway HTTP proof with shipped routes,
real JWT verification, private v1/v2 bindings and credential/privacy boundaries.
Native aws-lambda-go SDK proof and clean release builds are next before v0.3.0.

Existing Cognito lifecycle/SRP/custom Node triggers and SES sending capture remain
supported. SQLite preserves identities/signing keys; do not reset developer data.
No application stack has been reset or restarted in these tests.

Docs: GATEWAY.md, LAMBDA.md, ARCHITECTURE.md and AGENT-HARNESS.md.
API Gateway management APIs and Lambda async/warm runtime behavior are outside
this requested scope. Direct SQS sending and SES management remain follow-ups.
Temporary fixture children/stores are test-owned and removed. Test receipts use
existing /tmp 24-hour retention; native proof binary is held until final acceptance.
