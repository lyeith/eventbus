# Handoff

EventBus organization is implemented in the canonical SSD checkout on main.
The organization work is complete. The public repository and
MIT license remain as previously published. No release was produced.

The root now contains only a thin main.go entrypoint. internal/app owns CLI
configuration, construction, workers and HTTP/resource shutdown. The router in
internal/server imports no services or stores; handlers satisfy its HTTP ports.
internal/awsprotocol owns generic wire helpers formerly embedded in SQS.

Services own state and adapters in internal/cognito, messaging, ses, firehose,
ssm and secrets. Large Cognito/SES files split by purpose. Cognito tests still
exercise real SQLite and JWT/JWKS. SNS/SQS share one broker; queue collections
are private. Consumers are separate under internal/consumer, with configuration,
manager and process files and a consumer-owned QueueBroker port.

Tests follow their owner. Cross-resource shutdown tests live in app; router
proofs compose real service handlers. Service HTTP fixtures run only that
service. Firehose unit tests use owned HTTP sinks instead of localhost:9000.
Secrets owns region/account configuration for ARN construction.

tests/sdk now owns the tagged boto3/JWT proofs, shared process runner, Python
scripts and pinned AWS models with unchanged Apache license/NOTICE. Fixture
paths derive from the Go source directory. CI discovers Python contracts from
tests/sdk/python. Example YAML moved to examples.

Verified on SSD, 2026-10-06:
- go test -race -count=1 -tags sdksmoke ./...: passed all packages.
- go vet -tags sdksmoke ./...: passed.
- Python fixture contracts: 5 passed.
- Focused composed router/health/fallback/optional-service checks: passed.
- integration-tag Firehose compilation: passed; no native endpoint was used.
- Root CLI build plus help/invalid-flag exit, example fixture startup, trimmed
  health URLs, JWKS, live SES v1/v2 capture and SIGTERM shutdown: passed.
- git diff --check: passed. No dependency/lockfile changes.
- Read-only extraction review found no lost operations/tests or wire changes;
  all local documentation links resolve.

Logs: /tmp/eventbus-reorg-*-20261006.log, existing 24-hour retention.
Temporary .venv, Python bytecode, CLI probe binary and probe data were removed;
no owned processes remain. Plans' running stack and release pin were untouched.

docs/ARCHITECTURE.md explains ownership/import direction and test placement.
README, AGENTS, SDK guide, fixture/model links and workflow match the layout.
Follow-up scope remains SES management/rich rendering and a new binary release.
