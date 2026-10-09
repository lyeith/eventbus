# EventBus

EventBus is a standalone AWS API emulation and agent development harness.
It supplements local development stacks, including LocalStack, with repeatable
authentication fixtures, event consumers and structured email and notification capture to shorten
the agent verification and evaluation loop.

Run it independently or alongside LocalStack. Apps use normal AWS SDK clients
with selected service endpoints routed to EventBus; other services stay on
LocalStack or native backends. Apps own their scenarios and assertions.

## Start

Download the checksummed service and gateway binaries from
[the latest release](https://github.com/lyeith/eventbus/releases/latest), or build current
source with Go 1.25 or newer. Run on an unused port:

```sh
go build -o eventbus .
mkdir -p .local
./eventbus --port 14100 --issuer-base http://localhost:14100 \
  --cognito-db "$PWD/.local/cognito.db" --ses-log "$PWD/.local/ses.jsonl" \
  --sns-log "$PWD/.local/sns.jsonl" --cognito-log "$PWD/.local/cognito.jsonl"
```

From another terminal, check `curl -fsS http://localhost:14100/health`.
Point the selected app SDK clients at that endpoint, using local credentials and region
`us-east-1`. Stop with Ctrl-C or SIGTERM. The default port is `4100`;
`./eventbus --help` lists all flags.

- [Agent workflow](docs/AGENT-HARNESS.md): configure the app, seed resources,
  run consumers, inspect results and manage state.
- [Retained-owner recovery](docs/RETAINED-OWNER.md): opt-in source/callback fencing,
  joined native owners, declared cleanup and explicit resume for one exclusive suite owner.
- [Cognito contracts](docs/COGNITO.md): lifecycle, SRP, custom triggers and persistence.
- [API gateway](docs/GATEWAY.md): REST/HTTP REQUEST authorizers and HTTP/Lambda proxy integrations.
- [Lambda execution](docs/LAMBDA.md): application-owned handlers, opt-in warm Python/Node workers and explicit local reload.
- [S3 notifications](docs/S3-NOTIFICATIONS.md): validated LocalStack uploads into registered EventBus Lambda targets.
- [Current performance audit](docs/PERFORMANCE-AUDIT-ROUND-2.md): ranked remaining targets, measurements, owners and contracts.
- [Previous performance audit](docs/PERFORMANCE-AUDIT.md): implemented startup, capture, messaging and gateway fixes.
- [Runtime and launcher review](docs/PERFORMANCE-REVIEW.md): phase measurements and the separate SSD tooling fix.
- [SQS Lambda mappings](docs/EVENT-SOURCES.md): native provisioning and completion-based acknowledgment.
- [Scheduler](docs/SCHEDULER.md): one-time schedules targeting local Lambda events.
- [Firehose](docs/FIREHOSE.md): SNS delivery, Go jq partitioning, GZIP and S3 retries.
- [Secrets](docs/SECRETS.md) and [SSM](docs/SSM.md): values, versions and supported configuration.
- [Messaging](docs/MESSAGING.md): all SQS/SNS operations, queue behavior and notification capture.
- [SES capture](docs/SES.md): sending operations, fixtures and JSONL contract.
- [Architecture](docs/ARCHITECTURE.md): package ownership, seams and test placement.
- [Verification](tests/README.md): unit, SDK and native S3 test lanes.
- [Issue triage](docs/ISSUE-TRIAGE.md): AWS core priorities and development boundaries.
- [Backlog](docs/BACKLOG.md): planned AWS capabilities and deferred SES operations.

## Supported behavior

| Area | Local behavior |
| --- | --- |
| Cognito | Pools, clients and user lifecycle; password/admin/refresh/SRP auth, application-owned Node custom challenges, TOTP, native configuration/schema/client token policy, verification/recovery capture, signed JWT/JWKS and revocation. [Operation coverage](docs/COGNITO.md). |
| SNS | All 42 operations: topics/subscriptions, filters, batch/FIFO publishing, SMS and mobile push. SQS/Firehose delivery and registered Lambda execution are local; external delivery is captured. [Contracts](docs/MESSAGING.md). |
| SQS | All 23 operations, including direct/batch sending, attributes/checksums, visibility, FIFO, policies/tags and DLQ redrive. [Contracts](docs/MESSAGING.md). |
| API gateway | Separate `eventbus-gateway` executable; REST/HTTP API REQUEST authorizers, IAM/simple responses, HTTP_PROXY and AWS_PROXY 1.0/2.0. [Contracts](docs/GATEWAY.md). |
| Lambda | Synchronous/async Invoke, registered-target GetFunction metadata, and opt-in bounded warm Python/Node workers. Go/custom runtimes and command handlers remain fresh. [Execution](docs/LAMBDA.md), [S3 routing](docs/S3-NOTIFICATIONS.md). |
| Scheduler | Create/Get/Delete one-time schedules, local Lambda admission, named fixture groups and cancellation. |
| SQS Lambda mappings | Native Create/Get/Delete mappings, batches of 1–10 records (default 10), configured maximum concurrency, registered function/alias execution, FIFO ordering and native visibility/DLQ policy. [Contracts](docs/EVENT-SOURCES.md). |
| Dev consumers | Recipe-driven Go/Python handlers receive SQS events with harness timeouts, partial batch retry and dead-letter policy. |
| Retained-owner dev profile | Opt-in source/callback ownership barrier for native mappings, Scheduler, Cognito runners, Firehose and gateway roots; declared cleanup, diagnostics and generation-checked resume. [Contract](docs/RETAINED-OWNER.md). |
| SES | All six v1 and three v2 sending operations with original JSONL capture; six v1 configuration-set/destination operations, native Send events through SNS, and explicit local Open/Bounce outcomes. [Contracts](docs/SES.md). |
| Firehose | SNS/record/batch ingestion, Go jq partitions, GZIP, error prefixes and retained S3 delivery retries. |
| SSM | Four Standard parameter operations, versions, hierarchy pagination and local SecureString protection. |
| Secrets Manager | Values/stages/metadata/readback, random passwords and on-demand four-step Lambda rotation. |

The application owns SDK configuration, seeds, resource names, provisioning and
consumer handlers. S3 and DynamoDB run separately. EventBus implements a local
AWS subset; IAM enforcement, SMTP and full production AWS semantics are outside
its current scope.

Cognito identities and signing keys persist in SQLite. SES, SNS and Cognito notification JSONL files append across
restarts; templates and other prerequisites load from YAML. Topics, queues,
messages, SES configuration sets/correlations, Firehose buffers, SSM, secrets, schedules and mappings are in memory and need reprovisioning
after restart. Shutdown cancels and joins SQS mappings, stops scheduling and drains accepted rotation/Lambda work while the AWS
listener remains available, then drains HTTP, flushes Firehose and closes stores
and captures. Deadline failures are reported rather than claimed successful.
The opt-in `--retained-owner-callback-port` profile fences suite sources and joins
accepted native work while callback peers remain live. Declared cleanup functions
require another barrier before fixture assertions/resume. Its [endpoint contract](docs/RETAINED-OWNER.md)
requires an exclusive owner; default native API/runtime behavior is unchanged.

## Verify and build releases

```sh
go test ./...
go test -race -timeout 15m ./...
go vet ./...
npm ci --prefix tests/sdk/javascript --ignore-scripts --no-audit --no-fund
uv sync --frozen
uv run --frozen python -m unittest discover -s tests/sdk/python -p 'test_*.py'
EVENTBUS_SMOKE_PYTHON="$PWD/.venv/bin/python" go test -race -count=1 -timeout 15m -tags sdksmoke ./tests/sdk
go test -race -count=1 -tags sdksmoke ./internal/gateway -run '^TestNativeGatewayUnchangedExpressSwagger$'
```

The SDK lane requires Node 20 or newer and the frozen dependencies above.
Tests own their listeners and stores. Application acceptance tests belong in
the consuming application. The optional Firehose integration suite needs an
explicitly owned loopback RustFS endpoint:

```sh
S3_ENDPOINT_URL=http://localhost:9000 go test -race -tags integration ./internal/firehose
```

It creates and deletes a uniquely named bucket. Do not use an unrelated store.

`scripts/build-release.sh` produces CGO-free EventBus and gateway binaries for
Linux/macOS amd64/arm64 and `SHA256SUMS` in `dist/`; an optional argument selects another
output directory. Building does not publish. GitHub checks run on pull requests,
pushes to main and manual dispatch.
They enforce production import boundaries, run unit race/vet checks and a
separate SDK/Swagger lane. Run local checks before a release.

On SSD, wrap builds with `ssd-dev run --purpose build -- <command>` and tests
with `ssd-dev operation --purpose test -- <command>`. Save full test output
before filtering it. Commit, push and publish only when authorized.

Historical source: extracted from Plans commit `c4bf5de1a257022b8b985d91f93b1c68d5786220`.

## License

[MIT](LICENSE), copyright 2026 David Wong. Third-party dependencies retain their
own licenses. The reduced AWS model fixtures retain their Apache-2.0 license,
notices and [attribution](tests/sdk/aws_models/README.md).
