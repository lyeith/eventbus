# EventBus

EventBus is a standalone AWS API emulation and agent development harness.
It supplements local development stacks, including LocalStack, with repeatable
authentication fixtures, event consumers and structured email and notification capture to shorten
the agent verification and evaluation loop.

Run it independently or alongside LocalStack. Apps use normal AWS SDK clients
with selected service endpoints routed to EventBus; other services stay on
LocalStack or native backends. Apps own their scenarios and assertions.

## Start

This guide describes current main, including additions beyond the published
[v0.4.0 binary](https://github.com/lyeith/eventbus/releases/tag/v0.4.0). Build
current source with Go 1.25 or newer and run on an unused port:

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
- [Cognito contracts](docs/COGNITO.md): lifecycle, SRP, custom triggers and persistence.
- [API gateway](docs/GATEWAY.md): REST/HTTP REQUEST authorizers and HTTP/Lambda proxy integrations.
- [Lambda execution](docs/LAMBDA.md): application-owned Go, Python and Node handlers.
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
| SNS | All 42 operations: topics/subscriptions, filters, batch/FIFO publishing, SMS and mobile push. SQS/Firehose delivery is local; external delivery is captured. [Contracts](docs/MESSAGING.md). |
| SQS | All 23 operations, including direct/batch sending, attributes/checksums, visibility, FIFO, policies/tags and DLQ redrive. [Contracts](docs/MESSAGING.md). |
| API gateway | Separate `eventbus-gateway` executable; REST/HTTP API REQUEST authorizers, IAM/simple responses, HTTP_PROXY and AWS_PROXY 1.0/2.0. [Contracts](docs/GATEWAY.md). |
| Lambda | Synchronous/async Invoke with bounded execution and evidence for application-owned Go/custom runtimes, Python, Node and command handlers. [Execution](docs/LAMBDA.md). |
| Scheduler | Create/Get/Delete one-time schedules, local Lambda admission, named fixture groups and cancellation. |
| Consumers | Go binaries or Python handlers receive Lambda-style SQS events, with timeouts, partial batch retry and dead-letter queues. |
| SES | All six v1 and three v2 sending operations, captured as JSONL without delivery. Management APIs are backlogged. |
| Firehose | SNS/record/batch ingestion, Go jq partitions, GZIP, error prefixes and retained S3 delivery retries. |
| SSM | Four Standard parameter operations, versions, hierarchy pagination and local SecureString protection. |
| Secrets Manager | Values/stages/metadata/readback, random passwords and on-demand four-step Lambda rotation. |

The application owns SDK configuration, seeds, resource names, provisioning and
consumer handlers. S3 and DynamoDB run separately. EventBus implements a local
AWS subset; IAM enforcement, SMTP and full production AWS semantics are outside
its current scope.

Cognito identities and signing keys persist in SQLite. SES, SNS and Cognito notification JSONL files append across
restarts; templates and other prerequisites load from YAML. Topics, queues,
messages, Firehose buffers, SSM, secrets and schedules are in memory and need reprovisioning
after restart. Shutdown stops scheduling and drains accepted rotation/Lambda work while the AWS
listener remains available, then drains HTTP, flushes Firehose and closes stores
and captures. Deadline failures are reported rather than claimed successful.

## Verify and build releases

```sh
go test ./...
go test -race ./...
go vet ./...
npm ci --prefix tests/sdk/javascript --ignore-scripts --no-audit --no-fund
uv sync --frozen
uv run --frozen python -m unittest discover -s tests/sdk/python -p 'test_*.py'
EVENTBUS_SMOKE_PYTHON="$PWD/.venv/bin/python" go test -race -count=1 -tags sdksmoke ./...
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
output directory. Building does not publish. GitHub checks run manually through
`workflow_dispatch`; run local checks before a release.

On SSD, wrap builds with `ssd-dev run --purpose build -- <command>` and tests
with `ssd-dev operation --purpose test -- <command>`. Save full test output
before filtering it. Commit, push and publish only when authorized.

Historical source: extracted from Plans commit `c4bf5de1a257022b8b985d91f93b1c68d5786220`.

## License

[MIT](LICENSE), copyright 2026 David Wong. Third-party dependencies retain their
own licenses. The reduced AWS model fixtures retain their Apache-2.0 license,
notices and [attribution](tests/sdk/aws_models/README.md).
