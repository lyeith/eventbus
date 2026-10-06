# EventBus

EventBus is a standalone AWS API emulation and agent development harness.
It supplements local development stacks, including LocalStack, with repeatable
authentication fixtures, event consumers and structured email capture to shorten
the agent verification and evaluation loop.

Run it independently or alongside LocalStack. Apps use normal AWS SDK clients
with selected service endpoints routed to EventBus; other services stay on
LocalStack or native backends. Apps own their scenarios and assertions.

## Start

Download a checksummed binary from [v0.2.0](https://github.com/lyeith/eventbus/releases/tag/v0.2.0),
or build with Go 1.25 or newer. Run in the foreground on an unused port:

```sh
go build -o eventbus .
mkdir -p .local
./eventbus --port 14100 --issuer-base http://localhost:14100 \
  --cognito-db "$PWD/.local/cognito.db" --ses-log "$PWD/.local/ses.jsonl"
```

From another terminal, check `curl -fsS http://localhost:14100/health`.
Point the selected app SDK clients at that endpoint, using local credentials and region
`us-east-1`. Stop with Ctrl-C or SIGTERM. The default port is `4100`;
`./eventbus --help` lists all flags.

- [Agent workflow](docs/AGENT-HARNESS.md): configure the app, seed resources,
  run consumers, inspect results and manage state.
- [Cognito contracts](docs/COGNITO.md): lifecycle, SRP, custom triggers and persistence.
- [SES capture](docs/SES.md): sending operations, fixtures and JSONL contract.
- [Architecture](docs/ARCHITECTURE.md): package ownership, seams and test placement.
- [Verification](tests/README.md): unit, SDK and native S3 test lanes.
- [Backlog](docs/BACKLOG.md): deferred SES operations.

## Supported behavior

| Area | Local behavior |
| --- | --- |
| Cognito | Pools, clients and user lifecycle; password/admin/refresh/SRP auth, application-owned Node custom challenges, TOTP, signed JWT/JWKS and revocation. [Operation coverage](docs/COGNITO.md). |
| SNS | Topics, SQS subscriptions, attribute filters, publish and listing. |
| SQS | Queues, URL/attributes, receive, visibility, long polling, deletion and purge. Messages arrive through SNS; direct `SendMessage` is unsupported. |
| Consumers | Go binaries or Python handlers receive Lambda-style SQS events, with timeouts, partial batch retry and dead-letter queues. |
| SES | All six v1 and three v2 sending operations, captured as JSONL without delivery. Management APIs are backlogged. |
| Firehose | Streams and buffered record/batch delivery to a separately configured S3-compatible endpoint. |
| SSM | Put/get, lookup by path and delete. |
| Secrets Manager | Create/get, put versions, update and delete. |

The application owns SDK configuration, seeds, resource names, provisioning and
consumer handlers. S3 and DynamoDB run separately. EventBus implements a local
AWS subset; IAM enforcement, SMTP and full production AWS semantics are outside
its current scope.

Cognito identities and signing keys persist in SQLite. SES files append across
restarts; templates and other prerequisites load from YAML. Topics, queues,
messages, Firehose buffers, SSM and secrets are in memory and need reprovisioning
after restart. Shutdown drains HTTP, joins workers, flushes Firehose and closes
SQLite and the capture file.

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

`scripts/build-release.sh` produces CGO-free Linux/macOS binaries for
amd64/arm64 and `SHA256SUMS` in `dist/`; an optional argument selects another
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
