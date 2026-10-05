# EventBus

EventBus is a small Go development harness for the supported AWS Cognito, SNS,
SQS, Firehose, SSM Parameter Store and Secrets Manager APIs. It runs one local
HTTP endpoint; applications use their normal AWS SDKs with endpoint overrides.
It was extracted from Plans commit `c4bf5de1a257022b8b985d91f93b1c68d5786220`.

## Run

With Go 1.25 or newer:

```sh
go build -o eventbus .
mkdir -p .local
./eventbus --cognito-db "$PWD/.local/cognito.db"
```

The default HTTP port is `4100`; `GET /health` reports status and Cognito base
URLs. `./eventbus --help` lists all flags. Configure consuming applications' SDK
endpoints to `http://localhost:4100` with development credentials and region
`us-east-1`. S3 and DynamoDB are separate services; EventBus does not emulate them.

Optional application-owned configuration:

```sh
./eventbus \
  --cognito-db /path/to/project/.local/cognito.db \
  --cognito-pools /path/to/project/cognito_pools.yaml \
  --consumers /path/to/project/consumers.yaml \
  --work-dir /path/to/project \
  --s3-endpoint http://localhost:9000
```

Keep seeds, topic/queue names, resource provisioning and consumer binaries in
the consuming application. `cognito_pools.example.yaml` shows the seed format.
Seed writes retain existing user IDs and update mutable fields; signing keys
persist in the same SQLite database. Preserve the database, issuer, pool and
client IDs when switching an application to a standalone binary. Use separate
state paths for independent projects; the historical default is
`/tmp/cognito-dev.db`.

`--issuer-base` defaults to `http://localhost:4100`; `--jwks-base` defaults to the
issuer base. JWKS is served at `/<pool-id>/.well-known/jwks.json`. Access tokens
default to one hour, refresh tokens to 24 hours. If changing the HTTP port,
configure issuer/JWKS bases explicitly as needed.

## Supported subset

- SNS: topic creation/list/deletion, SQS subscriptions, attribute filter policies,
  publish and subscription listing.
- SQS: queue creation/list/deletion, URL/attributes, receive with visibility and
  long polling, deletion and purge. SNS fanout supplies messages; direct SQS
  `SendMessage` is unsupported.
- Consumers: application Go binaries or Python module handlers run as subprocesses
  with SQS records on stdin/handler input, bounded execution, partial batch retry
  and dead-letter queues. Python handlers require `uv` in the application's
  environment. Consumer configuration supplies handler environment variables.
- Firehose: stream creation/description/deletion and buffered record/batch
  delivery to a separately configured S3-compatible endpoint.
- SSM: put/get, lookup by path and delete.
- Secrets Manager: create, get, put a version, update and delete.
- Cognito: user/pool/client management needed by the tests; password, refresh and
  admin auth; new-password and software-token challenges; TOTP enrollment and
  preferences; password change; sign-out and token revocation; RSA JWT/JWKS.

Cognito uses SQLite; the other stores and message buffers are in memory. Graceful
shutdown drains HTTP, joins workers, flushes Firehose and closes SQLite. Restarting
clears in-memory resources; applications should reprovision them. This remains
a local development harness: it does not implement IAM enforcement or full AWS
semantics. SQS queue URLs use localhost and Firehose uses fixed development S3
credentials (`test` / `testtest123`).

## Verify and release

```sh
go test ./...
go test -race ./...
go vet ./...
uv sync --frozen
uv run --frozen python -m unittest discover -s tests -p 'test_*.py'
EVENTBUS_SMOKE_PYTHON="$PWD/.venv/bin/python" go test -race -count=1 -tags sdksmoke ./...
```

The SDK suite owns temporary listeners and identity stores. The optional native
Firehose test requires an explicitly owned loopback RustFS endpoint:

```sh
S3_ENDPOINT_URL=http://localhost:9000 go test -race -tags integration ./...
```

It creates and deletes its own uniquely named bucket. Do not point it at an
unrelated object store. Application-specific acceptance tests belong to the
consuming application.

```sh
scripts/build-release.sh
```

This builds CGO-free Linux/macOS binaries for amd64/arm64 in `dist/`, with
`SHA256SUMS`. Releases publish those five files. A custom output directory can
be passed as the first argument.

GitHub checks are invoked manually with `workflow_dispatch`; automatic hosted
runs are disabled. Run the commands above before releasing. On SSD, wrap builds
with `ssd-dev run --purpose build -- <command>` and tests with
`ssd-dev run --purpose test -- <command>` so the development owner controls their
processes and temporary state.
