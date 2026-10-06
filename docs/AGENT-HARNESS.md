# Agent verification and evaluation workflow

Start with [README](../README.md#start). Use an unused port and state paths
owned by the current project or test run. Keep the process running while the
application and acceptance tests use it; do not reset an existing developer stack.

The loop is: implement a change, run an app scenario, inspect SDK responses,
captures and app state, assert the expected behavior, then iterate. The app owns
scenario orchestration and evaluation assertions; EventBus supplies the runtime
and evidence.

## Alongside LocalStack

Select endpoints per service using the application's normal SDK configuration:

| Requests | Endpoint owner |
| --- | --- |
| Selected supported auth, event and email APIs | EventBus, e.g. `http://localhost:14100` |
| S3, DynamoDB and other services kept in LocalStack | LocalStack's configured endpoint |
| Services supplied by native local backends | That backend's configured endpoint |

EventBus consumers poll EventBus queues: provision their SNS-to-SQS pipeline
entirely there. Each system owns its resources and state; unsupported EventBus
operations return errors rather than forwarding to LocalStack.
See [AWS endpoint configuration](https://docs.aws.amazon.com/sdkref/latest/guide/feature-ss-endpoints.html)
and [LocalStack networking](https://docs.localstack.cloud/aws/customization/networking/accessing-endpoint-url/)
for client and container addressing.

## Connect and seed the app

1. Override the selected AWS SDK clients' endpoints to the EventBus URL.
   Use explicit local credentials (for example `test`/`test`) and region
   `us-east-1`. Keep S3 and DynamoDB endpoints separate.
2. Provision topics, queues, subscriptions, streams, parameters and secrets
   through supported SDK operations, using application-owned names.
3. For deterministic login, copy [cognito_pools.yaml](../examples/cognito_pools.yaml)
   into the app and pass `--cognito-pools /path/to/app/cognito_pools.yaml`.
   Reapplying preserves user IDs and lifecycle state for the same password.
   [Cognito contracts](COGNITO.md) explains identity, temporary passwords and migration.
4. Configure JWT validation with issuer `<issuer-base>/<pool-id>` and JWKS
   `<jwks-base>/<pool-id>/.well-known/jwks.json`. `/health` reports the base URLs;
   append the pool ID. `--jwks-base` defaults to `--issuer-base`.
5. For stored SES templates or strict identity checks, copy
   [ses.yaml](../examples/ses.yaml) and pass `--ses-config <path>`.
   Management APIs do not create these resources yet.

For custom SRP/email MFA, pass `--cognito-triggers <path> --work-dir <app-root>`
with the app-owned Node handlers and declared local SES endpoint/credentials.
[Custom triggers](CUSTOM-TRIGGERS.md) documents the execution contract.

An SDK endpoint override changes where real SDK requests go. Use the
application's existing SDK and configuration; EventBus does not install client
libraries or configure the application's credentials.

## Run event consumers

Pass `--consumers /path/to/app/consumers.yaml --work-dir /path/to/app`.
A minimal Go consumer configuration is:

```yaml
consumers:
  - name: audit
    type: go
    queue: audit-events
    handler: ./bin/audit-consumer
    env:
      AWS_REGION: us-east-1
```

Create the source queue, its DLQ (default `<queue>-dlq`) and SNS subscription
through the SDK. Without a provisioned DLQ, failures keep retrying. Publish
through SNS; direct SQS `SendMessage` is unsupported. The event has `Records`
with `messageId`, `receiptHandle` and `body`; parse the SNS JSON in `body` to
read its `Message`. Go handlers read this event from stdin. Python handlers use
`type: python` and a dotted `package.module.function`; `uv` runs them in
`--work-dir`. Consumers inherit only a small environment allowlist: supply
SDK endpoints, credentials and app configuration through `env`, even if those
variables are exported in the parent shell.

Defaults: batch size 1, timeout 60 seconds, five receives before dead-lettering,
DLQ name `<queue>-dlq`. Handlers can return
`{"batchItemFailures":[{"itemIdentifier":"<message-id>"}]}` to retry selected
records. Go handlers write one JSON result to stdout (`{}` accepts the whole
batch); Python handlers return the result object. Send diagnostic logs to stderr.

## Inspect and finish

- Check `/health` for readiness, then exercise the application's normal flows.
  Assert SDK responses, queue/DLQ outcomes and application state.
- Read operational logs on stderr. Add `--debug` to see captured handler
  diagnostics and batch settlement. Consumer stdout is the JSON result.
- Read SES JSONL from the selected `--ses-log` file. Use `request` and
  `outcome` for assertions; [SES capture](SES.md) explains bulk failures and
  derived email views. Only SES requests have this structured capture stream.
- Stop the owned process with Ctrl-C or SIGTERM. Shutdown drains admitted HTTP
  requests and joins consumers before releasing stores.

Keep Cognito's SQLite path, pool/client IDs and issuer stable when continuing
the same app environment. Use fresh owned paths for independent tests.
Reprovision in-memory resources after restart. Supply `--s3-endpoint` for
Firehose tests; its local S3 credentials are `test`/`testtest123`.

Use the application's tests for end-to-end behavior. When changing EventBus,
follow [AGENTS.md](../AGENTS.md) and [SDK verification](../tests/README.md).
