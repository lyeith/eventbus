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

EventBus consumers poll EventBus queues: provision their queues and any SNS-to-SQS pipeline
entirely there. Each system owns its resources and state; unsupported EventBus
operations return errors rather than forwarding to LocalStack.
See [AWS endpoint configuration](https://docs.aws.amazon.com/sdkref/latest/guide/feature-ss-endpoints.html)
and [LocalStack networking](https://docs.localstack.cloud/aws/customization/networking/accessing-endpoint-url/)
for client and container addressing. [Validated S3 notifications](S3-NOTIFICATIONS.md)
documents the mixed-provider routing needed for real LocalStack uploads to invoke
registered EventBus functions.

For retained-suite interruption/recovery, opt into [Retained owner](RETAINED-OWNER.md).
Its source URL is for suite roots; registered handlers/owned native peers use the
separate trusted callback URL. Fence and join before exact fixture cleanup, then
explicitly join declared cleanup/descendants before fixture assertions and resume.
The profile joins native mappings, Scheduler, Cognito runners and Firehose without
recreating resources; a retained gateway uses shared root leases. A stopped client
or absent PID is insufficient. Legacy consumers, rotation and message-move tasks
remain outside the profile; see the guide for exact refusals and endpoint settings.

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

## Native event delivery

Register application functions with `--lambda-functions <recipe>`. Subscribe
SNS topics with `Protocol="lambda"` and the registered local function/alias ARN.
Publish acceptance and `DeliveryAdmission` evidence do not prove execution;
correlate the invocation request ID with Lambda completion evidence and the
application side effect. [Messaging](MESSAGING.md) states filtering/retry limits.

For SQS, create a native mapping through the Lambda SDK with `BatchSize` from
1–10 (default 10) and optional `ScalingConfig={"MaximumConcurrency": 2}`. [Mappings](EVENT-SOURCES.md) shows provisioning and limits.
Mapping ACK waits for successful execution and actual child join; handler-issued
native deletes remain effective independently.
Queue visibility, FIFO and redrive settings govern retries. Delete the mapping
before deleting its queue. Mappings and registered targets belong to one owner;
provision them for each run.

## Dev recipe consumers

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
through the SDK. Without a provisioned DLQ, failures keep retrying. Send
directly with SQS `SendMessage`/`SendMessageBatch`, or publish through SNS.
`Records` preserve custom/system attributes, checksums, queue ARN and region.
Direct sends and raw SNS subscriptions put the application message in `body`;
otherwise parse the SNS JSON envelope to read its `Message`. Go handlers read this event from stdin. Python handlers use
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

- Check the AWS listener's `/health` for readiness, then exercise application flows.
  The separate gateway uses `dev_health_path` (default `/health`); select a private
  path when the application owns `/health`. See [Gateway readiness](GATEWAY.md#development-readiness).
  Assert SDK responses, queue/DLQ outcomes and application state.
- Read operational logs on stderr. `--debug` adds dev-recipe consumer diagnostics
  and batch settlement; consumer stdout is the JSON result.
- Opt into `--sqs-delivery-log` with `--lambda-functions` for [native message-to-invocation
  and joined receipt evidence](EVENT-SOURCES.md#correlated-delivery-evidence).
  Match exact producer message IDs and the mapping before asserting completion.
- Select recipe `dev_diagnostics.log_path` for [private native handler diagnostics](LAMBDA.md#private-invocation-diagnostics).
  Correlate actual request IDs/attempts; runtime success and logs never replace
  application business assertions. Keep these potentially sensitive files private.
  Inspect `termination_cause` and `elapsed_ms` before attributing a timeout;
  opt into [Python wait snapshots](LAMBDA.md#python-wait-snapshots) before a known
  shorter caller budget when terminal logs cannot locate a wait.
- Read Cognito notification JSONL from `--cognito-log`; native signup, recovery
  and invitation flows expose local codes/passwords there. Use schema and resource
  IDs to correlate requests; [Cognito](COGNITO.md) documents the evidence fields.
- Read SES JSONL from the selected `--ses-log` file. Use `request` and
  `outcome` for assertions; [SES capture](SES.md) explains bulk failures and
  derived email views. Native SES configuration-set SNS Send events correlate
  through `mail.messageId`; explicit local Open/Bounce controls reuse that exact
  accepted ID and configured destinations. See [SES events](SES.md).
- For repeated Python/Node calls, opt into [warm workers](LAMBDA.md#warm-workers).
  Reload a selected function explicitly after recipe/source/environment edits.
  Successful warm invocation evidence covers the handler response/log boundary;
  quiesce the retained owner before treating worker/child cleanup as complete.
- Read SNS JSONL from `--sns-log`; [Messaging](MESSAGING.md) documents the versioned
  schema, subscription tokens, sandbox OTPs and external delivery intents.
  Assert the message ID and per-delivery status; capture proves local intent,
  not delivery to a real recipient.
- Stop the owned process with Ctrl-C or SIGTERM. Native SQS mappings cancel/join,
  scheduling/dev consumers stop first;
  accepted rotation and Lambda Event work drain with the AWS listener available.
  HTTP drains before stores/captures close. A deadline abort is a failed shutdown.
  In retained-owner mode, shutdown first joins accepted callback chains with
  peers available and irreversibly disables resume. Use the resumable barrier
  while retaining the process between suites.

Keep Cognito's SQLite path, pool/client IDs and issuer stable when continuing
the same app environment. Use fresh owned paths for independent tests.
Reprovision in-memory resources after restart. Follow Lambda JSONL terminal
records for asynchronous completion, and Scheduler/rotation redacted operational
logs for target admission/workflow outcomes. HTTP acceptance alone is insufficient. Supply `--s3-endpoint` for
Firehose tests; its local S3 credentials are `test`/`testtest123`.

Use the application's tests for end-to-end behavior. When changing EventBus,
follow [AGENTS.md](../AGENTS.md) and [SDK verification](../tests/README.md).
