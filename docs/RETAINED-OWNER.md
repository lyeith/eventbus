# Retained-owner recovery

This opt-in **development harness** profile lets one application suite fence new
work, join accepted publication/native callback chains, clean its exact fixtures
and resume the same EventBus process. It preserves native AWS payloads and
handler code. It proves ownership has joined, not business success or persistence
across process restart.

## Start and configure two endpoints

Build current source and use unused ports and owned state paths:

```sh
mkdir -p .local/retained
./eventbus --port 14100 --retained-owner-callback-port 14101 \
  --work-dir "$PWD" --lambda-functions "$PWD/functions.yaml" \
  --issuer-base http://127.0.0.1:14100 \
  --cognito-db "$PWD/.local/retained/cognito.db" \
  --sns-log "$PWD/.local/retained/sns.jsonl" \
  --ses-log "$PWD/.local/retained/ses.jsonl" \
  --cognito-log "$PWD/.local/retained/cognito.jsonl"
```

`functions.yaml` is the application's existing [Lambda recipe](LAMBDA.md).
The callback flag defaults to `0` (off); enabled ports must differ and both
listeners bind `127.0.0.1`. Check `/health` on either listener.

| Endpoint | Callers |
| --- | --- |
| Source: `http://127.0.0.1:14100` | Suite roots, provisioning and retained-owner controls |
| Callback: `http://127.0.0.1:14101` | Registered handlers and their owned native service peers; exact cleanup while held |

The operator must own the whole process exclusively. No unrelated suite or
caller may use the callback endpoint: it remains available to accepted chains
while source admission is fenced. There is no per-suite authorization or causal
token. Dormant sentinel resources may share the store; exact cleanup must leave
them untouched.

For example, configure the suite's existing boto3 client with the source URL:

```python
import boto3
sns = boto3.client("sns", endpoint_url="http://127.0.0.1:14100",
                   region_name="us-east-1", aws_access_key_id="test",
                   aws_secret_access_key="test")
```

Configure registered handlers/owned peers through their existing endpoint
settings, using the callback URL. For an app that already uses these environment
names, its existing function recipe's `environment` section is:

```yaml
environment:
  SNS_ENDPOINT_URL: http://127.0.0.1:14101
  LAMBDA_ENDPOINT_URL: http://127.0.0.1:14101
  AWS_REGION: us-east-1
  AWS_DEFAULT_REGION: us-east-1
  AWS_ACCESS_KEY_ID: test
  AWS_SECRET_ACCESS_KEY: test
```

Use the names your app actually reads; EventBus does not install SDKs or rewrite
handler configuration. Keep other backend endpoints separate. Resource ARNs and
queue URLs remain native identifiers; direct cleanup requests below go to the
callback listener with the original resource identifier.

## Fence, join, clean and resume

Stop launching suite roots, but keep the owner and required peers running. These
controls exist only on the source listener:

```sh
source_url=http://127.0.0.1:14100
callback_url=http://127.0.0.1:14101
control_url="$source_url/__eventbus/dev/retained-owner"
curl -fsS "$control_url"
curl -fsS -X POST "$control_url/quiesce" \
  -H 'Content-Type: application/json' -d '{"timeout_ms":30000}'
```

`timeout_ms` must be `1..300000`. Proceed only after HTTP 200 reports
`state: "held"` and `fixture_safe: true`. Record its current `generation`.
A fresh idle owner's successful barrier looks like:

```json
{"schema_version":"eventbus.retained-owner.v1","state":"held","generation":1,"fixture_safe":true,"work_count":0,"cleanup_envelopes":0,"activities":[],"unlisted_activities":0}
```

| Snapshot field | Meaning |
| --- | --- |
| `state` | `open`, `draining`, `held` or irreversible `shutdown` |
| `generation` | Starts at 1; successful resume increments it |
| `fixture_safe` | Healthy held owner with no counted future business work or cleanup envelopes |
| `work_count` | Accepted HTTP envelopes plus whole native invocation/task lifetimes |
| `cleanup_envelopes` | In-flight cleanup/rejected envelopes; resume waits for zero |
| `activities`, `unlisted_activities` | Up to 64 live activities, plus the omitted count |
| `evidence_failure` | Optional sticky redacted ownership/evidence failure code |
| `last_timeout` | Optional `deadline_exceeded` or `canceled`; cleared by successful join |

Activity entries contain `id`, `kind`, `started_at` and an optional bounded
`request_id`. Kinds include `http.source`, `http.callback`, `lambda_async` and
`lambda_invoke`. HTTP is counted before body parsing/capture until handler
return, independently of caller cancellation. An async task stays counted
through queueing, execution and retry waits without a PID; independent synchronous
native invocations stay counted through child cleanup/join. Zero counts, missing
capture, a dead client or an absent PID alone do not authorize cleanup.
Readiness/control requests are outside work accounting. Diagnostics omit payloads,
credentials, handler output and raw errors. The barrier does not freeze native
stores: SQS requeue and Cognito session maintenance can still run.

While held, the callback listener permits only these synchronous native cleanup
operations: SNS Query `DeleteTopic`/`Unsubscribe`; SQS Query or JSON `DeleteQueue`,
`DeleteMessage`, `DeleteMessageBatch` and `PurgeQueue`. New `Publish`, `Invoke` and
other operations are refused. The source listener stays fenced. For example,
using identifiers returned by this suite's own provisioning:

```sh
topic_arn=arn:aws:sns:us-east-1:000000000000:retained-suite-events
queue_url=http://localhost:14100/queue/retained-suite-events
curl -fsS "$callback_url/" \
  --data-urlencode 'Action=DeleteTopic' --data-urlencode 'Version=2010-03-31' \
  --data-urlencode "TopicArn=$topic_arn"
curl -fsS "$callback_url/" -H 'Content-Type: application/x-amz-json-1.0' \
  -H 'X-Amz-Target: AmazonSQS.DeleteQueue' \
  -d "{\"QueueUrl\":\"$queue_url\"}"
curl -fsS "$control_url"
```

Replace the example identifiers with the suite's exact IDs. Assert each cleanup
result and preserve the sentinel. The application owns any exact cleanup in its
other stores; EventBus performs no automatic reset, data wipe or business cleanup.
Finish all cleanup before resuming. Use the current held snapshot's generation;
for the fresh-owner example above it is `1`:

```sh
curl -fsS -X POST "$control_url/resume" \
  -H 'Content-Type: application/json' -d '{"generation":1}'
```

A successful response is `open`, generation `2`. The next suite uses source
admission normally; its next barrier/resume must use the new generation.
Concurrent cleanup or a stale/non-held generation refuses resume.
Cleanup includes a final capture-health check; a failed confirmation capture can
make the owner unsafe even when its activity count returns to zero. Recheck the
held snapshot and its evidence before resume.

## Refusals and recovery

The first retained profile refuses **all** native SQS mapping and Scheduler
operations, Secrets `RotateSecret`, Firehose operations/subscriptions, and startup
with `--consumers` or `--cognito-triggers`. These autonomous owners are outside the
joined profile. Default mode and its native batch/concurrency contracts are
unchanged. The held cleanup allowlist is narrower than normal native API coverage.

Malformed/unknown/duplicate control fields return 400; wrong methods return 405.
A quiesce deadline/cancellation returns 408 with the current snapshot. It leaves
the source fence and dirty state in place and does not cancel accepted work.
Retain fixtures, inspect activities/evidence and explicitly call `/quiesce` again;
completion after a timeout does not automatically grant a safe held result.
Incomplete ownership evidence returns a 409 snapshot. `/resume` also returns 409
for a stale generation, non-safe state or shutdown. Fenced work requests and
unsupported profile operations return 503 in their native AWS error envelopes;
control responses remain JSON.

An `evidence_failure` is sticky: even zero activity cannot grant fixture safety
or resume. Retain fixtures/evidence, resolve the failure and reprovision the
owner. Business handler failures may settle normally; inspect native completion
records and application state separately from the ownership barrier.

Ctrl-C/SIGTERM irreversibly fences source admission and resume. Shutdown joins
tracked activity while callback peers remain available, then permanently drains
native owners, HTTP and stores/captures. If that shared join fails, shutdown keeps
the original failure, cancels/joins native Lambda while peers/stores remain live,
closes stalled HTTP sockets and joins the received envelopes before permanently
stopping owners. Store cleanup is withheld on that failure. This shutdown abort
policy does not apply to resumable `/quiesce`, which never cancels accepted work.
Shutdown never grants cleanup/resume permission.

Implementation owners: [coordinator/control API](../internal/devquiescence/),
[app profile/listeners](../internal/app/dev_retained_owner.go) and
[Lambda lifetime seam](../internal/lambda/dev_lifecycle.go).
