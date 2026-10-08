# Retained-owner recovery

This opt-in **development harness** lets one exclusive application suite fence
new roots, join accepted work, clean exact fixtures and resume the same EventBus
process and resources. Native payloads, handlers and authentication contracts stay
unchanged. The barrier proves joined ownership; the application must separately
assert business results and fixture absence. It makes no persistence claim across
process restart.

## Configure the owner and endpoints

Build current source. Use unused ports, owned state paths and the application's
existing [Lambda recipe](LAMBDA.md). This example assumes it registers the exact
cleanup target `fixture-cleanup:local`; replace that reference with yours:

```sh
mkdir -p .local/retained
./eventbus --port 14100 --retained-owner-callback-port 14101 \
  --retained-owner-cleanup-functions fixture-cleanup:local \
  --work-dir "$PWD" --lambda-functions "$PWD/functions.yaml" \
  --issuer-base http://127.0.0.1:14101 --jwks-base http://127.0.0.1:14101 \
  --cognito-db "$PWD/.local/retained/cognito.db" \
  --sns-log "$PWD/.local/retained/sns.jsonl" \
  --ses-log "$PWD/.local/retained/ses.jsonl" \
  --cognito-log "$PWD/.local/retained/cognito.jsonl"
```

The callback flag defaults to `0` (off). Enabled ports must differ; both listeners
bind `127.0.0.1`. `/health` works on either. Cleanup declarations are optional,
comma-separated exact registered names/ARNs with their intended qualifiers.
ARN account references must match the configured account; full ARNs must also
match the configured region. Foreign references are refused.
Startup rejects unknown, empty or duplicate resolved targets and declarations
without retained mode or registered functions. There are no wildcard declarations.
Declaring one target does not grant its other aliases/qualifiers.
Use ordinary provisioning and recipes for mappings, Scheduler, Cognito triggers
and Firehose/subscriptions.

| Endpoint | Callers |
| --- | --- |
| Source: `http://127.0.0.1:14100` | Suite roots, provisioning and owner controls |
| Callback: `http://127.0.0.1:14101` | Registered handlers/owned native peers; declared cleanup and allowed native deletion while held |
| Gateway public: `http://127.0.0.1:14180/local` | Suite/user HTTP roots |
| Gateway continuation: `http://127.0.0.1:14181/local` (optional) | Exclusively trusted registered handlers/declared cleanup calling application HTTP |

The operator owns the entire process and its gateway exclusively. No unrelated
suite or caller may use the trusted callback endpoint. It admits descendants of
accepted work while roots are fenced, without per-suite authorization or causal
tokens. Dormant sentinels may share the store; exact cleanup must preserve them.
Trusted OS handlers must await their side work and keep descendants in the owned
process group. Native callback invocations have independent leases even when the
originating process or response disappears.

Configure suite clients with the source URL, for example:

```python
import boto3
sns = boto3.client("sns", endpoint_url="http://127.0.0.1:14100",
                   region_name="us-east-1", aws_access_key_id="test",
                   aws_secret_access_key="test")
```

Configure handlers/owned peers through the endpoint settings they already read.
For an app using these environment names, its function recipe includes:

```yaml
environment:
  SNS_ENDPOINT_URL: http://127.0.0.1:14101
  LAMBDA_ENDPOINT_URL: http://127.0.0.1:14101
  AWS_REGION: us-east-1
  AWS_DEFAULT_REGION: us-east-1
  AWS_ACCESS_KEY_ID: test
  AWS_SECRET_ACCESS_KEY: test
```

Use your app's actual setting names. EventBus does not install SDKs, rewrite
handler configuration or generate authentication. Keep other provider endpoints
available until joined. ARNs and queue URLs remain native resource identifiers.

For standalone `eventbus-gateway`, set `retained_owner_control_url` to the source
control URL and point **every** AWS_PROXY integration and authorizer Invoke URL
at the advertised callback origin. Its [gateway recipe](GATEWAY.md#retained-owner-ingress)
requires a literal loopback HTTP control URL; external HTTP integrations and
frontend proxying are outside this joined profile and rejected.

For downstream application HTTP during drain, additionally select gateway recipe
`retained_owner_continuation_port: 14181` (CLI `--retained-owner-continuation-port`).
It defaults off, requires the retained control URL and must differ from the public
port. Point handlers/cleanup functions' existing application HTTP settings at this
private loopback origin; suite requests stay public. Native routing and auth stay
unchanged. This is an exclusive trusted-port assumption, not a causal token or
public authorization scope; no header can upgrade a public request.
See [gateway continuations](GATEWAY.md#trusted-http-continuations).

Cold JWT verification must fetch JWKS from the native callback endpoint, e.g.
`http://127.0.0.1:14101/<pool-id>/.well-known/jwks.json`. The example advertises its
issuer/JWKS bases there so issuer-derived discovery stays available during drain.
Configure the application's matching issuer/pool/client binding and retain full
signature/claim validation. An explicit JWKS URL may preserve a stable expected
issuer; neither gateway origin nor the fenced source is a continuation JWKS route.

## Fence, clean, join again and resume

Stop launching suite roots and keep the owner/providers/peers running. Controls
exist only on the source listener:

```sh
source_url=http://127.0.0.1:14100
callback_url=http://127.0.0.1:14101
control_url="$source_url/__eventbus/dev/retained-owner"
curl -fsS "$control_url"
curl -fsS -X POST "$control_url/quiesce" \
  -H 'Content-Type: application/json' -d '{"timeout_ms":30000}'
```

`timeout_ms` is `1..300000`. Require HTTP 200, `state: "held"` and
`fixture_safe: true`; retain its owner identity and current generation. A fresh
idle owner's successful result resembles:

```json
{"schema_version":"eventbus.retained-owner.v1","owner_id":"c96e26dd-969d-487f-8c79-bb71871d64d4","callback_origin":"http://127.0.0.1:14101","state":"held","generation":1,"fixture_safe":true,"work_count":0,"cleanup_envelopes":0,"activities":[],"unlisted_activities":0}
```

While held, invoke a declared cleanup target through native **RequestResponse**
(default if the invocation type is omitted). The application supplies its
unchanged authenticated payload, exact fixture IDs and business assertions;
`cleanup-event.json` below is that application-owned payload:

```sh
curl -fsS -D - -X POST \
  "$callback_url/2015-03-31/functions/fixture-cleanup:local/invocations" \
  -H 'X-Amz-Invocation-Type: RequestResponse' \
  -H 'Content-Type: application/json' --data-binary @cleanup-event.json
curl -fsS -X POST "$control_url/quiesce" \
  -H 'Content-Type: application/json' -d '{"timeout_ms":30000}'
```

Inspect native FunctionError and the application result; HTTP 200 alone is not
business success. Cleanup admission changes `held` to `draining`, adds a cleanup
activity and leaves sources fenced. Callback descendants, mapped sends and
Firehose puts remain joined. **Explicitly quiesce again** before fixture-absence
attestation, another held cleanup root or resume. No new generation header,
authentication bypass or cleanup payload format is introduced.

Held callbacks also permit exact synchronous native deletion:

| API | Allowed operations |
| --- | --- |
| SNS Query | `DeleteTopic`, `Unsubscribe` |
| SQS Query/JSON | `DeleteQueue`, `DeleteMessage`, `DeleteMessageBatch`, `PurgeQueue` |
| Lambda mappings REST | `DeleteEventSourceMapping` |
| Scheduler REST | `DeleteSchedule`, `DeleteScheduleGroup` |
| Firehose JSON | `DeleteDeliveryStream` |

For example, use the identifiers returned by this suite's own provisioning:

```sh
topic_arn=arn:aws:sns:us-east-1:000000000000:retained-suite-events
queue_url=http://localhost:14100/queue/retained-suite-events
curl -fsS "$callback_url/" \
  --data-urlencode 'Action=DeleteTopic' --data-urlencode 'Version=2010-03-31' \
  --data-urlencode "TopicArn=$topic_arn"
curl -fsS "$callback_url/" -H 'Content-Type: application/x-amz-json-1.0' \
  -H 'X-Amz-Target: AmazonSQS.DeleteQueue' \
  -d "{\"QueueUrl\":\"$queue_url\"}"
curl -fsS -X POST "$control_url/quiesce" \
  -H 'Content-Type: application/json' -d '{"timeout_ms":30000}'
```

Replace example IDs, assert cleanup results and preserve sentinels. Cleanup also
checks capture health; failed evidence can leave the owner unsafe after an
operation returns. EventBus performs no automatic reset, data wipe or business
cleanup. The application must inspect its other stores and any paused resources.

Only after the final safe held result and fixture assertions, resume with its
current generation. For the fresh-owner example it is `1`:

```sh
curl -fsS -X POST "$control_url/resume" \
  -H 'Content-Type: application/json' -d '{"generation":1}'
```

Success returns `open`, generation `2`, and reuses the same queues, mappings,
schedules, streams and registrations that cleanup retained. Use the returned
next generation for later barriers. Outstanding cleanup/work, dirty evidence,
a stale generation or shutdown prevents resume. Keep the same owner identity;
do not apply a held result from another process to this endpoint. Native age and
retry policies continue to apply to retained resources.

## What the barrier joins

| Owner | Joined work and retained state |
| --- | --- |
| HTTP and Lambda | Envelopes from before parsing/capture through handler return; whole queued/running/retrying Event tasks; independent synchronous invocations through child join |
| Enabled SQS mappings | Accepted batches plus message custody through delay, visibility, whole-batch retry, acknowledgment and redrive; callback/cleanup sends to mapped queues transfer custody |
| Scheduler | Accepted due target admissions and their retry waits; unclaimed schedules remain paused native resource state |
| Cognito runners | Actual trigger execution, owned children and accepted callback chains |
| Firehose | Buffered records, pending objects, late accepted puts and destination retries; reversible force-flush remains enabled through held cleanup until resume |
| Retained gateway | Public root and private `http.gateway.continuation` leases granted before body read, authorization or Invoke and held through handler return; independently accepted native descendants |

While roots are fenced, mapping workers receive only messages in retained
custody. Registering an enabled mapping adopts that queue's queued/in-flight
messages, including delayed/invisible records. Queues without an enabled mapping
and unclaimed future schedules are retained paused state, not active business work.
Resources are not deleted/recreated to pause them, and native enabled/state
settings are not rewritten. A successful barrier does not prove those retained
resources or application fixtures absent.
Firehose keeps providers alive and never uses irreversible Shutdown for resumable
controls. SQS requeue and Cognito session maintenance may still run.

Gateway leases have no expiry. Lost acquire/release acknowledgments are reconciled
idempotently; a missing reply never proves a root stopped. A candidate freezes
its cached owner generation at envelope entry before RPC and touches no app resources
without a grant; refusal cannot migrate that envelope into a resumed generation.
After resume, cache refresh may briefly refuse a fresh root; retry as a new request.
Private gateway continuations require clean `open`, `draining` or `shutdown` with
actual accepted work. Idle `open`/`held`, resuming, stale generations and uncertainty
refuse them. Keep the private listener live across owner shutdown until counted
work/cleanup and received envelopes join. After observing shutdown, bounded join
or control-loss timeout exits dirty; ordinary open-state polling has no such timeout.
Public roots remain fenced. The shared gateway ledger holds at most 4,096
active/completed root and continuation leases per generation; full ledgers refuse
new admission. A healthy quiesce/resume renews it. Completion
acknowledgments can replay in the current and immediately prior generation, so
reconcile lost replies promptly; older completion history is not retained.

| Snapshot field | Meaning |
| --- | --- |
| `owner_id`, `callback_origin` | Process identity and advertised callback origin; a new process is a new owner |
| `state`, `generation` | `open`, `draining`, `held`, `shutdown`; generation starts at 1 and increments on resume |
| `fixture_safe` | Healthy held owner with no counted future business work or cleanup envelopes |
| `work_count`, `cleanup_envelopes` | Joined work and in-flight cleanup/rejected envelopes |
| `activities`, `unlisted_activities` | Up to 64 live activities, plus omitted count |
| `evidence_failure`, `last_timeout` | Optional sticky redacted evidence code; optional `deadline_exceeded`/`canceled` cleared by successful join |

Activities contain `id`, `kind`, `started_at` and optional bounded `request_id`.
Kinds include `http.source`, `http.callback`, `http.gateway`,
`http.gateway.continuation`, `lambda_async`,
`lambda_invoke`, `lambda_cleanup`, `sqs_message`, `sqs_mapping`,
`scheduler.dispatch`, `cognito_trigger` and `firehose.record`. Diagnostics omit
payloads, credentials, handler output and raw errors. Caller cancellation, no
PID, missing capture, a grace period or zero counts alone are not joined proof.
Readiness/control requests are outside business-work accounting.

## Refusals, dirty recovery and shutdown

The remaining untracked sources are refused: startup `--consumers`, Secrets
`RotateSecret` and SQS `StartMessageMoveTask`. While held, undeclared Invoke,
Event/DryRun, Publish and other work-producing roots are refused; trusted
continuations during draining remain subject to exclusive ownership. Default
mode and supported native API, authentication and settlement contracts are unchanged.

Malformed/unknown/duplicate control fields return 400; wrong methods return 405.
A quiesce deadline/cancellation returns 408 with the current snapshot, leaving
the fence and dirty state without canceling accepted work. Retain fixtures and
providers, inspect activities/evidence and explicitly retry `/quiesce`.
Completion after timeout does not automatically grant a safe held result.
Incomplete evidence or invalid resume returns 409; fenced native work/profile
refusals return AWS-shaped 503 errors. Controls remain JSON.

Sticky `evidence_failure` prevents safe cleanup/resume even at zero counts.
Retain fixtures/evidence, resolve the failure and reprovision the owner. Business
handler failures may settle normally; they are separate from ownership uncertainty.

Ctrl-C/SIGTERM irreversibly fences sources/resume and joins all accepted owners
while callbacks, controls, providers and stores remain live, then drains HTTP and
closes stores/captures. If final join fails, shutdown preserves that failure,
aborts/joins local owners and closes transports before joining received
envelopes; stores/evidence are retained. Uncertain foreign leases may become
sticky dirt only after irreversible shutdown and transport closure, never
cleanup-safe completion. Resumable `/quiesce` never applies that abort policy.
Shutdown never grants cleanup or resume permission.

Implementation owners: [shared activity/source ports](../internal/devactivity/),
[coordinator/control and source leases](../internal/devquiescence/),
[app listeners/profile](../internal/app/dev_retained_owner.go),
[declared cleanup](../internal/app/dev_cleanup.go) and
[gateway admission](../internal/gateway/dev_retained_owner.go).
