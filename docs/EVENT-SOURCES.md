# SQS Lambda event-source mappings

EventBus supports native Lambda `CreateEventSourceMapping`,
`GetEventSourceMapping`, `ListEventSourceMappings`, `UpdateEventSourceMapping`
and `DeleteEventSourceMapping` for its owned SQS queues
and registered local Lambda functions/aliases. Use the Lambda and SQS SDKs with
EventBus's endpoint, fake credentials and the configured region. All execution
stays local; unknown resources never fall back to AWS.

Register functions using `--lambda-functions` (see [Lambda](LAMBDA.md)), create
the queue through SQS, then create the mapping using its native batch and concurrency
settings:

```python
queue_url = sqs.create_queue(
    QueueName="verification.fifo",
    Attributes={
        "FifoQueue": "true",
        "ContentBasedDeduplication": "true",
        "VisibilityTimeout": "30",
    },
)["QueueUrl"]
queue_arn = sqs.get_queue_attributes(
    QueueUrl=queue_url, AttributeNames=["QueueArn"],
)["Attributes"]["QueueArn"]
mapping = functions.create_event_source_mapping(
    EventSourceArn=queue_arn,
    FunctionName="verification-handler:live",
    BatchSize=30,
    MaximumBatchingWindowInSeconds=1,
    ProvisionedPollerConfig={"MinimumPollers": 2, "MaximumPollers": 5},
    Enabled=True,
)
sqs.send_message(
    QueueUrl=queue_url,
    MessageBody='{"scenario":"owned-verification"}',
    MessageGroupId="verification",
)
functions.get_event_source_mapping(UUID=mapping["UUID"])
functions.delete_event_source_mapping(UUID=mapping["UUID"])
```

`FunctionName` accepts a local name, qualified name, partial ARN or full ARN;
source and function region/account must match this emulator. Function aliases
must be registered explicitly. The function timeout must not exceed the queue's
visibility timeout. AWS recommends a visibility timeout at least six times the
function timeout. Queue redrive/DLQ configuration remains native SQS state.

The handler receives a native `Records` array of up to `BatchSize` entries:
original bodies, message IDs, current receipt handles, source ARN/region, body
digests, string/binary message attributes and system attributes including receive
counts, timestamps and FIFO identifiers. Binary attributes use base64. Selection
also respects the synchronous Lambda JSON payload limit of 6 MiB, including
metadata and JSON escaping. A batch may therefore contain fewer records than
BatchSize; remaining records stay unleased in queue order. With batching window
zero, polling invokes available records without waiting to fill the batch.

By default, the mapping ignores handler return values and settles the batch as a whole. A function error
or timeout skips mapping ACK; still-unsettled records retain native visibility
and redrive behavior. Handler-issued native deletes remain effective. Successful
completion of the actual Lambda runner and its child cleanup permits ACK of each
current, unexpired receipt or proven original receipt already settled by native
deletion. A stale successful HTTP delete no-op is not that proof. See
[receipt settlement](MESSAGING.md#native-mapping-receipt-settlement).
Asynchronous Lambda `Event` admission is never used for SQS settlement.

Set `FunctionResponseTypes=["ReportBatchItemFailures"]` on
`CreateEventSourceMapping` for selective retry. Mapping responses return
the configured setting. After the invocation joins successfully, a response
such as `{"batchItemFailures":[{"itemIdentifier":"<SQS messageId>"}]}` leaves
the reported records for native visibility retry or redrive. The mapping
acknowledges the other records using their original receipts. An empty/null
response or failure list acknowledges the entire batch. Invalid JSON, a malformed
failure entry or an ID outside the current batch leaves the entire batch
unacknowledged by the mapping. A function error or timeout takes precedence over
the response. For FIFO, stop processing at the first failure and include failed
and unprocessed IDs to preserve order.
[AWS SQS batch response behavior](https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-errorhandling.html)
The optional [worker-crash control](LAMBDA.md#one-shot-worker-crash) selects an
already admitted active Lambda request ID with attempt 1. Redelivery retains
the message ID and increments its receive count, with a new Lambda request ID.

Standard queues support `BatchSize` from 1 to 10,000, default 10.
`MaximumBatchingWindowInSeconds` is 0–300. A batch above ten requires at least
one second. The window begins with the first selected record and ends at the
configured delay, count or byte limit. The mapping may dispatch earlier to
reserve function execution time before the oldest receipt expires.

FIFO queues have a ten-record limit and zero window in ordinary mode.
`ProvisionedPollerConfig` enables the wider limits for FIFO, while each
message group still contributes at most ten records to a batch. Minimum and
maximum pollers default to 2 and 200 and read back through the SDK. The local
worker count uses the configured minimum and the local cap. The maximum is
validated, and AWS elastic scaling is outside this contract.
`ScalingConfig.MaximumConcurrency` accepts 2–1000 and works in ordinary mode.
It cannot be combined with `ProvisionedPollerConfig`.

For example, pass this option to Create or Update:

```python
FilterCriteria={"Filters": [{"Pattern": '{"body":{"kind":["ready"]}}'}]}
```

Up to five patterns are ORed. Lambda-style comparisons
include literals, `$or`, `anything-but`, `exists`, `numeric`, prefix/suffix,
and case-insensitive text. Nested patterns apply to JSON bodies. Plain text
bodies use text comparisons. Record metadata can also be matched. The queue
removes unmatched records without invoking Lambda or redriving them. Filters
run before the event byte limit.
[AWS filter syntax](https://docs.aws.amazon.com/lambda/latest/dg/invocation-eventfiltering.html#filtering-syntax)

SQS owns visibility retries, FIFO group ordering, message retention and redrive
after `maxReceiveCount`. All messages in a leased FIFO batch preserve their order;
other workers cannot receive that group while its messages remain in flight.
Different groups can execute concurrently within the mapping's worker limit.
Disabled mappings do not poll. List supports `EventSourceArn` and exact
normalized `FunctionName` filters. Pages contain at most 100 mappings with a
`NextMarker` for continuation. Create accepts `Tags`. Native Lambda tag
operations read, update and remove tags by exact mapping ARN.

Update supports function, batching, enable/disable, concurrency mode, filters
and partial batch settings. Omitting a field preserves it. An empty options
object or `FunctionResponseTypes` list clears the corresponding setting.
When removing provisioned FIFO polling, set the window to zero and batch size
to ten or less in the same request if needed. EventBus stops
old workers from receiving new messages, waits for accepted invocations and
receipt processing, then applies the new settings and returns `202` with the
same UUID and original queue binding. During the join, Get reports `Updating`.
The existing mapping stays active if validation rejects the update.

`LastProcessingResult` reports completion or a
redacted source/invocation/acknowledgment failure. A deleted source disables and
stops its mapping; recreating the same ARN does not rebind the old queue handle.
Deleting a mapping cancels and joins every worker's pending receives and handler
work before returning `202` with a `Deleting` snapshot; later Get returns
`ResourceNotFoundException`. Unsettled receipts keep their native visibility.
Owner shutdown uses the same join barrier before closing Lambda or queue state.
The first Close call's context governs the terminal shutdown result, which is
published after cleanup joins and returned consistently to every concurrent caller.

## Explicit limits

- Records excluded by the cumulative 6 MiB payload budget stay visible and
  unleased for a later batch. If no eligible visible record fits that limit, the
  queue adapter returns a source error without leasing any record; the mapping
  disables itself and cancels/joins its peer workers.
- Stream/Kafka/MQ settings, managed scaling and CloudWatch event metrics
  outside local SQS mapping scope are unsupported.
- Mappings are in-memory resources and must be provisioned for each owned run.
  Cross-account/region mappings and IAM/KMS policy evaluation are unsupported.
- Development resource bounds live in `eventsource.DevOptions`: mapping capacity,
  `MaxWorkersPerMapping` (default 32), `EmptyPollDelay` and a test clock. The worker
  cap must be positive and stays separate from native requests. Actual workers
  are `min(MaximumConcurrency, MaxWorkersPerMapping)` when a ceiling is selected;
  Create/Get continue to report the requested native ceiling. No selected ceiling
  means one worker. Managed cloud scaling and function/account reserved concurrency
  are not emulated.

## Correlated delivery evidence

Opt into private native SQS delivery JSONL in normal or retained-owner mode:

```sh
./eventbus --port 14100 --lambda-functions functions.yaml --work-dir "$PWD" \
  --sqs-delivery-log "$PWD/.local/sqs-delivery.jsonl"
```

`--sqs-delivery-log` defaults to off and requires `--lambda-functions`; omitting
that flag is refused before stores, capture files or listeners open. The sink owns
a regular file with permissions `0600`; existing files must belong to the current
user with those permissions. `-` and final symlinks are refused. Use trusted parent
directories and a path separate from [private Lambda diagnostics](LAMBDA.md#private-invocation-diagnostics).
Native events, handlers, retry and settlement rules stay unchanged.

Records use `schema_version: eventbus.sqs.delivery.v1`:

| Fields | Meaning |
| --- | --- |
| `delivery_id`, `mapping_uuid`, `event_source_arn`, `function_arn`, `time` | Delivery attempt, original source and configured target |
| `request_id`, `invoked_function_arn` | Actual Lambda identity when admitted; not a generated correlation substitute |
| `state`, `invocation_state`, `joined` | Delivery result, native execution result and confirmed invocation completion at its declared scope |
| `completion_scope` | `process` means launched process/listener/children joined; `invocation` means a successful warm response/log boundary, with the worker still separately owned |
| `messages[]` | `message_id`, `receive_count`; terminal `settlement` plus optional `acknowledge_attempted`, `retry_reported` and `evidence_error` |

An `admitted` record precedes child launch and has `joined: false`. Its terminal
follows the declared invocation completion boundary and receipt processing. Warm workers can remain alive after `completion_scope: invocation`; retire them through retained-owner quiescence, reload or service shutdown before asserting whole-worker/child cleanup. Terminal `state` is `succeeded`,
`failed`, `timed_out`, `canceled`, `not_started`, `partial_failure`, `ack_failed` or `uncertain`;
`invocation_state` uses the first five execution states. `partial_failure`
means the invocation succeeded, reported retries, and all eligible acknowledgments
settled. `retry_reported` identifies those messages. An invalid response is a
delivery failure even when the invocation succeeded. Required evidence excludes
bodies, attributes, receipt handles, credentials, logs and arbitrary error text.

| Receipt `settlement` | Meaning for the original receipt |
| --- | --- |
| `mapping_settled` | Actual mapping ACK settled the current, unexpired receipt |
| `native_settled` | A native caller previously deleted it while current and unexpired; caller identity/business success is not proved |
| `unacknowledged` | Original lease is still current and valid |
| `stale_or_expired` | Issued lease is no longer current, including supersession, purge or redrive |
| `unknown` | No retained receipt evidence, including expired history |
| `queue_unavailable` | Original queue was removed, replaced or belongs to another owner |

Match the producer's native message IDs (or its owned enqueue journal), mapping
UUID and expected target. For example, set `mapping_uuid` and `message_id` from
that suite's SDK responses, then inspect its lineage:

```sh
jq -c --arg mapping "$mapping_uuid" --arg message "$message_id" \
  'select(.schema_version == "eventbus.sqs.delivery.v1" and .mapping_uuid == $mapping and any(.messages[]; .message_id == $message)) | {delivery_id, request_id, invoked_function_arn, state, invocation_state, joined, messages}' \
  .local/sqs-delivery.jsonl
```

Require successful, `joined: true` terminal evidence and settled receipts for
the expected message IDs, then assert application business state separately.
Queue counts, global `LastProcessingResult`, admission and PID absence cannot
replace that proof. Failed or timed-out handlers can still have `native_settled`
receipts. Capture/ownership failure is sticky: fresh mapping polling stops,
`EvidenceErr`/close reports uncertainty
and retained-owner safe proof fails. Missing or `uncertain` evidence is never success.
Trusted handlers must await side work and keep it in the owned process group;
retained diagnostic pipes or failed cleanup cannot certify a join.

## Agent verification

Use an owned queue/function, assert handler side effects, then assert queue
settlement. API creation and invocation start alone do not prove delivery.
Check `ApproximateReceiveCount` and changed receipt handles when testing retries;
use SQS redrive policy to verify poison messages reach the selected DLQ. For FIFO,
assert later same-group records wait for earlier completion/retry. Delete the
mapping before deleting its queue or stopping the owner.

The frozen boto3 lane (`TestSQSLambdaPythonSDKSmoke`) covers native provisioning,
FIFO order, disabled/deleted mappings, alias execution, native record fields,
completion-only deletion, standard visibility retry, five receives to a FIFO
DLQ, real handler timeout, cross-owner isolation and teardown with pending work.
`TestSQSBatchPythonSDKSmoke` covers configured size five and concurrency two, whole-batch
success/failure, payload-budget selection and visibility/redrive. Core tests verify
overlapping invocations and their ceiling, separate local caps, snapshot ownership,
failed-batch retention, cancellation of peer workers and complete Delete/Close
join barriers. Batch size one remains supported and keeps its existing SDK lane.
See [SDK verification](../tests/sdk/README.md) for the lane command and dependencies.

Contracts follow AWS's [CreateEventSourceMapping API](https://docs.aws.amazon.com/lambda/latest/api/API_CreateEventSourceMapping.html),
[SQS mapping configuration](https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-configure.html),
[ScalingConfig](https://docs.aws.amazon.com/lambda/latest/api/API_ScalingConfig.html)
and [DeleteEventSourceMapping API](https://docs.aws.amazon.com/lambda/latest/api/API_DeleteEventSourceMapping.html).
