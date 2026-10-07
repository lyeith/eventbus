# SQS Lambda event-source mappings

EventBus supports native Lambda `CreateEventSourceMapping`,
`GetEventSourceMapping` and `DeleteEventSourceMapping` for its owned SQS queues
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
    BatchSize=5,
    ScalingConfig={"MaximumConcurrency": 2},
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

Handler return values are ignored for this whole-batch contract. A function error
or timeout skips mapping ACK; still-unsettled records retain native visibility
and redrive behavior. Handler-issued native deletes remain effective. Successful
completion of the actual Lambda runner and its child cleanup permits ACK of each
current, unexpired receipt or proven original receipt already settled by native
deletion. A stale successful HTTP delete no-op is not that proof. See
[receipt settlement](MESSAGING.md#native-mapping-receipt-settlement).
Asynchronous Lambda `Event` admission is never used for SQS settlement.

`BatchSize` accepts 1–10 and defaults to the native SQS value of 10.
`ScalingConfig.MaximumConcurrency` accepts 2–1000, is preserved in Create/Get,
and limits concurrent invocations of this mapping. An empty `ScalingConfig`
selects no ceiling. EventBus runs a fixed owned worker set, bounded by the selected
ceiling and its separate local worker cap. Without a selected ceiling, the local
policy uses one worker. This is local execution behavior, not AWS managed scaling.

SQS owns visibility retries, FIFO group ordering, message retention and redrive
after `maxReceiveCount`. All messages in a leased FIFO batch preserve their order;
other workers cannot receive that group while its messages remain in flight.
Different groups can execute concurrently within the mapping's worker limit.
Disabled mappings do not poll. `LastProcessingResult` reports completion or a
redacted source/invocation/acknowledgment failure. A deleted source disables and
stops its mapping; recreating the same ARN does not rebind the old queue handle.
Deleting a mapping cancels and joins every worker's pending receives and handler
work before returning `202` with a `Deleting` snapshot; later Get returns
`ResourceNotFoundException`. Unsettled receipts keep their native visibility.
Owner shutdown uses the same join barrier before closing Lambda or queue state.
The first Close call's context governs the terminal shutdown result, which is
published after cleanup joins and returned consistently to every concurrent caller.

## Explicit limits

- Only standard/FIFO SQS sources, `BatchSize` 1–10, batching window zero,
  `ScalingConfig.MaximumConcurrency` and whole-batch responses are supported.
  Standard queue batches above ten remain an explicit capability gap.
- Records excluded by the cumulative 6 MiB payload budget stay visible and
  unleased for a later batch. If no eligible visible record fits that limit, the
  queue adapter returns a source error without leasing any record; the mapping
  disables itself and cancels/joins its peer workers.
- List/Update mapping operations, nonzero batching windows, filters, partial batch
  responses, provisioned pollers, stream/Kafka/MQ settings, tags and other selected
  Create options return `InvalidParameterValueException`; they are not ignored.
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
| `state`, `invocation_state`, `joined` | Delivery result, native execution result and confirmed runner/child cleanup |
| `messages[]` | `message_id`, `receive_count`; terminal `settlement` plus optional `acknowledge_attempted` and `evidence_error` |

An `admitted` record precedes child launch and has `joined: false`. Its terminal
follows runner/cleanup and receipt processing. Terminal `state` is `succeeded`,
`failed`, `timed_out`, `canceled`, `not_started`, `ack_failed` or `uncertain`;
`invocation_state` uses the first five execution states. Required evidence excludes
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
