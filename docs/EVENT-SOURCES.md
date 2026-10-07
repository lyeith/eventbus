# SQS Lambda event-source mappings

EventBus supports native Lambda `CreateEventSourceMapping`,
`GetEventSourceMapping` and `DeleteEventSourceMapping` for its owned SQS queues
and registered local Lambda functions/aliases. Use the Lambda and SQS SDKs with
EventBus's endpoint, fake credentials and the configured region. All execution
stays local; unknown resources never fall back to AWS.

Register functions using `--lambda-functions` (see [Lambda](LAMBDA.md)), create
the queue through SQS, then create the mapping explicitly with `BatchSize=1`:

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
    BatchSize=1,
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

The handler receives one native `Records` entry: original body, message ID,
current receipt handle, source ARN/region, body digest, string/binary message
attributes and system attributes including receive count, timestamps and FIFO
identifiers. Binary attributes use base64. Handler return values are ignored
for this whole-batch contract; a function error or timeout retains the message.
Successful execution acknowledges the current receipt only after the Lambda runner
finishes successfully and joins its child processes. Asynchronous Lambda
`Event` admission is a different contract and is never used for SQS settlement.

Each enabled mapping polls and executes serially. SQS owns visibility retries,
FIFO group ordering, message retention and redrive after `maxReceiveCount`.
Disabled mappings do not poll. `LastProcessingResult` reports completion or a
redacted source/invocation/acknowledgment failure. A deleted source disables and
stops its mapping; recreating the same ARN does not rebind the old queue handle.
Deleting a mapping cancels and joins pending receives and handler work before
returning `202` with a `Deleting` snapshot; later Get returns
`ResourceNotFoundException`. Unsettled receipts keep their native visibility.
Owner shutdown uses the same join barrier before closing Lambda or queue state.
The first Close call's context governs the terminal shutdown result, which is
published after cleanup joins and returned consistently to every concurrent caller.

## Explicit limits

- Only standard/FIFO SQS sources, explicit `BatchSize=1`, batching window zero,
  and whole-batch responses are supported. The native omitted batch default is
  ten and is rejected until batching exists.
- List/Update mapping operations, filters, partial batch responses, concurrency
  scaling/provisioned pollers, stream/Kafka/MQ settings, tags and other selected
  Create options return `InvalidParameterValueException`; they are not ignored.
- Mappings are in-memory resources and must be provisioned for each owned run.
  Cross-account/region mappings and IAM/KMS policy evaluation are unsupported.
- Development resource bounds live in `eventsource.DevOptions`: mapping capacity,
  `EmptyPollDelay` and a test clock. They are distinct from native requests;
  one serial poller per mapping does not emulate AWS's managed scaling.

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
Core tests cover validation, capacity and cancellation/deadline join barriers.
See [SDK verification](../tests/sdk/README.md) for the lane command and dependencies.

Contracts follow AWS's [CreateEventSourceMapping API](https://docs.aws.amazon.com/lambda/latest/api/API_CreateEventSourceMapping.html),
[SQS mapping configuration](https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-configure.html)
and [DeleteEventSourceMapping API](https://docs.aws.amazon.com/lambda/latest/api/API_DeleteEventSourceMapping.html).
