# SQS and SNS contracts

EventBus implements the complete operation inventories: [23 SQS actions](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_Operations.html)
and [42 SNS actions](https://docs.aws.amazon.com/sns/latest/api/API_Operations.html).
Use normal AWS SDK clients with the EventBus endpoint, explicit local credentials
and the configured region/account. SQS supports AWS JSON and legacy Query/XML;
SNS supports Query/XML. Unknown operations and invalid inputs return AWS errors.

## Operation coverage

| Service | Operations |
| --- | --- |
| SQS queues | CreateQueue, DeleteQueue, GetQueueUrl, GetQueueAttributes, SetQueueAttributes, ListQueues, PurgeQueue |
| SQS messages | SendMessage, SendMessageBatch, ReceiveMessage, DeleteMessage, DeleteMessageBatch, ChangeMessageVisibility, ChangeMessageVisibilityBatch |
| SQS administration | AddPermission, RemovePermission, TagQueue, UntagQueue, ListQueueTags, ListDeadLetterSourceQueues, StartMessageMoveTask, CancelMessageMoveTask, ListMessageMoveTasks |
| SNS topics | CreateTopic, DeleteTopic, GetTopicAttributes, SetTopicAttributes, ListTopics, AddPermission, RemovePermission, GetDataProtectionPolicy, PutDataProtectionPolicy, TagResource, UntagResource, ListTagsForResource |
| SNS subscriptions | Subscribe, ConfirmSubscription, Unsubscribe, GetSubscriptionAttributes, SetSubscriptionAttributes, ListSubscriptions, ListSubscriptionsByTopic |
| SNS publishing | Publish, PublishBatch |
| SNS SMS | CheckIfPhoneNumberIsOptedOut, OptInPhoneNumber, GetSMSAttributes, SetSMSAttributes, GetSMSSandboxAccountStatus, CreateSMSSandboxPhoneNumber, DeleteSMSSandboxPhoneNumber, ListSMSSandboxPhoneNumbers, VerifySMSSandboxPhoneNumber, ListPhoneNumbersOptedOut, ListOriginationNumbers |
| SNS mobile push | CreatePlatformApplication, DeletePlatformApplication, GetPlatformApplicationAttributes, SetPlatformApplicationAttributes, ListPlatformApplications, CreatePlatformEndpoint, DeleteEndpoint, GetEndpointAttributes, SetEndpointAttributes, ListEndpointsByPlatformApplication |

Direct SQS sends and SNS fanout use the same queue engine. Messages preserve
custom String/Number/Binary attributes, body/attribute MD5 checksums, system
metadata, delays and retention. Receives renew receipts and track receive counts;
long polls observe request cancellation. Batch operations validate the batch and
return ordered per-entry successes/failures. Queue URLs must identify a local
queue in the configured account; changing the hostname for container addressing
is supported.

Create FIFO queues/topics through their AWS API with `FifoQueue=true` or
`FifoTopic=true` and a `.fifo` name. FIFO supports groups, deduplication,
sequence numbers and receive-attempt retries. Set `RedrivePolicy` and
`RedriveAllowPolicy` for automatic DLQ behavior; message-move tasks perform local
rate-controlled redrive and expose progress/cancellation. All messaging state is
in memory and must be reprovisioned after restart.

FIFO SNS archives retain publications for the configured 1..365 days. Replays
process the currently retained matching timestamp range synchronously and apply
the subscription filter. An explicit EndingPoint pauses subsequent live delivery;
a replay without EndingPoint resumes it. The local archive caps serialized request
bytes plus metadata at 64 MiB per topic and rejects capacity overflow before
acceptance. Disable ArchivePolicy with `{}` to clear it. Archives are not durable
and do not reproduce AWS asynchronous replay scheduling.

SNS supports attribute/body filters, raw SQS delivery, protocol-specific JSON
messages, confirmation tokens, tags, policies, SMS sandbox verification and
mobile application/endpoint lifecycle. SNS DLQs must match the topic account,
region and FIFO type. SQS destinations receive locally. Firehose subscriptions on standard topics
deliver through the local Firehose port to the configured S3/RustFS endpoint,
after filtering and raw/envelope selection; admission failures use the existing
DeliveryFailure/DLQ policy, and accepted-record S3 retries belong to Firehose.
See [Firehose configuration and SDK example](FIREHOSE.md) for processing,
buffering, limits and shutdown behavior. Lambda subscriptions use the registered
local asynchronous runtime described below. HTTP/S, email, SMS and mobile-push
delivery requests are captured without contacting their endpoints.
The SMS sandbox starts enabled;
read the captured OTP and call VerifySMSSandboxPhoneNumber before sending.
Origination numbers and opted-out numbers start empty; there is no provider
inventory or simulated carrier opt-out input.

IAM policies, KMS keys, tracing, feedback roles and data-protection policy APIs
retain local metadata; they do not enforce AWS IAM, encrypt messages, emit cloud
metrics or run provider data inspection. External retry/acknowledgement systems
and AWS production timing/throughput quotas are outside the local harness.
SNS notification envelopes are unsigned local fixtures; they omit AWS signature
and signing-certificate fields. Operation coverage does not claim every
production AWS behavior is reproduced.

## SNS to registered Lambda

Register application functions with `--lambda-functions functions.yaml` and
`--work-dir /path/to/application`, then use the normal SNS SDK `Subscribe` call with
`Protocol="lambda"` and a full ARN such as
`arn:aws:lambda:us-east-1:000000000000:function:notifications:live`.
The ARN must use EventBus's configured region/account and `arn:aws` partition.
Aliases and numeric versions must be explicitly registered as `name:qualifier`;
an unregistered alias never falls back to the base handler. Each EventBus owner
uses its own registry and bounded queue; it never calls AWS Lambda or a remote
fallback. See [Lambda registration and execution evidence](LAMBDA.md).

Standard topics deliver one native `Records` entry per matching subscription,
with `EventSource="aws:sns"`, `EventVersion="1.0"`, `EventSubscriptionArn` and
`Sns` notification metadata. Message ID, topic, subject, original publish timestamp,
body and attributes survive admission/retry. Number and String.Array attributes
become `Type="String"` for Lambda; Binary values use base64. Attribute/body filters
run before registry resolution, so a filtered unknown target causes no invocation
or delivery failure. Protocol-specific JSON messages select `lambda` or `default`
and omit message attributes, following SNS behavior. Notifications are unsigned;
the unsubscribe URL points to the local EventBus listener.

Lambda subscriptions reject raw delivery, HTTP delivery policies, Firehose roles,
FIFO topics and replay options. Subscribe validates resource identity, without
requiring an already registered function. An eligible publish to an unknown
function/alias or an owner without a configured runtime records `DeliveryFailure`.
Rejected bounded admission does the same. The existing SNS `RedrivePolicy` can
place the notification envelope in a local SQS DLQ. Native Publish success means
publication acceptance, including when an individual subscription fails delivery.

`Publish` capture records a `scheduled` intent. Successful runtime admission adds
`operation="DeliveryAdmission"`, `status="admitted"`, `subscription_arn` and
`invocation_request_id`. Join that last field to `request_id` in
`eventbus.lambda.async.v1` evidence and check its terminal state plus the handler's
business side effect. Admission and capture do not claim completed execution.

SNS makes one bounded local admission attempt; it does not reproduce production
SNS's multi-hour admission retries. Once admitted, Lambda owns execution,
deadlines and the two retries for function/runtime failure. Retries retain the
same SNS message ID and invocation request ID and can duplicate application
side effects. Eventual handler failure/timeout appears in Lambda execution
evidence, rather than SNS's admission DLQ. Handlers must be idempotent.

During shutdown, Lambda stops asynchronous admission and joins admitted work
while the AWS listener remains usable by accepted handlers. A shutdown deadline
cancels retries, joins child processes and reports cancellation. SNS introduces
no extra worker or child owner. Messaging resources and admitted event payloads
are in memory: restart requires reprovisioning, loses pending events and does
not replay append-only evidence files. There is no durable or exactly-once
execution guarantee. The contract follows the
[AWS SNS Lambda event shape](https://docs.aws.amazon.com/lambda/latest/dg/with-sns.html)
and [Lambda attribute conversion](https://docs.aws.amazon.com/sns/latest/dg/sns-message-attributes.html).

## Live notification capture

Pass `--sns-log /path/owned/by/app/sns.jsonl`. The default `-` writes to stdout;
operational logs use stderr. Use separate SNS/SES files when evaluating both.
Files append across restarts, one JSON object per line. Writes are serialized
and synced before acceptance; an initial capture failure refuses the send.
A partial write is terminal and requires repairing the file or selecting a fresh
path. Shutdown drains HTTP and workers before closing capture.

Every record uses `schema_version: "eventbus.sns.capture.v1"`, an RFC3339 UTC
`captured_at` and `operation`. Publishing records include the request/message IDs,
original message, target/phone number, subject/structure, custom attributes,
FIFO IDs/sequence and per-delivery intents. BinaryValue is base64 in JSON;
message attribute fields preserve AWS-style DataType/StringValue/BinaryValue.
Match assertions by message ID, not file position or time.

```json
{"schema_version":"eventbus.sns.capture.v1","captured_at":"2026-10-07T00:00:00Z","operation":"Publish","message_id":"example-id","phone_number":"+15555550123","message":"local test","deliveries":[{"protocol":"sms","endpoint":"+15555550123","status":"captured"}]}
```

| Delivery status | Meaning |
| --- | --- |
| captured | External delivery intent recorded locally |
| scheduled | SQS enqueue, Firehose record or Lambda admission scheduled locally |
| admitted | Local Lambda accepted the event; completion belongs to Lambda evidence |
| filtered | Subscription filter did not match |
| pending_confirmation | Subscription is awaiting confirmation |
| paused | Explicit replay end has paused live subscription delivery |
| dropped | Destination or payload failed local delivery |
| dead_lettered | Failed local delivery moved to its configured DLQ |

A successful Publish accepts the notification; it does not guarantee every
subscription delivered it. Later local failures use `operation: "DeliveryFailure"`
with the same message ID. If this later capture fails, the accepted Publish
remains successful, stderr records the error, and subsequent sends fail at their
initial capture. Assert SQS outcomes by receiving the queue message, and Firehose outcomes by
reading/decompressing the resulting S3 object and inspecting retained delivery errors.
Assert Lambda outcomes through the correlated terminal execution record and
application side effect, rather than its scheduled/admitted intent.
Replay records place original per-publication evidence in `details.publications`
inside the top-level record, rather than appending those entries separately.
SNS duplicates also produce capture evidence with `details.deduplicated=true`.
Subscription confirmation records expose `details.token`; SMS sandbox creation
records expose `details.otp`. These are deliberate local test fixtures.

Agents can stream with `tail -f`, parse each line as JSON and dispatch by schema
and operation. For example:

```sh
jq -c 'select(.schema_version == "eventbus.sns.capture.v1" and .operation == "Publish") | {message_id, message, deliveries}' .local/sns.jsonl
```

## Native mapping receipt settlement

Each receive issues a new receipt. `DeleteMessage` can accept an older issued
receipt as a successful no-op without removing a later delivery, consistent with
[AWS DeleteMessage](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_DeleteMessage.html).
Unknown handles return `ReceiptHandleIsInvalid`. `DeleteMessageBatch` applies the
same settlement rule per entry. HTTP success alone is not mapping ACK proof.

After successful whole-batch Lambda execution and its confirmed completion boundary, mappings use
the broker's [`AcknowledgeSQSLambdaReceiptContext`](../internal/messaging/sqs_receipts.go)
on the original queue instance. Fresh execution joins its child process. Opt-in warm execution joins its managed response/log boundary; a separate worker lease remains until retirement. Private delivery evidence exposes `completion_scope`, and retained quiescence joins warm workers before cleanup assertions.
It accepts either deletion of the current, unexpired receipt or retained proof
that this exact original receipt was already deleted while current and unexpired.
Both native `DeleteMessage` and `DeleteMessageBatch` record that proof. Thus one
batch can mix handler-deleted records with records settled by the mapping.
The queue-owned receipt history keeps its existing expiration.

Receipt issuance, absence, expiry/redelivery, redrive or purge alone never prove
settlement. Unknown/expired history and deleted/recreated queues cannot ACK a
later lease or another queue's work. App and SDK adapters delegate this native
queue-owned operation; it requires no development exception or SDK rewrite.

Failed/timed-out handlers skip mapping ACK. Handler-issued native deletes remain
effective; still-unsettled records retain normal visibility, FIFO and redrive
behavior. See [SQS Lambda mappings](EVENT-SOURCES.md) for execution and join rules.

## Consumer events and verification

Go/Python consumers receive the AWS Lambda SQS record shape, including queue ARN,
region, checksums, system/custom attributes and base64 binary attributes.
Direct sends and raw SNS delivery place the application payload in `body`;
non-raw SNS delivery places the notification envelope there. See
[Agent workflow](AGENT-HARNESS.md#run-event-consumers) for batch settlement.

Service tests cover all operation families, validation, state transitions,
filters, redrive, FIFO and capture failure. Server tests send real Go AWS SDK
requests through the complete dispatcher and also exercise legacy SQS Query.
The frozen Python SDK lane verifies direct send/receive/delete, binary checksums,
SNS raw fanout, push capture and typed errors. A separate frozen boto3 SNS/Lambda
proof executes real Python handlers for aliases, filtering, ownership, admission
pressure/DLQ and function-failure/timeout retries. See [SDK verification](../tests/sdk/README.md).
