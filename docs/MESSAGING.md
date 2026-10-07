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
buffering, limits and shutdown behavior. HTTP/S, email, SMS, Lambda and
mobile-push delivery requests are captured without contacting their endpoints.
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
| scheduled | SQS enqueue or Firehose record admission scheduled locally |
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
SNS raw fanout, push capture and typed errors. See [SDK verification](../tests/sdk/README.md).
