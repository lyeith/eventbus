# SES sending and capture

SES uses normal AWS SDK requests at the EventBus endpoint. All HTTP sending
operations are implemented; email is never delivered.

| SES v1: Query/XML | SES v2: REST/JSON |
| --- | --- |
| SendEmail | SendEmail: Simple, Raw, Template |
| SendRawEmail | SendBulkEmail |
| SendTemplatedEmail | SendCustomVerificationEmail |
| SendBulkTemplatedEmail | |
| SendCustomVerificationEmail | |
| SendBounce | |

## Capture

Add `--ses-log .local/ses.jsonl` to the [startup command](../README.md#start).
The default `--ses-log -` writes JSONL to stdout; operational logs use stderr.

```sh
tail -f .local/ses.jsonl | jq --unbuffered -c '{operation, request_id, emails, outcome}'
```

Each sending request produces one compact JSON object followed by a newline. File writes
append and sync before the API returns success; reopening preserves earlier
records. Capture failure returns an API server error and prevents further
appends. Startup refuses an incomplete final record without truncating it.

## Record contract for agents

| Field | Meaning |
| --- | --- |
| `schema_version` | Integer, currently `1`. Check before interpreting the record. |
| `timestamp` | UTC RFC3339 timestamp, with fractional seconds when available. |
| `request_id` | Correlation ID shared with the API response. |
| `api`, `api_version` | `ses` / `2010-12-01`, or `sesv2` / `2019-09-27`. |
| `operation` | AWS operation name, such as `SendEmail`. |
| `request` | Complete parsed AWS request; unavailable when decoding fails. Transport credentials/signing fields are excluded. |
| `emails` | Derived email views; may be null on failure. Includes message IDs, recipients, content and optional rendering details. |
| `outcome` | `http_status`, `response` (AWS result or null), and `error` (`code`, `message`) on request failure. |

Parse one JSON object per line and tolerate additional fields. Use `request`
for exact inputs and `outcome` for acceptance. Raw MIME and attachment bytes
remain base64 in the original request. `request_content_path` and attachment
`raw_content_path` are paths relative to `request`; derived views avoid
duplicating binary content.

A bulk HTTP 200 can contain failed entries. Inspect the ordered result array:
v1 `outcome.response.Status` uses `Success`; v2
`outcome.response.BulkEmailEntryResults` uses `SUCCESS`. Other statuses are
failures without message IDs. Match results to request entries by array index.
V1 email views use `index`; v2 successful views use `entry_index`.
A capture shows API acceptance, not delivery.

## Raw configuration-set selection

For v1 `SendRawEmail`, the MIME `X-SES-CONFIGURATION-SET` header can select a
seeded configuration set; header names are case-insensitive and ordinary MIME
folding is parsed. See [AWS's SES-specific headers](https://docs.aws.amazon.com/ses/latest/dg/event-publishing-send-email.html#event-publishing-send-email-headers).
An explicit API `ConfigurationSetName` takes precedence when both selectors are
present, following [AWS's documented precedence](https://aws.amazon.com/blogs/messaging-and-targeting/introducing-sending-metrics/).
Existing API-selected validation is preserved. A nonexistent header-selected set
returns [`ConfigurationSetDoesNotExist`, HTTP 400](https://docs.aws.amazon.com/ses/latest/APIReference/API_SendRawEmail.html#API_SendRawEmail_Errors).
Without either selector, no configuration set is selected.

Inspect `emails[].configuration_set` for the effective selection. The original
request remains unchanged: `request.RawMessage.Data` retains the submitted MIME
bytes as base64, including its headers. This is original submission evidence,
without a post-send representation or actual delivery.

## Configuration-set SNS events

The SES v1 Query API implements `CreateConfigurationSet`, `DescribeConfigurationSet`,
`DeleteConfigurationSet`, and create/update/delete configuration-set event destinations.
Use an unchanged SES SDK to provision an SNS destination:

```python
ses.create_configuration_set(ConfigurationSet={"Name": "tracking"})
ses.create_configuration_set_event_destination(
    ConfigurationSetName="tracking",
    EventDestination={
        "Name": "events",
        "Enabled": True,
        "MatchingEventTypes": ["send", "open", "bounce"],
        "SNSDestination": {"TopicARN": topic_arn},
    },
)
ses.describe_configuration_set(
    ConfigurationSetName="tracking",
    ConfigurationSetAttributeNames=["eventDestinations"],
)
```

Create the standard SNS topic first, in the emulator's account and region. SNS
subscriptions use the ordinary [messaging delivery paths](MESSAGING.md), including
native Lambda admission, failure/retry policy and joined shutdown. SES owns native
configuration and the event JSON; SNS owns envelopes, subscriptions and delivery.
`Enabled` defaults to `false`. Matching types use AWS's case-sensitive v1 values:
`send`, `reject`, `bounce`, `complaint`, `delivery`, `open`, `click`,
`renderingFailure`. This increment emits `Send`, explicit local `Open` and explicit
local `Bounce`; accepting other destination enums does not synthesize those events.
CloudWatch/Firehose event destinations and remaining management APIs are backlogged.
[Native destination contract](https://docs.aws.amazon.com/ses/latest/APIReference/API_EventDestination.html).

A successfully captured, configuration-selected send emits `Send` to each enabled,
matching destination. The event uses AWS's `eventType` spelling, `mail.messageId`
from the send response, original recipients, and `mail.tags["ses:configuration-set"]`.
V1 raw sends preserve the MIME/API selection precedence described above; all sending
operations use their accepted per-message results, including only successful bulk
entries. No selector means no configured event. Disabled, nonmatching and unrelated
sets publish nothing. Configuration-set CRUD is resource management and does not
append email capture records or depend on sending being enabled.

The original schema-version-1 send capture remains unchanged. It must succeed before
any event is admitted. Once captured, caller cancellation cannot remove the accepted
send's event. Downstream SNS admission failure leaves the accepted send intact and
logs a private failure with the SES MessageId, event type, topic and request ID.
That failure is notification evidence, not a claim about SMTP delivery.
[Native event payloads](https://docs.aws.amazon.com/ses/latest/dg/event-publishing-retrieving-sns-contents.html).

## Explicit local Open/Bounce outcomes

`POST /__eventbus/dev/ses/outcomes` is a development control, outside SES's AWS APIs.
It requires an actual accepted MessageId and routes the native event JSON through
that message's current enabled/matching SNS destinations:

```sh
curl -fsS "$EVENTBUS_ENDPOINT/__eventbus/dev/ses/outcomes" \
  -H 'Content-Type: application/json' \
  -d '{"message_id":"<SES MessageId>","event_type":"Open","ip_address":"192.0.2.12","user_agent":"local scenario"}'

curl -fsS "$EVENTBUS_ENDPOINT/__eventbus/dev/ses/outcomes" \
  -H 'Content-Type: application/json' \
  -d '{"message_id":"<SES MessageId>","event_type":"Bounce","recipients":["recipient@example.test"],"bounce_type":"Permanent","bounce_sub_type":"NoEmail"}'
```

`event_type` is exactly `Open` or `Bounce`. Open defaults to loopback IP and an
explicit local user agent. Bounce defaults to `Permanent` / `General` and all
original recipients; an explicit subset must contain original recipient addresses.
No outcome is inferred from capture or handler success. `SendBounce` remains the
separate native API for bouncing a seeded received message.

A successful response has `local: true`, `message_id`, `event_type`, `request_id`
and `destinations` with admitted topic ARNs. An empty destination list means the
current configuration filtered the outcome. Failed SNS admission returns HTTP 502
with per-topic errors; partial admission is reported and is not rolled back.
Unknown, expired or evicted MessageIds return HTTP 404; invalid fields or foreign
recipients return HTTP 400. Deleting a destination affects later outcomes. Deleting
and recreating a same-name configuration set cannot route an old message.

Correlation retains event metadata for 24 hours after durable acceptance, bounded
by the newest 10,000 configuration-selected messages and 64 MiB of serialized metadata. Expiry
is checked on the next send/outcome; idle expired metadata remains bounded until
then or shutdown. Bodies/attachments are not retained there, and eviction never
removes durable SES capture. Event header views follow AWS's 10 KiB truncation rule.
Application fixtures and assertions on application records belong in the consuming
repository; this endpoint only supplies a correlated local outcome.

## Fixtures and limits

Pass `--ses-config <path>` using [ses.yaml](../examples/ses.yaml).
It supplies stored/custom verification templates, configuration sets, verified
identities and original received messages for bounce requests. Fixture configuration-set names
seed the mutable native configuration-set registry. The other immutable fixtures
replace sending prerequisites while [remaining management APIs stay backlogged](BACKLOG.md).

Default sender checks validate address syntax. Set
`require_verified_identities: true` to require a seeded email/domain.
`SendBounce` requires a seeded original message less than 24 hours old.
Custom verification captures the request without verifying the recipient.

Malformed requests, invalid recipients and missing resources use SES errors.
Message quotas are 10 MiB for v1 and 40 MiB for v2. Raw quotas count decoded MIME;
structured quotas count deterministic MIME with transfer encoding and attachment
overhead, which can differ from AWS's private assembler. Wire limits are
64 MiB, or 256 MiB for v2 bulk.

The optional template capture view supports simple substitutions. Rich
Handlebars and unavailable variables produce `capture_rendering_error` while
preserving API acceptance and original input. SMTP capture is separate.

[SDK tests](../tests/README.md) verify all nine operations against
[pinned official AWS models](../tests/sdk/aws_models/README.md), including current
optional fields, binary round trips, partial bulk results and error envelopes.
