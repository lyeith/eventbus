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

One request produces one compact JSON object followed by a newline. File writes
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

## Fixtures and limits

Pass `--ses-config <path>` using [ses.yaml](../examples/ses.yaml).
It supplies stored/custom verification templates, configuration sets, verified
identities and original received messages for bounce requests. These immutable
fixtures replace sending prerequisites while [management APIs remain backlogged](BACKLOG.md).

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
