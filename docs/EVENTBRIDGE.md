# EventBridge scheduled rules

EventBus implements a bounded native EventBridge Rules capability for the local
default event bus. Rules are separate from the one-time [Scheduler](SCHEDULER.md)
API. Applications own resource provisioning, Terraform extraction and cleanup.

Use an unchanged AWS Events SDK with its endpoint set to the EventBus listener.
Requests use AWS JSON 1.1 and `X-Amz-Target: AWSEvents.<Operation>`.

## Supported operations

- `PutRule`, `DescribeRule`, `ListRules`, `EnableRule`, `DisableRule`, `DeleteRule`.
- `PutTargets`, `ListTargetsByRule`, `RemoveTargets`.

Rules support `rate(value unit)` with positive integer values and native units:
`minute`, `minutes`, `hour`, `hours`, `day`, `days`. One uses the singular unit;
larger values use the plural. The minimum interval is one minute, with real
timers. `rate(15 minutes)` is valid; `rate(1 second)` is rejected.

The bus may be omitted, `default`, or the local default-bus ARN. Rule names and
target IDs follow AWS's character and length limits. Rules default to `ENABLED`.
`PutRule` replaces omitted metadata while retaining attached targets. Updating a
target by ID replaces its omitted input/retry options. Lists return detached
snapshots and scoped opaque pagination tokens. Tokens belong to this service
instance and cannot be reused across listings or after a restart.

Only registered Lambda functions in the local account and region are targets.
Each rule supports five targets; each put/remove request supports ten entries.
Targets support optional `RetryPolicy`: up to 185 retries and an event age of
60–86,400 seconds. Defaults are 185 retries and 24 hours. EventBridge retries
retryable Lambda API admission failures with backoff and jitter. Once Lambda
accepts the event, Lambda owns execution and its native asynchronous retries.

Remove attached targets before deleting a rule. Repeated deletion of an absent
rule succeeds. `Force` is accepted but has no effect on these application-owned
rules. Resource state is in memory; provision resources again after a restart.

## Scheduled payload and target inputs

At each firing EventBus creates the normal scheduled event: `version: "0"`, a
fresh `id`, `detail-type: "Scheduled Event"`, `source: "aws.events"`, account,
region, UTC `time`, the rule ARN in `resources`, and empty `detail`. All targets
for that occurrence see the same original identity/time. API admission retries
reuse that occurrence's transformed payload.

Targets select at most one input option:

- `Input`: constant valid JSON, passed unchanged.
- `InputPath`: the selected JSON value from the original event.
- `InputTransformer`: named paths plus an input template.

Transformer paths support root/dot notation, array indices and simple wildcards.
Dot keys may contain letters, digits, dashes, underscores and forward slashes.
`InputPath` also accepts quoted bracket keys. `InputPathsMap` requires dot notation
for object keys. Templates support JSON values, quoted strings and documented
multiline quoted strings. Input and templates are limited to 8,192 characters;
path maps support up to 100 variables and paths up to 256 characters.

Reserved variables are available without path-map entries:

| Variable | Value |
| --- | --- |
| `aws.events.rule-arn` | Current rule ARN |
| `aws.events.rule-name` | Current rule name |
| `aws.events.event.ingestion-time` | UTC ISO 8601 firing time |
| `aws.events.event` | Original event without `detail` |
| `aws.events.event.json` | Full original event |

The last two variables must be JSON object-field values. They cannot be embedded
in strings or used as template keys. Other variables are also forbidden as keys.
Paths always reference the original event, independent of other targets.

For an HTTP-shaped Lambda input, provision a transformer such as:

```json
{
  "InputPathsMap": {"event_id": "$.id", "event_time": "$.time"},
  "InputTemplate": "{\"httpMethod\":\"POST\",\"path\":\"/expiry/process\",\"eventId\":<event_id>,\"eventTime\":<event_time>,\"ruleArn\":<aws.events.rule-arn>}"
}
```

AWS does not escape extracted values inserted inside template strings. EventBus
preserves that behavior; use whole JSON-value insertion to retain valid JSON
encoding. Embedded objects/arrays inside strings lose their internal quotes.
Missing-path whole object fields and array items are omitted; inline placeholders
become empty text. Missing standalone values and `InputPath` yield `null`.
Array/root missing behavior and sorted object-wildcard ordering are local
choices; they have not been verified against live AWS.

## Boundaries and errors

Event patterns, cron/one-time expressions, other buses/accounts/regions, non-Lambda
targets, rule tags/IAM roles, target roles/dead-letter queues and target-specific
parameter objects are explicitly unsupported. JSONPath filters, recursive
descent, slices, escaped bracket keys and bare-text templates are rejected.
Unsupported operations return `UnknownOperationException`; malformed or
unsupported request shapes return native `ValidationException` envelopes.
Valid target batches report unsupported/unregistered targets and target-capacity
failures through `FailedEntryCount`/`FailedEntries`; valid peers still apply.

Disable, target removal and deletion fence later first admissions. A callback
already crossing admission finishes before the mutation returns. Previously
accepted occurrences retain their target/payload snapshots through retry; an
accepted Lambda invocation can finish after rule changes.

The retained-owner source fence parks dormant due work without changing native
rule state or canceling accepted work. Resume coalesces missed ticks rather than
creating a burst. Rate cadence is anchored at creation or `PutRule`; enabling
resumes the next cadence rather than restarting the interval.

Shutdown stops recurring intake and joins admission/retry callbacks before
Lambda drains accepted work. If the shutdown budget expires, EventBus cancels
unaccepted API retry waits, joins actual callbacks and retains ownership
uncertainty. Application cleanup remains the application's responsibility.

During declared retained cleanup, `DisableRule`, `RemoveTargets` and `DeleteRule`
may reduce intake through the existing cleanup envelope. Producing operations
and runtime tuning remain fenced. Re-quiesce after declared cleanup before
asserting fixture safety or resuming.

Development-only options live in `internal/events/dev_*.go`: source leases,
deterministic component clocks, capacity bounds and redacted outcomes. They do
not reinterpret AWS rate units or expose a separate harness delivery route.
Defaults bound this instance to 300 rules and 1,500 pending occurrences. At the
pending bound, due work parks without retaining an additional source lease;
completion, native rule mutation or shutdown wakes it. Capacity delays rather
than silently discards an occurrence, and missed ticks coalesce on delivery.

## Verification and sources

Focused service tests cover real contracts with a deterministic timer port;
the maintained Events SDK scenario observes genuine one-minute recurring Lambda
invocations and stops later admissions through disable/remove/delete. See
[SDK verification](../tests/sdk/README.md) for the recurring lane.

AWS contracts: [PutRule](https://docs.aws.amazon.com/eventbridge/latest/APIReference/API_PutRule.html),
[PutTargets](https://docs.aws.amazon.com/eventbridge/latest/APIReference/API_PutTargets.html),
[rate expressions](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-scheduled-rule-pattern.html),
[input transformation](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-transform-target-input.html),
[retry policy](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-rule-retry-policy.html).
