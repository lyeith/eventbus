# EventBridge Scheduler

The AWS listener supports `CreateSchedule`, `GetSchedule` and `DeleteSchedule`
through the normal Scheduler REST JSON SDK. This is a bounded one-time Lambda
path, not the complete Scheduler management API.

Register the target with [Lambda fixtures](LAMBDA.md), then use the same local
endpoint/region/account in the Scheduler SDK. The `default` group always exists;
`--scheduler-groups reports,audit` provisions additional local fixture groups.
Group management APIs are unsupported. `Target.RoleArn` is validated local
metadata; EventBus does not evaluate IAM or assume a role.

```js
import { SchedulerClient, CreateScheduleCommand } from '@aws-sdk/client-scheduler';
const client = new SchedulerClient({
  endpoint: 'http://localhost:14100', region: 'us-east-1',
  credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
});
await client.send(new CreateScheduleCommand({
  Name: 'audit-once', ClientToken: 'audit-once-1',
  ScheduleExpression: 'at(2026-10-08T12:00:00)',
  ScheduleExpressionTimezone: 'UTC',
  FlexibleTimeWindow: { Mode: 'OFF' }, ActionAfterCompletion: 'DELETE',
  Target: {
    Arn: 'arn:aws:lambda:us-east-1:000000000000:function:audit:local',
    RoleArn: 'arn:aws:iam::000000000000:role/local-scheduler',
    Input: JSON.stringify({ job: 'audit', organisation: 'example' }),
  },
}));
```

Choose a future time. IANA timezones are supported with the embedded timezone
rules; nonexistent local times are rejected. Native execution uses minute
precision. `--scheduler-exact-seconds` selects exact seconds for development
proofs. It is a local timing override, not a request field.

`Target.Input` must be explicit valid JSON up to 256 KiB. Plain input reaches
Lambda byte-for-byte. The four native context placeholders—schedule ARN,
scheduled time, execution ID and attempt number—expand on each target attempt.
Default notification input is explicitly unsupported rather than fabricated.

Create tokens are idempotent even after completion/deletion; conflicting reuse
fails. Duplicate names fail. Get returns independent snapshots. Delete cancels
and joins a pending callback, including token retries. State `DISABLED` retains
metadata without a callback. `ActionAfterCompletion=DELETE` removes a schedule
after its final target API attempt; accepted Lambda handler work remains owned
by Lambda and may still be running.

Scheduler retries target **admission**, including local queue pressure, not
handler business failures. Native retry fields allow 0–185 retries and an event
age of 60–86,400 seconds (defaults 185 and 86,400). Local backoff starts at one
second and caps at five minutes. Lambda separately owns execution retries.
Redacted completion logs contain schedule ARN, attempts, status and error code.

Schedules and idempotency state are memory-only. The local default capacity is
10,000 schedules and 20,000 combined create/delete tokens; exhaustion fails
explicitly. Teardown cancels future schedules and joins target callbacks before
Lambda admission drains. Deadline aborts remain errors on later Close calls.
There is no restart recovery or exactly-once guarantee across ambiguous retries.

Only `at(...)`, `FlexibleTimeWindow.Mode=OFF`, and local registered Lambda ARNs
are admitted. Cron/rate, flexible windows, omitted input, KMS, DLQ, other target
parameters, update/list/tag/group-management APIs are rejected. StartDate/EndDate
are retained metadata for one-time requests; they do not change the at() time.

Verification covers core timing/timezones, idempotency, snapshots, retry age,
cancellation and callback joins, plus an unchanged pinned JavaScript Scheduler
SDK calling a real Node Lambda alias through two owned dispatcher instances.

AWS: [CreateSchedule](https://docs.aws.amazon.com/scheduler/latest/APIReference/API_CreateSchedule.html),
[schedule types](https://docs.aws.amazon.com/scheduler/latest/UserGuide/schedule-types.html),
[context attributes](https://docs.aws.amazon.com/scheduler/latest/UserGuide/managing-schedule-context-attributes.html).
