# Backlog

## Completed ticket scope

Tickets #2–#23 and recorded ownership-review findings have
accepted implementations with race, SDK and native delivery evidence.
[Issue triage](ISSUE-TRIAGE.md) records bounded scope, owners and dependencies.
The service guides state supported contracts and remaining capability limits.

Wire protocols/request IDs now have a shared owner with service-selected media
versions/namespaces/budgets. SSM snapshots/hierarchies/versioning and Firehose
batch validation/delivery ownership are corrected. Consumer execution policy is
consolidated; Cognito no longer exports its raw SQL connection in production.
These findings are implementation work, not deferred dev conveniences.

## Completed: retained-suite ownership recovery

[#17](https://github.com/lyeith/eventbus/issues/17) adds the opt-in development
[retained-owner contract](RETAINED-OWNER.md): fence suite sources, join accepted
native callback/retry work, permit exact fixture cleanup while held and resume.
The process must have one exclusive operator; sentinel fixtures may coexist,
but unrelated active callback callers may not. Timeout/incomplete evidence
retains dirty state. No automatic reset or application cleanup is included.
[HANDOFF](../HANDOFF.md) records the combined race and real SDK acceptance.
The completed native #2–#16 scope remains separate.

## Released in v0.8.0: full retained application owners

[#18](https://github.com/lyeith/eventbus/issues/18) extends the [retained profile](RETAINED-OWNER.md)
to native mapping custody, Scheduler, Cognito runners, Firehose and shared gateway
root leases. Declared RequestResponse cleanup keeps sources fenced and requires
another explicit join before fixture assertions/resume. Native resources are
paused/reused, not recreated; auth settings and payloads stay unchanged.
Applications own cleanup effects.
Implemented in commit `973e5f1`, with app and native SDK/RustFS race acceptance.
Released in [v0.8.0](https://github.com/lyeith/eventbus/releases/tag/v0.8.0); [HANDOFF](../HANDOFF.md) records verification.
Actual consuming-application cleanup acceptance belongs in that application's tests.

## Released in v0.8.0: handler-issued SQS deletion and mapping ACK

[#20](https://github.com/lyeith/eventbus/issues/20) corrects mapping completion after
native handler deletion, using queue-owned proof of actual original-receipt
settlement. Whole-batch execution/join and stale-lease safety remain unchanged;
see [Messaging](MESSAGING.md#native-mapping-receipt-settlement). Native SDK acceptance
passed, including mixed batch-five settlement, FIFO, durable SQLite effects and
real queue recreation. Released in [v0.8.0](https://github.com/lyeith/eventbus/releases/tag/v0.8.0); [HANDOFF](../HANDOFF.md) records
verification.

## Released in v0.8.0: native delivery evidence and diagnostics

[#21](https://github.com/lyeith/eventbus/issues/21) adds [correlated SQS delivery](EVENT-SOURCES.md#correlated-delivery-evidence);
[#22](https://github.com/lyeith/eventbus/issues/22) retains [private Lambda diagnostics](LAMBDA.md#private-invocation-diagnostics).
The actual SDK normal/race and tagged vet lane passed: HTTP producer/native
consumer lineage, manual deletion followed by blocked child work, same-mapping
second suite, retries/timeouts, sentinels and caught business failures with
successful runtime terminals. Release v0.8.0 and final verification
are recorded in [HANDOFF](../HANDOFF.md).

## Released in v0.8.0: raw SES configuration-set header

[#23](https://github.com/lyeith/eventbus/issues/23) fixes native header-only
`SendRawEmail` selection/validation, API precedence and truthful effective
capture while preserving original MIME submission. See [SES](SES.md#raw-configuration-set-selection).
Focused protocol, full SES race/vet and unchanged SDK proofs passed; released in [v0.8.0](https://github.com/lyeith/eventbus/releases/tag/v0.8.0); [HANDOFF](../HANDOFF.md)
records acceptance.

## Released: retained gateway HTTP continuations

[#24](https://github.com/lyeith/eventbus/issues/24) adds optional exclusively trusted
private gateway ingress for registered handlers and declared cleanup. Public
roots stay fenced; native routing/auth and real JWT checks remain intact.
Generation-bound leases count work through handler return and shutdown joins.
See [Gateway](GATEWAY.md#trusted-http-continuations); [HANDOFF](../HANDOFF.md)
records final acceptance and cleanup. Released in [v0.9.0](https://github.com/lyeith/eventbus/releases/tag/v0.9.0).

## Low priority: remaining SES APIs

SES sending is capture-only. Implement the non-sending APIs after the nine
sending operations, preserving the AWS wire contracts and state transitions.
The current official inventories contain 65 remaining SES v1 and 113 remaining
SES v2 operations:

- [SES v1 actions](https://docs.aws.amazon.com/ses/latest/APIReference/API_Operations.html): identities and verification, templates/rendering, configuration sets/events/tracking, account/quota/statistics, receiving rules/filters/policies.
- [SES v2 actions](https://docs.aws.amazon.com/ses/latest/APIReference-V2/API_Operations.html): identities/certificates/policies, templates/rendering, configuration and account settings, contacts/lists/suppression, tags, dedicated IPs, deliverability/metrics/insights, import/export jobs, multi-region endpoints, tenants/resources/reputation.

Until those APIs exist, `--ses-config` supplies immutable sending prerequisites.
Fixture configuration is a development convenience, not an AWS operation.
Advanced stored-template Handlebars rendering is also low priority; captures
retain the source request and template data, with an optional simple rendering
view. SMTP and SES Mail Manager are separate interfaces and are not in the
current HTTP sending scope.

## Messaging follow-up

All SQS/SNS operations are implemented for the local harness; see
[Messaging](MESSAGING.md) for supported state and external capture boundaries.
Production IAM enforcement, KMS encryption, CloudWatch metrics, provider delivery
and production delivery retry infrastructure are outside this scope. Native
SQS mappings execute registered Go/Python/Node functions with batches of 1–10
records and a configured concurrency ceiling. Nonzero batching windows/larger
batches, Update/List, filters, partial responses and AWS managed scaling remain
separate work. The Go/Python dev recipe consumer retains harness policy.

## Separate gateway and Lambda work

Request gateway contracts are covered in GATEWAY.md. API Gateway management/deployment
APIs, other authorizer types are separate features. HTTP API authorizers/Lambda proxy 1.0/2.0 and Lambda
Event invocation are now implemented in the bounded request/runtime scope.
Warm runtime reuse and management/provisioning APIs remain separate work.
