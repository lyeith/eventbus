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

SES captures all nine sending operations and now supports six v1 configuration-set/destination operations with SNS event publishing. Remaining non-sending APIs are low priority; preserve AWS wire contracts and state transitions. The remaining operation families include:

- [SES v1 actions](https://docs.aws.amazon.com/ses/latest/APIReference/API_Operations.html): identities and verification, templates/rendering, remaining configuration-set operations/tracking, account/quota/statistics, receiving rules/filters/policies.
- [SES v2 actions](https://docs.aws.amazon.com/ses/latest/APIReference-V2/API_Operations.html): identities/certificates/policies, templates/rendering, configuration and account settings, contacts/lists/suppression, tags, dedicated IPs, deliverability/metrics/insights, import/export jobs, multi-region endpoints, tenants/resources/reputation.

For remaining APIs, `--ses-config` supplies sending prerequisites. Its configuration-set names seed the mutable native registry.
Fixture configuration is a development convenience, not an AWS operation.
Native duplicate `{{ses:openTracker}}` rejection remains a sending validation
gap; the local view renders a single pixel without claiming that validation.
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

## SQS mapping and SNS delivery coverage

Native SQS mappings support Create/Get/Delete and opt-in partial batch responses.
Remaining application-facing work, in dependency order:

1. Add paginated List and Update, including disabling, enabling, retargeting and
   clearing settings. Keep mapping identity and the bound queue across joined
   worker changes. Add mapping tags through Create and Lambda tag operations.
   [List](https://docs.aws.amazon.com/lambda/latest/api/API_ListEventSourceMappings.html),
   [Update](https://docs.aws.amazon.com/lambda/latest/api/API_UpdateEventSourceMapping.html),
   [Tags](https://docs.aws.amazon.com/lambda/latest/dg/tags-esm.html)
2. Add SQS event filters, including native deletion of unmatched records
   and JSON/plain-text body behavior.
   [AWS SQS filtering](https://docs.aws.amazon.com/lambda/latest/dg/with-sqs-filtering.html)
3. Support batching windows and larger batches within the existing 6 MiB
   event budget. Keep lease selection and FIFO group ownership in SQS and
   invocation admission in the mapping. Qualify provisioned poller settings,
   which affect FIFO limits and are exclusive with maximum concurrency.
   [AWS polling and batch behavior](https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html)

SNS uses subscriptions. Standard topics push individual events to Lambda. A
FIFO topic can reach Lambda through SQS and its mapping. Supported SNS API
operations do not imply all delivery behavior. Follow-ups are transient
SNS-to-SQS/Lambda admission retries with subscription DLQ routing, Lambda-owned
async invocation configuration and destinations, and optional local HTTP/S
delivery with confirmation, effective delivery-policy readback and retry behavior.
[AWS SNS retries](https://docs.aws.amazon.com/sns/latest/dg/sns-message-delivery-retries.html),
[AWS Lambda async configuration](https://docs.aws.amazon.com/lambda/latest/dg/invocation-async-configuring.html)

AWS-managed autoscaling, CloudWatch metrics, IAM/KMS enforcement and cross-account
behavior require their own coverage goals. Avoid accepting settings that the
emulator cannot honor.

## Separate gateway and Lambda work

Request gateway contracts are covered in GATEWAY.md. API Gateway management/deployment
APIs and TOKEN/Cognito/JWT authorizers are separate features. HTTP API REQUEST
authorizers, local header/query-SigV4 AWS_IAM identities, Lambda proxy 1.0/2.0 and Lambda
Event invocation are implemented in the bounded request/runtime scope.
Effective Invoke policies, credential expiry and atomic local rotation are
implemented. IAM/STS issuance and SCP/role-trust/boundary resolution remain
separate service scope. REST SigV4a is an expansion (AWS HTTP APIs exclude it).
Unsigned/AWS-chunked signing, long execute-api presign lifetimes and policy
dialect bounds remain qualified in the gateway guide. Exact encoded-separator routing
and noncanonical direct-URL signing forms still lack live AWS comparison.
Warm Python/Node reuse and read-only registered GetFunction metadata are implemented; other Lambda management/provisioning APIs remain separate work.
