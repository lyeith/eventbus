# Backlog

## Triaged AWS core work

[Issue triage](ISSUE-TRIAGE.md) records all ten open tickets, priorities,
dependencies, core owners and separate development harness adapters. Start with
Secrets Manager stage correctness (#3) and Cognito configuration/readback (#9),
then per-client validity (#10) and incremental settings enforcement (#11).
Lambda async (#7), HTTP API authorizers/proxy (#5/#6), Firehose delivery (#2),
rotation (#4) and Scheduler (#8) remain required core capabilities.

## Protocol follow-up from the ownership review

`awsprotocol` now owns target extraction and bounded body-reading mechanics.
SSM/Secrets/Firehose retain their existing local 1 MiB transport budgets explicitly;
these must be reviewed against native operation limits (especially Firehose #2).
Their legacy JSON helpers still use 1.0, including the Firehose adapter marked 1.1.
Query fallback errors still use SNS namespace/Sender, and request-ID placement varies.
Service adapters own selecting native protocol versions, namespaces, error classes
and quotas; shared wire helpers should accept those choices. Verify the matrix with
actual SDK/error/boundary requests before changing supported wire behavior.
This is separate core compatibility work, not a development feature.

## Ownership follow-ups

A second independent review found no blockers in the cleanup and two pre-existing
seams to improve separately:

- `consumer` owns consolidating Go/Python timeout, output/logging, cleanup and batch
  result handling behind one private helper; command construction stays separate.
- `cognito` owns retiring the production `DB()` accessor used by app/SDK tests.
  App liveness checks should use domain reads; SDK time/state fixtures need an
  owned seam, coordinated with #10's clock/expiration work.

Other source-confirmed AWS core work: SSM should return snapshots and select path
hierarchies; Firehose must validate malformed batch entries/report native ordered
failures and own its delivery client. The latter belongs with #2's delivery work.
These are core behavior, not harness conveniences. No new generic lifecycle,
store or execution framework is required.

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
and delivery retry infrastructure are outside this scope. Node queue consumers
are a separate feature from the existing Go/Python consumer runner.

## Separate gateway and Lambda work

Request gateway contracts are covered in GATEWAY.md. API Gateway management/deployment
APIs, other authorizer types and HTTP API v2 are separate features. Lambda
asynchronous invocation is tracked in #7; HTTP API v2 contracts in #5/#6.
Warm runtime reuse and management/provisioning APIs remain separate work.
