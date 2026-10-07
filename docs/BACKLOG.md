# Backlog

## Completed ticket scope

Tickets #2–#16 and recorded ownership-review findings have
accepted implementations with race, SDK and native delivery evidence.
[Issue triage](ISSUE-TRIAGE.md) records bounded scope, owners and dependencies.
The service guides state supported contracts and remaining capability limits.

Wire protocols/request IDs now have a shared owner with service-selected media
versions/namespaces/budgets. SSM snapshots/hierarchies/versioning and Firehose
batch validation/delivery ownership are corrected. Consumer execution policy is
consolidated; Cognito no longer exports its raw SQL connection in production.
These findings are implementation work, not deferred dev conveniences.

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
