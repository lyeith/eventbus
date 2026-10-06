# Backlog

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

Direct SQS SendMessage is separate from the Cognito work in
[issue #1](https://github.com/lyeith/eventbus/issues/1). Add it for a concrete
consumer with real SDK receive/delete/visibility/retry/DLQ checks; batch/FIFO and
Node queue consumers need their own demonstrated requirements.

## Separate gateway and Lambda work

Request gateway contracts are covered in GATEWAY.md. API Gateway management/deployment
APIs, other authorizer types and HTTP API v2 are separate features. Lambda
asynchronous invocation, warm runtime reuse and provisioning APIs are also separate.
