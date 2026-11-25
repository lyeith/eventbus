# Handoff

v0.22.0 is public and latest. Tag source 616f89a, CI 45553342666, all nine
public assets verified. Proof: /tmp/eventbus-sqs-partial-final-race.log and
/tmp/eventbus-v0.22.0-package.log. The latter uses the real packaged Linux
service, boto3 and a local Python Lambda with delivery capture. All outputs
and release worktree removed. Live AWS was not used.

The first local mapping coverage change on main adds paginated List,
Update and mapping tags with AWS SDK request and response shapes. Native
Update returns after the old workers have joined their accepted work and
original receipt processing. It preserves the bound queue and UUID, and
restarts only when enabled. Concurrent tag changes survive the join.
The release tag does not contain this follow-up.

Local verification: /tmp/eventbus-sqs-management-sdk.log (real boto3
pagination/filter, tag CRUD, enable/disable/reset, and full prior SQS batch
scenario); /tmp/eventbus-sqs-management-race.log (app, server, retained
admission); /tmp/eventbus-sqs-management-final-v2.log (final mapping race and
tagged vet, after updating stale unsupported-feature assertions).

Remaining application-facing gaps are in docs/BACKLOG.md. The SQS queue owns
leases, exact payload admission, FIFO and redrive. Eventsource owns filtering
policy and joined mapping generations. SNS owns admission retry and webhooks,
Lambda owns execution retry and destinations. No live or default stack was
changed. No dependencies added.
