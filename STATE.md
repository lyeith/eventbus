# EventBus state

Canonical source: /home/spite/Projects/eventbus on SSD, main.
Public MIT repository: https://github.com/lyeith/eventbus.
Latest public release is v0.5.1; v0.6.0 is prepared for publication.
Plans pins and developer stacks remain untouched.

Tickets #13–#15 are implemented and verified on main:
- SNS Lambda subscriptions resolve registered local aliases, apply filters and
  admit native Records through the existing bounded async runtime. SNS evidence
  correlates admission identity; terminal execution belongs to Lambda evidence.
- Native SQS Create/Get/DeleteEventSourceMapping supports explicit BatchSize=1.
  eventsource owns mappings/pollers; SQS owns current leases/FIFO/redrive;
  Lambda owns completion and children. Failure/timeout never acknowledges.
- Queue handles bind the original owned instance. Deletion/recreation cannot
  consume a replacement queue. Delete/Close cancel and join pending work.
- One first-context Close owner publishes the shared terminal result after join.
- Explicit trailing-slash gateway routes retain original event paths. Full route
  matches precede greedy/default, preserving authorization boundaries.

Ownership: app adapts typed ports with shared redacted Execute/Admit mechanics;
server only dispatches. sqsevent owns wire types; messaging projects SQS receive
snapshots for both native mappings and dev recipe consumers. Dev recipe retry
policy remains separate. Expired-receipt settlement is atomic in SQS core.

Verified: affected package race suites and tagged real Swagger integrations;
real SNS/SQS/Lambda/Secrets/Scheduler SDKs; production startup/alias delivery,
completion acknowledgment, shutdown with a child and capture-order barriers;
50 repeated mapping race runs/concurrent closers; scoped tagged vet and frozen
SDK fixture checks. See HANDOFF for complete log names and publication status.
Read-only ownership review has no remaining bounded findings.

Approved test-only Express 5.2.1 and swagger-ui-express 5.0.1 are pinned in the
existing npm lane. No emulator dependency added. Ordinary tests need no SDK deps.

Next: publish v0.6.0 from clean source, verify packaged Linux amd64/macOS arm64
and downloaded release checksums, remove owned artifact/fixture staging, then
record actual publication. Other architectures are cross-built only.
Native mappings are single-record/in-memory/local-only; broader batching,
List/Update/filter/concurrency/partial responses remain unsupported and explicit.
SNS production admission retry infrastructure and durable async recovery remain
outside scope. Public ingress owns readiness blocking; no live AWS slash-parity
or consuming application acceptance is claimed. SES management stays low priority.
