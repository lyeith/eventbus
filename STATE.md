# EventBus state

Canonical source: /home/spite/Projects/eventbus on SSD, main.
Public MIT repository: https://github.com/lyeith/eventbus.
Latest public binaries: v0.6.0 (2026-10-07); this work is not yet released.
Plans pins and developer stacks remain untouched.

#16 is implemented and verified:
- Native SQS mappings accept BatchSize 1–10/default 10 and configured
  ScalingConfig.MaximumConcurrency 2–1000 with accurate detached readback.
- Fixed workers own each receive/invoke/settlement sequence; the separate dev
  worker cap defaults to 32. No selected ceiling keeps local serial execution.
- SQS selects exact native event JSON within 6 MiB before committing leases.
  Excluded records retain visibility/counts/receipt state and FIFO head order.
- Actual whole-batch Lambda completion permits current-receipt acknowledgment;
  failure/timeout retains all receipts for native visibility/retry/redrive.
- Mapping Delete/Close and unavailable-source cancellation join every worker.
- Unsupported larger batches/windows/partial responses/Update/List remain explicit.

Verified: eventsource race suite repeated 20 times; messaging race and vet;
production app/consumer race; combined actual SQS/SNS/Lambda Python SDK race
39.907s, including preserved batch-one behavior, batch-five concurrency two,
[7,3] payload-bound selection, retries/DLQ/FIFO/isolation/two-child teardown.
Read-only ownership review found no remaining #16 defect.
No dependency added. Full logs follow existing finite SSD /tmp retention.

#17 is in flight: opt-in retained-owner quiescence, a development harness feature.
Explicit exclusive owner: suite roots use a source endpoint; registered handlers
and owned native peers use a separate callback endpoint. Shared activity tracks
received HTTP before capture, async tasks across retry intervals, and independent
sync execution until child cleanup. Fence/join/held exact cleanup/explicit resume
must fail closed on deadline, evidence failure or uncertain child ownership.
Unsupported autonomous sources will be refused in this first retained profile;
ordinary native mapping/runtime contracts stay unchanged outside the opt-in mode.
Implementation and real SDK acceptance are underway; no #17 guarantee is claimed.

Next: finish #17, combined review/verification, then publish the next minor release.
SES management and durable async restart recovery remain outside these tickets.
