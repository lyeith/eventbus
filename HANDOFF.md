# Handoff

Canonical checkout: SSD /home/spite/Projects/eventbus, main.
Latest published release remains v0.6.0; no new binaries published yet.
No developer stack, Plans pin or identity database changed.

#16 implementation is complete and reviewed:
- eventsource owns native batch/default/scaling admission and fixed joined workers.
- messaging owns pre-lease exact SQS Lambda event-byte admission (6 MiB), retaining
  native FIFO/fairness/retry-attempt state. sqsevent owns wire types.
- app and SDK adapters delegate projection/lease policy to messaging; Lambda
  completion governs whole-batch ack. No legacy consumer routing or new dependency.
- docs state native BatchSize 1–10/default 10, ScalingConfig 2–1000, separate local
  cap 32, omitted-ceiling serial policy, zero window and remaining explicit limits.

Verification logs under SSD /tmp, governed by existing finite retention:
- eventbus-eventsource-batch-final-race.log: full core x20 PASS (5.667s).
- eventbus-eventsource-batch-final-vet.log: scoped vet PASS.
- eventbus-sqs-lambda-batch-final-race.log: full messaging race PASS (5.196s).
- eventbus-sqs-lambda-batch-final-vet.log: final messaging vet PASS.
- eventbus-issue16-app-consumer-race.log: app/consumer PASS (10.755s/11.059s),
  including actual production batch-five alias wiring and existing lifecycle proofs.
- eventbus-issue16-combined-sdk-race.log: four real Python SDK lanes PASS (39.907s):
  native messaging, SNS Lambda, batch/concurrency and preserved batch-one mapping.
  Actual batches [5,5,5] overlap at two; payload-bound backlog splits [7,3] without
  leasing excluded records; full-batch failure/timeout/DLQ/FIFO/isolation and pending
  Delete/Close joining two actual children are verified.
- eventbus-sqs-batch-sdk-{normal,race,vet}.txt: focused new lane PASS.
Read-only review found no remaining #16 ownership/seam/correctness defect.

#17 is implementation work in flight, not accepted or published:
- New devquiescence owner tracks/fences source HTTP and trusted callback HTTP.
- Lambda optional dev lifetime port spans async queue/retry/terminal evidence and
  independent sync child cleanup; private cleanup uncertainty must fail closed.
- Non-closing SNS capture error inspection prevents a false safe recovery result.
- Parent owns app two-endpoint/control/cleanup composition and explicit unsupported
  producer refusals; SDK acceptance owns fresh fixtures and registered handlers.
- Exact application cleanup stays application-owned; no broker reset or data wipe.
No new dependencies or live resources were provisioned. Temporary fixtures are
owned by tests and cleaned; reusable frozen SDK environments remain in place.

Next: finish #17, integrate/review/test, publish next minor release and clean staging.
