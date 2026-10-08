# EventBus state

Canonical SSD /home/spite/Projects/eventbus, main. Public MIT standalone emulator
and agent harness; Plans and consuming application state remain untouched.
Latest published: https://github.com/lyeith/eventbus/releases/tag/v0.10.0
Preparing v0.11.1 for #27/#28. Failed v0.11.0 draft removed; public source tag
preserved. New tagged artifacts and native packaged acceptance are pending.

Commits:6e53ceb CLI guard;24de500 Lambda phase accounting;e50418d independent
performance review;fb8d02b shared Darwin process cleanup. No new dependencies.
Both CLIs refuse positionals before construction. Native Python/Node/provided
Init has10s; readiness starts configured Invoke. One cleanly joined fallback
shares configured Init+Invoke budget. Command retains whole-process timeout.
Caller/service/gateway and SQS leases remain independent; native IDs/Event
attempts unchanged. Core owns deadlines/facts; dev owns private phase schema.
Darwin EPERM reconciliation only probes actual group absence for at most1s;
persistent denial stays dirty and actual native process/resource joins remain.

PASS final affected-owner race:Lambda106.864s,localexec2.067s,
cognitotrigger7.838s,consumer11.036s,gateway40.141s; all tagged vet.
Earlier app/eventsource/native SDK and retained-stack race checks passed.
Native macOS old cleanup fails the real zombie regression; corrected process
owner race2.614s/vet pass, including descendant/pipes and persistent-denial proof.

Separate docs/PERFORMANCE-REVIEW.md measured180 invocations+80 appends.
Same-interpreter median Python Init29.675ms direct versus1160.549ms managed UV.
Durable XFS append median3.35–3.92ms. Report records source/method boundaries
and ranked unmeasured owner follow-ups; no application-chain outcome claimed.
Next: build all8 clean tagged artifacts, Linuxamd64/macOSarm64 packaged proof,
draft/download checksum verification, publish/close tickets and owned cleanup.
