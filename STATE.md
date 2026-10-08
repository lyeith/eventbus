# EventBus state

Canonical SSD /home/spite/Projects/eventbus, main. Public MIT standalone emulator
and agent harness; Plans and consuming application state remain untouched.
Latest published release: https://github.com/lyeith/eventbus/releases/tag/v0.10.0
Preparing v0.11.0 for #27/#28; tagged builds and packaged acceptance are pending.

Commits:6e53ceb CLI guard;24de500 Lambda phase accounting;e50418d separate
performance review and opt-in fixtures. No dependency changes.
Both CLIs reject positional input before fixture/resource construction.
Native Python/Node/provided Init has10s; readiness starts configured Invoke.
One cleanly joined fallback shares configured budget across Init+Invoke.
Caller/service/gateway and SQS lease owners remain independent. Command keeps
whole-process timeout. Native request ID/Event attempt remain unchanged.
Core owns deadlines/causes/frozen facts; dev adapter owns private phase schema.

PASS: Lambda full race101.852s; final phase/projection race23.812s; additional
protocol/dirty-ownership race3.522s; app24.922s/gateway39.356s/eventsource1.431s;
CLI focused app2.497s/gateway2.128s; native SDK/retained race40.624s;
full sdksmoke+performance-tagged vet.
Separate docs/PERFORMANCE-REVIEW.md measured180 invocations+80 appends, with
actual child/group/listener/sink joins. Python median Init29.675ms direct versus
1160.549ms managed UV with the same interpreter. Private terminal wall added
about4ms in controlled300ms fixture; durable XFS append median3.35–3.92ms.
Other source hotspots remain unmeasured follow-ups; no business-chain claim.

Next: clean tagged build, all8 artifact metadata, native Linux/macOS packaged
CLI/phase/privacy/ownership proofs, uploaded-asset checksum verification,
publication, issue closure and owned temporary-resource cleanup.
