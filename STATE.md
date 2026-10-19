# EventBus state

Canonical SSD /home/spite/Projects/eventbus, main; public MIT AWS emulator
and agent development harness. #42 source is complete. User requested publication
when verified; v0.18.0 preparation is next. Published binaries remain v0.17.0:
https://github.com/lyeith/eventbus/releases/tag/v0.17.0

#42 failure-only private diagnostics:
- Optional dev_diagnostics.mode: failures; omitted or all preserves every
  existing owned-attempt record. No diagnostic configuration means no sink.
- Diagnostics alone selects final joined attempts using native state, frozen
  cancellation, ownership uncertainty and injected native retry/crash facts.
  It does not classify free-form process errors or recovered Init phases.
- Clean success emits no private row; caught/logged business errors need all.
  Failed async attempts remain retained independently of later successful retry.
  Owned launch/Init, timeout/cancellation and native429 failures retain detail.
- Python uses its existing bounded session record. In failures mode it defers
  append until terminal selection, then stack precedes invocation; clean success
  writes neither schema. All mode retains existing live snapshot visibility.
- Native responses/Tail, warm reuse, retry/ACK owners, async/SQS metadata, private
  file protection, output limits and sticky capture failures stay authoritative.
  This changes retained storage, with no latency improvement claim/dependency.
- Lambda/agent/architecture/triage docs cover configuration and boundaries.

Verification:
- Focused real runtime race3.640s; real Python collection/fault race4.000s.
- Native SNS CLI race4.740s: failure then successful retry, same native event/
  request/message identity, private failure detail retained, later success
  omitted only in failures mode. Default all retains both; children/owners join.
  Maintained proof supports an actual package through EVENTBUS_SMOKE_BINARY.
- Config/final-selection table race1.053s; invalid modes fail before file effects.
- Full affected race passed: Lambda161.595s, app64.742s, gateway25.374s,
  eventsource1.604s, architecture1.051s. All-tag vet and diff checks passed.
- Independent production, Python ownership and documentation review is clean.

Owned source tests joined; successful managed test scratch removed.
Raw /tmp/eventbus-ticket42-* logs and one corrected test-expectation failure
use existing24h unpinned retention. No task worktrees/branches/new environments,
application/default-state changes or dependencies. Laptop Plans untouched.
Publish after source CI and packaged artifact verification; do not stop at source.
