# EventBus state

Canonical SSD /home/spite/Projects/eventbus, main. Public MIT standalone emulator
and agent harness. Plans and consuming application state are untouched.
Latest public release v0.9.0; #25/#26 implementation and acceptance are complete.
Next: tag/build/package proofs and publish v0.10.0, then close both issues.

#25 private diagnostics distinguish caller cancellation/deadline, service stop
and configured function timeout. Original native response/state/retry policies
remain compatible; misleading legacy function diagnostics are omitted for early
external cancellation. Cause is frozen after native execution cleanup; elapsed
time includes the joined private collector.
#26 opt-in Python thread/task snapshots use separate bounded JSONL descriptors,
the existing private0600 sink and original request/attempt identity. One explicit
or scheduled attempt, including configured earlier gateway budgets. Hard kills
never wait for capture. Locals/source/names/reprs/raw exceptions are excluded.
Missing optional evidence is explicit; pipe/sink ownership failures remain dirty.

PASS: full Go race suite; final Lambda race75.424s after sole-join policy fix;
actual native SDK/retained consumer race69.998s; full sdksmoke-tagged vet.
Gated append tests preserve native success after budget/service cancellation,
while collector joins still hold the admitted lifetime. No dependencies added.
Parent owns the serial SSD test/build lane, GOMAXPROCS4/-p2. Lane currently idle.
Disposable parent staging /tmp/eventbus-25-26-parent awaits release proofs/cleanup.
