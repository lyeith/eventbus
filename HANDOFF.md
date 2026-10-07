# Handoff

Canonical SSD checkout: /home/spite/Projects/eventbus, main.
Latest public release: v0.7.0. No Plans pins/stacks/databases changed.
#19 committed/pushed9b8f56f. #18 accepted, ready to commit; #20 newly on the plate.

#18 shared ownership:
- devactivity typed ports; devquiescence owns fences, held generations, transitions,
  cleanup admission, owner identity and idempotent bounded remote lease ledger.
- SQS owns accepted mapped-message custody through visibility/ack/redrive/removal;
  eventsource owns joined workers and restricted continuations of exact custody.
- Scheduler retains accepted dispatch/retry ownership and parks unclaimed timers.
- Cognito runner/Lambda join direct child/pipes/cleanup; private uncertainty sticky.
- SES/Cognito captures expose nonclosing evidence; Firehose evidence is terminal
  only, accepted work counted through buffer/retry/S3 response/flush-slot join.
- Gateway roots lease before auth/body/Invoke, freeze entry generation, reconcile
  ACKs without replay and join actual delayed envelopes through shutdown.
- App composes ports, exact --retained-owner-cleanup-functions, native metadata
  and namespace policy, then permanent abort/actual rejoin after failed shutdown.
- No source reopening, auth bypass or payload rewrite. Explicit re-quiesce after
  cleanup descendants before safe attestation/resume. Same resources reused.
- Exclusive/trusted process-group boundary and foreign-lease shutdown uncertainty
  are documented; legacy consumer/rotation/move-task profile refusals remain.

Verification (finite SSD /tmp logs):
- Independent affected-owner race/vet suites PASS; combined read-only review PASS.
- eventbus-retained-full-stack-app-race-20261008.log: full app PASS19.587s.
- eventbus-retained-full-stack-app-vet-20261008.log: app vet PASS.
- eventbus-retained-stack-sdk-race-final-20261008.log: SDK/RustFS PASS23.832s.
- eventbus-retained-stack-sdk-vet-20261008.log: tagged SDK vet PASS.
- eventbus-sqs-uri-{core-race,core-vet,sdk-race,sdk-vet}.log: #19 PASS both exact
  SDK models and unchanged native batches/retries/teardown.
SDK uses generic JWT-authenticated native cleanup, not unavailable production
Identity handler. All SDK/proof children, listeners/RustFS/data joined/removed.

#20 proposed core owner: upgrade existing issued-receipt history with Settled bit,
set only by actual current/unexpired deletion. New bound-queue mapping ACK succeeds
for current delete or proven prior settlement; issued stale HTTP no-op stays false.
MQ owns receipt source/tests; eventsource owns port comment/ACK join tests;
SDK owner owns real handler manual-settlement proof; parent owns app adapter.
No edits granted yet while #18 commit is prepared.

Finish #20, final affected regressions, then build/publish clean v0.8.0 (eight
binaries + SHA256SUMS), native packaged Linux/macOS proof and downloaded checksums.
Portable smoke is /tmp/eventbus-v0.8.0-native-smoke.py; remove after proof.
Legacy env /tmp/eventbus-legacy-sdk.UAIkso is held only for final SDK, then remove.
No release staging/tag yet. Preserve reusable frozen caches/unrelated user state.
