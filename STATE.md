# EventBus state

Canonical source: SSD /home/spite/Projects/eventbus, main.
Public MIT: https://github.com/lyeith/eventbus.
Latest public binaries remain v0.7.0; no Plans or developer stack state changed.

#18 implementation and acceptance are complete, ready to commit.
The exclusive retained profile joins SQS mapping message custody/retry, accepted
Scheduler dispatch, Cognito trigger children and buffered/retrying Firehose S3
delivery. Future unclaimed schedules park; current resources survive resume.
Exact declared RequestResponse cleanup revokes held safety and admits descendants;
explicit re-quiesce joins them before attestation/resume. Native payload/auth stays
unchanged; full/partial ARN cleanup selectors respect configured region/account.
Gateway roots acquire versioned remote leases before body/auth/Invoke, pin process
identity, freeze entry generation and reconcile lost ACKs without business replay.
No active lease expiry. Failed final shutdown aborts/rejoins native owners with
peers live, then dirty-abandons unresolved remote leases and retains stores.
Legacy consumers, Secrets rotation and SQS move tasks remain profile refusals.
Trusted OS handlers await side work/stay in owned groups; escaped processes are
outside joined ownership. This profile remains exclusive to one operator.

#19 is committed/pushed as 9b8f56f: accept both exact native create mapping URIs.
Both unchanged current/1.39.4 SDK batch/retry/teardown proofs passed (48.701s).
User approved the isolated pinned test-only legacy SDK; frozen current lock intact.
Owned legacy env is retained only for final combined verification, then removed.

All affected owner race/vet suites and combined read-only review passed.
Production app full race passed19.587s. Actual full-stack Python/JavaScript SDK +
owned RustFS race passed23.832s with exact persisted-delivery comparisons,
authenticated generic cleanup, sentinels, resume and shutdown. Unavailable consuming
Identity business handler is not claimed verified. Logs have finite SSD /tmp retention.

New #20: handler-issued current DeleteMessage causes false mapping ACK failure.
MQ/eventsource/SDK owners are investigating read-only; no #20 implementation yet.
Fix canonical receipt-settlement evidence while preserving stale-lease safety,
then combined regressions and publish v0.8.0 under existing authorization.
Temporary portable packaged smoke /tmp/eventbus-v0.8.0-native-smoke.py is ready;
no release staging exists yet. Repo-owned fixtures joined; reusable caches retained.
