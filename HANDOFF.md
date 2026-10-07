# Handoff

Canonical checkout: SSD /home/spite/Projects/eventbus, main.
Latest public binaries remain v0.6.0; accepted #16/#17 await v0.7.0.
No Plans pin, developer stack or identity database changed; no dependency added.

#16 is committed/pushed as 360c0f8: native SQS batch/concurrency support.
eventsource owns joined workers/native validation; messaging owns pre-lease
6 MiB projection and FIFO/lease/redrive state; Lambda owns whole-batch completion.
Actual SDK proof covers overlapping [5,5,5] at concurrency two, payload [7,3]
without leasing exclusions, failure/timeout/DLQ/FIFO/isolation and two-child
Delete/Close. Existing batch-one behavior remains covered.

#17 is implemented, reviewed and accepted:
- devquiescence owns exclusive source fencing, counts, held proof and generations.
- app/dev_retained_owner.go composes two loopback endpoints, controls and bounded
  exact cleanup; autonomous untracked producers are refused only in this profile.
- Lambda dev_lifecycle.go observes whole async tasks and independent sync calls;
  private ownership errors cannot be forged by handler results or erased by retry.
- Provided Runtime API joins every admitted HTTP handler, including duplicate
  next polls and post-ack handlers; closing its listener is insufficient.
- devcapture owns sticky non-closing evidence inspection; SNS exposes its seam.
  Capture failure during held Unsubscribe prevents safe completion/resume.
- awsprotocol owns shared JSON target media-version selection for dispatcher
  and native dev refusals; service/REST admission remains caller-owned.
- Shutdown fences irreversibly, preserves peers while joining, then drains owners.
  A failed join aborts/joins native work and received envelopes and retains stores.
- docs/RETAINED-OWNER.md defines endpoint ownership, controls and failure limits.
  Applications still own SDK settings, exact fixtures and business assertions.

Latest combined verification logs in SSD /tmp under existing finite retention:
- eventbus-issue17-final-core-race.log: app/server/eventsource/messaging/Lambda/
  consumer/devcapture/devquiescence/gateway integration all PASS.
- eventbus-issue17-final-sdk-race.log: Messaging, retained-owner barrier, SNS
  Lambda, SQS batches and original SQS Lambda actual Python SDK lanes PASS (50.031s).
- eventbus-issue17-other-native-sdk-race.log: Lambda Event, JavaScript Scheduler
  and Python Secrets rotation PASS (12.434s).
- eventbus-issue17-final-vet.log: tagged scoped vet PASS.
- eventbus-issue17-wire-owner-race.log: awsprotocol/server/devquiescence PASS.
  SSD wrapper finalization hit EAGAIN after success; exact run inspected inactive/
  quiescent with only empty scratch directories, not a Go test failure.
- Earlier focused runtime/lifecycle race repeats and evidence proofs also PASS.
Read-only combined review found no remaining bounded ownership/correctness issue.

Next: commit/push #17, tag clean v0.7.0, build all eight CGO-free binaries,
execute Linux amd64/macOS arm64 packaged native batch+retained-owner smoke,
publish/check download manifest, then remove owned staging and record release.
Reusable frozen SDK caches stay in place; tests clean their owned fixtures.
