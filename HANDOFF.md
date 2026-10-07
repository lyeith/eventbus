# Handoff

Canonical checkout: SSD /home/spite/Projects/eventbus, main.
Published latest: https://github.com/lyeith/eventbus/releases/tag/v0.7.0.
Immutable release source: a4bed486162edd892e5ba28d78c81fbbcd23f3e8.
No Plans pin, developer stack or identity database changed; no dependency added.

#16 is committed/pushed as 360c0f8: native SQS batch/concurrency support.
eventsource owns joined workers/native validation; messaging owns pre-lease
6 MiB projection and FIFO/lease/redrive state; Lambda owns whole-batch completion.
Actual SDK proof covers overlapping [5,5,5] at concurrency two, payload [7,3]
without leasing exclusions, failure/timeout/DLQ/FIFO/isolation and two-child
Delete/Close. Existing batch-one behavior remains covered.

#17 is committed/pushed as a4bed48, accepted and published:
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
  quiescent; empty owned scratch was removed. This was not a Go test failure.
- Earlier focused runtime/lifecycle race repeats and evidence proofs also PASS.
Read-only combined review found no remaining bounded ownership/correctness issue.

Release verification:
- eventbus-v0.7.0-build.log / metadata.log: eight CGO-free EventBus/gateway targets
  match v0.7.0, a4bed48, clean VCS state and their declared OS/architecture.
- eventbus-v0.7.0-{linux,macos}-native.log: packaged Linux amd64/macOS arm64 PASS:
  actual SQLite startup, two gated five-record children, completion-only receipt
  settlement, native deletion, SNS async -> independent nested Invoke, timed
  unsafe fence, live peer completion, exact cleanup/sentinel/resumed second suite.
- Both native gateway binaries' --help executed; gateway integration race passed.
  Other six cross-built targets were inspected, not executed.
- Draft assets downloaded; all eight checksums and SHA256SUMS comparison passed.
  Final release is public/latest with nine assets; remote annotated tag resolves
  to the verified source. #16/#17 are closed; no open issues at final check.

Cleanup complete: owned native processes/fixtures joined; source/download/laptop
release staging, temporary smoke/notes scripts, new task bytecode and empty
failed-wrapper scratch removed. Frozen SDK caches and unrelated state preserved.
Evidence logs remain under existing finite SSD /tmp retention. No task resources
are held. SES management and durable async restart recovery are separate backlog.
