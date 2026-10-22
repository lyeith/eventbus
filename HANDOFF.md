# Handoff

Canonical SSD /home/spite/Projects/eventbus, main. #45/#46/#47 implemented,
reviewed and locally verified on 2026-10-23; source push is the session finish.
Latest published binaries are still v0.19.0. No release requested this pass.

Ownership:
- Firehose s3_path.go owns strict wire-path construction; the existing SigV4
  signer remains. Native VersityGW qualifies actual compatibility independently
  of the test receiver, without changing bucket keys or Terraform prefixes.
- delivery_retry.go owns per-object 1..60s cooldown and coalesced diagnostics in
  the existing flush. Background/retained force-readiness honors retryAt; final
  delete/shutdown bypasses cooldown once, preserving failure and original bytes.
  Dynamic retry-window expiry preempts cooldown and retains the error object.
- SES native core owns per-message tracking overrides, configuration generations
  and current destination selection. Explicit outcomes and pixels reuse it.
- ses/dev_tracking*.go owns optional HTML/MIME/template rendering and opaque
  pixel/discovery HTTP resources. App composes narrow dev ports; server stays AWS.
  No second retention cache, worker or expiry loop. Eviction retires token/body
  references even when a bulk Send still holds native metadata.
- template.go owns the existing UseNumber template-data decoder shared by both
  optional renderers. V1/v2 retain exact 9007199254740993 and original capture.
- Default tracking off; flags, routes, status codes, retention and limitations:
  docs/SES.md#local-email-views-and-tracking-pixels. Native duplicate-marker
  rejection remains in BACKLOG as a core validation gap.

Verification evidence on SSD:
- /tmp/eventbus-tickets45-47-affected-race-20261023.log
  SES26.203s, Firehose12.675s, app107.210s, server3.327s,
  messaging5.707s, devquiescence1.047s and tests1.049s, all race PASS.
- /tmp/eventbus-tickets45-47-vet-20261023.log: all-tag vet PASS.
- /tmp/eventbus-ticket47-ses-template-number-race-20261023.log:
  final template/tracking/bulk race1.090s; companion SES vet PASS.
- /tmp/eventbus-ticket45-versity-final-race.log: real provider race13.877s;
  SDK control + actual Firehose for six key cases, exact native list/get bytes.
  The baseline log reproduces old Firehose =/+403 while unchanged SDK succeeds.
- /tmp/eventbus-47-ses-tracking-sdk-race-final.log: production CLI race9.298s,
  actual SESv1/v2 sends/raw MIME -> HTTP pixel -> SNS/native Python subscriber;
  enabled7 Opens/24 observations, default-off0 Opens/5 observations. Includes
  repeats, older message after resend, current disabled/filter/deleted/generation
  behavior, v2 override and unchanged capture. No synthetic outcomes/SNS Publish.
- /tmp/eventbus-ticket47-browser-csp.log: existing Chromium loads actual GIF under
  actual CSP after trailing HTML comment; script blocked. This is a focused
  browser/parser proof, not a browser-native SES consumer completion claim.
- Low-level tests cover TTL/eviction, byte bounds, native selector, admission
  errors, template numbers, retry cap/diagnostics and final failure semantics.
  App proves held pixel GET retains work and later held/callback GET is refused.
- Independent source/ownership/docs review clean. No new dependencies.
  Packaged binaries not built/tested for this source-only pass.

Cleanup: managed owners quiescent with scratch removed; isolated provider,
listeners and temporary storage stopped/removed. Failed empty scratch inspected
and removed. Raw evidence follows existing24h retention, unpinned.
No task worktrees/branches/environments, live/default-stack changes or cache
deletions. Laptop Plans untouched. Observe source CI; release only on request.
