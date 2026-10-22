# EventBus state

Canonical SSD /home/spite/Projects/eventbus, main; public MIT AWS emulator
and agent development harness. #45, #46 and #47 are implemented on 2026-10-23.
Source-only changes; latest published binaries remain v0.19.0:
https://github.com/lyeith/eventbus/releases/tag/v0.19.0
No binary publication was requested for this pass.

#45 Firehose signing:
- Strict encoded S3 wire paths preserve decoded object keys and encoded endpoint
  segments. Existing SigV4 signs actual bytes without a provider-specific mode.
- Native VersityGW 1.9.0 passes six SDK controls and six Firehose PUT/list/get
  cases, including =, +, space, Unicode and literal %/%2F with exact binary bytes.
  Independent canonicalization covers encoded endpoint base paths.

#46 Firehose retry/logging:
- Existing pending objects retry at 1,2,4,8,16,32,60-second capped intervals.
  Buffer readiness, native dynamic retry windows and cooldown are independent.
- Retained force-readiness honors cooldown and keeps leases; explicit delete
  and final shutdown attempt once despite cooldown and report retained failures.
- Redacted first/change/minute-summary/recovery logs replace per-second errors.
  No dropped 4xx data, changed retry keys or separate delivery owner.

#47 SES local tracking:
- Opt-in --ses-tracking exposes dev message discovery/HTML and opaque GIF URLs.
  Ordinary image GET publishes native Open through current SNS destinations,
  using original MessageId and actual request timestamp/socket IP/user agent.
- Normal sending responses and exact capture remain unchanged. Shared native
  selection honors destination generations and v2 per-message DISABLED tracking.
- Named dev adapters share the accepted-message 24h/10k/64MiB owner. Raw MIME
  decoding is bounded; bulk views are rendered/retired one at a time after capture.
- Views are sandboxed; scripts/forms/external images are blocked. Retained HTTP
  counts pixel GET as work and refuses held source/cleanup callback publication.
- Simple template rendering preserves large JSON numbers through one decoder.
  Duplicate native openTracker-marker rejection stays a documented core gap.

Verification:
- Affected SES/Firehose/app/server/messaging/devquiescence/tests race passed;
  all integration+sdksmoke-tag vet passed. Final template-only race/vet passed.
- Native Versity race13.877s; SES SDK/native SNS/Python subscriber race9.298s:
  enabled7 Opens/24 observations, default-off0 Opens/5 observations.
- Real Chromium loaded the trailing sandboxed pixel once and blocked mail script.
- Independent ownership/code/docs review clean; no dependencies added.
- Managed test owners inspected quiescent and scratch removed. Raw /tmp ticket
  evidence follows existing24h expiry, no pins. No task branches/environments
  or live/default-stack/cache changes. Laptop Plans untouched.

Next: observe source CI after push; publish new binaries when requested.
