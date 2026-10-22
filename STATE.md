# EventBus state

Canonical SSD /home/spite/Projects/eventbus, main; public MIT AWS emulator
and agent development harness. #45/#46/#47 are published in latest v0.20.0
on 2026-10-23:
https://github.com/lyeith/eventbus/releases/tag/v0.20.0

Release:
- Annotated tag/source: 552e827b36c3dcac351363d7790e4758dcdff97f.
- Full source CI39837337269 passed Go race/vet, frozen SDK/fixture contracts
  and unchanged Express/Swagger handlers.
- Eight CGO-free service/gateway binaries for Linux/macOS amd64/arm64 plus
  SHA256SUMS. Correct target formats and clean source metadata verified.
- All nine public downloads match the built manifest and pass checksums.
  GitHub confirms latest/public/non-prerelease.
- Actual Linux amd64 and Mac arm64 packages pass SES tracking acceptance:
  enabled7 Opens/24 native observations, default-off0 Opens/5 observations.
  Native SES SDK sends -> HTTP pixel -> SNS/Python handler, with original capture.
- Linux package additionally passes native Firehose SDK ingestion, selected
  credentials/session-token signatures and joined held-S3-response shutdown.
  Both native service/gateway help checks pass.

#45 Firehose signing:
- Strict wire-path encoding preserves decoded keys and endpoint segments.
  Native VersityGW1.9.0 source proof covers six SDK controls plus six Firehose
  PUT/list/get cases, including =/+/space/Unicode/literal %/%2F and binary bytes.
  Independent canonical verification covers encoded endpoint bases.

#46 Firehose retry/logging:
- Existing pending objects back off 1,2,4,8,16,32,60 seconds independently of
  buffer readiness. Dynamic retry-window expiry preempts cooldown.
- Retained force-readiness keeps leases and honors retries; explicit delete/final
  shutdown attempt once despite cooldown and report retained failures.
- Redacted first/change/minute-summary/recovery diagnostics; stable keys/bytes,
  no dropped 4xx records and no separate delivery owner.

#47 SES tracking:
- Opt-in --ses-tracking; named dev adapters expose bounded sandboxed views/pixels.
  Shared native selection honors current destinations/generations/v2 overrides.
- Capture and sending responses stay unchanged; views share24h/10k/64MiB
  retention. Bulk rendering/eviction releases optional HTML references promptly.
- Retained routing counts/fences pixel GET work. Template JSON numbers stay exact.
  Native duplicate openTracker-marker rejection remains a documented core gap.
- Affected race/all-tag vet, final template race/vet and independent review passed.

Cleanup:
- Build/test/download managed owners quiescent, scratch auto-removed.
- Release outputs/downloads/notes and Mac source/env/packages/driver removed.
  Complete /tmp/eventbus-v0.20.0-* evidence follows existing24h expiry, no pins.
- No task worktrees/branches/environments or live/default-stack/cache-policy
  changes. No dependencies added. Laptop Plans untouched.
Publication and cleanup complete.
