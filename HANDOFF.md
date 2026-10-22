# Handoff

Canonical SSD /home/spite/Projects/eventbus, main. #45/#46/#47 published
as latest v0.20.0 on 2026-10-23:
https://github.com/lyeith/eventbus/releases/tag/v0.20.0

Release:
- Annotated tag/source 552e827b36c3dcac351363d7790e4758dcdff97f.
- Source CI39837337269 success: full Go race/vet, frozen SDK/fixture contracts
  and unchanged Express/Swagger handlers.
- Eight CGO-free service/gateway binaries for Linux/macOS amd64/arm64 plus
  SHA256SUMS. All formats, clean source metadata and public-download SHA verified.
  GitHub confirms latest/public/non-prerelease and nine assets.

Ownership:
- Firehose s3_path.go owns strict wire encoding; existing SigV4 stays.
  delivery_retry.go keeps per-object1..60s cooldown and coalesced diagnostics
  in the existing flush owner. Buffer/window/cooldown remain independent.
  Retained force-readiness honors retries; final delete/shutdown bypasses once.
- SES core owns current destination/generation and per-message override policy.
  Explicit outcomes and dev pixels reuse its retainedEventLocked selector.
- ses/dev_tracking*.go owns optional rendering/MIME/discovery/pixels; app composes
  narrow dev ports. Server stays AWS. Existing24h/10k/64MiB correlation owner
  retires HTML/token references, including prepared bulk Send references.
- template.go owns shared UseNumber parsing; v1/v2 preserve9007199254740993.
- docs/SES.md documents opt-in/default-off resources/status/retention/limits.
  Native duplicate-marker rejection remains a core BACKLOG gap.

Verification:
- Local affected SES/Firehose/app/server/messaging/devquiescence/tests race and
  all-tag vet passed; final template-only race/vet passed; independent review clean.
- Native VersityGW1.9.0 source race13.877s covers six SDK controls and six actual
  Firehose key/body cases; independent receiver covers encoded endpoint bases.
- /tmp/eventbus-v0.20.0-linux-package.log: real packaged service, race driver,
  SES SDK/native SNS/Python flow8.344s; on7 Opens/24 observations, off0/5.
  Includes original IDs/capture, repeats, older message after resend, disabled/
  filtered/deleted/generation destinations, v2 override and negative HTTP reads.
  Native Firehose SDK credential/session-token/held-write shutdown proof1.356s.
- /tmp/eventbus-v0.20.0-mac-ses-tracking-race.log: same native SES proof against
  Mac arm64 package with locally compiled race driver,7.71s; on7/24, off0/5.
  Mac package hashes/metadata/help and exact source archive identity verified.
  Both packaged CLI processes joined exit0; gateway coverage here is help only.
- /tmp/eventbus-v0.20.0-artifact-audit.log: all eight targets clean/CGO-free.
  /tmp/eventbus-v0.20.0-download-verification.log: every public asset verified.
- Prior focused Chromium proof loaded the sandboxed trailing GIF once and
  blocked mail script. Consumer completion remains an application assertion.

Cleanup:
- Build041816af3d04449aa7b11a285dddfdf1, Linux package test
  16caf428f57f437e8f2148800bad0085 and public-download verification
  c9b7123d52134153aa9e48db4e31bc89 inspected quiescent; scratch auto-removed.
- Owned release outputs/notes/downloads removed after public verification.
  Mac source export, private UV environment/interpreter/cache, binaries and
  race driver removed after process/lsof inspection and evidence copy-back.
- Raw /tmp/eventbus-v0.20.0-* evidence follows existing24h expiry, unpinned.
  No task worktrees/branches/environments, live/default-stack or cache-policy
  changes. No dependencies added. Laptop Plans untouched.
Publication and cleanup complete.
