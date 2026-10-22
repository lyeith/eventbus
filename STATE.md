# EventBus state

Canonical SSD /home/spite/Projects/eventbus, main; public MIT AWS emulator
and agent development harness. #43 and #44 are published in v0.19.0:
https://github.com/lyeith/eventbus/releases/tag/v0.19.0

Release v0.19.0 (2026-10-22):
- Annotated tag/source: b457954c682e691260ea3ed09ee1346dee2915ce.
- Source CI39727887097 passed full Go race/vet, the frozen SDK suite, fixture
  contracts and unchanged Express/Swagger handlers.
- Eight CGO-free Linux/macOS amd64/arm64 service/gateway binaries + SHA256SUMS.
  Target formats and clean source metadata verified; all uploaded downloads
  matched the built manifest and passed checksums. Latest/public/non-prerelease.
- Actual Linux amd64 and Mac arm64 packages passed maintained native Runtime API
  and Go SDK proofs for both tickets, plus service/gateway help.
- Release build, downloads, Mac package copies and temporary test drivers removed.
  Inspected managed source/build/package owners quiescent; scratch auto-removed.

#43 final shutdown:
- Positive --shutdown-timeout controls ordinary and retained final joins;
  default30s and graceful/discard-pending behavior remain.
- Lambda freezes bounded native IDs/state/attempt/retry metadata before abort.
  App reports final async timeout without payload, environment, errors or output.
- Native retries and per-request HTTP deadlines remain. Final timeout fails and
  cancels in-memory work; restart cannot recover it. Retained /quiesce remains
  resumable and preserves accepted work on timeout.

#44 Firehose:
- Independent --s3-access-key/--s3-secret-key/--s3-session-token selections,
  defaults test/testtest123/empty; no ambient provider chain or role assumption.
- Existing SigV4 signs actual object bytes and optional session token.
- Redirects fail without forwarding credentials/body; prepared objects retain
  their key for retry at the configured endpoint. No dependency added.

Validation:
- Full local affected race and all-tag vet passed before publication; current
  source CI covers the complete race/vet/SDK/Swagger lanes.
- Linux package proof39.347s; Mac package proof passes all maintained cases:
  real third retry >30s after SIGTERM, original identity/payload, overlapping
  handler, SDK/JWKS callbacks, final timeout metadata and resumable recovery.
- Native SDK Firehose ingestion flushes on SIGTERM to held actual S3 responses;
  selected secret/token signatures, legacy defaults and redaction are verified.
  Core independently authenticates HMAC and redirect failure/same-key retry.
- CI exposed a Python fixture readiness/exit-observation race. Atomic publication
  and bounded descendant checks preserve actual PID/ownership assertions.
  Focused race24.966s and vet pass; no production process leak demonstrated.
- Independent implementation/privacy/docs and final fixture reviews are clean.

Raw /tmp/eventbus-v0.19.0-* and /tmp/eventbus-v019-* evidence follows existing24h
retention, no pins. Inspected failed scratch removed. No task worktrees, branches,
environments, live-stack/default-state changes or dependencies. Laptop Plans
untouched. Source, publication and cleanup complete.
