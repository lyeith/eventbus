# Handoff

Canonical SSD /home/spite/Projects/eventbus, main. #43/#44 implementation complete.
Latest binary release is still v0.18.0; this pass changes source only.

#43:
- App --shutdown-timeout is a positive duration, default30s, wired into the
  existing final listener budget for ordinary/retained graceful or discard stop.
- Named lambda/dev_async_drain.go snapshots first permanent abort under the
  existing owner mutex before cancellation clears retry state. Getter detaches
  capped16 metadata rows and accounts for unpublished admission reservations.
- app/dev_shutdown.go consumes that port only for final deadline failure.
  Error summary is pre-abort evidence, with no payload/env/error/output.
- Final timeout still fails and cancels in-memory work; no restart/replay claim.
  Existing retained /quiesce timeout preserves work and can be retried.
- Commit1ffec6f owns this change; ordinary broker needs no callback profile.

#44:
- Explicit --s3-access-key, --s3-secret-key and optional --s3-session-token
  replace hidden CLI literals, preserving test/testtest123/empty defaults.
- Firehose S3Credentials value feeds the existing AWS SigV4 signer; the legacy
  constructor delegates. No provider chain, credential refresh or dependency.
- Client rejects all redirects, preserving failed prepared objects for same-key
  retry at the configured endpoint. Credentials/body never follow Location.
- App flags validate non-empty access/secret before opening owned resources.

Validation:
- Full race receipts:2296a50496944131bf4736e25c627fe5 core,
  1552ff0897a445829dcd50791792bee5 consumers; both success/quiescent.
  Lambda188.171s, Firehose9.307s, app154.283s, gateway29.725s,
  eventsource1.940s, messaging7.599s, devquiescence1.076s, architecture1.188s.
- All-tag vet:3c5bd1be2a1d41ada3348887aef830c0 success/quiescent.
- Final ordinary35s retry proof is included in the full app gate; retained
  timeout/recovery and existing discard/exhausted-business-failure cases pass.
- Core receiver independently authenticates SigV4 with stdlib HMAC; app receiver
  re-signs with the selected secret to prove real CLI composition, retaining
  held-response join and no-secret/token health/readback/log assertions.
- Config tests cover defaults/overrides, invalid durations and empty credentials.
  Source/owner/docs review, gofmt and diff checks pass.

Maintained package proof seams (not exercised against release artifacts here):
EVENTBUS_SMOKE_BINARY=/absolute/eventbus-package
go test -race -count=1 -timeout 3m ./internal/app \
  -run '^(TestCLIConfiguredGrace.*|TestCLIQuiesceTimeoutRecoversNativeRetryBeforeFinalStop|TestCLIFirehoseS3Credentials)$'
The test binary supplies real native handlers; paths/listeners/state are owned.
Full frozen SDK smoke is left to normal GitHub CI; native Go SDK consumers ran.

Raw logs: /tmp/eventbus-ticket43-44-*, /tmp/eventbus-ticket43-* and
/tmp/eventbus-44-* follow existing24h expiry, no pins. Successful managed scratch
auto-removed; inspected disposable failed scratch removed. No task worktrees,
branches, environments, live-stack/default-state changes or dependencies.
Laptop Plans unchanged. Source is ready for the next binary release.
