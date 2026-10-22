# Handoff

Canonical SSD /home/spite/Projects/eventbus, main. #43/#44 complete and published
as latest v0.19.0 on 2026-10-22:
https://github.com/lyeith/eventbus/releases/tag/v0.19.0

Release:
- Annotated tag/source b457954c682e691260ea3ed09ee1346dee2915ce.
- Source CI39727887097 success: complete Go race/vet, frozen Python/JavaScript
  SDK suite, fixture contracts and unchanged Express/Swagger handlers.
- Eight CGO-free service/gateway binaries: Linux/macOS amd64/arm64.
  Correct formats, clean VCS/source metadata and every downloaded SHA verified;
  GitHub confirms latest/public/non-prerelease and nine assets.
- Actual Linux amd64 and Mac arm64 artifacts pass service/gateway help and the
  maintained native proofs below. Linux race driver39.347s; Mac cases all pass.
- Build6ad925c8e93845a8a7076660d09f400d and Linux package test
  46eaeba17e194960a8358050a61ffbfa inspected quiescent; scratch auto-removed.
  Cross-built Mac driver owner30f69a6df9704df69ee577c725ff2353 likewise quiescent.
- Build outputs/downloads/notes, both Mac package copies and all temporary test
  drivers removed. Full package evidence copied to SSD before Mac cleanup.

#43:
- --shutdown-timeout is a positive final budget, default30s, for ordinary and
  retained graceful/discard shutdown. Ordinary brokers need no callback profile.
- lambda/dev_async_drain.go freezes first-abort metadata under existing mutex:
  capped16 detached native rows plus reservation/total/omitted accounting.
  app/dev_shutdown.go reports it only on final deadline failure, without private
  payload/environment/error/output. No new queue or recovery owner.
- Native retry/HTTP budgets remain. Final timeout still cancels/fails and loses
  pending in-memory work; repeatable retained /quiesce preserves accepted work.

#44:
- Explicit S3 access/secret/optional-token flags preserve test/testtest123/empty.
  App injects immutable S3Credentials into existing SigV4; legacy constructor
  remains, with no provider chain/refresh/role assumption or dependency.
- Redirects retain failed objects at original keys without forwarding token/body.
  Core independently authenticates actual HMAC; CLI proves selected secret
  wiring, held destination join and no secret/token in health/readback/logs.

Release acceptance:
EVENTBUS_SMOKE_BINARY=/absolute/eventbus-package
go test -race -count=1 -timeout 3m ./internal/app \
  -run '^(TestCLIConfiguredGrace.*|TestCLIQuiesceTimeoutRecoversNativeRetryBeforeFinalStop|TestCLIFirehoseS3Credentials)$'
Real native attempts prove a third retry beyond30s after SIGTERM with original
request/payload, active overlap and SDK/JWKS callbacks; separate retained tests
prove failed final timeout metadata and recoverable quiesce before final stop.
Firehose uses actual SDK ingestion/default/custom/session-token signed PUTs.

Publication found a test-only Python readiness/exit-observation issue in initial
CI39726678581. Commitb457954 atomically publishes its marker and bounds actual
descendant checks, preserving ownership/terminal assertions. Immediate assertion
probe100x passed; no production leak was established. Focused race24.966s and
Lambda vet passed, followed by the full green source CI above.

Raw /tmp/eventbus-v0.19.0-* and /tmp/eventbus-v019-* logs follow existing24h expiry,
no pins. Inspected failed scratch removed. No retained task resources beyond
expiring evidence, or live-stack/default-state changes/dependencies. Laptop Plans
untouched. Publication and cleanup complete.
