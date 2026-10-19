# Handoff

Canonical SSD /home/spite/Projects/eventbus, main. #42 source is complete.
User explicitly requested publication once done. Prepare v0.18.0 after source CI
and actual package proofs; current latest public release remains v0.17.0.
https://github.com/lyeith/eventbus/releases/tag/v0.17.0

#42:
- DevDiagnosticsConfig.Mode accepts all/failures; omitted defaults all.
  dev_diagnostics.go owns configuration and final joined-attempt retention.
- failures omits clean native successes, including caught/logged business errors,
  successful provided-runtime cleanup and successful internal Init fallback.
  It retains non-success/uncertain owned attempts, native admission rejection,
  failed async attempts before later success and injected completion loss/crash.
- Private Python publish keeps its existing bounded session.record in failures
  mode; terminal diagnostics flushes it only for retained outcomes, before the
  invocation row. All-mode live publication is unchanged. Collectors always join.
  Deferred append failure remains sticky, without fabricating healthy evidence.
- Native responses/Tail, queue/retry/ACK behavior, warm reuse, redacted metadata,
  private-file protection, output limits and no-sink default remain unchanged.
  No new dependency, execution policy, buffer owner or latency claim.
- Native errors before any owned invocation still create no diagnostic row.

Validation:
- Focused real Node/Python fresh/warm/provided runtime race3.640s: success/caught
  omission, logs/detail retention, same-ID async retry, timeout/cancel/Init/start
  failures, actual concurrency429 and ordinary all compatibility.
- Actual Python explicit snapshots with active file empty: success leaves zero
  rows of both schemas; failure/cancel appends stack then terminal. Sticky sink
  append fault preserves native outcome, waits joins and fails evidence/Close.
  Focused race4.000s; final mutex-only assertion cleanup included in full gate.
- Native SNS real CLI race4.740s: all keeps failed+successful attempts, failures
  keeps failed attempt only; original event/request/message IDs and five redacted
  async transitions preserved. Children/listener/CLI join.
- Full affected race: Lambda161.595s, app64.742s, gateway25.374s,
  eventsource1.604s, architecture1.051s. All-tag vet and diff checks pass.
- Config/selector tables passed1.053s; invalid modes create no output directory.
- Independent final owner/privacy/docs review is clean.

Release verification seam:
EVENTBUS_SMOKE_BINARY=/absolute/eventbus-package
go test -race -count=1 -timeout 2m ./internal/app \
  -run '^TestCLIFailureOnlyDiagnosticsPreserveNativeSNSRetries$'
This uses the same maintained native Go SNS/Runtime API proof, with owned paths
and no external stack. Test binary itself provides the real child handler.

Raw /tmp/eventbus-ticket42-* logs/failed expectation evidence retain existing24h
expiry, no pins. Successful inspected source tests quiescent/scratch removed.
No temporary worktrees/branches/new environments/live-stack changes/dependencies.
Laptop Plans unchanged. Remaining work: CI, build/checksum/package proofs,
publication and release-only STATE/HANDOFF update/cleanup.
