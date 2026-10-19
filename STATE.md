# EventBus state

Canonical SSD /home/spite/Projects/eventbus, main; public MIT AWS emulator
and agent development harness. #40 source is complete; binaries remain v0.16.0.
https://github.com/lyeith/eventbus/releases/tag/v0.16.0
No new binary publication was requested in this ticket pass.

#40 private native admission diagnostics:
- The existing native execution owner retains its initial prelaunch rejection
  and freezes that decision before optional stack/evidence joins.
- Opt-in eventbus.lambda.invocation-diagnostic.v1 adds admission_error:
  {"code":"TooManyRequestsException","http_status":429} on native not_started
  execution-capacity rejections. Dev diagnostics owns the allowlisted projection.
- No admission Message, arbitrary code/text, payload, environment or authorization
  data is copied. No private sink remains the default; no new config/dependency.
- Caller cancellation, ownership/observer and launch errors do not become native
  admission facts. Already-launched Init fallback failures keep their original
  classification. Later optional evidence/caller cancellation cannot relabel429.
- Native Invoke429/Retry-After and existing gateway integration/REQUEST-authorizer
  500 responses remain unchanged; configured concurrency limits stay enforced.
- Later accepted requests omit admission_error. Existing logs, evidence ownership,
  async retry policy and private Cognito boundaries remain unchanged.
- LAMBDA/GATEWAY/agent guides document correlation, field scope and limitations.

Verification:
- Real held Go provided handler cap1 reproduces baseline missing admission field.
  Focused app race1.258s verifies native429, both gateway500 paths, exact native
  IDs, no rejected handler start, sanitized records, recovery and omitted field.
- Focused Lambda race1.054s covers allowlist/privacy bounds, cancellation/
  observer spoofed InvokeError/ownership/launch negatives, and a deterministic
  optional Python-stack append hold followed by late cancellation after429.
- Full affected race passed: Lambda140.885s, app45.990s, gateway18.347s,
  server2.209s, architecture1.044s. All-tag vet and diff checks passed.
- Independent cause/ownership/privacy and final documentation review is clean.
  No new dependencies, supported AWS fields or gateway policies changed.

Owned tests/processes joined; successful inspected scratch removed automatically.
No worktrees, branches, extra environments or application/default-state changes.
Raw /tmp/eventbus-ticket40-* logs and unpinned failed scratch use existing24h
retention. Unrelated laptop Plans changes preserved.
