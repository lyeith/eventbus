# Handoff

Canonical SSD /home/spite/Projects/eventbus, main. #40 source is complete.
Latest published binaries remain v0.16.0; no new publication requested this pass.
https://github.com/lyeith/eventbus/releases/tag/v0.16.0

#40 fixes missing native rejection facts in opt-in private Lambda diagnostics:
- service.go retains the actual initial warm.acquire InvokeError in the joined
  invocation result and freezes context/native completion at that decision.
- dev_diagnostics.go alone projects admission_error with allowlisted native
  code/http_status. Current captured pair is TooManyRequestsException/429 for
  exhausted execution concurrency, including constrained fresh execution.
- Native Invoke/gateway responses, public errors, concurrency budgets and
  asynchronous retry behavior remain unchanged. No error Message/text or
  event/environment/auth values are added to diagnostics.
- Caller/observer/ownership and launch errors remain separately classified.
  A callback-supplied InvokeError cannot create a native admission fact.
  Already-launched Init fallback does not become not_started. No prevalidation
  diagnostic row is added, and successful/later accepted attempts omit the field.
- Existing optional Python-stack evidence can hold the lifetime after rejection;
  late caller cancellation no longer changes its already-decided diagnostic cause.
- LAMBDA/GATEWAY/agent workflow/architecture and issue triage document this scope.

Verification:
- app/lambda_admission_diagnostics_test.go reproduced missing field on baseline.
  Real provided Runtime API handler holds cap1; native Invoke429/Retry-After,
  AWS_PROXY and REQUEST-authorizer500 retain their exact response contracts.
  Actual native IDs correlate three sanitized not_started records, absent
  handler starts, and subsequent successful real handler/authorizer execution.
- Focused consumer race1.258s; focused Lambda owner/privacy race1.054s.
- Owner tests cover bounded allowlisted fields with large arbitrary message and
  unknown code, successful omission, observer errors/spoofing, cancellation,
  launch failure and real native pool rejection before a gated optional append.
- Full affected race: Lambda140.885s, app45.990s, gateway18.347s,
  server2.209s, ownership1.044s. All-tag vet and diff checks passed.
- Independent source/consumer/documentation review found no remaining correction.

Evidence: /tmp/eventbus-ticket40-affected-race.log, admission-owner.log,
admission-integration-red-contract.log, admission-integration-green.log and vet.log
(all names share eventbus-ticket40-). Logs and failed scratch retain existing24h
expiry with no pins; inspected successful owners are quiescent, scratch removed.
No task processes/worktrees/branches/extra environments or live-stack changes.
Unrelated laptop Plans changes preserved. No dependencies added.
