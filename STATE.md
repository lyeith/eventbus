# EventBus state

Canonical SSD /home/spite/Projects/eventbus, main; standalone public MIT AWS
emulator and agent harness. Plans/application data remains untouched.
Current public release: https://github.com/lyeith/eventbus/releases/tag/v0.11.4
The v0.12.0 candidate implements all currently open tickets (#30, #31, #32).
Source owners are committed separately; release builds/publication remain.

- #30: opt-in bounded warm Python/Node workers; default fresh execution remains.
  Lambda owns immutable generations, native Runtime API, contexts/deadlines/logs,
  global capacity, worker leases and joined retirement. Explicit local recipe
  reload is in app; completion_scope distinguishes invocation/process completion.
- #31: SES owns six v1 configuration-set/destination operations, accepted Send
  events and bounded mail correlation. App connects SES to native SNS; explicit
  local Open/Bounce belongs in the named dev adapter. Capture stays unchanged.
- #32: registered GetFunction exposes documented Configuration metadata only.
  Actual validated LocalStack S3 notifications invoke native EventBus handlers;
  no EventBus object store or synthetic upload event was added.

PASS: affected Lambda/app/dispatcher/architecture race suites; final warm race
9.077s Linux / 5.206s macOS arm64; final app32.762s / eventsource2.301s races.
Final three real managed fallback regressions17.145s; unknown-scope ACK refusal
1.046s. Earlier Node fallback fixture failed under load before module markers;
only deliberate test timing/observed-budget assertions changed, not deadlines.
SES core race13.721s, acceptance-cancellation1.018s, final bulk/concurrent-update
1.025s; unchanged SES/SNS/Lambda SDK proof2.081s.
Full SDK race277.526s includes actual warm SNS/SQS delivery/ACK/quiescence.
Actual LocalStack3.8.1 S3 race13.134s covers normal validation, uploads/copy/
multipart/version/filtering, retries and pending graceful drain.
Tagged all-package vet and five Python fixture contracts passed.
Independent integrated ownership/contract/lifecycle review accepted.

Eight real SDK sends per runtime/mode, including first call:
Python fresh/warm wall medians216.078/8.565ms; Node272.591/12.150ms.
Warm idle RSS median46,610,432/97,937,408 bytes; Node grows during eight samples,
so no long-run memory plateau/leak claim. Docs/LAMBDA.md records distributions.

Next: clean eight-target release build, packaged Linux amd64/macOS arm64 proof,
push/CI, publish v0.12.0 and verify uploaded checksums, then final cleanup.
One serial SSD test/build lane. Owned LocalStack containers/image/fixtures removed.
macOS source scratch retained only for packaged verification; failed receipts
779b0793 / 20a07398 / 4eaefff6 quiescent, unpinned, existing 24h expiry.
No dependencies/default DB/live stack/temporary worktrees touched.
SSD project-mode launcher optimization remains a separate host-tooling follow-up.
