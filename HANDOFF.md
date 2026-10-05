# Handoff

SES sending is committed as 6602261 in the standalone emulator. The repository
is public at https://github.com/lyeith/eventbus with the MIT license (2a1fb18),
recognized by GitHub. No new binary release; v0.1.0 binaries predate SES.

- V1: SendEmail, SendRawEmail, SendTemplatedEmail, SendBulkTemplatedEmail,
  SendCustomVerificationEmail and SendBounce.
- V2: SendEmail (Simple/Raw/Template), SendBulkEmail and
  SendCustomVerificationEmail.
- Capture: schema-versioned JSONL with original request, normalized emails,
  request/message IDs and ordered API outcomes. Binary fields stay base64.
- Successful file capture is appended and synced before returning success.
  Writer failure is terminal; HTTP drain precedes capture closure.
- Current optional fields are preserved. Fixtures provide templates,
  configuration sets, identities and original messages for bounce testing.
- Other SES APIs are low priority in docs/BACKLOG.md. Rich template rendering
  and SMTP remain separate follow-ups.

Verified on SSD, 2026-10-06:
- Focused SES race/SDK lane: 30 captured requests, all nine operations.
- Full Go/race/SDK regression lane: passed, 71.366s.
- go vet -tags sdksmoke ./...: passed.
- Python smoke contracts: 5 tests passed.
- Native binary proof: v1/v2 live capture, restart append, stdout default,
  SIGTERM shutdown and incomplete-file startup refusal passed.
- git diff --check: passed.

Test logs are /tmp/eventbus-ses-*-20261006.log under existing 24-hour retention.
SDK models are pinned, reduced official botocore fixtures with license/notice.
Local SDK tests select SigV4 so EndpointId probes need no AWS CRT dependency.
No dependencies or lockfiles changed. Publication review found no secrets,
private runtime data or live app configuration; AWS fixture notices are retained.

Documentation has a short README, a dedicated agent workflow and an SES
record reference. A fresh-agent static review checked SDK/auth setup, consumers,
capture parsing and shutdown; corrections explain DLQ provisioning, explicit
consumer environment and debug diagnostics. Local links were verified.
Positioning explicitly states standalone LocalStack supplementation and agent
verification/evaluation. Service routing and event-pipeline ownership are
documented; coexistence was reviewed statically, not exercised against LocalStack.

The task's temporary virtualenv, probes and CLI binary have been removed.
Plans' running stack and v0.1.0 pin were left unchanged.
