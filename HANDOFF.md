# Handoff

Completed ticket triage and a bounded ownership/duplication cleanup on SSD main.
The ten open GitHub tickets (#2–#11) are AWS core gaps, with four P1/six P2 and
six crosscutting labels. docs/ISSUE-TRIAGE.md records owners, dependencies and
acceptance. No feature ticket is closed or claimed implemented by this cleanup.

Changes and owners:
- devcapture owns shared durable JSONL/file mechanics; SES/SNS retain capture
  schemas, timestamps and acceptance/fanout ordering. Borrowed writers stay open.
- localexec owns process groups, cancellation, descendant/pipe cleanup and bounded
  output. Lambda/triggers/consumers retain execution and result policy. Consumers
  now stop descendants and refuse overflow before trusting a batch response.
- awsprotocol owns target extraction and bounded JSON reading. SSM/Secrets/Firehose
  keep their existing 1 MiB budgets; complete oversized prefixes now fail.
- Cognito shared challenge continuation, lifecycle lookup and password policy have
  proper owners. Seed persistence is dev_seed_store.go; unguarded test mutation
  helpers moved outside production builds. Existing behavior remains supported.
- Named Cognito dev provisioning/seed and gateway/Lambda YAML adapters demarcate
  local controls. Gateway copies resolved configuration with typed core code;
  request behavior/cache and caller/default isolation have regression coverage.
- Application-root discovery moved from consumer to app. Dead Query parsing code
  and duplicated capture/process/output implementations were removed.

Two fresh independent reviews found no blockers; a focused fixture re-review also
passed. The main agent reviewed the combined source, tests and ownership map.
Two pre-existing follow-ups are recorded in docs/BACKLOG.md: consolidate private
Go/Python consumer execution handling and replace Cognito's raw DB() test seam.
Native protocol/budget differences and SSM/Firehose state gaps remain core work.

Verification on SSD, through ssd-dev operation --purpose test:
- go test -race ./...: every package except consumer passed in the initial run.
  Consumer's new Python fixture lacked SSD's managed uv ownership environment.
- Final go test -race ./internal/consumer passed (9.460s), including a real Python
  timeout/start marker and real Python/Go descendant cleanup. The fixture supplies
  an explicit existing-owner environment; production inheritance stays unchanged.
  Together these runs cover every Go package after the production changes.
- go vet ./... passed. Gofmt and git diff --check passed.
- GOOS=darwin/windows CGO_ENABLED=0 go test -exec=true for localexec, lambda,
  cognitotrigger and consumer passed: compilation only, not target execution.
- SDK and owned RustFS integration lanes were not rerun for this cleanup;
  no destination/storage implementation was changed.

Logs: /tmp/eventbus-ownership-final-race-20261007.log,
/tmp/eventbus-ownership-consumer-final-race-20261007.log,
/tmp/eventbus-ownership-final-vet-20261007.log and the darwin/windows-compile logs.
No task children/worktrees or build/probe artifacts remain. Three failed nested
uv allocations were inspected: quiescent, unpinned and subject to existing SSD GC
(unverified aging, then failure expiry). Successful fixtures reuse their parent
owner. Do not rewrite receipts or retain temporary test resources indefinitely.

Published service v0.4.0 remains b60caa1; gateway v0.3.0 and Plans pins unchanged.
No dependencies, external contract break, database migration, live stack restart
or developer data reset. Current architecture/test map: docs/ARCHITECTURE.md.

Next: #3 and #9 independently; then #10 and incremental #11. Per-client validity
belongs to Cognito core across auth paths. #7 enables #8; #5/#6 formats remain
independent; #4 follows secret stages; #2 uses SNS-owned delivery ports and
Firehose-owned buffering/processing/destination behavior. SES management is low
priority. Preserve unsupported native behavior as an explicit core gap.
