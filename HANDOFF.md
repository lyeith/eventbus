# Handoff

Completed all ten triaged ticket scopes #2–#11 plus ownership-review findings on
SSD main, after cleanup baseline 063e6966. Source is committed/pushed through
c005666. User now requested publication: prepare v0.5.0 service/gateway binaries
with the existing build script. Plans pins and live stacks remain unchanged.

Owners: Cognito persists metadata/schema/permissions/token policy and workflows;
Secrets owns immutable versions/stages/rotation; Lambda owns execution/Event queue;
gateway owns independent authorizer/integration formats; SNS owns its Firehose
port and Firehose owns processing/buffers/S3 retries; Scheduler owns time/state
and target admission. App adapts typed Lambda ports and owns staged shutdown.
Common capture/process/wire mechanics remain devcapture/localexec/awsprotocol.
Consumer Go/Python execution policy consolidated; production Cognito DB() removed.

Quiesce stops producers while the AWS listener/sync invocation stay usable by
accepted rotation/Event SDK handlers. All independent owners drain even after a
peer fails. HTTP then drains, final owners close, and failed prerequisite barriers
retain stores/captures. Deadline aborts cancel/join children and retain errors.
Reviews fixed transactional NPR immutable-first-value/contact verification,
retired Firehose references/ARN limits and final admission/cancellation races.

Approved: pinned gojq v0.12.19 (+timefmt-go v0.1.8), test-only JS Scheduler
SDK 3.1146.0. Frozen npm ci and go mod tidy completed. No jq binary/framework.
Go jq implements the documented tested profile, not literal jq 1.6 equivalence.

Verification through ssd-dev on Linux:
- go test -race -count=1 -p 3 ./...: every package except Cognito passed.
  Cognito (355.267s) failed exactly two old legacy policy input fixtures; all other
  cases passed. Those fixtures lacked explicit PoolId despite legacy aliases.
  Fixture-only repair, then targeted race of policy/default/precedence and native
  unsupported-setting refusal passed (2.92s). No production change after full run;
  these runs together cover every package/test in the final source.
- Full frozen SDK race suite passed (133.224s) (tests/sdk, sdksmoke, timeout 8m):
  existing Cognito/SRP/custom/SES/messaging plus native provisioning/restart,
  real Python Secrets rotation, Lambda Event and real JS Scheduler/Node alias.
- go vet ./... passed. All affected Go files are gofmt-clean; diffcheck and local
  documentation links passed. Focused fixes also have colocated race checks.
- Native Go SNS/Firehose SDK→owned RustFS pipeline race suite passed, including
  filters/raw/envelope, partitions/GZIP/errors, failed delivery recovery and drain.
  Later backing-reference/ARN guard fixes passed focused Firehose race/vet.
- Darwin arm64 and Windows amd64 CGO-free runner/Scheduler/app/server tests
  compile with -exec=true. Target-platform execution was not performed.

Evidence under /tmp on SSD, governed by existing finite retention:
 eventbus-plate-final-{race,sdk,vet}-20261007.log
 eventbus-cognito-policy-fixture-race.log
 eventbus-firehose-agent-verified.txt / eventbus-firehose-ownership-verified.txt
 eventbus-background-drain-race-20261007.log
 eventbus-plate-{darwin,windows}-compile-20261007.log

All fixtures owned listeners/state/processes; no live stack reset. RustFS 40955
and its task volume are gone; staging/probes/bytecode removed, no worktrees.
Canonical frozen SDK environments remain reusable. Failed managed allocations
are quiescent/unpinned and expire through SSD GC; no receipts were rewritten.
STATE.md and service guides record current truth and explicit unsupported limits.

Next: verify native release binaries and checksums, publish v0.5.0 and clean
owned release scratch; then consuming-app acceptance. Low-priority SES
management. Do not imply all management APIs, production IAM/KMS or durable
async/schedule recovery are present. Preserve developer identity SQLite data.
