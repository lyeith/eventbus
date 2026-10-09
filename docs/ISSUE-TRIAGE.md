# Issue triage: AWS core and development harness

Reviewed 7 October 2026 against v0.4.0/main and current AWS documentation.
The original ten tickets describe AWS capability or correctness gaps. Their local
fixture, execution and evidence adapters are still required, with separate owners.
The bounded scopes below are implemented and accepted on main, including the
recorded ownership-review findings, published in v0.5.0. HANDOFF.md records
verification; the service guides state exact limits.

P1 fixes misleading successful results or establishes the configuration needed
to verify them. P2 adds documented missing capabilities. P2 work remains needed;
it precedes the low-priority SES management backlog.

| Ticket | Priority | AWS core owner and scope | Dependency / coordination |
| --- | --- | --- | --- |
| [#3](https://github.com/lyeith/eventbus/issues/3) Secret stages | P1 | `secrets`: immutable values, label ownership, selectors, idempotency, metadata-only create and Describe | First; foundation for #4 |
| [#9](https://github.com/lyeith/eventbus/issues/9) Cognito region/readback | P1 | `cognito`: configured region/account, persisted names/configuration/timestamps, Describe and parent checks | Configuration foundation for #10/#11 |
| [#10](https://github.com/lyeith/eventbus/issues/10) Per-client token validity | P1 | `cognito`: persisted native durations/units, selected-client token policy and refresh expiration | After #9; all auth paths and seed/store migration |
| [#11](https://github.com/lyeith/eventbus/issues/11) Ignored Cognito settings | P1 | `cognito`: explicit capability validation, schema, attribute permissions, signup/verification/recovery policy | Reject unsupported selected settings first; accepted state builds on #9/#10 |
| [#7](https://github.com/lyeith/eventbus/issues/7) Lambda Event | P2 | `lambda`: asynchronous admission/execution, queue bounds, resolution, retries and shutdown | Before #8; coordinate executor ownership with #4 |
| [#5](https://github.com/lyeith/eventbus/issues/5) HTTP API authorizers | P2 | `gateway`: native 2.0 event, identities, policy/context and cache semantics | Coordinate representation with #6; formats remain independent |
| [#6](https://github.com/lyeith/eventbus/issues/6) AWS_PROXY | P2 | `gateway`: 1.0/2.0 application events and HTTP result decoding | Existing Lambda Invoke; no requirement for #7 Event |
| [#2](https://github.com/lyeith/eventbus/issues/2) SNS → Firehose → S3 | P2 | `messaging`: subscription dispatch; `firehose`: processing, partition/buffer, compression, destination/retry | SNS-owned delivery port; app composes Firehose implementation |
| [#4](https://github.com/lyeith/eventbus/issues/4) Secret rotation | P2 | `secrets`: native metadata, pending token, stage transitions, four-step asynchronous rotation workflow | After #3; reuse Lambda execution/lifecycle rather than another runner |
| [#8](https://github.com/lyeith/eventbus/issues/8) Scheduler | P2 | New `scheduler`: REST JSON, schedule state/idempotency, time and target admission | After #7; typed asynchronous Lambda port |

Implementation dependencies: #3 and #9 independently; then #10 and incremental #11.
The #11 unsupported-setting guard can start earlier. In the capability lane,
#7 enables #8; #5/#6 coordinate without coupling their format choices; #2 is
independent; #4 follows stage correctness. Crosscutting labels identify multiple
service owners or a persisted policy that affects several authentication flows.

## Readiness follow-up

[#12](https://github.com/lyeith/eventbus/issues/12) is a development routing defect,
owned by gateway `dev_health.go`. `dev_health_path` keeps readiness separate from
application authorization/integration semantics, defaults to `/health`, validates
concrete route collisions and lets protected application `/health` retain its
original native event paths. Greedy routes and `$default` reserve only the chosen
readiness GET endpoint. Shipped in v0.5.1; native gateway management parity
is outside scope. [Gateway guide](GATEWAY.md) states the recipe and collision rules.

## Delivery and route follow-ups

| Ticket | Owner | Implemented bounded contract |
| --- | --- | --- |
| [#13](https://github.com/lyeith/eventbus/issues/13) SNS Lambda | `messaging` filtering/event/admission; `lambda` execution; `app` composes | Native Records, registered aliases, bounded async admission, correlated evidence and joined shutdown |
| [#14](https://github.com/lyeith/eventbus/issues/14) SQS mappings | `eventsource` lifecycle; SQS lease/redrive; Lambda completion | Create/Get/Delete, bound queue identity, FIFO and completion-only current-receipt ack; #16 extends batches/concurrency |
| [#15](https://github.com/lyeith/eventbus/issues/15) Docs slash | `gateway` native route matching | Explicit literal trailing slash, full-match precedence and original event paths; application owns public ingress readiness guard |
| [#16](https://github.com/lyeith/eventbus/issues/16) SQS batches/concurrency | `eventsource` worker bound; SQS lease/projection budget; Lambda completion | BatchSize 1–10/default 10, native per-mapping ceiling, pre-lease 6 MiB event budget and whole-batch settlement |

These are AWS core gaps. Registration, local capacity/evidence and dev recipe
consumers remain named harness adapters. See [Messaging](MESSAGING.md),
[Mappings](EVENT-SOURCES.md) and [Gateway](GATEWAY.md) for exact limits.

## Handler-issued SQS deletion: native core

[#20](https://github.com/lyeith/eventbus/issues/20) fixes a false mapping ACK failure
after a handler successfully deletes its original receipt. Messaging owns bounded
receipt-settlement proof; app/SDK adapters delegate its native ACK operation.
`eventsource` still waits for successful whole-batch execution and actual child join.
Stale HTTP delete success alone never proves settlement; native wire rules remain
unchanged. [Messaging](MESSAGING.md#native-mapping-receipt-settlement) states the
contract. Native SDK verification passed; released in [v0.8.0](https://github.com/lyeith/eventbus/releases/tag/v0.8.0);
[HANDOFF](../HANDOFF.md) records verification.

## Native delivery evidence and private diagnostics

[#21](https://github.com/lyeith/eventbus/issues/21) adds optional correlated SQS
delivery evidence: `eventsource` owns actual invocation lineage and post-join
terminal records, messaging owns receipt classification, and app binds the
observed native runner. [#22](https://github.com/lyeith/eventbus/issues/22) adds
Lambda-owned private attempt diagnostics. These harness sinks preserve native
events, wire results, retry policy and redacted async metadata. Runtime success
does not prove business success; uncertainty never becomes a completion attestation.
See [SQS evidence](EVENT-SOURCES.md#correlated-delivery-evidence) and
[private diagnostics](LAMBDA.md#private-invocation-diagnostics). Native SDK
acceptance passed; released in [v0.8.0](https://github.com/lyeith/eventbus/releases/tag/v0.8.0); [HANDOFF](../HANDOFF.md) records final
verification.

## Raw SES configuration-set selection: native core

[#23](https://github.com/lyeith/eventbus/issues/23) belongs to SES selection and
validation: header-only `SendRawEmail`, API-parameter precedence and effective
normalized capture, preserving submitted MIME bytes and existing typed errors.
[SES](SES.md#raw-configuration-set-selection) states the contract. Final protocol
proof passed; released in [v0.8.0](https://github.com/lyeith/eventbus/releases/tag/v0.8.0); [HANDOFF](../HANDOFF.md) records acceptance.

## Retained-suite recovery: development harness

[#17](https://github.com/lyeith/eventbus/issues/17) established the opt-in,
process-exclusive ownership barrier. [#18](https://github.com/lyeith/eventbus/issues/18)
extends it to mapped-message custody, Scheduler, Cognito runners, Firehose,
gateway root leases and declared native cleanup functions. `devquiescence` owns
fence/held/resume and leases; optional `devactivity` ports leave native state,
execution and settlement with services. App composes listeners/cleanup declarations;
gateway acquires root leases before auth/body/Invoke. Applications own exact
authenticated cleanup and fixture assertions, followed by explicit re-quiesce.
These are development controls; #16's native batch contract remains separate.
[Retained owner](RETAINED-OWNER.md) states endpoints and remaining refusals;
[HANDOFF](../HANDOFF.md) records current verification.

## Released: trusted gateway HTTP continuations

[#24](https://github.com/lyeith/eventbus/issues/24) is a development retained-owner
admission gap, not an AWS capability gap. Gateway owns optional private loopback
ingress through native routes/auth; the coordinator owns generation-bound,
nonexpiring continuation leases and command lifetime preserves accepted callback
chains. Applications configure existing HTTP/JWKS endpoints and own business
assertions. See [gateway continuations](GATEWAY.md#trusted-http-continuations).
Final regression acceptance passed; released in [v0.9.0](https://github.com/lyeith/eventbus/releases/tag/v0.9.0); [HANDOFF](../HANDOFF.md)
records current verification.

## Completed: cancellation cause and Python wait diagnostics

[#25](https://github.com/lyeith/eventbus/issues/25) separates the actual private
termination cause/elapsed time from the preserved legacy native timeout response.
[#26](https://github.com/lyeith/eventbus/issues/26) adds opt-in, bounded Python
thread/task snapshots to the same Lambda-owned private sink. These development
diagnostics leave native deadlines, results, retries and joins unchanged; a
snapshot cannot identify an unproven business cause or attest completion. See
[private diagnostics](LAMBDA.md#private-invocation-diagnostics) and
[Python snapshots](LAMBDA.md#python-wait-snapshots). Final regression acceptance
passed and shipped in [v0.10.0](https://github.com/lyeith/eventbus/releases/tag/v0.10.0);
[HANDOFF](../HANDOFF.md) records verification and release status.

## Completed: CLI startup argument guard

[#27](https://github.com/lyeith/eventbus/issues/27) is a low-priority core startup
guard. Broker `internal/app/config.go` and gateway `cmd/gateway/config.go` reject unexpected
positional arguments, such as `version`, with a nonzero exit before opening stores,
captures, listeners or invocation processes. Parser/startup regressions and focused
app/gateway race/vet and packaged Linux/macOS proofs passed; released in
[v0.11.1](https://github.com/lyeith/eventbus/releases/tag/v0.11.1).
[HANDOFF](../HANDOFF.md) records verification and release status.

## Completed: Lambda Init and Invoke accounting

[#28](https://github.com/lyeith/eventbus/issues/28) is a Lambda execution-core
timeout gap. Lambda owns managed readiness/provided Runtime API readiness,
bounded initial Init and configured Invoke deadlines, with one joined Init
fallback sharing its configured budget across Init and Invoke. Command execution
keeps its whole-process timeout. Private phase evidence is a harness adapter;
neither it nor deadline separation proves an application business-chain result.
See [phase accounting](LAMBDA.md#init-and-invoke-budgets).
Final race/SDK and packaged Linux/macOS verification passed; released in
[v0.11.1](https://github.com/lyeith/eventbus/releases/tag/v0.11.1). Shared `localexec`
also reconciles Darwin zombie-group EPERM through bounded absence probes while
persistent ownership errors still refuse completion. The separate
[performance review](PERFORMANCE-REVIEW.md) records measured startup/capture costs
and unmeasured follow-ups. [HANDOFF](../HANDOFF.md) records acceptance and cleanup.

## Core versus harness code

| Concern | Owner |
| --- | --- |
| Native request fields, defaults, validation, resource identity, stored configuration, state transitions and AWS events/errors | Service core under `internal/<service>` |
| Per-Cognito-app-client token validity and read/write permissions | Cognito core; every authentication path selects the same persisted client policy |
| Authorizer payload format versus integration payload format | Independent gateway core configuration, even when provisioned through YAML |
| Async acceptance/retries, rotation steps, scheduled target dispatch, SNS Firehose delivery | Respective AWS service core with consumer-owned typed ports |
| YAML loading, deterministic fixture IDs/aliases, fixture profiles, local executable/endpoint selection | Explicit `dev_*.go` adapters; composition in `internal/app` |
| Captures, agent wait/reset/inspection controls, fault injection and accelerated clocks | Development harness adapters; HTTP controls use a distinct namespace |
| Retained-suite fence, source leases, declared cleanup and resume | `devquiescence`/`devactivity`; app composes profile, service seams retain custody/lifetimes, gateway leases roots/trusted continuations, consuming apps own cleanup effects |
| Consumer polling/process recipes, frontend hosting and private application mappings | Existing harness owners; never substitute for missing AWS operations |

Development adapters translate into validated core configuration and operations.
Keep real AWS fields in core even when a fixture supplies them. Cognito's
`DeveloperOnlyAttribute` is an AWS feature, despite its name. Native unsupported
scheduling, IAM or processing behavior remains an explicit core capability limit;
it must not be relabeled as a development feature.

Existing fixture loading is now named `cognito/dev_seed.go`,
`gateway/dev_config.go` and `lambda/dev_config.go`. Cognito's legacy flat
`PoolId`, `ClientId` and `PasswordPolicy` extensions are declared in
`dev_provisioning.go`. Existing wire/seed behavior remains available through the explicit
`legacy-fixtures` profile; native admission rejects fixture extensions. Native
client policy is independent of process-level legacy defaults.
Shared capture and OS child mechanics have concrete owners in `devcapture` and
`localexec`; no global client abstraction is introduced. See [ownership](ARCHITECTURE.md).

## Contract checks for implementation

- **#3/#4:** absent AWSPREVIOUS must fail, AWSPENDING must not promote current,
  both selectors must agree, same-token conflicting values must fail, and
  metadata-only/pending-token entries must be distinct from stored values.
  Snapshot state safely; never run a rotation handler while holding a store lock.
  RotateSecret acceptance is asynchronous; terminal failures need redacted evidence.
- **#9/#10:** persist native values, not fabricated Describe fields. Native client
  defaults are independent one-hour access/ID tokens and 30-day refresh tokens.
  Explicit persisted client settings win. Preserve legacy fixture/store behavior
  through an identified profile, not process flags overriding native clients.
  Validate normalized duration limits; refresh must not renew its original expiry.
- **#11:** first reject selected unsupported settings before mutation. Then
  implement schema/attribute permissions and separately the lifecycle workflows.
  Legacy digit-only SOFTWARE_TOKEN_MFA/SMS_MFA acceptance needs an explicit
  development profile; custom auth already requires enrolled TOTP.
  AdminCreateUser may omit required attributes; NEW_PASSWORD_REQUIRED must collect
  them. Client permissions apply to client operations/ID claims, with distinct
  administrator/trigger semantics. Readback alone does not prove enforcement.
- **#5/#6:** support native `$default` stage; validate HTTP API identities even at
  TTL=0. Preserve format-specific context, including supported structured v2
  context, without changing REST contracts. Test all authorizer/integration format
  combinations, repeated values/cookies, binary data, aliases and function errors.
- **#7/#8:** Lambda Event returns 202/empty on admission, with a current 1 MiB
  payload limit. Handler failure/retries belong to Lambda. Scheduler retries
  target admission failures and deletes after its final target API invocation,
  not after handler business completion. Scheduling has 60-second AWS precision;
  accelerated/exact-second execution is an explicit harness policy. Create/Get/
  Delete one-time schedules are a subset of Scheduler's 12 operations.
- **#2:** Firehose owns GZIP, partitioned buffering, output/error prefixes and
  destination persistence. Apply SNS filters/raw behavior before its delivery port.
  Reuse retained-buffer/lifecycle ownership; ambiguous S3 failures need a stable
  retry identity. AWS inline extraction uses jq 1.6. The user approved pinned Go jq instead of an
  executable dependency; the tested semantic profile and upstream differences
  are documented rather than claiming exact jq 1.6 equivalence.

Each implementation needs lowest-level state/validation tests plus real SDK
request/readback proof. Cross-service work also needs actual handler/destination,
failure/recovery, instance isolation and teardown evidence. Use controlled clocks
for boundaries. Combined compatibility results are recorded in HANDOFF.md; service guides describe current scope and explicit limits.

AWS references: [Cognito clients](https://docs.aws.amazon.com/cognito-user-identity-pools/latest/APIReference/API_CreateUserPoolClient.html),
[attributes](https://docs.aws.amazon.com/cognito/latest/developerguide/user-pool-settings-attributes.html),
[secret values](https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html),
[rotation](https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_RotateSecret.html),
[HTTP authorizers](https://docs.aws.amazon.com/apigateway/latest/developerguide/http-api-lambda-authorizer.html),
[Lambda proxy formats](https://docs.aws.amazon.com/apigateway/latest/developerguide/http-api-develop-integrations-lambda.html),
[Invoke](https://docs.aws.amazon.com/lambda/latest/api/API_Invoke.html),
[Scheduler](https://docs.aws.amazon.com/scheduler/latest/APIReference/API_CreateSchedule.html),
[Firehose partitioning](https://docs.aws.amazon.com/firehose/latest/dev/dynamic-partitioning-partitioning-keys.html).

## Per-launch process attribution and launcher overhead

[#29](https://github.com/lyeith/eventbus/issues/29) is a development evidence
contract owned by Lambda's private diagnostics adapter. Native launch facts
remain in execution core; the adapter bounds/projects per-launch errors,
ownership and frozen cancellation causes. Top process detail is the final
launch; ownership stays cumulative. Source race/SDK/vet acceptance passed;
released in v0.11.2 after packaged Linux/macOS fallback verification.

Measured startup overhead belongs to SSD tooling, fixed in
[796c1dd](https://github.com/lyeith/ssd-dev-tools/commit/796c1dd), with exact owner
reuse and batched fresh storage proofs. [Performance review](PERFORMANCE-REVIEW.md#launcher-fix-and-follow-up-8-october-2026)
records measurements and the unchanged baseline tooling-suite blockers.


## Runtime and native event increments after v0.11.4

- [#30](https://github.com/lyeith/eventbus/issues/30): warm Python/Node worker
  lifetime, admission and per-request context belong to Lambda execution core.
  Local mode, bounded worker count and explicit reload controls belong in named
  development adapters. Preserve fresh-process mode; verify generation changes,
  deadlines, failure retirement and actual child joins before runtime reuse.
- [#31](https://github.com/lyeith/eventbus/issues/31): configuration sets,
  destination CRUD, native event fields/filtering and MessageId correlation are
  SES core. SES owns a narrow SNS publication port; app injects the existing SNS
  broker/delivery owner. Explicit local Open/Bounce outcomes belong in a dev
  adapter using those same destinations; capture cannot imply actual delivery
  or opening. Application effects/assertions stay in the consuming repository.

- [#32](https://github.com/lyeith/eventbus/issues/32): registered GetFunction
  metadata and native qualifier errors belong to Lambda core; the public subset
  omits local paths/environment/code. LocalStack owns S3 generation and normal
  GetFunction/DryRun destination validation, with app/test-owned transport.
  Actual original S3 events enter the existing native Event/retry owner.

These three increments are implemented in current source; final combined
verification and release are recorded in STATE/HANDOFF. They are absent from
v0.11.4. [S3 notifications](S3-NOTIFICATIONS.md), [Lambda](LAMBDA.md) and
[SES](SES.md) state their contracts. The earlier bounded audit remains at
[current measurements](PERFORMANCE-AUDIT.md#follow-up-fixes-and-measurements).
The earlier SSD launcher fix covered the no-project path. Project-mode launches
still repeat workspace/digest/ownership work; this belongs to ssd-dev-tools.
The same fixed consumer workload measured 1.20s managed versus 0.25s native-uv
for attribution, with unchanged interpreter/environment identity and joins.
Do not replace the ownership wrapper as a production workaround.
