# SDK verification

Run from the repository root with Node >=20 installed:

```sh
uv sync --frozen
uv run --frozen python -m unittest discover -s tests/sdk/python -p 'test_*.py'
(cd tests/sdk/javascript && npm ci --ignore-scripts --no-audit --no-fund)
EVENTBUS_SMOKE_PYTHON="$PWD/.venv/bin/python" go test -race -count=1 -tags sdksmoke ./...
```

Python dependencies come from `uv.lock`. JavaScript uses the exact Cognito IDP,
SESv2 and Scheduler SDK v3 version `3.1146.0`, including the transitive npm lockfile. The tagged gateway framework proof also
pins test-only Express `5.2.1` and swagger-ui-express `5.0.1`.
`EVENTBUS_SMOKE_NODE` optionally selects an absolute Node executable; otherwise
the lane finds `node` on PATH. CI uses Node 22.

The Go fixtures run the complete HTTP dispatcher on owned loopback listeners,
SQLite databases and SES/SNS capture files. They do not use a running application
stack or reset developer state.

JavaScript runs two isolated lifecycle scenarios: usernames distinct from email,
`AdminGetUser`, forced password change, temporary/permanent passwords,
secret-hash authentication and refresh, enable/disable, paginated listing,
RESEND and delete/reinvite. Node crypto independently verifies signed JWTs
against JWKS and computes the client SRP proof.

The custom flow is `SRP_A → PASSWORD_VERIFIER → CUSTOM_CHALLENGE → tokens`.
Real Node Define/Create/Verify fixture handlers use AWS-shaped events; Create
sends its code through the real SESv2 SDK. The client reads that code from live
JSONL evidence. Typed SDK errors must reject wrong proofs/answers, forged,
replayed, cross-user, expired and disabled sessions, plus trigger failures.
Closing and reopening the same listener address, store, capture and runner must
preserve identity, refresh and a pending custom challenge.

Python SNS Lambda verifies real asynchronous alias execution, native events,
filtering, handler failures/timeouts, pressure, owner isolation and correlated evidence.
SQS mapping proofs cover native Create/Get/Delete, FIFO, completion acknowledgment,
visibility retry, DLQ counts and pending-child teardown. The batch lane proves
five-record events, two concurrent invocations, FIFO groups, default-ten batching,
whole-batch retry/DLQ and joined cancellation of multiple actual children. The tagged gateway proof
uses unchanged Express/Swagger middleware behind real Node Lambda integrations
for redirects, assets, original paths and protected/default-route boundaries.

`TestSQSLambdaManualAcknowledgePythonSDKSmoke` is the accepted #20 proof: native
SDK deletion plus durable SQLite effects, mixed manual/mapping settlement of
batch-five records, FIFO, stale/expired leases and joined teardown.
[Receipt settlement](../../docs/MESSAGING.md#native-mapping-receipt-settlement)
defines the native contract. Released in [v0.8.0](https://github.com/lyeith/eventbus/releases/tag/v0.8.0);
[HANDOFF](../../HANDOFF.md) records verification.

`TestNativeEvidencePythonSDKSmoke` passed for #21/#22 and shipped in [v0.8.0](https://github.com/lyeith/eventbus/releases/tag/v0.8.0).
An actual HTTP producer and native handlers prove exact message/request lineage,
joined terminals after manual deletion/blocked child work, visibility retries,
timeout, a second suite on the same mapping and foreign-message sentinels.
SNS cases retain exact private caught-error diagnostics while runtime success
coexists with failed business state; unhandled failure, retry, truncation and
normal completion are covered. [SQS evidence](../../docs/EVENT-SOURCES.md#correlated-delivery-evidence)
and [private diagnostics](../../docs/LAMBDA.md#private-invocation-diagnostics)
state the contracts; [HANDOFF](../../HANDOFF.md) records final verification.

Python messaging covers direct SQS sending, binary attributes/checksums, typed
errors, SNS raw fanout and locally captured mobile-push intents. Ordinary Go SDK
contract tests additionally cover all SQS/SNS operation families and dispatch.

Python covers boto3 admin operations, password/refresh with PyJWT/JWKS, client
secrets and pool/client management. SES covers all nine v1/v2 sending operations,
current optional fields, exact binary capture and ordered bulk results using
[the pinned official sending models](aws_models/README.md).
The #23 SES header protocol proof passed: native SDK header-only raw sending,
typed missing-set errors, API precedence, MIME casing/folding, absent selection
and exact submitted bytes. [SES selection](../../docs/SES.md#raw-configuration-set-selection)
states the contract; [HANDOFF](../../HANDOFF.md) records final acceptance.

Native Cognito provisioning proofs start with an empty eu-west-1 store, use
Python/Node Describe readback, client lifetimes/schema/permissions and live
verification capture, then restart and check persistence/instance isolation.
Secrets uses boto3 and an actual Python rotation Lambda. Lambda Event uses boto3
for 202/empty acceptance and handler/retry evidence. Scheduler uses the approved
pinned JS client and a real Node alias, checking exact input and completion
removal before handler business completion. All fixtures own state and children.

`TestRetainedOwnerPythonSDKBarrier` covers baseline SNS/Lambda retained recovery.
`TestRetainedStackNativeSDKRecovery` (`sdksmoke,integration`) adds native batch/retry
custody, gateway/Cognito callbacks, Firehose delivery, declared cleanup/descendants,
timeout recovery, sentinels and same-resource resume. It starts an already installed
RustFS binary with owned listeners/data; select its absolute path explicitly:

```sh
EVENTBUS_SMOKE_PYTHON="$PWD/.venv/bin/python" \
EVENTBUS_SMOKE_RUSTFS=/absolute/path/to/rustfs \
go test -race -count=1 -tags sdksmoke,integration ./tests/sdk \
  -run '^TestRetainedStackNativeSDKRecovery$'
```

[Retained owner](../../docs/RETAINED-OWNER.md) states the contract;
[HANDOFF](../../HANDOFF.md) records current verification. Consuming applications
own their actual authenticated cleanup and business assertions.

## Optional older Lambda SDK compatibility

The current lock uses boto3 `1.40.61`/botocore `1.40.76`.
`TestSQSMappingCurrentPythonSDKURI` checks its native CreateEventSourceMapping URI.
The optional `TestSQSMappingLegacyPythonSDKSmoke` reuses the unchanged batch script
with boto3/botocore `1.39.4`; provision the [fully pinned requirements](python/requirements-lambda-legacy.txt)
in a separate temporary environment, preserving the project's frozen environment:

```sh
legacy_sdk_dir=$(mktemp -d /tmp/eventbus-legacy-sdk.XXXXXX)
uv venv --python "$PWD/.venv/bin/python" "$legacy_sdk_dir"
uv pip install --python "$legacy_sdk_dir/bin/python" \
  -r tests/sdk/python/requirements-lambda-legacy.txt
EVENTBUS_SMOKE_PYTHON="$PWD/.venv/bin/python" \
EVENTBUS_SMOKE_PYTHON_LEGACY="$legacy_sdk_dir/bin/python" \
go test -race -count=1 -tags sdksmoke ./tests/sdk \
  -run 'TestSQSMappingCurrentPythonSDKURI|TestSQSMappingLegacyPythonSDKSmoke|TestSQSBatchPythonSDKSmoke'
```

Absent `EVENTBUS_SMOKE_PYTHON_LEGACY` skips only that legacy proof. When configured,
it must be an absolute existing executable with exact boto3/botocore `1.39.4`.
After tests and all users of the environment have joined, remove that exact
temporary environment with `rm -rf -- "$legacy_sdk_dir"`. See
[HANDOFF](../../HANDOFF.md) for verification status.

Each runner requires a successful exit and exactly one PASS marker. Missing
dependencies, false success, assertion failures and timeouts fail the lane.
Network waits are bounded; inherited AWS profiles, proxies and interpreter
preloads/options are excluded. Fixtures quiesce background SDK callers while listeners are available, then close
HTTP and resources; fixtures without background callbacks drain HTTP first.

Ordinary Go tests need neither Python nor SDK packages. These checks verify the
supported contracts; they do not claim complete AWS compatibility.

## Retained gateway continuations

`TestRetainedGatewayAuthenticatedNativeContinuations` verifies #24 with real
registered Python Lambdas, Cognito-issued tokens and cold callback JWKS. It
covers downstream HTTP during drain and shutdown, typed cleanup, public-root
fencing, wrong signatures/audiences and final native ownership joins.
`TestProductionDeclaredCleanupCallsAuthenticatedGatewayContinuation` under
`internal/app` separately proves actual configured cleanup declarations and
authenticated native gateway integration; the SDK fixture implements no app
cleanup allowlist.

```sh
EVENTBUS_SMOKE_PYTHON="$PWD/.venv/bin/python" \
go test -race -tags sdksmoke ./tests/sdk \
  -run '^TestRetainedGatewayAuthenticatedNativeContinuations$'
go test -race ./internal/app \
  -run '^TestProductionDeclaredCleanupCallsAuthenticatedGatewayContinuation$'
```

Both proofs passed; [Gateway](../../docs/GATEWAY.md#trusted-http-continuations)
documents endpoint configuration and the exclusive trusted-port contract.
[HANDOFF](../../HANDOFF.md) records release verification.
