# Secrets Manager

Use normal AWS SDK clients against EventBus, with explicit local credentials and
region. Secrets uses AWS JSON 1.1 at the AWS listener (default
`http://127.0.0.1:4100`). State is in memory; provision secrets again after restart.

| Operations | Behavior |
| --- | --- |
| CreateSecret, UpdateSecret | Values are optional. Metadata-only requests create no value version. |
| PutSecretValue, GetSecretValue | Immutable string/binary versions, native selectors and staging labels. |
| DescribeSecret | Metadata and `VersionIdsToStages`; no secret values. |
| UpdateSecretVersionStage | Attach, move or remove a label; validate its current owner. |
| GetRandomPassword | Cryptographic randomness, native length/character controls; default length 32. |
| RotateSecret, CancelRotateSecret | Registered Lambda workflow, asynchronous acceptance and cancellation. |
| DeleteSecret | Immediate deletion; recovery windows and restoration are not emulated. |

## Values and stages

A value is `SecretString` or `SecretBinary`, exclusively, with a 65,536-byte limit.
SDKs handle binary base64 encoding on the wire. Version IDs/request tokens accept
32–64 characters; UUIDs are suitable. Each staging label has one owner, a version
can have several labels, and a secret supports at most 20 labels.

`GetSecretValue` defaults to `AWSCURRENT`. `VersionStage` selects that label's
owner; `VersionId` selects the exact version. Supplying both requires the same
version. Missing versions/labels return `ResourceNotFoundException`; mismatched
selectors return `InvalidParameterException`. `CreatedDate` belongs to the selected
version, and `VersionStages` reports its current labels.

A fresh secret has no `AWSPREVIOUS`. A first stored value receives `AWSCURRENT`.
Later puts default to current, while explicit `VersionStages=["AWSPENDING"]` keeps
the existing current value. Moving `AWSCURRENT` automatically moves `AWSPREVIOUS`
to its former owner. Moving an attached label with `UpdateSecretVersionStage`
requires the correct `RemoveFromVersionId`; omitting `MoveToVersionId` removes it.
[Native stage contract](https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_UpdateSecretVersionStage.html).

Repeating `PutSecretValue` with identical token/value data succeeds without changing
values or labels. Different data for that token returns `ResourceExistsException`.
Use `UpdateSecretVersionStage` to move labels on an existing version.
[Native put contract](https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_PutSecretValue.html).

## Rotation and evidence

Register application handlers through `--lambda-functions` as described in
[Lambda](LAMBDA.md). Supply their full Lambda ARN to `RotateSecret`, or reuse the
ARN already configured on the secret. A current stored value is required.

A new immediate rotation reserves its token under `AWSPENDING`, without a value, and
returns before execution completes. Handlers receive `SecretId` (the secret ARN),
`ClientRequestToken` and `Step`, sequentially: `createSecret`, `setSecret`,
`testSecret`, `finishSecret`. Handlers own generation, target credential updates,
testing and promotion through ordinary SDK calls. The emulator invokes outside
state locks and requires the handler to promote its stored value before recording
successful completion. Retry handlers idempotently against the same pending token.
Failure before promotion preserves pending state and the current value; another token is
refused while an unrelated pending version remains.
[Native rotation workflow](https://docs.aws.amazon.com/secretsmanager/latest/userguide/rotate-secrets_lambda-functions.html).

`RotateImmediately` defaults to true. False invokes only `testSecret`, using a
temporary pending copy of current credentials, then removes that temporary state
on success, failure or cancellation. Copying current credentials is the emulator's
interpretation of the documented configuration-only check; live AWS comparison
has not verified this detail. Scheduled rotation rules are explicitly refused.
[Native request semantics](https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_RotateSecret.html).

`CancelRotateSecret` disables rotation and cancels selected work. Ordinary pending
labels remain for caller cleanup with `UpdateSecretVersionStage`. Handler failures
are asynchronous outcomes, rather than failures of the accepted HTTP request.
Operational stderr logs include secret ARN, token, step, status and attempt count.
Embedded hosts can record the redacted `RotationOutcome` observer, including its
UTC `completed_at`, as JSONL. Outcomes exclude handler messages, logs and values.
Statuses are `succeeded`, `handler_failure`, `timed_out`, `canceled` or `invalid_finish`.
Timeouts include both coordinator deadlines and redacted Lambda runtime timeouts.
Canceled requests are refused before admission mutates rotation state.

Shutdown calls rotation `Drain` while Lambda execution and the AWS listener are
available. Healthy accepted workflows complete before Lambda/HTTP draining.
A drain deadline cancels and joins owned work, returning a retained error; completed
healthy draining takes precedence over an already-canceled later caller. Resource
`Close` follows draining. Local worker limits, retry timing and observers belong
to `dev_rotation.go`; native operations/state remain in service core.

## Agent verification

Assert metadata-only creation, missing previous, pending/current separation, exact
binary readback, token replay/conflict and label promotion through SDK responses.
For rotation, inspect completion evidence and read current/previous values after
the accepted request; HTTP 200 alone establishes admission.

```sh
go test -race -count=1 ./internal/secrets
go vet ./internal/secrets
EVENTBUS_SMOKE_PYTHON="$PWD/.venv/bin/python" go test -race -count=1 -tags sdksmoke ./tests/sdk -run TestSecretsPythonSDKAndLambdaRotationSmoke
```

The frozen boto3 proof uses a registered Python Lambda handler and an owned
credential target: generate → set → test → promote, failure/retry and configuration
checking. See [SDK setup](../tests/sdk/README.md); on SSD use the documented
`ssd-dev operation --purpose test` wrapper and retain complete output.

This covers the listed operations' local contracts. IAM authorization, KMS
cryptography, cross-account rotation tokens, replication, managed external
rotation, production retention/quotas and other Secrets management APIs are
outside scope. KMS key identifiers are metadata only. Unsupported rotation
configuration returns native `InvalidParameterException`/`InvalidRequestException`;
internal failures return `InternalServiceError`, without handler error text.
