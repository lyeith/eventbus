# S3 notifications into registered Lambda functions

LocalStack owns S3 object storage and notification generation. EventBus owns the
registered function, native `Invoke(Event)` admission, retries and joined child
lifetime. Applications own their handlers and business assertions. There is no
EventBus S3 store or fixture-generated substitute event.

## Read-only GetFunction

`GET /2015-03-31/functions/{FunctionName}?Qualifier={alias-or-version}` supports
registered names, full/partial ARNs, aliases, numeric registrations and
`$LATEST`. The optional qualifier follows the same target selection as Invoke;
unknown targets/qualifiers return `ResourceNotFoundException` (404), malformed
identifiers/conflicting qualifiers return `InvalidParameterValueException`
(400). Lookup never starts a process. See the
[AWS GetFunction contract](https://docs.aws.amazon.com/lambda/latest/api/API_GetFunction.html).

The documented development subset is `Configuration` with:

| Field | Registered-target projection |
| --- | --- |
| `FunctionName` | Base function name, without its qualifier |
| `FunctionArn` | ARN of the selected registration; full input ARN preserves its partition/region/account |
| `Runtime` | Configured execution family: `python`, `node`, `provided` or `command` |
| `Handler` | Python/Node module basename and exported handler, such as `processor.handle`; omitted for provided/command |
| `Timeout` | Configured deadline rounded up to integer seconds |
| `Version` | Numeric registration version, otherwise `$LATEST` |
| `State` | `Active` for an available registered target |

Generic Python/Node families deliberately do not claim an AWS runtime version.
Alias registrations select their own recipe; they do not manufacture a
published-version mapping. The returned public handler reference is not a local
filesystem path. No environment, executable arguments, source directories,
deployment package, synthetic download URL, IAM role, tags or code hash are
invented or disclosed. This is registered-target metadata, not Lambda resource
creation/deployment management. Subsecond local deadlines remain unchanged at
execution; their metadata uses the AWS integer-second field.

## Mixed-provider routing

For LocalStack **3.8.1**, use both S3 and Lambda services and let its internal SDK
choose the Lambda-specific endpoint:

```sh
SERVICES=s3,lambda
DISTRIBUTED_MODE=1
AWS_ENDPOINT_URL=http://localhost:4566
AWS_ENDPOINT_URL_LAMBDA=http://host.containers.internal:<eventbus-port>
```

With a Docker bridge, use the engine's reachable host name/gateway (commonly
`host.docker.internal`, explicitly added with `--add-host=host.docker.internal:host-gateway`
on Linux). EventBus must listen on an address reachable from that container.
Neither container `localhost` nor a host-loopback-only listener is a bridge
transport. Restrict any externally reachable development listener to its owned
network.

The automated Linux fixture uses native host networking instead: both endpoints
remain on `127.0.0.1`, and `GATEWAY_LISTEN` selects an unused owned LocalStack
port. Its general endpoint and Lambda endpoint therefore use the actual private
listener URLs, without bridge forwarding or shared stack state. Its numeric loopback endpoints
do not need LocalStack DNS; `DNS_ADDRESS=0` prevents binding the host DNS port.

`AWS_ENDPOINT_URL_LAMBDA` alone does not redirect LocalStack's default internal
client. `DISTRIBUTED_MODE` enables SDK endpoint selection; the general endpoint
keeps its other internal clients on LocalStack. Enabling Lambda is required for
both validation and delivery. This does not require LocalStack to execute or
deploy the function itself. The normal notifier validates with GetFunction and
DryRun, then forwards the original S3 Records with Event. Sources:
[LocalStack internal client selection](https://github.com/localstack/localstack/blob/v3.8.1/localstack-core/localstack/aws/connect.py#L98-L106),
[Lambda notification validation and delivery](https://github.com/localstack/localstack/blob/v3.8.1/localstack-core/localstack/services/s3/notifications.py#L583-L644),
[notification dispatch](https://github.com/localstack/localstack/blob/v3.8.1/localstack-core/localstack/services/s3/notifications.py#L772-L791).

Configure the bucket notification through the ordinary S3 SDK API with
`LambdaFunctionArn`, `Events` and optional prefix/suffix filters. Leave
`SkipDestinationValidation` absent/false. The fixture proves that an unknown
Lambda is refused before a registered alias is accepted.

## Owned compatibility proof

Pre-pull the pinned image; the suite will not pull automatically:

```sh
docker pull localstack/localstack:3.8.1
EVENTBUS_SMOKE_PYTHON="$PWD/.venv/bin/python" \
EVENTBUS_LOCALSTACK_S3_INTEGRATION=1 \
go test -count=1 -race -tags sdksmoke,integration ./tests/sdk \
  -run '^TestLocalStackS3RegisteredLambdaSDKIntegration$' -v
```

On SSD wrap the test with `ssd-dev operation --purpose test --`, preserve full
output before filtering, and run one owned test/build lane at a time.

The suite creates one uniquely named, labelled, ephemeral container with no
Docker socket or persistent mount, a versioned bucket and private EventBus
listeners/state. It checks:

- Real boto3 GetFunction response fields, alias selection, privacy and typed
  missing-target errors, without execution.
- Normal LocalStack GetFunction/DryRun validation, including refusal of an
  unknown destination. No direct test Invoke or SNS Publish generates delivery.
- Actual PutObject, CopyObject and completed multipart events, with the original
  bucket, encoded key, size, version ID, event type and producer record preserved.
- Prefix/suffix refusal and exclusion of ObjectRemoved during a bounded
  three-second observation.
- Two native asynchronous function-error retries of one actual S3 upload,
  preserving its record and admission request ID.
- A final actual upload held in its handler while graceful drain remains pending,
  then released and joined. All observed child PIDs are reaped before successful shutdown.

The multipart fixture declares CRC32 consistently at creation, upload and part
completion. This avoids the pinned old LocalStack provider's mismatch with newer
botocore's automatic UploadPart checksums while using standard S3 fields.

Drain deadlines follow the existing native runtime contract: cancel accepted
async work, join its children and retain the non-nil deadline across later drain
or Close calls. The S3 proof exercises successful graceful joining; it does not
reinterpret a canceled drain as successful completion.

The Python client disables the exact notification, removes every owned object
version/delete marker and unfinished multipart upload, deletes its bucket and
checks absence. The Go owner stops only its returned container ID and verifies
automatic removal, including after a failed assertion. It releases and joins
its handler and removes private temporary files. Cached images are outside the
test's cleanup scope.

This proof exercises the pinned LocalStack version and development runtime. It
does not establish application business acceptance, arbitrary LocalStack
versions, AWS resource policy enforcement or exact-once S3 delivery. Production
S3 can deliver duplicates and out-of-order events; applications own idempotency.

Executed on 2026-10-09 with LocalStack 3.8.1 image digest
`sha256:b279c01f4cfb8f985a482e4014cabc1e2697b9d7a6c8c8db2e40f4d9f93687c7`,
Linux amd64, Go 1.26.0, Python 3.12.11 and frozen boto3 1.40.61 /
botocore 1.40.76. The actual mixed-provider race proof passed in **13.134 s**;
registered metadata race cases passed in **1.028 s**. All exact fixtures and
the image introduced for this verification were removed after confirming it
had no other consumers. Initial failed fixture diagnostics use the existing
24-hour managed retention policy; they are not passing proof.
