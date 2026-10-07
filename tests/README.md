# Verification

Tests follow [Architecture](../docs/ARCHITECTURE.md). Go unit and service HTTP
tests live beside their packages under `internal/`; `tests/sdk/` owns complete
HTTP-dispatcher proofs with frozen Python and JavaScript SDKs.

Run from the repository root:

```sh
go test ./...
go test -race ./...
go vet ./...
```

[SDK verification](sdk/README.md) adds lifecycle, SRP/custom authentication,
Node trigger execution, native Cognito provisioning, SES/SNS capture, direct SQS
sending, SNS Lambda, SQS mappings, Secrets rotation, Lambda Event, Scheduler,
real Express/Swagger gateway redirects and restart evidence. It requires Node >=20
and frozen SDK dependencies, with no running application stack.

Retained-owner checks belong beside their owners: fence/source leases/evidence
in `internal/devquiescence`, native custody/execution/drain in service seam tests,
root ingress in gateway tests, and profile/declared cleanup/shutdown in `internal/app`.
`TestRetainedOwnerPythonSDKBarrier` is the baseline SNS/Lambda recovery lane;
`TestRetainedStackNativeSDKRecovery` adds the `sdksmoke,integration` full-profile
fixture with its own RustFS process and data.
[Retained owner](../docs/RETAINED-OWNER.md) defines its supported boundary.
Current expanded-profile verification is recorded in [HANDOFF](../HANDOFF.md);
consuming applications own actual authenticated cleanup and fixture assertions.

Firehose's optional native S3 suite requires an explicitly owned loopback RustFS
endpoint. It creates and removes a unique bucket:

```sh
S3_ENDPOINT_URL=http://localhost:9000 go test -race -tags integration ./internal/firehose
```

Do not point it at an unrelated store. Application scenarios and eval assertions
remain in the consuming application's tests.
