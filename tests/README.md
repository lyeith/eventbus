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
Node trigger execution, SES capture and restart evidence. It requires Node >=20
and frozen SDK dependencies, with no running application stack.

Firehose's optional native S3 suite requires an explicitly owned loopback RustFS
endpoint. It creates and removes a unique bucket:

```sh
S3_ENDPOINT_URL=http://localhost:9000 go test -race -tags integration ./internal/firehose
```

Do not point it at an unrelated store. Application scenarios and eval assertions
remain in the consuming application's tests.
