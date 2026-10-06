# Verification

Tests follow the ownership described in [Architecture](../docs/ARCHITECTURE.md).
Go unit and service HTTP tests live beside their packages under `internal/`;
`tests/sdk/` owns complete-dispatcher Python SDK/JWT proofs and model fixtures.

Run from the repository root:

```sh
go test ./...
go test -race ./...
go vet ./...
```

For the frozen Python and boto3/JWT lane, follow [SDK verification](sdk/README.md).
It requires no running application stack.

Firehose's optional native S3 suite requires an explicitly owned loopback RustFS
endpoint. It creates and removes a unique bucket:

```sh
S3_ENDPOINT_URL=http://localhost:9000 go test -race -tags integration ./internal/firehose
```

Do not point it at an unrelated store. Application scenarios and eval assertions
remain in the consuming application's tests.
