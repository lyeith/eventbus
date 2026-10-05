# EventBus

A standalone Go development harness for the supported Cognito, SNS, SQS,
Firehose, SSM and Secrets Manager APIs. Preserve existing wire behavior and
startup flags; this is a development subset, not complete AWS emulation.

Keep consuming applications' seeds, resource names and Lambda consumers in those
applications. Plans owns its configuration and application acceptance tests;
this repository owns the emulator and generic SDK tests.

Use scoped `go test ./...`, `go test -race ./...` and `go vet ./...`. SDK smoke:
`uv sync --frozen`, then
`EVENTBUS_SMOKE_PYTHON="$PWD/.venv/bin/python" go test -tags sdksmoke ./...`.
The tests create their own stores/listeners. Never reset a developer's identity
database or interrupt an application stack to run tests. The opt-in integration
suite requires an explicitly owned loopback RustFS endpoint.

On SSD, run builds under `ssd-dev run --purpose build -- <command>` and tests
under `ssd-dev run --purpose test -- <command>`.
Save complete test output before filtering it. Run Python through `uv run`.
Preserve unrelated changes; don't commit, push or publish without authorization.
Release artifacts are produced by `scripts/build-release.sh` and excluded from
Git. Temporary probes and task-owned environments should be cleaned up after use.
