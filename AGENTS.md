# Working on EventBus

EventBus is a standalone AWS emulator and agent development harness that
supplements LocalStack workflows. Its purpose is a shorter verification/eval
loop: run application scenarios, inspect evidence, assert outcomes and iterate.
Start with [README](README.md), [Agent workflow](docs/AGENT-HARNESS.md) and
[SES capture](docs/SES.md).
[STATE.md](STATE.md) and [HANDOFF.md](HANDOFF.md) record current work, not API contracts.

## Ownership and contracts

- This repository owns the emulator, generic fixture formats and SDK tests.
  Apps own SDK configuration, seeds, resource names, provisioning and consumers.
- Preserve supported AWS wire behavior and startup flags. The README lists
  coverage and limits; source and tests are authoritative.
- `server.go` dispatches APIs. Service handlers own AWS operations;
  `ses_v1.go` and `ses_v2.go` own SES protocol adapters.
  `ses.go` owns fixtures/capture; `ses_message.go` owns shared MIME validation.
- Preserve SES JSONL schema/version and exact request/binary capture. A send
  succeeds only after capture; closure follows HTTP drain.
- Do not reset a developer identity database or interrupt an application stack.
  Tests own their state paths, listeners and resources.

## Verify and finish

Run scoped Go tests, then race/vet checks appropriate to the change.
[README](README.md#verify-and-build-releases) lists commands;
[SDK verification](tests/README.md) explains the frozen Python lane.
The opt-in Firehose integration suite requires an explicitly owned loopback
RustFS endpoint.

On SSD, wrap builds with `ssd-dev run --purpose build -- <command>` and tests
with `ssd-dev operation --purpose test -- <command>`. Save complete output
before filtering it. Run Python through `uv run`.

Preserve unrelated changes. Commit, push and publish only when authorized.
Release artifacts come from `scripts/build-release.sh` and are excluded from
Git. Stop owned processes and remove temporary probes/environments when done.
Update STATE/HANDOFF when work materially changes; keep each under 80 lines.
