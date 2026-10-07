# EventBus state

Canonical source: /home/spite/Projects/eventbus on SSD, main.
Public MIT repository: https://github.com/lyeith/eventbus.
Published latest v0.5.0 on 2026-10-07: service and gateway, Linux/macOS amd64/arm64.
Release source: 1f770c9d24969edd50641150bd78ea303406b7b9; eight CGO-free binaries
and SHA256SUMS. All GitHub downloads match checksums; tag/source metadata verified.
Plans pins and live stacks are unchanged; binaries were published without deployment.

All ten agreed ticket scopes #2–#11 and ownership-review findings are complete:
- Cognito native regional metadata/readback, client token lifetimes, schema,
  permissions, signup/confirmation/recovery and live notification capture.
- Secrets immutable version stages and on-demand four-step Lambda rotation.
- Lambda Event admission, bounded retries/evidence and healthy/abort draining.
- REST/HTTP API authorizers and AWS_PROXY 1.0/2.0 in the separate gateway.
- SNS→Firehose→S3, Go jq extraction, partitions/GZIP/error prefixes and retries.
- Create/Get/Delete one-time Scheduler requests targeting local Lambda events.
- Native wire versions/namespaces/request IDs, SSM snapshots/path/version state,
  shared consumer execution policy and removal of production Cognito DB() access.

App composes consumer-owned typed ports. Background callers quiesce while HTTP
remains available, then HTTP drains and resources close. Independent drains still
join after another fails; dependent stores remain retained on failed barriers.
Native settings stay core; local recipes/profiles/clocks/evidence use named dev
adapters. Native behavior is default; legacy Cognito fixtures select their profile.

Approved dependencies: gojq v0.12.19/timefmt-go v0.1.8 and test-only JavaScript
Scheduler SDK 3.1146.0. No jq executable or extra framework. Limits are explicit
in the service guides; no blanket cloud parity or complete management API claim.

Verified: full SDK race suite, combined vet, all package race coverage, native
RustFS pipeline and platform compilation. The initial race run's two legacy
Cognito fixtures were corrected and their targeted race checks passed; production
validation was unchanged. HANDOFF.md preserves precise commands/results/logs.

No task children/worktrees or temporary volumes remain. Isolated RustFS 40955 is
stopped/deleted. Canonical frozen SDK environments are reusable; bytecode scratch
removed. /tmp evidence and quiescent failed allocations use existing finite GC.
Developer SQLite/live stack state was preserved.

Packaged Linux amd64 passed gateway/Node, native Cognito/SES capture, async Lambda,
Scheduler and graceful shutdown. macOS arm64 passed both CLI startups plus service
health, native Cognito SQLite write and shutdown. Other targets were cross-built.
Owned release/download/macOS staging and smoke state/processes are cleaned.

Next: consuming-app acceptance. SES management remains low priority. API Gateway
management, full Scheduler APIs, production IAM/KMS/provider delivery and durable queue/schedule recovery remain
outside the agreed subset. Reprovision in-memory resources after restart.
