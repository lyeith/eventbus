# EventBus state

Canonical SSD /home/spite/Projects/eventbus, main; standalone public MIT AWS
emulator and agent development harness. #48/#49 published as latest v0.21.0:
https://github.com/lyeith/eventbus/releases/tag/v0.21.0

Publication complete on 2026-10-28:
- Annotated tag/source4276a0685328f1dea2164b23df3b64cb9b21439f.
- Source CI40661265617 passed full Go race/vet, frozen SDK/fixture contracts and
  unchanged Express/Swagger handlers.
- Eight CGO-free service/gateway binaries: Linux/macOS amd64/arm64. Target
  formats, v0.21.0 module version, exact revision and clean metadata verified.
- All nine public downloads match the built manifest; eight checksums passed.
  GitHub confirms latest/public/non-prerelease.
- Actual Linux amd64 and Mac arm64 gateway packages pass the maintained native
  SDK-signed HTTP -> real Go Lambda race proof, both event versions, caller/API
  account separation, escaped paths/query bytes, tokens and tamper refusals.
  Both service/gateway help commands pass. Other targets cross-build only.

#48 preserves wire component boundaries through routing/base mapping, REQUEST
policy identity and HTTP proxy captures. Raw Lambda paths/query survive.
Readiness cannot alias encoded separators, including held continuation ingress.
#49 verifies original header SigV4 request bytes with explicit local static
identity bindings; v2 IAM/v1 identity projection and one verified-body owner.
REQUEST/anonymous/NoAuth stay separate. No dependencies added.

Cleanup complete:
- Managed build/test/download owners quiescent; success scratch auto-removed.
  Earlier failed scratch inspected empty/quiescent and removed.
- Owned dist binaries/manifest/notes and private downloads removed after public
  verification. Mac source/package/driver export28MiB removed after process/
  open-file inspection. No owned processes, worktrees or environments remain.
- Raw /tmp/eventbus-v0.21.0-* evidence follows existing host retention, no pins.
  No live/default-stack/cache-policy or laptop Plans changes.

Next, user requested remaining gateway parity gaps closed after publication.
Primary-source audit in progress: request signing forms, IAM invoke policies and
credential lifecycle, encoded routing/query qualification. IAM/STS provisioning
remains separate service scope. No post-release source changes yet.
