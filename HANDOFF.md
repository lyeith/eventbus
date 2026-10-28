# Handoff

Canonical SSD /home/spite/Projects/eventbus, main. #48/#49 published as latest
v0.21.0 on 2026-10-28:
https://github.com/lyeith/eventbus/releases/tag/v0.21.0

Release evidence:
- Tag/source4276a0685328f1dea2164b23df3b64cb9b21439f.
- CI40661265617 success: full race/vet, frozen SDK/fixture contracts, unchanged
  Express/Swagger handlers. /tmp/eventbus-v0.21.0-source-ci.log.
- All eight Linux/macOS amd64/arm64 service/gateway assets: CGO0, correct target
  formats, v0.21.0 module version, exact revision, vcs.modified=false.
  /tmp/eventbus-v0.21.0-artifact-audit.log.
- Actual Linux amd64 gateway SDK HTTP -> native Go Lambda race proof1.897s;
  actual Mac arm64 package proof1.959s, both payload versions. Encoded paths,
  exact query, caller/API accounts, tokens, unsigned/tamper rejection before
  invoke, actor spoofing and REQUEST/anonymous/NoAuth isolation.
  /tmp/eventbus-v0.21.0-linux-gateway-package.log and ...-mac-sdk-race.log.
- Service binary acceptance here is hash/metadata/format/help, no new runtime
  behavior claimed. Other CPU targets cross-build/metadata verified only.
- All nine public downloads match built SHA; GitHub latest/public/nonprerelease.
  /tmp/eventbus-v0.21.0-download-verification.log.

Owners:
- routes.go: once-decoded wire component routing and safe policy identity.
  mapping.go: exact raw-capture provenance without recursive substitution;
  explicit trusted recipe mappings retain existing multi-segment behavior.
- dev_health.go: shared readiness boundary for normal/retained ingress.
- iam.go: native header SigV4 verification against original bytes.
  dev_iam.go: explicit local key/token/principal fixture binding.
- gateway.go composes authentication and a detached integration view;
  events.go projects IAM caller separately from API owner, payload reused once.
- Native capability gaps stay core; local fixture controls stay named dev adapters.

Cleanup:
- Build305bbf5b86bd4dab921cfe2d884366bf, artifact audit
  a02b6c89516f461dbfd6c0011226a75b, Linux package
  1e472155ebb14fd0a05a1964bafad94c, downloads
  7b24ee8efd514bdea730034603e41f8b quiescent; success scratch auto-removed.
- Earlier failed test scratch was empty/quiescent and removed.
- Owned dist/output/notes/downloads removed. Mac export/packages/tempdriver28MiB
  removed after process/open-file inspection; ...-mac-cleanup.log copied to SSD.
- No task worktrees/branches/environments, owned processes, dependencies or live/
  default-stack/cache-policy/laptop Plans changes. Evidence uses host retention.

Next:
User requested work to close remaining gateway parity gaps after publication.
Read-only primary-source audits in progress; no post-release source changes yet.
Check Date-header signing, presigning/payload forms, native invoke permissions
and explicit local credential expiry/rotation seams. AWS docs exclude SigV4a
for HTTP/WebSocket APIs; REST supports it. No live AWS path/query comparison or
credentials/provisioning used. IAM/STS provisioning remains separate scope.
