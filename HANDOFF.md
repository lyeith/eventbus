# Handoff

Canonical SSD /home/spite/Projects/eventbus, main. v0.21.0 published/latest:
https://github.com/lyeith/eventbus/releases/tag/v0.21.0
Tag4276a0685328f1dea2164b23df3b64cb9b21439f; CI40661265617 success.
All8 target binaries/9 downloads verified; actual Linux/Mac gateway SDK proof,
clean metadata, checksums and help. Release cleanup complete.

User then requested remaining gateway parity gaps closed. Post-release source:
- f9fd37e protects HTTPv2 context path/identity escaped separators/percent,
  with AWS-maintained fixture evidence; opt-in live AWS observation seam.
- iam.go one verifier; iam_signing.go one header/Date/query metadata parser and
  RFC3986 query canonicalization, actual payload hashing, conservative5min cap.
- invoke_policy*.go owns shared compiled Invoke/condition matching; native
  iam_policy.go parser separate from REQUEST output parser/512 limit.
- iam_credentials.go owns immutable signing/identity/permission generations;
  dev_iam.go validates/detaches principal/policy/lifetime facts.
- Gateway builds facts from original ingress and same verified credential,
  evaluates current API-owner execution ARN, then rechecks expiry/generation.
- CLI dev_iam_reload.go uses SIGHUP through existing serving owner; reload only
  credential fixtures, invalid candidate unchanged, empty valid deny-all.
- No new verifier, application dependency, IAM/STS service or background owner.

Evidence under /tmp:
- eventbus-gateway-parity-final-race-v2-20261028.log: affected gateway17.054s,
  cmd3.342s/tests1.058s. final-vet-v2: all tags passed.
- eventbus-gateway-parity-native-sdk-20261028.log:1.994s real SDK header/presign
  HTTP -> native provided Go Lambda, both formats/static/session, binary body,
  path/query/identity, explicit policy refusal and tamper invocation counts.
- eventbus-gateway-parity-swagger-20261028.log: unchanged handlers9.005s.
- eventbus-aws-parity-signer-modes-offline-20261028.log + botocore-offline:
  race passes; optional existing pinned1.40.76 interpreter exercised against
  fake transport, no cloud. CI step repeats offline contracts only.
- botocore Date/query and official Smithy vector generation saved as
  eventbus-gateway-parity-*-vectors-20261028.log / *-vector-20261028.log.
- Independent review found/fixed last-key revocation, tested CLI empty/recovery;
  tests also cover same-key replacement/expiry during blocked body, token
  rotation, detached ownership, concurrent snapshots and policy conditions.
- Initial fixture compile/cleanup failures fixed; final gates above passed.

Qualifications:
No live AWS evidence yet; user was asked for existing echo URL/profile/region.
Observer requires explicit dedicated marked HTTPS fixtures and creates none.
Presign>5min, unsigned/AWS-chunked forms unqualified; HTTP APIs exclude SigV4a.
Combined set+IfExists/nonstring variables and malformed static ARN conditions
are rejected; exact native policy bounds documented. IAM/STS/SCP/trust/boundary
resolution is separate scope. v0.21.0 binaries do not include this follow-up.

Finish: push new source, await full CI, record result. No additional release was
requested for post-publication follow-up. All owned author scratch/empty failed
managed scratch cleaned after quiescent inspection; success scratch auto-removed.
No processes/worktrees/envs/deps/live-stack/cache-policy/laptop Plans changes.
Evidence follows existing host retention, no pins.
