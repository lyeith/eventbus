# EventBus state

Canonical SSD /home/spite/Projects/eventbus, main; public MIT AWS emulator and
agent development harness. Latest public binaries are v0.21.0 (#48/#49):
https://github.com/lyeith/eventbus/releases/tag/v0.21.0

Published2026-10-28 from4276a0685328f1dea2164b23df3b64cb9b21439f.
Source CI40661265617 passed full race/vet, frozen SDK/fixture contracts and
unchanged Swagger. Eight CGO-free Linux/macOS amd64/arm64 service/gateway assets
have correct formats, clean metadata and public-download SHA verification.
Actual Linux amd64/Mac arm64 gateway -> real Go Lambda SDK proofs passed.
Release outputs/downloads/Mac export28MiB removed; no owned processes remain.

Post-release parity work (source, not in v0.21.0 binaries):
- HTTPv2 context path/authorizer identity retain fixture-proven slash/percent
  escapes. Raw path, capture and v1 owners remain separate.
- One verifier handles header/Date/query SigV4, actual bodies, strict RFC3986
  query parsing (literal plus/semicolon) and a conservative5min presign bound.
- One effective Invoke-policy evaluator shared with REQUEST: complements,
  conditions, variables, implicit deny/explicit Deny and strict native parsing.
- One immutable credential/identity/policy registry: expiry and atomic local
  replacement fence pending authentications; admitted work completes normally.
- Named dev fixture adapter and CLI SIGHUP reload only credentials; invalid
  candidates preserve active generation, empty registry revokes all keys.
  Legacy omitted/null policy explicitly retains fixture allow-all compatibility.
- Explicit-profile awsparity observer uses existing echo fixtures, private
  sanitized output, Go serialized/original-query and optional pinned botocore
  raw-URL modes. CI runs offline contracts only; no cloud resources are created.

Verification:
- Combined affected race gateway17.054s/cmd3.342s/tests1.058s, all passed.
- Integration/sdksmoke/awsparity-tagged vet passed.
- Native presigned/header HTTP -> real Go Lambda proof1.994s: both formats,
  static/session, exact bodies, policy denial and tamper no-invocation counts.
- Unchanged Express/Swagger race9.005s; observer offline race incl real frozen
  botocore fake-transport proof passed. No dependencies added.
- Full post-release source CI pending push; final docs will record result.

Remaining: live AWS encoded-route/raw-query behavior, long execute-api presign
lifetimes and unsigned/AWS-chunked acceptance need evidence. SigV4a is excluded
by AWS HTTP API docs; REST support is a separate expansion. Policy dialect bounds
are explicit in docs/GATEWAY.md. IAM/STS provisioning remains separate scope.
User asked for live echo endpoint/profile/region; awaiting input. No live AWS
calls/provisioning, default-stack/cache-policy or laptop Plans changes.

All task scratch/processes/temporary exports cleaned. Raw /tmp evidence uses
existing host retention, no pins. Canonical source is retained in Git.
