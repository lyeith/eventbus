# EventBus state

Canonical SSD /home/spite/Projects/eventbus, main; standalone public MIT AWS
emulator and agent development harness. #48/#49 implemented; v0.21.0 publication
authorized and in progress. Latest public release remains v0.20.0 until verified.

#48:
- Original wire components drive route/base mapping and REQUEST policy identity.
  Encoded slash/backslash stay component data; raw Lambda paths and renamed/
  greedy HTTP captures preserve original escapes. Original query bytes survive.
- Readiness cannot alias an encoded separator, including held root/continuation
  ingress. Malformed framing, dot and interior-empty components remain refused.

#49:
- Native header SigV4 execute-api verification owns original URL/headers/body.
  Explicit dev_iam.go key/secret/token-to-principal registry supplies identity;
  config snapshots are private. No ambient credentials or second verifier.
- AWS_IAM routes project verified v2 authorizer.iam / v1 identity separately from
  the API owner account. Anonymous, REQUEST and NoAuth contracts remain separate.
- Ten-MiB hashing bound; verified bytes reused once by HTTP/Lambda integrations.
  Six-MiB encoded Lambda Invoke limit remains.

Verification on 2026-10-28 after SSD restart:
- Affected gateway/cmd/tests race passed; all-tag gateway/cmd vet passed.
- Unchanged real Express/Swagger handler smoke passed.
- Native SDK-signed actual HTTP -> real Go Lambda covers both event versions,
  escaped components, session credentials, tamper/unsigned controls, NoAuth and
  exact invocation counts. Independent frozen botocore signature control passed.
- Raw logs: /tmp/eventbus-v0.21.0-final-{race,vet,swagger}.log.

Limits:
- No live AWS encoded-route normalization or noncanonical raw-query comparison.
  Strict standard SDK query serialization is qualified; no fallback verifier.
- Presign/SigV4a/streaming/unsigned payloads, STS/refresh and IAM policies remain
  documented core gaps. Every registered key is allowed on this gateway's IAM
  routes. Static local binding is explicitly a harness fixture.

Next: push source, green CI, tag/build v0.21.0, actual Linux amd64/Mac arm64
gateway acceptance, public assets/checksums, owned-output cleanup and final docs.
No dependencies added; no live/default stack or laptop Plans changes.
