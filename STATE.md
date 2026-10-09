# EventBus state

Canonical SSD /home/spite/Projects/eventbus, main; public MIT AWS emulator
and agent development harness. Plans/application data remains untouched.
Published v0.12.0: https://github.com/lyeith/eventbus/releases/tag/v0.12.0
Eight CGO-free service/gateway binaries + SHA256SUMS; downloaded assets match.
Clean tagged source 953ca65, Go 1.26.0, module v0.12.0.
Tickets #30/#31/#32 completed and closed; issue board is clear.

- #30: bounded opt-in warm Python/Node workers; fresh execution remains default.
  Lambda owns generations, capacity, native Runtime API, independent contexts,
  deadlines/logs, worker leases and joined retirement. App owns local recipe
  reload; completion_scope distinguishes invocation and process completion.
- #31: SES owns six v1 configuration-set/destination operations, accepted Send
  events and bounded mail correlation. App connects its port to native SNS.
  Explicit local Open/Bounce belongs in dev adapters; original capture remains.
- #32: GetFunction exposes a documented Configuration subset only. Actual
  LocalStack S3 validates destinations and invokes native EventBus handlers with
  original upstream events; EventBus adds no object store or synthetic upload.

Independent ownership/lifecycle review accepted; scoped core races, full SDK,
tagged all-package vet and five Python fixture contracts passed.
Release-source CI passed both full unit race/vet and SDK/Swagger jobs:
https://github.com/lyeith/eventbus/actions/runs/37941175215
CI Lambda race 103.356s / SES 11.294s / SDK 231.840s / Swagger 4.603s.
Linux warm race 9.077s; macOS arm64 warm race 5.206s.
Actual LocalStack 3.8.1 S3 race 13.134s: normal validation, Put/Copy/multipart,
versions/filtering, native retries and graceful drain.
Packaged Linux amd64/macOS arm64 metadata, warm contexts/logs, recipe/source/env
reload, native SES/SNS/SQS Send/Open/Bounce and joined shutdown passed.
Other two platforms cross-built with clean metadata/checksums; not executed.

Eight real SDK sends per runtime/mode, including first call:
Python fresh/warm wall medians 216.078/8.565ms; Node 272.591/12.150ms.
Warm idle RSS medians 46,610,432/97,937,408 bytes; Node grew during eight calls.
Docs/LAMBDA.md records distributions and excludes long-run memory claims.

All owned processes joined. LocalStack containers/image, fixture resources,
macOS scratch, release stage, probes and downloaded duplicates removed.
Task evidence retained under failed receipt 4eaefff6, quiescent/unpinned;
existing expiry Oct10 13:43:54 UTC. S3 fixture failures 779b0793/20a07398 expire
Oct10 13:26:28/13:27:22 UTC. Successful SSD scratch removes automatically.
No dependencies, default DB, live stack or temporary worktrees changed.
SSD project-mode launcher optimization remains a separate host-tooling follow-up.
