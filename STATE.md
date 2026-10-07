# EventBus state

Canonical source: /home/spite/Projects/eventbus on SSD, main.
Public MIT repository: https://github.com/lyeith/eventbus.
Latest public binaries: v0.6.0; #16/#17 are accepted and not yet released.
Plans pins, developer stacks and identity databases remain untouched.

#16: native SQS BatchSize 1–10/default 10; ScalingConfig ceiling 2–1000.
Fixed workers own receive/invoke/whole-batch settlement. The separate local cap
defaults to 32; omitted native ceiling remains serial locally. Exact 6 MiB event
selection occurs before leasing, preserving excluded messages and FIFO order.
Delete/Close join all workers/children. Larger batches/windows, partial responses,
Update/List and AWS managed scaling remain explicit separate capabilities.

#17: opt-in retained-owner development harness, documented in
docs/RETAINED-OWNER.md. Enable --retained-owner-callback-port for two loopback
endpoints. Suite roots/control use source; accepted native handlers/peers use
callback. One exclusive operator must own all active callback callers.
Count received HTTP before capture, async admission through retry/terminal
evidence and independent sync execution through actual child/runtime-handler
join. Fence/join/held exact SNS/SQS cleanup/explicit generation resume never
reset stores or perform app-specific cleanup.
Deadline retains fenced dirty state; capture/cleanup uncertainty forbids safe
cleanup and resume. Shutdown preserves peers during join, aborts/joins native
work after failed join and withholds store cleanup on failure.
The first profile explicitly refuses mappings, Scheduler, Firehose/subscriptions,
RotateSecret, legacy consumers and Cognito triggers; ordinary mode is unchanged.

Verification: all affected core/app/default consumer and gateway integration
race suites pass; eight real Python/JavaScript SDK lanes pass, including lost
SNS/nested responses, pre-capture received publications, process-free retries,
timeout/shutdown, sentinel preservation and a resumed second suite.
Scoped tagged vet passes. Read-only combined ownership review has no open finding.
No new dependency. Full logs use existing finite SSD /tmp retention.

Next: build v0.7.0 from clean tagged source, verify packaged Linux amd64/macOS
arm64 behavior, publish eight EventBus/gateway binaries with verified checksums,
then remove owned release staging. SES management and durable async restart
recovery remain outside these tickets.
