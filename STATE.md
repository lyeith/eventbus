# EventBus state

- Public repository: https://github.com/lyeith/eventbus.
- Branch: main; SES implementation committed as 6602261.
- License: MIT, copyright 2026 David Wong; GitHub recognizes the license.
- SES, standalone agent documentation and MIT licensing are committed/pushed.
- No new binary release. v0.1.0 binaries predate SES; consumer pins are unchanged.
- No task-owned emulator or test process remains running.

SES supports the six v1 and three v2 sending operations. Captures are synchronous
JSONL, to stdout by default or an append-only file selected by --ses-log.
--ses-config provides sending prerequisites while management APIs remain
low-priority backlog work. Email is never delivered.

Verification completed: focused SES unit/SDK tests, full Go suite and real SDK
smoke under race, vet, Python fixture contracts, executable startup/shutdown,
live file/stdout capture and restart append.

The capture template renderer supports simple substitutions. Rich Handlebars
is retained with a capture rendering error. Structured-message size accounting
uses deterministic MIME and does not reproduce AWS's private assembler.

EventBus is standalone, supplementing LocalStack and other local AWS stacks.
README and AGENTS make the verification/evaluation purpose explicit.
docs/AGENT-HARNESS.md gives the app workflow; docs/SES.md owns capture details.
Documentation links and consumer/auth instructions were checked against source.

Next: publish an SES-capable binary release when requested, or continue the
low-priority SES backlog. LocalStack coexistence is not integration-tested.
