# EventBus state

Canonical SSD /home/spite/Projects/eventbus, main; public MIT standalone AWS
emulator and agent harness. Plans/application state remains untouched.
Latest published: https://github.com/lyeith/eventbus/releases/tag/v0.11.1
Preparing v0.11.2 for #29; source acceptance passed, packaged checks pending.

#29 keeps top process_error attached to the final native launch. Retired Init
errors, native ownership and frozen typed causes are bounded per-launch facts.
Ownership remains cumulative over all launches and the optional collector.
Native outcomes, IDs/Event attempts, Init/Invoke budgets and retries unchanged.
Provided runtimes may reply successfully then be stopped while long-polling;
raw OS Wait detail does not determine native success.

Linux full race: Lambda84.689s/localexec2.077s/devcapture1.169s PASS.
Native Event/evidence/retained-owner SDK race34.994s and actual app cleanup
continuation race1.530s PASS; tagged vet across all packages PASS.
Final SDK smoke needs no new dependencies; existing frozen environment used.

Overhead fixed/pushed in SSD tooling796c1dd. Already-owned managed wrapper reuses
exact receipt/cgroup checks and batches fresh storage proofs;13 broker calls→3.
Same-interpreter launcher median1154ms→349ms; native UV Init1161ms→389ms.
docs/PERFORMANCE-REVIEW.md records method/source boundaries and limitations.
All41 Policy/shell tests passed. Full tooling18 failures/3 import errors
reproduce on unchanged26f8468; existing fixture/environment blockers documented.
No root broker/config changes, restart or cross-invocation proof cache.

#27/#28 shipped in v0.11.1; Darwin group EPERM reconciliation remains fail closed.
One serial SSD lane; GOMAXPROCS4/GOFLAGS-p2. Owned probes pending final cleanup;
saved evidence follows existing24h TTL. No application/default DB exercised.
