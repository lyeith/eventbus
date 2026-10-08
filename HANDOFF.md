# Handoff

Canonical SSD main; latest published v0.10.0. Preparing v0.11.1 for #27/#28.
Failed v0.11.0 draft removed; source tag preserved. Plans/application state
untouched; no dependencies, retained environments or worktrees added.

6e53ceb rejects CLI positionals before runtime effects.24de500 separates bounded
native Init from configured Invoke; fallback retains original ID/Event attempt
and shares configured Init+Invoke budget after actual first-process join.
Caller/service cancellation and dirty ownership prevent retry. Command keeps
whole-process timeout. Managed fd6/fd7 READY/ACK leave native resultfd3 and
optional stackfd4/fd5 distinct. Provided /next selects the effective deadline.
Core owns causes/facts; dev adapter owns private schema. Python collector follows
current phase and remains joined through terminal evidence.

fb8d02b fixes a packaged macOS ownership failure at the shared localexec owner:
Darwin negative-group SIGKILL may return EPERM for only unreaped exited children.
After EPERM, bounded signal0 probes only accept ESRCH; persistent denial/live
group remains dirty. No additional destructive signals or skipped Wait joins.
Old-code native regression fails EPERM; corrected native race/vet passed.

PASS evidence SSD /tmp under existing24h TTL:
- issue27 CLI race/vet: parser and side-effect guard; app2.497s/gateway2.128s.
- issue28 Lambda full race101.852s; channel/dirty-ownership race3.522s.
- issue27-28 consumers race:app24.922s,gateway39.356s,eventsource1.431s.
- issue28 native-sdk-final-race:Lambda23.812s,native SDK/retained40.624s.
- issue27-28-owners-final-race:Lambda106.864s,localexec2.067s,
  cognitotrigger7.838s,consumer11.036s,gateway40.141s; owners-final-vet all tags.
- macos-group-native-old-regression:expected old-owner EPERM failure;
  native-race2.614s/vet pass; localexec-linux-race2.075s/vet pass.
Earlier v0.11.0 packaged Linux proof/8metadata/downloaded checksums passed,
but macOS cancellation/provided timeout exposed the shared-owner fault. Candidate
was withheld; revised v0.11.1 binaries must pass actual packaged acceptance.

Independent e50418d performance audit:180 invocations/80 appends, non-race Lambda
47.402s/capture0.168s, actual groups/listeners/sinks joined,20 captured handler
snapshots. Report has source boundary, first/min/median/max, same interpreter,
managed XFS and ranked unmeasured owner actions. No runtime optimization or
consumer business-chain claim. README links this separate review.

Next: clean v0.11.1 tag/build, all8metadata, native Linuxamd64/macOSarm64 packaged
proof; draft/download checksums, publication, ticket closure and owned cleanup.
Owned staging:/tmp/eventbus-issue28-root (laptop), packaged script on both hosts,
v0.11.0/v0.11.1 build/metadata staging. Native regression test directory removed.
