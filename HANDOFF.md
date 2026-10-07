# Handoff

Canonical SSD /home/spite/Projects/eventbus, main; latest public v0.7.0.
Plans/application state untouched. Finish/commit/push/publish authorized.

Accepted source: #19 9b8f56f, #18 973e5f1, #20 a6b48d5,
capture072a180, receipt outcomes4de060a, eventsource91e2678,
Lambda83a9b6c, app/native SDK b8c4628 and #23 SES5480973.
Docs are ready for commit; no runtime dependencies/frozen locks changed.

#21 evidence uses actual Lambda/request/mapping/message lineage and six
queue-owned receipt outcomes; terminal follows runner/children and receipt work.
Capture uncertainty is sticky/bounded and stops source intake. App composes
typed ports, shared redaction, private/public path alias refusals and retained health.
#22 diagnostics retain bounded private stream/tail/failure details per joined
attempt. One native engine serves ordinary/observed execution. Strict Close and
async-only DrainAsync retain uncertainty; native outputs/admission/retries unchanged.
Completed bounded result readers skip deadline setup; pending/read/close/pipe
faults remain strict. Independent final review accepted.
#23 SES raw header fallback validates effective configuration set, with explicit
API-field precedence verified in AWS's Introducing Sending Metrics blog.
Original submitted request/base64/headers remain unchanged, including errors.
Runtime success is not business success. Trusted handlers await side work within
owned OS groups; escaped/unawaited pipe holders cannot certify healthy completion.

Saved complete PASS logs under finite SSD /tmp retention:
- eventbus-pre-evidence-combined-race-20261008.log: all14owners.
- eventbus-issue22-devcapture-{race,vet}.log.
- eventbus-sqs-receipt-evidence-messaging-{race,vet}-20261008.log.
- eventbus-sqs-delivery-core-{test,race,vet}.log.
- eventbus-lambda-private-owner-final-fixed-race-20261008.txt (34.824s)
  and eventbus-lambda-private-owner-final-vet-20261008.txt.
- eventbus-final-combined-sdk-race-20261008.log (13 selected,208.846s).
- eventbus-final-app-gateway-race-20261008.log (23.067s/45.455s/1.043s).
- eventbus-final-combined-vet-20261008.log.
- eventbus-ses-raw-config-{focused,race,sdk,vet}.log (14cases,11.274s/1.747s).
Prior #18 full native SDK/RustFS race23.832s, #19 current/legacy and #20 actual
manual-receipt normal79.177s/race80.299s passed; final combined includes all.
Unavailable consuming production Identity business handler is not claimed verified.
All completed fixtures/children/listeners joined; isolated legacy env removed28MiB.
Task-generated SDK bytecode removed; reusable .venv/node_modules/tool caches kept.

Root owns sole Go/build lane; no test or agent edit is in flight.
Next docs commit/push, clean v0.8.0 tag, build8binaries+SHA256SUMS from tagged source.
Task staging /tmp/eventbus-v0.8.0-release.w467OX contains notes only.
Portable567line /tmp/eventbus-v0.8.0-native-smoke.py on SSD/laptop: run physical
Linuxamd64/macOSarm64 proof, other targets cross-build/inspect; no physical run yet.
Draft/upload/download/check assets before publication, close18-23, update state
and clean staging/script copies. Logs retained under existing finite policies.
