# EventBus state

Canonical SSD /home/spite/Projects/eventbus, main. Public MIT standalone emulator
and agent harness; Plans/application stacks/databases are untouched.
Latest published release: https://github.com/lyeith/eventbus/releases/tag/v0.8.0
Tickets #18–#23 are published and closed.

#24 implemented and verified; preparing v0.9.0 publication.
Optional retained_owner_continuation_port / --retained-owner-continuation-port
adds a separate exclusively trusted 127.0.0.1 gateway listener.
Public requests always remain roots. Native REQUEST auth and Invoke unchanged.
Shared lease ledger admits continuations only with actual accepted work,
atomically checking owner/generation/health before body read and native routing.
Idle open/held, resume transitions, stale generation and uncertain ownership
refuse; no public header grants continuation status. Kind-bound replay shares
the bounded no-expiry root ledger. Handler return confirms completion; panic/
Goexit remain dirty. Accepted shutdown chains keep private peers available
through join, with a bounded sticky failure after observed shutdown.
Apps configure existing HTTP and callback issuer/JWKS bindings; no Trust import.

PASS: full affected-owner race (coordinator1.048s, gateway39.652s, app23.523s,
cmd1.066s), actual registered Lambda/Cognito SDK/JWT/cold JWKS race5.635s.
Production app declaration→authenticated gateway→native integration proof PASS.
Scoped tagged vet PASS. Release binary builds/physical CLI proofs pending.
No dependency changes, temporary worktrees/branches or new environments.
Parent owns the serial managed SSD test/build lane.
