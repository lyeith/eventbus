# Application-owned Cognito custom triggers

EventBus executes the application's actual `DefineAuthChallenge`,
`CreateAuthChallenge` and `VerifyAuthChallengeResponse` handlers. Cognito owns
the AWS-shaped event, challenge history, response validation and authentication
decision. The trigger adapter supplies its event contract to the shared Lambda
runtime; it includes no email, IAM or MFA business logic.

Copy [the configuration example](../examples/cognito_triggers.yaml) into the
application. Supply its path with `--cognito-triggers` and use `--work-dir` for
the application's root directory. Install the application's dependencies there
with its own lockfile. Relative configuration paths and all relative handler paths resolve against
that application directory, rather than the configuration file's directory.
With no --cognito-triggers flag, custom handlers are disabled and Node is not
required by the EventBus runtime.

```yaml
pools:
  us-east-1_LocalPool:
    DefineAuthChallenge: {handler: ./auth/define.mjs#handler}
    CreateAuthChallenge: {handler: ./auth/create.cjs#handler}
    VerifyAuthChallengeResponse: {handler: ./auth/verify.mjs#handler}
```

Each configured pool requires all three handlers. The optional top-level `node`
selects an executable; it defaults to `node`. Startup rejects incomplete
configuration, unknown fields, missing executables and missing handler files.
Custom authentication for an unconfigured pool fails.

Handlers export an async Lambda function, accept `(event, context)` and return
the complete event with an object `response`. ESM (`.mjs`, or `.js` under
`"type": "module"`) and CommonJS (`.cjs`, or CommonJS `.js`) are supported.
Omit `#handler` to select the `handler` export.

```js
export async function handler(event, context) {
  // Apply the application's challenge policy.
  return event;
}
```

Cognito sends chronological challenge history to Define/Create, and keeps
Create's private parameters for Verify. Public parameters are returned to the
client. See the AWS contracts for
[Define](https://docs.aws.amazon.com/cognito/latest/developerguide/user-pool-lambda-define-auth-challenge.html),
[Create](https://docs.aws.amazon.com/cognito/latest/developerguide/user-pool-lambda-create-auth-challenge.html)
and [Verify](https://docs.aws.amazon.com/cognito/latest/developerguide/user-pool-lambda-verify-auth-challenge-response.html).
The handler chooses the challenge; EventBus does not accept an arbitrary answer.

Fresh Node execution is the default. Results use a dedicated runtime channel;
stdout, stderr and console output are private diagnostics. An invalid result,
handler failure, oversized output or timeout fails authentication. Result and
event limits are 1 MiB; combined diagnostic output is bounded at 64 KiB. Default
timeout is Cognito's synchronous five seconds; per-handler `timeout_seconds`
can select one to five seconds. Handlers must await their work and must not
detach children.

Execution supports Linux and macOS. Lambda owns process groups, descendants and
response/output joins. Fresh calls retire their process before returning; a warm
success joins its response and output boundary while its worker stays owned.
Shutdown stops new invocations, cancels admitted ones and joins all workers.
A caller's shutdown timeout does not release still-owned execution.

Only standard path, locale, certificate and temporary-directory variables are
inherited. AWS credentials/profiles, proxies, Node options and the parent's home
directory are not inherited. Shared AWS files default to the null device and
instance metadata lookup is disabled. Declare the application's endpoint,
region, local credentials and settings in each handler's `env`; declared values
override defaults. Trigger diagnostics are bounded and discarded, including with
EventBus `--debug`; private execution cannot enable Lambda diagnostic or async
capture. Keep private answers and credentials out of application logs. EventBus
never logs event/result JSON or reflects a thrown message into an API error.

## Warm trigger execution

Add a top-level block to reuse Node imports across authentication steps:

```yaml
dev_warm:
  max_workers: 3
  max_concurrency: 64
```

Omission keeps fresh execution. `max_workers` limits retained idle imports
(default two, range 1–32); three suit separate Define/Create/Verify modules.
`max_concurrency` limits reserved, executing and execution-retiring environments
(default 64, range 1–1,024). Saturation refuses the trigger promptly; it does not
wait behind handlers making nested calls. These private limits are separate
from the public Lambda recipe. Each runtime owns at most its idle plus execution
limits; the two runtime totals add. Limits count environments, not handler RSS.
Handlers with identical module, export,
environment and timeout share imports within a pool. Different pools are
isolated. Module state persists in warm mode, so handlers must reset per-call
state and await work.

Config/environment changes require reconfiguration or restart.
[Selected source reload](#development-source-reload) refreshes private imports
without restarting the broker. Public Lambda reload controls remain separate.

Strict response and output validation happens before worker reuse. Failed,
invalid, timed-out or canceled workers retire. Retained-owner quiescence drains
these workers and joins their actual retirement; resume permits reuse again.
No trigger target appears on EventBus's public Lambda Invoke surface.

Embedded hosts compose this same owner with
`app.NewCognitoTriggers(config, workDir)`, then inject the returned runner through
Cognito's TriggerInvoker port and close it before its store. Tests use that
composition boundary too.

## Development source reload

After editing a configured handler or its imported modules, reload one selected
pool/trigger through the local dev endpoint:

```sh
curl -X POST \
  http://localhost:4100/__eventbus/dev/cognito/pools/us-east-1_LocalPool/triggers/CreateAuthChallenge/reload
```

HTTP 200 returns
`{"pool_id":"us-east-1_LocalPool","trigger":"CreateAuthChallenge","status":"reloaded"}`.
No request body is required. Select the configured `DefineAuthChallenge`,
`CreateAuthChallenge` or `VerifyAuthChallengeResponse` name.

Identical entries within that pool share one imported generation. Reloading
any one of them replaces that shared generation for all matching entries.
Distinct entries and other pools keep their generations, even if they reference
the same source file. Public Lambda functions also keep their own generations.

Lambda's existing runtime validates the source path, publishes the replacement,
then joins old workers. Active old calls finish normally; subsequent calls can
use the new imports while the reload request is still waiting. HTTP 200 confirms
old warm-worker retirement. Caller cancellation/deadline returns
`reload_wait_interrupted` (HTTP 504); a replacement already published remains
active, and that canceled wait does not cancel old invocations.
Reload does not pre-execute the handler or attest application success.

This is source-only reload: handler selection, environment and timeouts retain
their configured values. Cognito pools, clients, users, signing keys, tokens and
challenge/session state remain intact. Pending challenges run their subsequent
stages with the newly imported code. No global drain or broker restart is needed.
The retained-owner profile keeps its ordinary source/callback admission fence.

An unknown pool/trigger returns `trigger_not_configured` (404); other methods
return `reload_requires_post` (405). Unsupported/closed execution or reload
failure returns a static 503 error. Source paths, hidden target names,
environment values and runtime diagnostics never enter the response.
With no configured trigger runner, the dev route is absent (404).
This endpoint is a harness control, not an AWS Cognito operation; private
targets still have no public Lambda Invoke route.

Embedded hosts call `runner.DevReloadTrigger(ctx, poolID, triggerName)` or attach
`cognitotrigger.NewDevReloadHandler(runner)` through `server.Services.CognitoDev`.
The optional consumer-owned `DevTriggerReloader` port keeps generation retirement
in the existing Lambda runtime.
