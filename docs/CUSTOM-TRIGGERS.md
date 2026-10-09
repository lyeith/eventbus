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
```

Omission keeps fresh execution. An empty block defaults to two workers; an
explicit cap accepts 1–32. Three workers suit separate Define/Create/Verify
modules. The trigger cap is separate from the public Lambda recipe's cap; total
retained capacity is their sum. Handlers with identical module, export,
environment and timeout share imports within a pool. Different pools are
isolated. Module state persists in warm mode, so handlers must reset per-call
state and await work.

Config/environment changes require reconfiguration or restart. Module edits become
visible after actual worker retirement, using retained quiesce/resume or restart.
Public Lambda reload controls do not expose these private targets.

Strict response and output validation happens before worker reuse. Failed,
invalid, timed-out or canceled workers retire. Retained-owner quiescence drains
these workers and joins their actual retirement; resume permits reuse again.
No trigger target appears on EventBus's public Lambda Invoke surface.

Embedded hosts compose this same owner with
`app.NewCognitoTriggers(config, workDir)`, then inject the returned runner through
Cognito's TriggerInvoker port and close it before its store. Tests use that
composition boundary too.
