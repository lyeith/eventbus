# Application-owned Cognito custom triggers

EventBus executes the application's actual `DefineAuthChallenge`,
`CreateAuthChallenge` and `VerifyAuthChallengeResponse` handlers. Cognito owns
the AWS-shaped event, challenge history, response validation and authentication
decision. This runner supplies local Node execution; it includes no email, IAM
or MFA business logic.

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

Each invocation starts a fresh Node process. Module initialization and handler
logs go to stderr; stdout holds exactly one JSON result. An invalid result,
handler failure, oversized output or timeout fails authentication. Result and
event limits are 1 MiB; diagnostics are bounded at 64 KiB. Default timeout is
Cognito's synchronous five seconds; per-handler `timeout_seconds` can select
one to five seconds. Handlers must await their work and must not detach children.

Execution supports Linux and macOS. The runner owns a process group, kills
remaining descendants and waits for its direct process before returning.
Shutdown stops new invocations, cancels admitted ones and joins their cleanup.
A caller's shutdown timeout does not release still-owned execution.

Only standard path, locale, certificate and temporary-directory variables are
inherited. AWS credentials/profiles, proxies, Node options and the parent's home
directory are not inherited. Shared AWS files default to the null device and
instance metadata lookup is disabled. Declare the application's endpoint,
region, local credentials and settings in each handler's `env`; declared values
override defaults. `console` diagnostics are available with EventBus `--debug`.
Keep private answers and credentials out of application logs. The runner never
logs event/result JSON or reflects a thrown message into an API error.
