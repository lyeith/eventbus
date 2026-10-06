# Cognito API and authentication

EventBus supplies the named Cognito contracts below through ordinary AWS SDK
calls. Use the local endpoint, explicit test credentials and region
`us-east-1`. IAM, hosted sign-in, device authentication, phone aliases and
passwordless sign-in are outside this subset.

| Operation | Supported behavior |
| --- | --- |
| CreateUserPool, DeleteUserPool, CreateUserPoolClient, DeleteUserPoolClient | Provisioning, password policy, username/email sign-in rules, app-client secret, explicit auth flows and challenge lifetime |
| AdminCreateUser | Separate Username/email/sub, temporary password, FORCE_CHANGE_PASSWORD, SUPPRESS and RESEND |
| AdminGetUser, ListUsers | Attributes, enabled/status/timestamps; email equality/prefix filters, pagination and disabled users |
| AdminSetUserPassword | Permanent or temporary password, policy enforcement and status transition |
| AdminDisableUser, AdminEnableUser, AdminDeleteUser | Account lifecycle with stable identity across disable/enable and RESEND |
| InitiateAuth, AdminInitiateAuth | Password, refresh, USER_SRP_AUTH and SRP-backed CUSTOM_AUTH; admin password flows use AdminInitiateAuth |
| RespondToAuthChallenge, AdminRespondToAuthChallenge | PASSWORD_VERIFIER, CUSTOM_CHALLENGE, NEW_PASSWORD_REQUIRED, software/SMS fixture MFA |
| GetUser, ChangePassword | Access-token authorization and self-service password replacement |
| AssociateSoftwareToken, VerifySoftwareToken, SetUserMFAPreference, AdminSetUserMFAPreference | Real TOTP enrollment and preferences |
| GlobalSignOut, AdminUserGlobalSignOut, RevokeToken | Cognito-side grant revocation |
| Pool JWKS endpoint | Persisted RSA signing keys and signed access/ID/refresh tokens |

Unsupported operations return typed errors. This list describes the implemented
subset, rather than every Cognito feature or optional member of those APIs.

## Identity and invitation lifecycle

A default pool accepts its canonical Username. The email attribute can differ
from Username; sub is the durable account ID. ListUsers can find email without
changing either identity. AdminCreateUser preserves supplied attributes and does
not manufacture email verification.

CreateUserPool accepts email `AliasAttributes` or email `UsernameAttributes`,
with `UsernameConfiguration.CaseSensitive`; these modes are mutually exclusive.
Email aliases permit verified email sign-in alongside canonical Username.
Email-only pools generate canonical Username equal to sub. Case-insensitive
pools normalize their sign-in identities. Existing populated pools cannot change
sign-in rules. Phone/preferred-username aliases and ForceAliasCreation are
unsupported.

AdminCreateUser with a password starts in FORCE_CHANGE_PASSWORD. Authentication
returns NEW_PASSWORD_REQUIRED before ordinary MFA; a valid replacement makes
the account CONFIRMED. AdminSetUserPassword with Permanent=true also confirms it;
false starts another temporary-password cycle. Temporary passwords expire after
the policy lifetime (default seven days). RESEND preserves identity, creation
time, attributes and enabled state while replacing the temporary password.
Cognito invitations are never delivered or captured; SUPPRESS has no mail effect.
Application mail calls go through SES capture.

API-created pools use AWS password-policy defaults. Existing fixture pools may
omit a policy to accept any nonempty password. AWS JSON names, including
MinimumLength and RequireNumbers, and legacy snake_case fixture names are
accepted. Credentials support the AWS 256-character limit.

## Custom SRP and email challenges

Configure the application's three Node handlers using
[custom trigger configuration](CUSTOM-TRIGGERS.md). Enable ALLOW_CUSTOM_AUTH on
the app client. The flow is:

```text
AdminInitiateAuth(CUSTOM_AUTH, USERNAME, CHALLENGE_NAME=SRP_A, SRP_A)
  -> PASSWORD_VERIFIER
RespondToAuthChallenge(USERNAME, real SRP proof, Session)
  -> Define/Create handlers -> CUSTOM_CHALLENGE
Application Create handler sends through its SES client -> captured email
RespondToAuthChallenge(USERNAME, ANSWER, Session)
  -> Verify/Define handlers -> tokens, another challenge, or rejection
```

PASSWORD_VERIFIER validates Cognito SRP math and signature. Custom auth cannot
issue tokens before password proof. An enrolled software MFA factor precedes
custom challenges and contributes its result to their history. Custom auth with
software MFA requires a real TOTP secret; it rejects the six-digit fixture mode. Each response claims a session once and each continuation gets
a fresh session. Sessions bind pool/client/user and the current account credential
version. Public parameters reach the client; private parameters and history
persist in SQLite for the configured handlers. RespondToAuthChallenge
ClientMetadata reaches those handlers; initiation ClientMetadata does not.

For secret clients, send SECRET_HASH using the submitted login on initiation and
the canonical Username on challenge responses. Refresh uses canonical Username
in username-enabled pools and sub in email-only pools.
See [AWS secret-hash rules](https://docs.aws.amazon.com/cognito/latest/developerguide/signing-up-users-in-your-app.html)
and [AWS custom challenge ordering](https://docs.aws.amazon.com/cognito/latest/developerguide/user-pool-lambda-challenge.html).

## Persistence and token checks

Keep the SQLite path, pool/client IDs and issuer stable across restarts. The
schema transition preserves existing sub, email-as-legacy-Username, status,
creation time, signing keys and grants. Reapplying the same seed password
preserves lifecycle state; a changed seed password replaces credentials.
Seeds may specify `username`, `enabled` and pool `sign_in`; omitted username
retains the legacy email fixture behavior.

A bcrypt-only legacy record cannot yield SRP credentials. Reapply its plaintext
seed or set/change its password once; do not reset the identity database.
Existing password authentication continues to work.

Disabling or administratively resetting credentials invalidates pending sessions
and prior emulator grants. Re-enabling permits fresh authentication without
reviving those grants. Self-service ChangePassword preserves existing grants.
Sign-out/revocation is checked by Cognito operations. An unrelated offline JWT
verifier sees signatures and expiry, not current account/revocation state.
See [AWS access-token behavior](https://docs.aws.amazon.com/cognito/latest/developerguide/amazon-cognito-user-pools-using-the-access-token.html).

[SDK verification](../tests/sdk/README.md) exercises real JavaScript/Python SDKs,
independent SRP proofs, Node handlers, SES capture and restart persistence.
Application scenarios and assertions belong in the consuming repository.
