# Cognito contracts and agent workflow

EventBus emulates the Cognito user-pool APIs below through ordinary AWS SDK
calls. Use the EventBus endpoint, local credentials and configured region.
Native resource settings persist in SQLite; local fixtures, trigger execution
and notification capture belong to the development harness.

Start a source-built EventBus with a separate evidence file:

```sh
mkdir -p .local
./eventbus --port 14100 --issuer-base http://localhost:14100 \
  --cognito-db "$PWD/.local/cognito.db" \
  --cognito-log "$PWD/.local/cognito.jsonl"
```

For an agent scenario, provision a pool/client, make the SDK request, read the
matching captured code, submit it and assert the API result and user state.
Keep endpoint, issuer, IDs and database stable for restart assertions.
See [SDK verification](../tests/sdk/README.md) for repeatable Python/JavaScript
proofs and [agent workflow](AGENT-HARNESS.md) for application-owned scenarios.

## Operation coverage

| Operations | Implemented behavior |
| --- | --- |
| CreateUserPool, DescribeUserPool, DeleteUserPool | Generated identity, schema, password/sign-in/workflow settings and persisted metadata |
| CreateUserPoolClient, DescribeUserPoolClient, DeleteUserPoolClient | Client identity/secret, auth flows, attribute permissions and token/challenge lifetimes |
| AdminCreateUser, AdminSetUserPassword | Temporary/permanent password, SUPPRESS, RESEND and captured invitations |
| AdminGetUser, ListUsers | Attributes, enabled/status/timestamps; email `=`/`^=` filters, pagination, disabled users |
| AdminDisableUser, AdminEnableUser, AdminDeleteUser | Account lifecycle and grant invalidation |
| SignUp, ConfirmSignUp, AdminConfirmSignUp, ResendConfirmationCode | Self-signup policy, required attributes and confirmation |
| ForgotPassword, ConfirmForgotPassword | Verified recovery destination, captured code and password reset |
| GetUser, UpdateUserAttributes, AdminUpdateUserAttributes | Client-aware reads/writes and atomic attribute batches |
| GetUserAttributeVerificationCode, VerifyUserAttribute | Email/phone verification with captured codes |
| InitiateAuth, AdminInitiateAuth | Password, refresh, USER_SRP_AUTH and SRP-backed CUSTOM_AUTH |
| RespondToAuthChallenge, AdminRespondToAuthChallenge | PASSWORD_VERIFIER, CUSTOM_CHALLENGE, NEW_PASSWORD_REQUIRED and software TOTP; legacy fixture SMS MFA |
| ChangePassword | Authorized password replacement with existing grants preserved |
| AssociateSoftwareToken, VerifySoftwareToken, SetUserMFAPreference, AdminSetUserMFAPreference | Software TOTP enrollment and preferences |
| GlobalSignOut, AdminUserGlobalSignOut, RevokeToken | Cognito-side grant revocation |
| `/<pool-id>/.well-known/jwks.json` | Persisted RSA keys for signed tokens; unknown pools return 404 |

Unknown operations return `InvalidAction`; optional settings are limited to
the supported subset below.

## Native configuration

Provision with AWS member names. Describe operations retain supported settings,
names, timestamps and client secrets across restart. Pool IDs include the
configured region; ARNs retain the resource's region/account and appropriate
AWS partition.

| Resource settings | Behavior/default |
| --- | --- |
| UsernameAttributes / AliasAttributes | Email only, mutually exclusive; omitted means canonical username sign-in |
| UsernameConfiguration.CaseSensitive | Defaults true; false normalizes sign-in identities |
| Policies.PasswordPolicy | Defaults: minimum 8, uppercase/lowercase/numbers/symbols required, temporary password lifetime 7 days |
| Schema | Standard defaults plus declared custom/developer attributes; constraints enforced |
| AutoVerifiedAttributes | Email and/or phone; omitted sends no signup confirmation code |
| AdminCreateUserConfig.AllowAdminCreateUserOnly | Defaults false; true refuses SignUp |
| AccountRecoverySetting.RecoveryMechanisms | Prioritized verified_email/verified_phone_number, or sole admin_only; omitted tries verified phone then email |
| GenerateSecret / ClientSecret | Generated secret or supplied valid secret; mutually exclusive |
| ExplicitAuthFlows | Defaults ALLOW_REFRESH_TOKEN_AUTH, ALLOW_USER_SRP_AUTH, ALLOW_CUSTOM_AUTH |
| AuthSessionValidity | Challenge lifetime 3–15 minutes; defaults 3 |
| ReadAttributes / WriteAttributes | Client attribute permissions described below |
| AccessTokenValidity / IdTokenValidity / RefreshTokenValidity / TokenValidityUnits | Persisted independent token lifetimes described below |

Enable `ALLOW_USER_PASSWORD_AUTH` for direct password authentication and
`ALLOW_ADMIN_USER_PASSWORD_AUTH` for admin password authentication. Supported
legacy flow names cannot be mixed with `ALLOW_*` names. `ALLOW_USER_AUTH` is
rejected. Sign-in rules cannot change after users exist.

Username, email and `sub` are separate: `sub` is the durable identity; an email
alias signs in only after verification. Email-as-username pools generate a
canonical Username equal to `sub`. Admin lookup accepts the pool-scoped canonical
username/sub and configured aliases; ListUsers email search does not change
identity or enable email sign-in. Passwords accept up to 256 non-whitespace
characters and provision both bcrypt and SRP credentials without persisting
plaintext.

### Schema and permissions

Schema supports String/Number/Boolean/DateTime, string/numeric bounds,
mutable/immutable fields and required standard attributes. DateTime
values use RFC3339. Custom/developer attributes cannot be required. Use
`custom:<name>` or `dev:<name>` in user data; undeclared attributes are rejected.
`sub` is generated and immutable. Only administrators can write verification
flags or developer attributes.

Omitted client permissions grant standard attributes; custom/developer access
must be explicit. An empty list grants no optional access. `sub` remains readable,
and required non-developer attributes remain writable. `oidc:profile` expands to
standard profile attributes. Read restrictions apply to GetUser and ID-token
attributes; admin APIs retain the complete view. Native access tokens contain
protocol claims rather than profile attributes and use
`aws.cognito.signin.user.admin` for user APIs. User attributes cannot replace
signed protocol claims.

SignUp enforces required values and client write permissions. AdminCreateUser
may omit required values; NEW_PASSWORD_REQUIRED reports missing
`userAttributes.<name>` values and completes attributes, password and session
consumption atomically. Already-valued required attributes cannot be replaced
in that challenge. Admin updates can fill mutable omissions incrementally. Attribute
batches reject forbidden/immutable writes and required deletions before changing
state. Blank optional values delete attributes; changing/deleting a contact
invalidates its verification and old destination codes.

### Token lifetimes

| Token | Native default | Supported duration |
| --- | --- | --- |
| Access | 1 hour | 5 minutes–1 day |
| ID | 1 hour | 5 minutes–1 day |
| Refresh | 30 days | 1 hour–3,650 days |

Each TokenValidityUnits member accepts seconds/minutes/hours/days. Omitted units
are hours for access/ID and days for refresh; refresh validity 0 uses 30 days.
A unit-only selection preserves the default duration when representable;
otherwise supply an explicit numeric value. Client policy controls issuance
across restart. REFRESH_TOKEN_AUTH returns new access/ID tokens while
preserving the original refresh token and its expiry; it does not rotate or
extend that token.

## Confirmation, recovery and captured messages

SignUp creates UNCONFIRMED users. With an available auto-verified contact, it
captures a confirmation code; if both contacts qualify, phone is selected first.
ConfirmSignUp confirms the user and verifies that contact. Without auto-verification,
use AdminConfirmSignUp; administrative confirmation does not invent verification.
ForgotPassword uses a verified contact selected by recovery priority;
`admin_only` or no eligible verified contact refuses self-service recovery.

AdminCreateUser creates FORCE_CHANGE_PASSWORD users. NEW_PASSWORD_REQUIRED or
AdminSetUserPassword(Permanent=true) confirms them; Permanent=false starts another
temporary-password cycle. RESEND replaces the temporary password while retaining
username/sub, creation time, attributes and enabled state. SUPPRESS emits no
invitation. Otherwise DesiredDeliveryMediums selects EMAIL/SMS, defaulting to SMS;
the corresponding contact is required.

Native Cognito messages use the typed NotificationSink port, with local
[dev_notifications.go](../internal/cognito/dev_notifications.go) JSONL capture.
`--cognito-log` defaults to `-` (stdout); use a file to keep agent evidence separate
from process logs. Records append across restart and have discriminator
`eventbus.cognito.notification.v1`:

```json
{"schema_version":"eventbus.cognito.notification.v1","operation":"SignUp","pool_id":"us-east-1_example","client_id":"exampleclient","username":"stable-user","user_sub":"durable-sub","destination":"invitee@example.test","delivery_medium":"EMAIL","attribute_name":"email","purpose":"signup","code":"012345","timestamp":"2026-10-07T00:00:00Z"}
```

Match `operation`, `pool_id`, `username`/`user_sub` and `purpose` to the scenario.
The API returns a masked CodeDeliveryDetails destination; capture retains the
full destination and `code` or `temporary_password` for agent verification.
Purposes are signup, recovery, attribute:email, attribute:phone_number and
invitation. No email/SMS leaves the harness. Application-owned custom challenge
mail sent through SES appears in [SES capture](SES.md).

Signup/attribute codes last 24 hours; recovery codes last 1 hour. Codes are
single-use and bind user, destination and account revision; reissue replaces
that purpose's code. Five incorrect attempts block that code until reissue.
SQLite stores salted code digests; plaintext codes live in capture evidence.
Capture failure returns CodeDeliveryFailureException. An admitted user/attribute
mutation can remain after delivery fails, so inspect state before retrying.
A concurrent account/contact change may leave an obsolete captured intent while
the API refuses issuance; capture alone does not prove successful admission.

## Development fixtures and custom auth

Native behavior is the default. Select `--cognito-profile legacy-fixtures`
explicitly for retained local consumers. This permits local PoolId/ClientId and
flat PasswordPolicy extensions, fixture policy spellings, and digit-only
software/SMS MFA only for non-native fixture clients. Native clients remain
strict even in that process. Snake-case policy members and RequireDigits also
require an explicit fixture PoolId; generated native pools keep native member
validation. Legacy fixture pools skip invitation delivery.
Non-native fixture clients with omitted read permissions retain their complete
attribute view. Custom auth always requires enrolled TOTP when software MFA is
used.

[dev_seed.go](../internal/cognito/dev_seed.go) loads `--cognito-pools` YAML into
the same store. Seeds may supply username, enabled, sign_in, totp_secret and
attributes. Omitted username preserves legacy email-as-username fixtures.
Unchanged seed passwords preserve identity/lifecycle state; changed passwords
replace credentials. A bcrypt-only legacy row needs its plaintext seed reapplied
or password set once to establish an SRP verifier; keep the identity database.
Legacy clients without explicit policy use process `--access-token-ttl` /
`--refresh-token-ttl` (defaults 1 hour / 24 hours). YAML client
access_token_validity, id_token_validity, refresh_token_validity and
token_validity_units override those defaults and persist; unit fields are
access_token, id_token and refresh_token.

Configure application-owned Node Define/Create/Verify handlers through
[custom trigger configuration](CUSTOM-TRIGGERS.md), with ALLOW_CUSTOM_AUTH.
CUSTOM_AUTH requires SRP_A → PASSWORD_VERIFIER, any enrolled software MFA,
then CUSTOM_CHALLENGE. Sessions are single-use and bind pool/client/user/revision.
Private parameters/history persist in SQLite; public parameters reach the
client. RespondToAuthChallenge ClientMetadata reaches
handlers; initiation ClientMetadata does not. Secret clients hash the submitted
login on initiation, canonical Username on challenge responses, and canonical
Username (or sub in email-only pools) on refresh.

Disabling, admin password reset, recovery reset or global sign-out invalidates
old emulator grants/challenges. Re-enabling never restores them. Self-service
ChangePassword preserves grants. Offline JWT consumers validate signature/expiry
without current Cognito account/revocation state. Signing keys, users, settings
and codes persist across restart.

## Explicit limits

Selected unsupported non-default provisioning settings return
InvalidParameterException before mutation: OAuth/hosted login/federation,
analytics/threat protection, device/passkey/passwordless/USER_AUTH modes,
AWS LambdaConfig ARNs, native email/SMS delivery configuration or message/link
templates, pool MFA configuration other than OFF, custom invitation templates,
non-default deprecated UnusedAccountValidityDays, refresh-token rotation,
disabling token revocation and PreventUserExistenceErrors=ENABLED. Workflow
Session, ForceAliasCreation, risk/analytics input and pre-signup ValidationData
are also rejected. ClientMetadata is accepted without adding unconfigured
workflow triggers.

UpdateUserPool/UpdateUserPoolClient, IAM enforcement, cloud delivery and other
unlisted APIs remain unsupported. These contracts describe the tested local
subset; SDK proofs are not cloud parity comparisons.

AWS references: [pool settings](https://docs.aws.amazon.com/cognito-user-identity-pools/latest/APIReference/API_CreateUserPool.html),
[client settings](https://docs.aws.amazon.com/cognito-user-identity-pools/latest/APIReference/API_CreateUserPoolClient.html),
[signup/confirmation](https://docs.aws.amazon.com/cognito/latest/developerguide/signing-up-users-in-your-app.html),
[password recovery](https://docs.aws.amazon.com/cognito/latest/developerguide/managing-users-passwords.html).
