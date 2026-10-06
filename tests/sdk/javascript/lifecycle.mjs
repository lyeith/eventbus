import assert from "node:assert/strict";
import {
  AdminCreateUserCommand, AdminDeleteUserCommand, AdminDisableUserCommand,
  AdminEnableUserCommand, AdminGetUserCommand, AdminInitiateAuthCommand,
  AdminSetUserPasswordCommand, AdminUserGlobalSignOutCommand, CreateUserPoolClientCommand, GetUserCommand, GlobalSignOutCommand,
  InitiateAuthCommand, ListUsersCommand, RespondToAuthChallengeCommand, RevokeTokenCommand,
} from "@aws-sdk/client-cognito-identity-provider";
import {
  attrs, captures, client, expectError, loadState, parameters, poolID,
  saveState, secretHash, send, verifyTokens,
} from "./common.mjs";

const runID = process.env.SMOKE_RUN_ID;
const password = "PermanentPass2!";
const temporary = "TemporaryPass1!";
const get = username => send(new AdminGetUserCommand({ UserPoolId: poolID, Username: username }));
const setPassword = (username, Password, Permanent) => send(new AdminSetUserPasswordCommand({ UserPoolId: poolID, Username: username, Password, Permanent }));
const adminLogin = (state, Password) => send(new AdminInitiateAuthCommand({
  UserPoolId: poolID, ClientId: state.clientID, AuthFlow: "ADMIN_USER_PASSWORD_AUTH",
  AuthParameters: parameters(state, state.username, { PASSWORD: Password }),
}));
const refresh = (state, RefreshToken) => send(new InitiateAuthCommand({
  ClientId: state.clientID, AuthFlow: "REFRESH_TOKEN_AUTH",
  AuthParameters: { REFRESH_TOKEN: RefreshToken, SECRET_HASH: secretHash(state.secret, state.username, state.clientID) },
}));

async function exercise() {
  assert(runID, "isolated run ID is required");
  const appClient = await send(new CreateUserPoolClientCommand({
    UserPoolId: poolID, ClientName: `sdk-${runID}`, GenerateSecret: true,
    ExplicitAuthFlows: ["ALLOW_ADMIN_USER_PASSWORD_AUTH", "ALLOW_USER_PASSWORD_AUTH", "ALLOW_CUSTOM_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
  }));
  const state = { username: `${runID}-owner`, email: `${runID}-owner@sdk-smoke.test`,
    clientID: appClient.UserPoolClient.ClientId, secret: appClient.UserPoolClient.ClientSecret, password };
  assert(state.clientID && state.secret);
  const initialCaptureCount = (await captures()).length;
  const input = { UserPoolId: poolID, Username: state.username, TemporaryPassword: temporary,
    MessageAction: "SUPPRESS", UserAttributes: [
      { Name: "email", Value: state.email }, { Name: "email_verified", Value: "true" },
      { Name: "name", Value: "retained" },
    ] };
  const created = (await send(new AdminCreateUserCommand(input))).User;
  assert.equal(created.Username, state.username);
  assert.equal(created.UserStatus, "FORCE_CHANGE_PASSWORD");
  assert.equal(created.Enabled, true);
  state.sub = attrs(created.Attributes).sub;
  assert(state.sub && state.username !== state.email);
  assert.equal((await captures()).length, initialCaptureCount, "SUPPRESS must not emit a notification");
  await expectError(() => send(new AdminCreateUserCommand(input)), "UsernameExistsException");
  const fetched = await get(state.username);
  assert.equal(fetched.Username, state.username);
  assert.equal(fetched.UserStatus, "FORCE_CHANGE_PASSWORD");
  assert.equal(attrs(fetched.UserAttributes).email, state.email);
  assert.equal(attrs(fetched.UserAttributes).sub, state.sub);
  await expectError(() => get(`${runID}-missing`), "UserNotFoundException");
  await expectError(() => send(new AdminGetUserCommand({ UserPoolId: "us-east-1_missing", Username: state.username })), "ResourceNotFoundException");

  const forced = await adminLogin(state, temporary);
  assert.equal(forced.ChallengeName, "NEW_PASSWORD_REQUIRED");
  assert(forced.Session && !forced.AuthenticationResult);
  const completed = await send(new RespondToAuthChallengeCommand({
    ClientId: state.clientID, ChallengeName: "NEW_PASSWORD_REQUIRED", Session: forced.Session,
    ChallengeResponses: parameters(state, state.username, { NEW_PASSWORD: password }),
  }));
  await verifyTokens(completed.AuthenticationResult, state);
  assert.equal((await get(state.username)).UserStatus, "CONFIRMED");
  await expectError(() => adminLogin(state, temporary), "NotAuthorizedException");

  await setPassword(state.username, temporary, false);
  assert.equal((await get(state.username)).UserStatus, "FORCE_CHANGE_PASSWORD");
  await setPassword(state.username, password, true);
  assert.equal((await get(state.username)).UserStatus, "CONFIRMED");
  const login = await adminLogin(state, password);
  await verifyTokens(login.AuthenticationResult, state);
  state.refreshToken = login.AuthenticationResult.RefreshToken;
  state.accessToken = login.AuthenticationResult.AccessToken;
  const self = await send(new GetUserCommand({ AccessToken: state.accessToken }));
  assert.equal(self.Username, state.username);
  assert.equal(attrs(self.UserAttributes).email, state.email);
  const refreshed = await refresh(state, state.refreshToken);
  await verifyTokens(refreshed.AuthenticationResult, state, { refresh: false });
  assert.equal(refreshed.AuthenticationResult.RefreshToken, undefined);
  await expectError(() => send(new AdminInitiateAuthCommand({ UserPoolId: poolID, ClientId: state.clientID,
    AuthFlow: "ADMIN_USER_PASSWORD_AUTH", AuthParameters: { USERNAME: state.username, PASSWORD: password } })), "NotAuthorizedException");
  await expectError(() => send(new InitiateAuthCommand({ ClientId: state.clientID, AuthFlow: "REFRESH_TOKEN_AUTH",
    AuthParameters: { REFRESH_TOKEN: state.refreshToken, SECRET_HASH: "wrong" } })), "NotAuthorizedException");

  // Revoke a distinct grant so the main grant proves session isolation.
  const revokedGrant = (await adminLogin(state, password)).AuthenticationResult;
  const revokeInput = { ClientId: state.clientID, Token: revokedGrant.RefreshToken };
  await expectError(() => send(new RevokeTokenCommand(revokeInput)), "UnauthorizedException");
  const wrongSecret = (state.secret[0] === "A" ? "B" : "A") + state.secret.slice(1);
  await expectError(() => send(new RevokeTokenCommand({ ...revokeInput, ClientSecret: wrongSecret })), "UnauthorizedException");
  for (let repeat = 0; repeat < 2; repeat++) {
    const revoked = await send(new RevokeTokenCommand({ ...revokeInput, ClientSecret: state.secret }));
    assert.equal(revoked.$metadata.httpStatusCode, 200);
    assert(revoked.$metadata.requestId, "successful revocation must include request ID");
  }
  await expectError(() => refresh(state, revokedGrant.RefreshToken), "NotAuthorizedException");
  await expectError(() => send(new GetUserCommand({ AccessToken: revokedGrant.AccessToken })), "NotAuthorizedException");
  await verifyTokens((await refresh(state, state.refreshToken)).AuthenticationResult, state, { refresh: false });

  for (let repeat = 0; repeat < 2; repeat++) await send(new AdminDisableUserCommand({ UserPoolId: poolID, Username: state.username }));
  assert.equal((await get(state.username)).Enabled, false);
  await expectError(() => adminLogin(state, password), "NotAuthorizedException");
  await expectError(() => refresh(state, state.refreshToken), "NotAuthorizedException");
  for (let repeat = 0; repeat < 2; repeat++) await send(new AdminEnableUserCommand({ UserPoolId: poolID, Username: state.username }));
  assert.equal((await get(state.username)).Enabled, true);
  await verifyTokens((await adminLogin(state, password)).AuthenticationResult, state);

  // Fresh sign-in must succeed immediately after each global sign-out,
  // including when both tokens were issued within the same wall-clock second.
  const globalFirst = (await adminLogin(state, password)).AuthenticationResult;
  const globalSecond = (await adminLogin(state, password)).AuthenticationResult;
  await send(new GlobalSignOutCommand({ AccessToken: globalFirst.AccessToken }));
  for (const grant of [globalFirst, globalSecond]) {
    await expectError(() => send(new GetUserCommand({ AccessToken: grant.AccessToken })), "NotAuthorizedException");
    await expectError(() => refresh(state, grant.RefreshToken), "NotAuthorizedException");
  }
  const afterGlobal = (await adminLogin(state, password)).AuthenticationResult;
  await verifyTokens(afterGlobal, state);
  assert.equal((await send(new GetUserCommand({ AccessToken: afterGlobal.AccessToken }))).Username, state.username);
  await verifyTokens((await refresh(state, afterGlobal.RefreshToken)).AuthenticationResult, state, { refresh: false });
  await send(new AdminUserGlobalSignOutCommand({ UserPoolId: poolID, Username: state.username }));
  await expectError(() => send(new GetUserCommand({ AccessToken: afterGlobal.AccessToken })), "NotAuthorizedException");
  await expectError(() => refresh(state, afterGlobal.RefreshToken), "NotAuthorizedException");
  const afterAdminGlobal = (await adminLogin(state, password)).AuthenticationResult;
  await verifyTokens(afterAdminGlobal, state);
  assert.equal((await send(new GetUserCommand({ AccessToken: afterAdminGlobal.AccessToken }))).Username, state.username);
  state.refreshToken = afterAdminGlobal.RefreshToken;
  state.accessToken = afterAdminGlobal.AccessToken;

  const pageNames = [];
  for (let i = 0; i < 5; i++) {
    const username = `${runID}-page-${i}`;
    pageNames.push(username);
    await send(new AdminCreateUserCommand({ UserPoolId: poolID, Username: username,
      TemporaryPassword: temporary, MessageAction: "SUPPRESS",
      UserAttributes: [{ Name: "email", Value: `${username}@sdk-smoke.test` }] }));
  }
  await send(new AdminDisableUserCommand({ UserPoolId: poolID, Username: pageNames[1] }));
  const listing = { UserPoolId: poolID, Limit: 2, Filter: `email ^= "${runID}-page-"`, AttributesToGet: ["sub", "email"] };
  let token;
  const users = [];
  let firstToken;
  do {
    const page = await send(new ListUsersCommand({ ...listing, ...(token ? { PaginationToken: token } : {}) }));
    assert(page.Users.length <= 2);
    users.push(...page.Users);
    token = page.PaginationToken;
    firstToken ??= token;
  } while (token);
  assert.deepEqual(users.map(user => user.Username).sort(), pageNames.sort());
  assert.equal(new Set(users.map(user => attrs(user.Attributes).sub)).size, 5);
  assert.equal(users.find(user => user.Username === pageNames[1]).Enabled, false);
  assert(firstToken, "bounded listing must issue a cursor");
  await expectError(() => send(new ListUsersCommand({ ...listing, Filter: `email = "${state.email}"`, PaginationToken: firstToken })), "InvalidParameterException");
  await expectError(() => send(new ListUsersCommand({ UserPoolId: poolID, Limit: 61 })), "InvalidParameterException");

  const beforeResend = await get(state.username);
  const resent = (await send(new AdminCreateUserCommand({ ...input, MessageAction: "RESEND", TemporaryPassword: "ResendTemporary3!" }))).User;
  assert.equal(resent.Username, state.username);
  assert.equal(attrs(resent.Attributes).sub, state.sub);
  assert.equal(resent.UserCreateDate.getTime(), beforeResend.UserCreateDate.getTime());
  assert.equal(attrs(resent.Attributes)["name"], "retained");
  assert.equal(resent.UserStatus, "FORCE_CHANGE_PASSWORD");
  await expectError(() => adminLogin(state, password), "NotAuthorizedException");
  await setPassword(state.username, password, true);
  state.refreshToken = (await adminLogin(state, password)).AuthenticationResult.RefreshToken;

  const invitationName = `${runID}-reinvite`;
  const invitation = { ...input, Username: invitationName, UserAttributes: [{ Name: "email", Value: `${invitationName}@sdk-smoke.test` }] };
  const oldSub = attrs((await send(new AdminCreateUserCommand(invitation))).User.Attributes).sub;
  await send(new AdminDeleteUserCommand({ UserPoolId: poolID, Username: invitationName }));
  await send(new AdminDeleteUserCommand({ UserPoolId: poolID, Username: invitationName }));
  const newSub = attrs((await send(new AdminCreateUserCommand(invitation))).User.Attributes).sub;
  assert(newSub && newSub !== oldSub, "delete/reinvite must issue a new principal");
  await saveState(state);
}

async function restarted() {
  const state = await loadState();
  const user = await get(state.username);
  assert.equal(user.Username, state.username);
  assert.equal(attrs(user.UserAttributes).sub, state.sub);
  assert.equal(user.UserStatus, "CONFIRMED");
  assert.equal(user.Enabled, true);
  await verifyTokens((await adminLogin(state, state.password)).AuthenticationResult, state);
  await verifyTokens((await refresh(state, state.refreshToken)).AuthenticationResult, state, { refresh: false });
}

try {
  if (process.argv[2] === "restart") await restarted(); else await exercise();
  console.log("PASS");
} catch (error) {
  console.error(error);
  console.log("FAIL");
  process.exitCode = 1;
} finally { client.destroy(); }
