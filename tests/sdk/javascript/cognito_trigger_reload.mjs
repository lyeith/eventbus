import assert from "node:assert/strict";
import fs from "node:fs/promises";
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";
const require = createRequire(import.meta.url);
const {
  AdminCreateUserCommand, AdminSetUserPasswordCommand, AdminGetUserCommand,
  CreateUserPoolClientCommand, DescribeUserPoolClientCommand,
  InitiateAuthCommand, RespondToAuthChallengeCommand, GetUserCommand,
} = require(process.env.RELOAD_COGNITO_PACKAGE);
const { beginSRP, passwordProof } = await import(pathToFileURL(process.env.RELOAD_SRP_HELPER));
const { attrs, captures, client, send, parameters, secretHash, verifyTokens } =
  await import(pathToFileURL(process.env.RELOAD_COMMON_HELPER));
const endpoint = process.env.COGNITO_ENDPOINT_URL;
const poolA = process.env.COGNITO_USER_POOL_ID;
const poolB = process.env.RELOAD_OTHER_POOL;
const observationsPath = process.env.RELOAD_OBSERVATIONS;
const readObservations = async () => (await fs.readFile(observationsPath, "utf8"))
  .trim().split("\n").filter(Boolean).map(line => JSON.parse(line));
const poolObservations = async pool => (await readObservations()).filter(row => row.pool_id === pool);
const fetchJSON = async path => {
  const response = await fetch(endpoint + path, { signal: AbortSignal.timeout(10000) });
  assert.equal(response.status, 200);
  return response.json();
};

async function provision(pool, suffix) {
  const app = (await send(new CreateUserPoolClientCommand({
    UserPoolId: pool, ClientName: "reload-client-" + suffix, GenerateSecret: true,
    ExplicitAuthFlows: ["ALLOW_CUSTOM_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
  }))).UserPoolClient;
  const state = { pool, username: "reload-user-" + suffix,
    email: "reload-user-" + suffix + "@sdk-smoke.test", password: "PermanentReload2!",
    clientID: app.ClientId, secret: app.ClientSecret };
  const user = (await send(new AdminCreateUserCommand({
    UserPoolId: pool, Username: state.username, TemporaryPassword: "TemporaryReload1!",
    MessageAction: "SUPPRESS", UserAttributes: [
      { Name: "email", Value: state.email }, { Name: "email_verified", Value: "true" },
    ],
  }))).User;
  state.sub = attrs(user.Attributes).sub;
  assert(state.sub && state.secret);
  await send(new AdminSetUserPasswordCommand({
    UserPoolId: pool, Username: state.username, Password: state.password, Permanent: true,
  }));
  return state;
}
async function challenge(state, version) {
  const srp = beginSRP();
  const initial = await send(new InitiateAuthCommand({
    ClientId: state.clientID, AuthFlow: "CUSTOM_AUTH",
    AuthParameters: parameters(state, state.username, { CHALLENGE_NAME: "SRP_A", SRP_A: srp.SRP_A }),
  }));
  assert.equal(initial.ChallengeName, "PASSWORD_VERIFIER");
  const values = initial.ChallengeParameters;
  const proof = passwordProof({
    poolID: state.pool, username: values.USER_ID_FOR_SRP, password: state.password,
    ...srp, B: values.SRP_B, salt: values.SALT, secretBlock: values.SECRET_BLOCK,
  });
  const pending = await send(new RespondToAuthChallengeCommand({
    ClientId: state.clientID, ChallengeName: "PASSWORD_VERIFIER", Session: initial.Session,
    ChallengeResponses: parameters(state, proof.USERNAME, proof),
  }));
  assert.equal(pending.ChallengeName, "CUSTOM_CHALLENGE");
  assert(pending.Session && !pending.AuthenticationResult);
  assert.deepEqual(pending.ChallengeParameters, { delivery: "email", version, setting: "unchanged", USERNAME: state.username });
  const emails = (await captures()).filter(row =>
    row.api === "sesv2" && row.operation === "SendEmail" && row.outcome.http_status === 200 &&
    row.request.Destination.ToAddresses.includes(state.email));
  assert(emails.length);
  const email = JSON.parse(emails.at(-1).request.Content.Simple.Body.Text.Data);
  assert.equal(email.username, state.username);
  assert.equal(email.version, version);
  assert(/^\d{6}$/.test(email.code));
  assert(!JSON.stringify(pending).includes('"answer"'));
  return { session: pending.Session, code: email.code };
}
async function answer(state, pending) {
  const completed = await send(new RespondToAuthChallengeCommand({
    ClientId: state.clientID, ChallengeName: "CUSTOM_CHALLENGE", Session: pending.session,
    ChallengeResponses: parameters(state, state.username, { ANSWER: pending.code }),
  }));
  assert(completed.AuthenticationResult?.AccessToken && completed.AuthenticationResult.RefreshToken);
  const self = await send(new GetUserCommand({ AccessToken: completed.AuthenticationResult.AccessToken }));
  assert.equal(self.Username, state.username);
  assert.equal(attrs(self.UserAttributes).sub, state.sub);
  return completed.AuthenticationResult;
}
async function publicInvoke(label) {
  const response = await fetch(endpoint + "/2015-03-31/functions/public-shared/invocations", {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ label }), signal: AbortSignal.timeout(10000),
  });
  assert.equal(response.status, 200);
  assert.equal(response.headers.get("x-amz-function-error"), null);
  return response.json();
}

try {
  const stateA = await provision(poolA, "A");
  const stateB = await provision(poolB, "B");
  const beforeA = await answer(stateA, await challenge(stateA, "v1"));
  await verifyTokens(beforeA, stateA);
  const beforeB = await answer(stateB, await challenge(stateB, "v1"));
  const pendingA = await challenge(stateA, "v1");
  const jwksBefore = await fetchJSON("/" + poolA + "/.well-known/jwks.json");
  const clientBefore = (await send(new DescribeUserPoolClientCommand({
    UserPoolId: poolA, ClientId: stateA.clientID,
  }))).UserPoolClient;
  const userBefore = await send(new AdminGetUserCommand({ UserPoolId: poolA, Username: stateA.username }));
  const observedA = await poolObservations(poolA);
  const observedB = await poolObservations(poolB);
  assert(observedA.length && observedB.length);
  assert(observedA.every(row => row.version === "v1" && row.pid === observedA[0].pid));
  assert(observedB.every(row => row.version === "v1" && row.pid === observedB[0].pid));
  const publicBefore = await publicInvoke("before-private-reload");
  assert.equal(publicBefore.version, "v1");
  assert.equal(publicBefore.count, 1);

  const source = await fs.readFile(process.env.RELOAD_SHARED_SOURCE, "utf8");
  assert(source.includes('const VERSION = "v1";'));
  await fs.writeFile(process.env.RELOAD_SHARED_SOURCE,
    source.replace('const VERSION = "v1";', 'const VERSION = "v2";'));
  const response = await fetch(endpoint + "/__eventbus/dev/cognito/pools/" +
    poolA + "/triggers/CreateAuthChallenge/reload", {
    method: "POST", signal: AbortSignal.timeout(10000),
  });
  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), {
    pool_id: poolA, trigger: "CreateAuthChallenge", status: "reloaded",
  });
  assert.throws(() => process.kill(observedA[0].pid, 0), { code: "ESRCH" },
    "reload must join the retired selected worker before returning success");
  process.kill(observedB[0].pid, 0);
  process.kill(publicBefore.pid, 0);

  // Session was signed and its code captured before source reload. New Verify
  // and Define handlers complete that unchanged native pending session.
  await verifyTokens(await answer(stateA, pendingA), stateA);
  const afterA = await answer(stateA, await challenge(stateA, "v2"));
  await verifyTokens(afterA, stateA);
  const changedA = (await poolObservations(poolA)).slice(observedA.length);
  assert(changedA.length);
  assert(changedA.every(row => row.version === "v2" && row.pid === changedA[0].pid));
  assert.notEqual(changedA[0].pid, observedA[0].pid);
  assert.deepEqual(changedA.map(row => row.count), changedA.map((_, index) => index + 1),
    "shared Define/Create/Verify must use one freshly imported module");
  assert.deepEqual(new Set(changedA.map(row => row.trigger_source)), new Set([
    "DefineAuthChallenge_Authentication", "CreateAuthChallenge_Authentication",
    "VerifyAuthChallengeResponse_Authentication",
  ]));
  assert(changedA.every(row => row.setting === "unchanged" && row.function_name === "handler"));

  await answer(stateB, await challenge(stateB, "v1"));
  const unchangedB = (await poolObservations(poolB)).slice(observedB.length);
  assert(unchangedB.length);
  assert(unchangedB.every(row => row.version === "v1" && row.pid === observedB[0].pid));
  assert.equal(unchangedB[0].count, observedB.at(-1).count + 1);
  const publicAfter = await publicInvoke("after-private-reload");
  assert.equal(publicAfter.version, "v1");
  assert.equal(publicAfter.pid, publicBefore.pid);
  assert.equal(publicAfter.count, 2);
  assert.equal(publicAfter.setting, "unchanged");
  assert.deepEqual(publicAfter.event, { label: "after-private-reload" });

  assert.deepEqual(await fetchJSON("/" + poolA + "/.well-known/jwks.json"), jwksBefore);
  const clientAfter = (await send(new DescribeUserPoolClientCommand({
    UserPoolId: poolA, ClientId: stateA.clientID,
  }))).UserPoolClient;
  assert.deepEqual(clientAfter, clientBefore);
  const userAfter = await send(new AdminGetUserCommand({ UserPoolId: poolA, Username: stateA.username }));
  assert.deepEqual(userAfter.UserAttributes, userBefore.UserAttributes);
  assert.equal(userAfter.UserStatus, userBefore.UserStatus);
  assert.equal(userAfter.Enabled, userBefore.Enabled);
  for (const [state, tokens] of [[stateA, beforeA], [stateB, beforeB]]) {
    const self = await send(new GetUserCommand({ AccessToken: tokens.AccessToken }));
    assert.equal(self.Username, state.username);
    assert.equal(attrs(self.UserAttributes).sub, state.sub);
  }
  await verifyTokens(beforeA, stateA);
  const refreshed = await send(new InitiateAuthCommand({
    ClientId: stateA.clientID, AuthFlow: "REFRESH_TOKEN_AUTH",
    AuthParameters: { REFRESH_TOKEN: beforeA.RefreshToken,
      SECRET_HASH: secretHash(stateA.secret, stateA.username, stateA.clientID) },
  }));
  await verifyTokens(refreshed.AuthenticationResult, stateA, { refresh: false });
  console.log("PASS");
} catch (error) {
  console.error(error);
  console.log("FAIL");
  process.exitCode = 1;
} finally { client.destroy(); }
