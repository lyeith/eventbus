import assert from "node:assert/strict";
import {
  AdminCreateUserCommand, AdminDisableUserCommand, AdminEnableUserCommand,
  AdminSetUserPasswordCommand, InitiateAuthCommand, RespondToAuthChallengeCommand,
} from "@aws-sdk/client-cognito-identity-provider";
import { beginSRP, passwordProof, timestamp } from "./srp.mjs";
import { attrs, captures, client, expectError, loadState, parameters, poolID, saveState, send, verifyTokens } from "./common.mjs";

async function begin(state, username = state.username) {
  const srp = beginSRP();
  const response = await send(new InitiateAuthCommand({
    ClientId: state.clientID, AuthFlow: "CUSTOM_AUTH",
    AuthParameters: parameters(state, username, { CHALLENGE_NAME: "SRP_A", SRP_A: srp.SRP_A }),
  }));
  assert.equal(response.ChallengeName, "PASSWORD_VERIFIER");
  assert(response.Session && !response.AuthenticationResult);
  const values = response.ChallengeParameters;
  assert.equal(values.USER_ID_FOR_SRP, state.username);
  assert(values.SRP_B && values.SALT && values.SECRET_BLOCK);
  const proof = passwordProof({ poolID, username: values.USER_ID_FOR_SRP, password: state.password,
    ...srp, B: values.SRP_B, salt: values.SALT, secretBlock: values.SECRET_BLOCK });
  return { response, proof, srp, values };
}
function respondPassword(state, attempt, proof = attempt.proof, session = attempt.response.Session) {
  return send(new RespondToAuthChallengeCommand({
    ClientId: state.clientID, ChallengeName: "PASSWORD_VERIFIER", Session: session,
    ChallengeResponses: parameters(state, proof.USERNAME, proof),
  }));
}
async function codeFor(state) {
  const records = await captures();
  for (const record of records.toReversed()) {
    if (record.api !== "sesv2" || record.operation !== "SendEmail" || record.outcome.http_status !== 200) continue;
    if (!record.request.Destination.ToAddresses.includes(state.email)) continue;
    const data = JSON.parse(record.request.Content.Simple.Body.Text.Data);
    if (data.username === state.username) {
      assert(/^\d{6}$/.test(data.code), "SES capture must contain the fixture code");
      return data.code;
    }
  }
  assert.fail("successful custom challenge must be visible synchronously in SES JSONL");
}
async function challenge(state) {
  const attempt = await begin(state);
  const response = await respondPassword(state, attempt);
  assert.equal(response.ChallengeName, "CUSTOM_CHALLENGE");
  assert(response.Session && response.Session !== attempt.response.Session);
  assert(!response.AuthenticationResult);
  assert.equal(response.ChallengeParameters.delivery, "email");
  assert(!JSON.stringify(response).includes('"answer"'), "private challenge parameters must remain private");
  return { response, code: await codeFor(state), attempt };
}
function answer(state, pending, value = pending.code, username = state.username) {
  return send(new RespondToAuthChallengeCommand({
    ClientId: state.clientID, ChallengeName: "CUSTOM_CHALLENGE", Session: pending.response.Session,
    ChallengeResponses: parameters(state, username, { ANSWER: value }),
  }));
}

async function exercise(state) {
  const beforeWrongPassword = (await captures()).length;
  const badPassword = await begin(state);
  const wrongProof = passwordProof({ poolID, username: state.username, password: "WrongPassword9!",
    ...badPassword.srp, B: badPassword.values.SRP_B, salt: badPassword.values.SALT, secretBlock: badPassword.values.SECRET_BLOCK });
  await expectError(() => respondPassword(state, badPassword, wrongProof), "NotAuthorizedException");
  assert.equal((await captures()).length, beforeWrongPassword, "a wrong password must never reach CreateAuthChallenge");

  const wrongSignature = await begin(state);
  await expectError(() => respondPassword(state, wrongSignature, { ...wrongSignature.proof,
    PASSWORD_CLAIM_SIGNATURE: Buffer.alloc(32).toString("base64") }), "NotAuthorizedException");
  const wrongBlock = await begin(state);
  await expectError(() => respondPassword(state, wrongBlock, { ...wrongBlock.proof,
    PASSWORD_CLAIM_SECRET_BLOCK: Buffer.alloc(32).toString("base64") }), "NotAuthorizedException");
  const staleTimestamp = await begin(state);
  const oldProof = passwordProof({ poolID, username: state.username, password: state.password,
    ...staleTimestamp.srp, B: staleTimestamp.values.SRP_B, salt: staleTimestamp.values.SALT,
    secretBlock: staleTimestamp.values.SECRET_BLOCK, time: timestamp(new Date(Date.now() - 10 * 60 * 1000)) });
  await expectError(() => respondPassword(state, staleTimestamp, oldProof), "NotAuthorizedException");

  const forged = await begin(state);
  const position = Math.floor(forged.response.Session.length / 2);
  const original = forged.response.Session[position];
  const altered = forged.response.Session.slice(0, position) + (original === "A" ? "B" : "A") + forged.response.Session.slice(position + 1);
  await expectError(() => respondPassword(state, forged, forged.proof, altered), "NotAuthorizedException");
  const pending = await challenge(state);
  const completed = await answer(state, pending);
  await verifyTokens(completed.AuthenticationResult, state);
  await expectError(() => answer(state, pending), "NotAuthorizedException");
  await expectError(() => respondPassword(state, pending.attempt), "NotAuthorizedException");

  const wrongAnswer = await challenge(state);
  await expectError(() => answer(state, wrongAnswer, "000000"), "NotAuthorizedException");
  const otherName = state.username + "-other";
  const other = (await send(new AdminCreateUserCommand({ UserPoolId: poolID, Username: otherName,
    TemporaryPassword: state.password, MessageAction: "SUPPRESS",
    UserAttributes: [{ Name: "email", Value: `${otherName}@sdk-smoke.test` }] }))).User;
  assert(attrs(other.Attributes).sub !== state.sub);
  await send(new AdminSetUserPasswordCommand({ UserPoolId: poolID, Username: otherName, Password: state.password, Permanent: true }));
  const crossUser = await challenge(state);
  await expectError(() => answer(state, crossUser, crossUser.code, otherName), "NotAuthorizedException");

  const disabled = await challenge(state);
  await send(new AdminDisableUserCommand({ UserPoolId: poolID, Username: state.username }));
  await expectError(() => answer(state, disabled), "NotAuthorizedException");
  await expectError(() => begin(state), "NotAuthorizedException");
  await send(new AdminEnableUserCommand({ UserPoolId: poolID, Username: state.username }));
  await expectError(() => answer(state, disabled), "NotAuthorizedException");
  const finalLogin = (await answer(state, await challenge(state))).AuthenticationResult;
  await verifyTokens(finalLogin, state);
  state.refreshToken = finalLogin.RefreshToken;
  state.accessToken = finalLogin.AccessToken;
  await saveState(state);
}

try {
  const state = await loadState();
  const phase = process.argv[2] ?? "exercise";
  if (phase === "exercise") {
    await exercise(state);
  } else if (phase === "prepare-restart" || phase === "prepare-expiry") {
    const pending = await challenge(state);
    state.pending = { response: pending.response, code: pending.code };
    await saveState(state);
  } else if (phase === "resume-restart") {
    await verifyTokens((await answer(state, state.pending)).AuthenticationResult, state);
    await expectError(() => answer(state, state.pending), "NotAuthorizedException");
  } else if (phase === "assert-expiry") {
    await expectError(() => answer(state, state.pending), "NotAuthorizedException");
  } else if (phase === "trigger-error") {
    await expectError(() => begin(state), process.env.EXPECT_TRIGGER_ERROR);
  } else {
    assert.fail(`Unknown custom-auth fixture phase ${phase}`);
  }
  console.log("PASS");
} catch (error) {
  console.error(error);
  console.log("FAIL");
  process.exitCode = 1;
} finally { client.destroy(); }
