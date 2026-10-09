// Fixed opt-in fixture: imports once in the SDK driver; every registered trigger
// remains a real cold native handler. Provisioning is outside each auth sample.
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

const moduleStartedUnixMS = Date.now();
const importStarted = process.hrtime.bigint();
const {
  AdminCreateUserCommand, AdminSetUserPasswordCommand, CreateUserPoolClientCommand,
  InitiateAuthCommand, RespondToAuthChallengeCommand,
} = await import("@aws-sdk/client-cognito-identity-provider");
const { beginSRP, passwordProof } = await import("./srp.mjs");
const { attrs, captures, client, parameters, poolID, send, verifyTokens } = await import("./common.mjs");
const sdkImportNS = Number(process.hrtime.bigint() - importStarted);
const packageData = JSON.parse(await readFile(new URL("./node_modules/@aws-sdk/client-cognito-identity-provider/package.json", import.meta.url), "utf8"));
assert.equal(packageData.version, "3.1146.0");
const metric = row => console.log("PERF " + JSON.stringify(row));
metric({ schema_version: "eventbus.performance.sdk-import.v1", pid: process.pid,
  node_executable: process.execPath, node_version: process.version,
  sdk_version: packageData.version, module_started_unix_ms: moduleStartedUnixMS,
  sdk_import_ns: sdkImportNS });

async function measuredSend(command) {
  const started = process.hrtime.bigint();
  const response = await send(command);
  const elapsed = Number(process.hrtime.bigint() - started);
  assert.equal(response.$metadata.httpStatusCode, 200);
  assert(response.$metadata.requestId);
  return { response, elapsed };
}

try {
  const runID = process.env.SMOKE_RUN_ID;
  assert(runID);
  const createdClient = await send(new CreateUserPoolClientCommand({
    UserPoolId: poolID, ClientName: `performance-${runID}`, GenerateSecret: true,
    ExplicitAuthFlows: ["ALLOW_CUSTOM_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
  }));
  const state = { username: `${runID}-performance`, email: `${runID}-performance@sdk-smoke.test`,
    clientID: createdClient.UserPoolClient.ClientId, secret: createdClient.UserPoolClient.ClientSecret,
    password: "PermanentPass2!" };
  assert(state.clientID && state.secret);
  const created = await send(new AdminCreateUserCommand({
    UserPoolId: poolID, Username: state.username, TemporaryPassword: "TemporaryPass1!", MessageAction: "SUPPRESS",
    UserAttributes: [{ Name: "email", Value: state.email }, { Name: "email_verified", Value: "true" }],
  }));
  state.sub = attrs(created.User.Attributes).sub;
  assert(state.sub);
  await send(new AdminSetUserPasswordCommand({ UserPoolId: poolID, Username: state.username,
    Password: state.password, Permanent: true }));
  assert.equal((await captures()).length, 0);

  for (let sample = 1; sample <= 3; sample++) {
    const flowStarted = process.hrtime.bigint();
    const srpStarted = process.hrtime.bigint();
    const srp = beginSRP();
    let srpNS = Number(process.hrtime.bigint() - srpStarted);
    const initial = await measuredSend(new InitiateAuthCommand({
      ClientId: state.clientID, AuthFlow: "CUSTOM_AUTH",
      AuthParameters: parameters(state, state.username, { CHALLENGE_NAME: "SRP_A", SRP_A: srp.SRP_A }),
    }));
    assert.equal(initial.response.ChallengeName, "PASSWORD_VERIFIER");
    assert(initial.response.Session && !initial.response.AuthenticationResult);
    const challenge = initial.response.ChallengeParameters;
    assert.equal(challenge.USER_ID_FOR_SRP, state.username);
    const proofStarted = process.hrtime.bigint();
    const proof = passwordProof({ poolID, username: challenge.USER_ID_FOR_SRP, password: state.password,
      ...srp, B: challenge.SRP_B, salt: challenge.SALT, secretBlock: challenge.SECRET_BLOCK });
    srpNS += Number(process.hrtime.bigint() - proofStarted);
    const password = await measuredSend(new RespondToAuthChallengeCommand({
      ClientId: state.clientID, ChallengeName: "PASSWORD_VERIFIER", Session: initial.response.Session,
      ChallengeResponses: parameters(state, proof.USERNAME, proof),
    }));
    assert.equal(password.response.ChallengeName, "CUSTOM_CHALLENGE");
    assert(password.response.Session && !password.response.AuthenticationResult);
    assert.equal(password.response.ChallengeParameters.delivery, "email");
    const captureStarted = process.hrtime.bigint();
    const emailRecords = await captures();
    assert.equal(emailRecords.length, sample, "one captured native SES send per successful auth chain");
    const lastEmail = emailRecords.at(-1);
    assert.equal(lastEmail.api, "sesv2");
    assert.equal(lastEmail.operation, "SendEmail");
    assert.equal(lastEmail.outcome.http_status, 200);
    assert(lastEmail.request.Destination.ToAddresses.includes(state.email));
    const email = JSON.parse(lastEmail.request.Content.Simple.Body.Text.Data);
    assert.equal(email.username, state.username);
    assert(/^\d{6}$/.test(email.code));
    const captureNS = Number(process.hrtime.bigint() - captureStarted);
    const answer = await measuredSend(new RespondToAuthChallengeCommand({
      ClientId: state.clientID, ChallengeName: "CUSTOM_CHALLENGE", Session: password.response.Session,
      ChallengeResponses: parameters(state, state.username, { ANSWER: email.code }),
    }));
    const flowNS = Number(process.hrtime.bigint() - flowStarted);
    assert(answer.response.AuthenticationResult && !answer.response.ChallengeName);
    const validationStarted = process.hrtime.bigint();
    await verifyTokens(answer.response.AuthenticationResult, state);
    const validationNS = Number(process.hrtime.bigint() - validationStarted);
    metric({ schema_version: "eventbus.performance.sdk-auth.v1", sample,
      native_auth_http_calls: 3, expected_cold_trigger_invocations: 5, captured_email_count: emailRecords.length,
      flow_ns: flowNS, initiate_auth_http_ns: initial.elapsed, password_verifier_http_ns: password.elapsed,
      custom_challenge_http_ns: answer.elapsed, native_http_ns: initial.elapsed + password.elapsed + answer.elapsed,
      client_srp_ns: srpNS, capture_lookup_ns: captureNS, token_validation_ns: validationNS });
  }
  console.log("PASS");
} catch (error) {
  // Never print challenge answers, secrets, tokens or SDK request objects.
  console.error("performance auth failed: " + (error?.name === "AssertionError" ? "assertion" : "operation"));
  console.log("FAIL");
  process.exitCode = 1;
} finally {
  client.destroy();
}
