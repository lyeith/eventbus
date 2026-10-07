import assert from "node:assert/strict";
import { readFile, writeFile } from "node:fs/promises";
import {
  CognitoIdentityProviderClient, CreateUserPoolClientCommand, AdminCreateUserCommand,
  AdminSetUserPasswordCommand, InitiateAuthCommand, RespondToAuthChallengeCommand,
} from "@aws-sdk/client-cognito-identity-provider";
import { SchedulerClient, CreateScheduleCommand, GetScheduleCommand } from "@aws-sdk/client-scheduler";
import { beginSRP, passwordProof } from "./srp.mjs";

const root = process.env.STACK_ROOT;
const endpoint = process.env.STACK_SOURCE;
const poolID = process.env.STACK_POOL;
const phase = process.env.STACK_PHASE;
assert(root && endpoint && poolID && phase);
assert(["127.0.0.1", "localhost", "[::1]"].includes(new URL(endpoint).hostname));
const options = { endpoint, region: "us-east-1", maxAttempts: 1,
  credentials: { accessKeyId: "test", secretAccessKey: "testtest123" } };
const cognito = new CognitoIdentityProviderClient(options);
const scheduler = new SchedulerClient(options);
const send = command => cognito.send(command, { abortSignal: AbortSignal.timeout(10000) });
const save = (name, value) => writeFile(`${root}/${name}`, JSON.stringify(value), { mode: 0o600 });
const load = async name => JSON.parse(await readFile(`${root}/${name}`, "utf8"));
const password = "OwnedRetained9!";

async function begin(state) {
  const srp = beginSRP();
  const response = await send(new InitiateAuthCommand({ ClientId: state.clientID, AuthFlow: "CUSTOM_AUTH",
    AuthParameters: { USERNAME: state.username, CHALLENGE_NAME: "SRP_A", SRP_A: srp.SRP_A } }));
  assert.equal(response.ChallengeName, "PASSWORD_VERIFIER");
  const parameters = response.ChallengeParameters;
  const proof = passwordProof({ poolID, username: state.username, password, ...srp,
    B: parameters.SRP_B, salt: parameters.SALT, secretBlock: parameters.SECRET_BLOCK });
  return { response, proof };
}

try {
  if (phase === "provision") {
    const result = await send(new CreateUserPoolClientCommand({ UserPoolId: poolID, ClientName: "retained-stack",
      GenerateSecret: false, ExplicitAuthFlows: ["ALLOW_USER_PASSWORD_AUTH", "ALLOW_CUSTOM_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"] }));
    const clientID = result.UserPoolClient.ClientId;
    for (const username of ["stack-auth", "stack-cleanup", "stack-sentinel"]) {
      await send(new AdminCreateUserCommand({ UserPoolId: poolID, Username: username, TemporaryPassword: password,
        MessageAction: "SUPPRESS", UserAttributes: [{ Name: "email", Value: `${username}@sdk-smoke.test` }] }));
      await send(new AdminSetUserPasswordCommand({ UserPoolId: poolID, Username: username, Password: password, Permanent: true }));
    }
    const authenticated = await send(new InitiateAuthCommand({ ClientId: clientID, AuthFlow: "USER_PASSWORD_AUTH",
      AuthParameters: { USERNAME: "stack-auth", PASSWORD: password } }));
    assert(authenticated.AuthenticationResult.IdToken);
    const response = await fetch(`${endpoint}/${poolID}/.well-known/jwks.json`, { signal: AbortSignal.timeout(5000) });
    assert.equal(response.status, 200);
    await save("auth.json", { poolID, clientID, username: "stack-auth", cleanupUser: "stack-cleanup",
      issuer: `${endpoint}/${poolID}`, idToken: authenticated.AuthenticationResult.IdToken, jwks: await response.json() });
    console.log("native client/users provisioned; actual JWT and persisted JWKS retained");
  } else if (phase === "schedules") {
    const resources = await load("resources.json");
    const start = Date.now();
    const schedules = [];
    for (const [name, chain, delay] of [["stack-accepted", "scheduled", 1200], ["stack-future", "future", 4000]]) {
      const due = new Date(Math.ceil((start + delay) / 1000) * 1000);
      const request = { Name: name, ClientToken: `retained-${name}`, ScheduleExpression: `at(${due.toISOString().slice(0, 19)})`,
        ScheduleExpressionTimezone: "UTC", FlexibleTimeWindow: { Mode: "OFF" }, ActionAfterCompletion: "NONE",
        Target: { Arn: "arn:aws:lambda:us-east-1:000000000000:function:stack-scheduled:live",
          RoleArn: "arn:aws:iam::000000000000:role/scheduler", Input: JSON.stringify({ suite: resources.suite, chain }),
          RetryPolicy: { MaximumRetryAttempts: 0 } } };
      const created = await scheduler.send(new CreateScheduleCommand(request), { abortSignal: AbortSignal.timeout(5000) });
      assert.equal(created.$metadata.httpStatusCode, 200);
      schedules.push({ name, arn: created.ScheduleArn, expression: request.ScheduleExpression });
    }
    await save("schedules.json", schedules);
    console.log("native accepted/future schedule resources provisioned");
  } else if (phase === "lost-auth") {
    const state = await load("auth.json");
    const attempt = await begin(state);
    try {
      await send(new RespondToAuthChallengeCommand({ ClientId: state.clientID, ChallengeName: "PASSWORD_VERIFIER",
        Session: attempt.response.Session, ChallengeResponses: { USERNAME: state.username, ...attempt.proof } }));
      assert.fail("Cognito origin did not lose its accepted native reply");
    } catch (error) {
      assert(!error.$metadata?.httpStatusCode, `expected owned transport fault, got native refusal ${error.name}`);
      assert.notEqual(error.code, "ERR_ASSERTION");
    }
    console.log("actual Node Cognito SDK origin exited while independent Create trigger remained live");
  } else if (phase === "resume") {
    const state = await load("auth.json");
    const pending = await load("cognito-response.json");
    assert.equal(pending.ChallengeName, "CUSTOM_CHALLENGE");
    const captures = (await readFile(process.env.STACK_SES_CAPTURE, "utf8")).split("\n").filter(Boolean).map(JSON.parse);
    const email = captures.filter(row => row.api === "sesv2" && row.operation === "SendEmail").at(-1);
    const code = JSON.parse(email.request.Content.Simple.Body.Text.Data).code;
    const completed = await send(new RespondToAuthChallengeCommand({ ClientId: state.clientID, ChallengeName: "CUSTOM_CHALLENGE",
      Session: pending.Session, ChallengeResponses: { USERNAME: state.username, ANSWER: code } }));
    assert(completed.AuthenticationResult?.IdToken && completed.AuthenticationResult?.AccessToken);
    for (const schedule of await load("schedules.json")) {
      const readback = await scheduler.send(new GetScheduleCommand({ Name: schedule.name }), { abortSignal: AbortSignal.timeout(5000) });
      assert.equal(readback.Arn, schedule.arn);
      assert.equal(readback.ScheduleExpression, schedule.expression);
      assert.equal(readback.State, "ENABLED");
    }
    console.log("same pending challenge and native schedule resources survived fence/cleanup/resume");
  } else {
    assert.fail(`unknown retained-stack Node phase ${phase}`);
  }
  console.log("PASS");
} finally {
  cognito.destroy();
  scheduler.destroy();
}
