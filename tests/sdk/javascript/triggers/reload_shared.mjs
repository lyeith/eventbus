// One actual application handler is configured for all three native trigger
// names. Reloading any one must replace their shared imported module together.
import { randomInt, timingSafeEqual } from "node:crypto";
import fs from "node:fs";
import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
const VERSION = "v1";
let count = 0;

export async function handler(event, context) {
  count++;
  fs.appendFileSync(process.env.RELOAD_OBSERVATIONS, JSON.stringify({
    version: VERSION, count, pid: process.pid,
    pool_id: event.userPoolId ?? "", trigger_source: event.triggerSource ?? "",
    function_name: context.functionName, setting: process.env.RELOAD_SETTING,
  }) + "\n");
  if (!event.triggerSource) {
    return { version: VERSION, count, pid: process.pid, setting: process.env.RELOAD_SETTING, event };
  }
  if (event.triggerSource === "DefineAuthChallenge_Authentication") {
    const last = (event.request.session ?? []).at(-1);
    event.response = { issueTokens: false, failAuthentication: false };
    if (event.request.userNotFound || !last?.challengeResult) {
      event.response.failAuthentication = true;
    } else if (last.challengeName === "SRP_A") {
      event.response.challengeName = "PASSWORD_VERIFIER";
    } else if (last.challengeName === "PASSWORD_VERIFIER") {
      event.response.challengeName = "CUSTOM_CHALLENGE";
    } else if (last.challengeName === "CUSTOM_CHALLENGE") {
      event.response.issueTokens = true;
    } else {
      throw new Error("unexpected challenge");
    }
  } else if (event.triggerSource === "CreateAuthChallenge_Authentication") {
    const { SESv2Client, SendEmailCommand } = require(process.env.RELOAD_SES_PACKAGE);
    const code = String(randomInt(100000, 1000000));
    const client = new SESv2Client({
      endpoint: process.env.SES_ENDPOINT_URL, region: "us-east-1", maxAttempts: 1,
      credentials: { accessKeyId: "reload-fixture", secretAccessKey: "reload-fixture" },
    });
    try {
      await client.send(new SendEmailCommand({
        FromEmailAddress: "verification@eventbus.test",
        Destination: { ToAddresses: [event.request.userAttributes.email] },
        Content: { Simple: {
          Subject: { Data: "Cognito reload SDK challenge" },
          Body: { Text: { Data: JSON.stringify({ username: event.userName, code, version: VERSION }) } },
        } },
      }), { abortSignal: AbortSignal.timeout(5000) });
    } finally { client.destroy(); }
    event.response = {
      publicChallengeParameters: { delivery: "email", version: VERSION, setting: process.env.RELOAD_SETTING },
      privateChallengeParameters: { answer: code },
      challengeMetadata: "EMAIL_CODE",
    };
  } else if (event.triggerSource === "VerifyAuthChallengeResponse_Authentication") {
    const expected = Buffer.from(event.request.privateChallengeParameters.answer ?? "");
    const answer = Buffer.from(event.request.challengeAnswer ?? "");
    event.response = { answerCorrect: expected.length === answer.length && timingSafeEqual(expected, answer) };
  } else {
    throw new Error("unexpected trigger");
  }
  return event;
}
