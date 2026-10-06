import { randomInt } from "node:crypto";
import { SESv2Client, SendEmailCommand } from "@aws-sdk/client-sesv2";

export async function handler(event) {
  if (event.request.challengeName !== "CUSTOM_CHALLENGE") {
    throw new Error("Create fixture requires CUSTOM_CHALLENGE");
  }
  const code = String(randomInt(100000, 1000000));
  const client = new SESv2Client({
    endpoint: process.env.SES_ENDPOINT_URL,
    region: "us-east-1",
    credentials: { accessKeyId: "sdk-test", secretAccessKey: "sdk-test" },
    maxAttempts: 1,
  });
  try {
    await client.send(new SendEmailCommand({
      FromEmailAddress: "verification@eventbus.test",
      Destination: { ToAddresses: [event.request.userAttributes.email] },
      Content: {
        Simple: {
          Subject: { Data: "EventBus SDK challenge" },
          Body: { Text: { Data: JSON.stringify({ username: event.userName, code }) } },
        },
      },
    }), { abortSignal: AbortSignal.timeout(5000) });
  } finally {
    client.destroy();
  }
  event.response = {
    publicChallengeParameters: { delivery: "email" },
    privateChallengeParameters: { answer: code },
    challengeMetadata: "EMAIL_CODE",
  };
  return event;
}
