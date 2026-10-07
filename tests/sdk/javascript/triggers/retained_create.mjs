// Gate/evidence wrapper around the existing actual SESv2 challenge handler.
// Challenge contents and SDK sending remain in the unchanged application fixture.
import { access, rename, writeFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import { handler as createChallenge } from "./create.mjs";

export async function handler(event) {
  const requestID = randomUUID();
  async function record(stage) {
    const observation = { stage, chain: "cognito", pid: process.pid, request_id: requestID };
    const path = `${process.env.STACK_ROOT}/row-${stage}-${requestID}.json`;
    await writeFile(path + ".tmp", JSON.stringify(observation), { mode: 0o600 });
    await rename(path + ".tmp", path);
    const response = await fetch(process.env.STACK_CONTROL + "/__fixture/observed", {
      method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(observation),
      signal: AbortSignal.timeout(3000),
    });
    if (response.status !== 204) throw Error("owned trigger observation failed");
  }
  await record("trigger-started");
  for (;;) {
    try { await access(`${process.env.STACK_ROOT}/release-cognito`); break; }
    catch (error) { if (error.code !== "ENOENT") throw error; }
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  const result = await createChallenge(event);
  await record("trigger-completed");
  return result;
}
