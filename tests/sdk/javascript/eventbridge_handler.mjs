import assert from "node:assert/strict";
import fs from "node:fs";
import { setTimeout as sleep } from "node:timers/promises";

function rows() {
  if (!fs.existsSync(process.env.EVENTS_OUTPUT)) return [];
  return fs.readFileSync(process.env.EVENTS_OUTPUT, "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line));
}
function append(phase, event, context) {
  fs.appendFileSync(process.env.EVENTS_OUTPUT, JSON.stringify({ phase, event, pid: process.pid,
    request_id: context.awsRequestId, function_arn: context.invokedFunctionArn }) + "\n");
}
export async function handler(event, context) {
  const priorTransformed = rows().filter(row => row.phase === "start" && row.event?.kind === "transformed").length;
  append("start", event, context);
  if (event?.kind === "transformed" && priorTransformed === 1) {
    const deadline = Date.now() + 90000;
    while (!fs.existsSync(process.env.EVENTS_GATE)) {
      assert(Date.now() < deadline, "owned transformed invocation gate deadline");
      await sleep(10);
    }
  }
  append("finish", event, context);
  return { observed: true };
}
