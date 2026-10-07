import assert from "node:assert/strict";
import { readFile, writeFile } from "node:fs/promises";
import { setTimeout as sleep } from "node:timers/promises";
import {
  SchedulerClient, CreateScheduleCommand, GetScheduleCommand, DeleteScheduleCommand,
} from "@aws-sdk/client-scheduler";

const endpoints = [process.env.SCHEDULER_ENDPOINT_URL, process.env.SCHEDULER_OTHER_ENDPOINT_URL];
for (const endpoint of endpoints) {
  assert(endpoint, "owned Scheduler endpoint is required");
  assert(["127.0.0.1", "localhost", "[::1]"].includes(new URL(endpoint).hostname));
}
assert.notEqual(endpoints[0], endpoints[1], "instance isolation requires separate owned listeners");
const clients = endpoints.map(endpoint => new SchedulerClient({
  endpoint, region: "us-east-1", maxAttempts: 1,
  credentials: { accessKeyId: "sdk-test", secretAccessKey: "sdk-test" },
}));
const files = [process.env.SCHEDULER_OUTPUT_FILE, process.env.SCHEDULER_OTHER_OUTPUT_FILE];
assert(files.every(Boolean) && process.env.SCHEDULER_GATE_FILE);
const targetARN = "arn:aws:lambda:us-east-1:000000000000:function:scheduled:live";
const roleARN = "arn:aws:iam::000000000000:role/local/scheduler";
const dueDate = () => new Date(Math.ceil(Date.now() / 1000) * 1000 + 2500);
const expression = date => `at(${date.toISOString().slice(0, 19)})`;
const request = (name, token, date, payload, extra = {}) => ({
  Name: name, ClientToken: token, ScheduleExpression: expression(date),
  ScheduleExpressionTimezone: "UTC", FlexibleTimeWindow: { Mode: "OFF" },
  ActionAfterCompletion: "DELETE",
  Target: { Arn: targetARN, RoleArn: roleARN, Input: JSON.stringify(payload), RetryPolicy: { MaximumRetryAttempts: 0 } },
  ...extra,
});
async function send(index, command) {
  const result = await clients[index].send(command, { abortSignal: AbortSignal.timeout(5000) });
  assert.equal(result.$metadata.httpStatusCode, 200);
  assert(result.$metadata.requestId, "successful AWS responses require request IDs");
  return result;
}
async function expectError(operation, name, status) {
  try { await operation(); } catch (error) {
    assert.equal(error.name, name, error.message);
    assert.equal(error.$metadata.httpStatusCode, status);
    assert(error.$metadata.requestId, "typed native errors require request IDs");
    return error;
  }
  assert.fail(`expected ${name}`);
}
async function records(index) {
  let text;
  try { text = await readFile(files[index], "utf8"); } catch (error) {
    if (error.code === "ENOENT") return [];
    throw error;
  }
  assert(!text || text.endsWith("\n"), "handler evidence must have complete JSONL records");
  return text.split("\n").filter(Boolean).map(line => JSON.parse(line));
}
async function eventually(operation, description) {
  const deadline = Date.now() + 10000;
  do {
    if (await operation()) return;
    await sleep(25);
  } while (Date.now() < deadline);
  assert.fail(description);
}
async function deleted(index, name, group) {
  try { await send(index, new GetScheduleCommand({ Name: name, GroupName: group })); return false; }
  catch (error) {
    assert.equal(error.name, "ResourceNotFoundException");
    assert.equal(error.$metadata.httpStatusCode, 404);
    assert(error.$metadata.requestId);
    return true;
  }
}

try {
  const payload = { id: "first", block: true, nested: { count: 7, text: "exact JSON input" }, values: [null, false, "界"] };
  const first = request("same-name", "first-token", dueDate(), payload, { GroupName: "owned", Description: "SDK one-time alias" });
  const created = await send(0, new CreateScheduleCommand(first));
  assert.equal(created.ScheduleArn, "arn:aws:scheduler:us-east-1:000000000000:schedule/owned/same-name");
  const retry = await send(0, new CreateScheduleCommand(first));
  assert.equal(retry.ScheduleArn, created.ScheduleArn);
  const readback = await send(0, new GetScheduleCommand({ Name: first.Name, GroupName: first.GroupName }));
  assert.equal(readback.Arn, created.ScheduleArn);
  assert.equal(readback.Name, first.Name);
  assert.equal(readback.GroupName, "owned");
  assert.equal(readback.ScheduleExpression, first.ScheduleExpression);
  assert.equal(readback.ScheduleExpressionTimezone, "UTC");
  assert.equal(readback.State, "ENABLED");
  assert.equal(readback.ActionAfterCompletion, "DELETE");
  assert.deepEqual(readback.FlexibleTimeWindow, { Mode: "OFF" });
  assert.equal(readback.Target.Arn, targetARN);
  assert.equal(readback.Target.RoleArn, roleARN);
  assert.equal(readback.Target.Input, first.Target.Input);
  assert.equal(readback.Target.RetryPolicy.MaximumRetryAttempts, 0);
  assert(readback.CreationDate instanceof Date && readback.LastModificationDate instanceof Date);
  await expectError(() => send(1, new GetScheduleCommand({ Name: first.Name, GroupName: "owned" })), "ResourceNotFoundException", 404);
  await eventually(() => deleted(0, first.Name, "owned"), "schedule was not deleted after Lambda admission");
  assert.deepEqual(await records(0), [], "completion means API admission while the actual handler is still gated");
  await writeFile(process.env.SCHEDULER_GATE_FILE, "execute", { mode: 0o600 });
  await eventually(async () => (await records(0)).length === 1, "actual registered Node alias did not receive the input");
  assert.deepEqual(await records(0), [{ kind: "alias", event: payload }]);
  assert.equal((await send(0, new CreateScheduleCommand(first))).ScheduleArn, created.ScheduleArn);
  assert(await deleted(0, first.Name, "owned"), "old Create token must not resurrect completed work");
  await expectError(() => send(0, new CreateScheduleCommand({ ...first, Description: "changed request" })), "ConflictException", 409);

  const due = dueDate();
  const isolatedPayload = { id: "second", nested: { instance: "independent" } };
  const isolated = request("same-name", "second-token", due, isolatedPayload, { GroupName: "owned" });
  await send(1, new CreateScheduleCommand(isolated));
  const canceled = request("canceled", "cancel-create-token", due, { id: "must-not-deliver" });
  await send(0, new CreateScheduleCommand(canceled));
  await send(0, new DeleteScheduleCommand({ Name: canceled.Name, ClientToken: "cancel-delete-token" }));
  await send(0, new DeleteScheduleCommand({ Name: canceled.Name, ClientToken: "cancel-delete-token" }));
  assert.equal((await send(0, new CreateScheduleCommand(canceled))).ScheduleArn, "arn:aws:scheduler:us-east-1:000000000000:schedule/default/canceled");
  assert(await deleted(0, canceled.Name));
  await eventually(async () => (await records(1)).length === 1, "the separate instance did not execute its own schedule");
  await eventually(() => deleted(1, isolated.Name, "owned"), "isolated schedule did not complete deletion");
  await sleep(150);
  assert.deepEqual(await records(0), [{ kind: "alias", event: payload }], "retry/cancellation/isolation must not create extra delivery");
  assert.deepEqual(await records(1), [{ kind: "alias", event: isolatedPayload }]);

  for (const [name, overrides, error, status] of [
    ["rate", { ScheduleExpression: "rate(1 minute)" }, "ValidationException", 400],
    ["invalid-time", { ScheduleExpression: "at(2030-02-30T01:00:00)" }, "ValidationException", 400],
    ["timezone", { ScheduleExpressionTimezone: "Missing/Zone" }, "ValidationException", 400],
    ["window", { FlexibleTimeWindow: { Mode: "FLEXIBLE", MaximumWindowInMinutes: 1 } }, "ValidationException", 400],
    ["missing-group", { GroupName: "missing" }, "ResourceNotFoundException", 404],
    ["unknown-target", { Target: { Arn: targetARN.replace("scheduled:live", "unregistered"), RoleArn: roleARN, Input: "{}" } }, "ResourceNotFoundException", 404],
    ["foreign-target", { Target: { Arn: targetARN.replace("000000000000", "111111111111"), RoleArn: roleARN, Input: "{}" } }, "ValidationException", 400],
    ["missing-input", { Target: { Arn: targetARN, RoleArn: roleARN } }, "ValidationException", 400],
  ]) {
    const invalid = request(name, `invalid-${name}`, dueDate(), {}, overrides);
    await expectError(() => send(0, new CreateScheduleCommand(invalid)), error, status);
    await expectError(() => send(0, new GetScheduleCommand({ Name: name })), "ResourceNotFoundException", 404);
  }
  console.log("PASS");
} finally {
  for (const client of clients) client.destroy();
}
