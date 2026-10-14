import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { setTimeout as sleep } from "node:timers/promises";
import {
  EventBridgeClient, PutRuleCommand, DescribeRuleCommand, ListRulesCommand,
  EnableRuleCommand, DisableRuleCommand, DeleteRuleCommand, PutTargetsCommand,
  ListTargetsByRuleCommand, RemoveTargetsCommand,
} from "@aws-sdk/client-eventbridge";

const endpoint = process.env.EVENTS_ENDPOINT_URL;
assert(endpoint && ["127.0.0.1", "localhost", "[::1]"].includes(new URL(endpoint).hostname));
const recurring = process.env.EVENTS_RECURRING === "1";
const output = process.env.EVENTS_OUTPUT_FILE;
const gate = process.env.EVENTS_GATE_FILE;
assert(output && gate);
const client = new EventBridgeClient({ endpoint, region: "us-east-1", maxAttempts: 1,
  credentials: { accessKeyId: "sdk-test", secretAccessKey: "sdk-test" } });
const prefix = "sdk-events-" + randomUUID().replaceAll("-", "").slice(0, 12);
const names = [prefix + "-main", prefix + "-removed", prefix + "-deleted"];
const arn = name => `arn:aws:events:us-east-1:000000000000:rule/${name}`;
const targetARN = "arn:aws:lambda:us-east-1:000000000000:function:events:live";
const owned = new Set();

async function send(command) {
  const result = await client.send(command, { abortSignal: AbortSignal.timeout(5000) });
  assert.equal(result.$metadata.httpStatusCode, 200);
  assert(result.$metadata.requestId, "native successful responses require request IDs");
  return result;
}
async function expectError(operation, name, status = 400) {
  try { await operation(); } catch (error) {
    assert.equal(error.name, name, error.message);
    assert.equal(error.$metadata.httpStatusCode, status);
    assert(error.$metadata.requestId, "native typed errors require request IDs");
    return error;
  }
  assert.fail(`expected ${name}`);
}
async function records() {
  let data;
  try { data = await readFile(output, "utf8"); } catch (error) {
    if (error.code === "ENOENT") return [];
    throw error;
  }
  return data.split("\n").filter(Boolean).map(line => JSON.parse(line));
}
async function eventually(operation, label, timeout = 10000) {
  const deadline = Date.now() + timeout;
  do {
    if (await operation()) return;
    await sleep(50);
  } while (Date.now() < deadline);
  assert.fail(`${label}: ${JSON.stringify(await records())}`);
}
const transformer = {
  InputPathsMap: { eventId: "$.id", fired: "$.time", rule: "$.resources[0]", detail: "$.detail" },
  InputTemplate: '{"kind":"transformed","httpMethod":"POST","path":"/api/requests/expiry","eventId":<eventId>,"fired":"<fired>","rule":<rule>,"detail":<detail>,"ruleName":<aws.events.rule-name>,"ruleArn":<aws.events.rule-arn>,"ingestion":<aws.events.event.ingestion-time>,"original":<aws.events.event.json>,"withoutDetail":<aws.events.event>}',
};
const targets = [
  { Id: "transformed", Arn: targetARN, InputTransformer: transformer },
  { Id: "constant", Arn: targetARN, Input: JSON.stringify({ kind: "constant", unicode: "界", values: [null, false, 7] }) },
  { Id: "path", Arn: targetARN, InputPath: "$.resources[0]" },
  { Id: "original", Arn: targetARN },
];
function native(event, name) {
  assert.equal(event.version, "0");
  assert.match(event.id, /^[0-9a-f-]{36}$/i);
  assert.equal(event["detail-type"], "Scheduled Event");
  assert.equal(event.source, "aws.events");
  assert.equal(event.account, "000000000000");
  assert.equal(event.region, "us-east-1");
  assert.deepEqual(event.resources, [arn(name)]);
  assert.deepEqual(event.detail, {});
  assert(Number.isFinite(Date.parse(event.time)));
}
async function removeAll(name) {
  const result = await send(new ListTargetsByRuleCommand({ Rule: name }));
  if (result.Targets.length) {
    const removed = await send(new RemoveTargetsCommand({ Rule: name, Ids: result.Targets.map(target => target.Id) }));
    assert.equal(removed.FailedEntryCount, 0);
  }
}
try {
  for (const name of names) {
    const result = await send(new PutRuleCommand({ Name: name, Description: "owned SDK rate rule", ScheduleExpression: "rate(1 minute)", State: "DISABLED" }));
    owned.add(name);
    assert.equal(result.RuleArn, arn(name));
    const readback = await send(new DescribeRuleCommand({ Name: name }));
    assert.equal(readback.Name, name);
    assert.equal(readback.Arn, arn(name));
    assert.equal(readback.State, "DISABLED");
    assert.equal(readback.ScheduleExpression, "rate(1 minute)");
    assert.equal(readback.Description, "owned SDK rate rule");
    assert.equal(readback.EventBusName, "default");
  }
  let page = await send(new ListRulesCommand({ NamePrefix: prefix, Limit: 1 }));
  const listed = [...page.Rules];
  while (page.NextToken) {
    page = await send(new ListRulesCommand({ NamePrefix: prefix, Limit: 1, NextToken: page.NextToken }));
    listed.push(...page.Rules);
  }
  assert.deepEqual(listed.map(rule => rule.Name).sort(), [...names].sort());
  const update = await send(new PutRuleCommand({ Name: names[0], Description: "updated", ScheduleExpression: "rate(1 minute)", State: "DISABLED" }));
  assert.equal(update.RuleArn, arn(names[0]));
  assert.equal((await send(new DescribeRuleCommand({ Name: names[0] }))).Description, "updated");
  let attached = await send(new PutTargetsCommand({ Rule: names[0], Targets: targets }));
  assert.equal(attached.FailedEntryCount, 0);
  assert.deepEqual(attached.FailedEntries ?? [], []);
  const readTargets = (await send(new ListTargetsByRuleCommand({ Rule: names[0] }))).Targets;
  assert.deepEqual(readTargets.sort((a, b) => a.Id.localeCompare(b.Id)), [...targets].sort((a, b) => a.Id.localeCompare(b.Id)));
  targets[1].Input = JSON.stringify({ kind: "constant", unicode: "界", values: [null, false, 7], updated: true });
  const updatedTarget = await send(new PutTargetsCommand({ Rule: names[0], Targets: [targets[1]] }));
  assert.equal(updatedTarget.FailedEntryCount, 0);
  const constantReadback = (await send(new ListTargetsByRuleCommand({ Rule: names[0] }))).Targets.find(target => target.Id === "constant");
  assert.deepEqual(constantReadback, targets[1], "same target ID updates only its native input");
  for (const name of names.slice(1)) {
    attached = await send(new PutTargetsCommand({ Rule: name, Targets: [{ Id: "native", Arn: targetARN }] }));
    assert.equal(attached.FailedEntryCount, 0);
  }
  for (const extra of [
    { ScheduleExpression: "rate(1 second)" }, { ScheduleExpression: "rate(2 minute)" },
    { ScheduleExpression: "cron(0 * * * ? *)" }, { EventPattern: '{"source":["app"]}' },
    { EventBusName: "custom" },
  ]) {
    await expectError(() => send(new PutRuleCommand({ Name: names[0], ScheduleExpression: "rate(1 minute)", State: "DISABLED", ...extra })), "ValidationException");
    assert.equal((await send(new DescribeRuleCommand({ Name: names[0] }))).State, "DISABLED");
  }
  for (const target of [
    { Id: "invalid", Arn: targetARN, Input: "{}", InputPath: "$.detail" },
    { Id: "invalid", Arn: targetARN, Input: "{bad-json" },
    { Id: "invalid", Arn: targetARN, InputTransformer: { InputPathsMap: { x: "$.id" }, InputTemplate: '{"id":<unknown>}' } },
    { Id: "invalid", Arn: targetARN, InputTransformer: { InputPathsMap: { "aws.events.rule-name": "$.id" }, InputTemplate: '"x"' } },
  ]) {
    await expectError(() => send(new PutTargetsCommand({ Rule: names[0], Targets: [target] })), "ValidationException");
  }
  const invalidTarget = await send(new PutTargetsCommand({ Rule: names[0], Targets: [{ Id: "missing", Arn: targetARN.replace("events:live", "unregistered") }] }));
  assert.equal(invalidTarget.FailedEntryCount, 1);
  assert.deepEqual(invalidTarget.FailedEntries.map(entry => [entry.TargetId, entry.ErrorCode]), [["missing", "ResourceNotFoundException"]]);
  assert.equal((await send(new ListTargetsByRuleCommand({ Rule: names[0] }))).Targets.length, targets.length);
  await expectError(() => send(new DeleteRuleCommand({ Name: names[0] })), "ValidationException");
  await send(new EnableRuleCommand({ Name: names[0] }));
  assert.equal((await send(new DescribeRuleCommand({ Name: names[0] }))).State, "ENABLED");
  if (!recurring) {
    await send(new DisableRuleCommand({ Name: names[0] }));
    assert.equal((await send(new DescribeRuleCommand({ Name: names[0] }))).State, "DISABLED");
    assert.deepEqual(await records(), [], "CRUD proof must not accelerate valid rate scheduling");
  } else {
    for (const name of names.slice(1)) await send(new EnableRuleCommand({ Name: name }));
    await eventually(async () => (await records()).filter(row => row.phase === "start").length === 12,
      "two actual minute-rate firings of all configured targets", 140000);
    const started = (await records()).filter(row => row.phase === "start");
    const transformed = started.filter(row => row.event?.kind === "transformed");
    assert.equal(transformed.length, 2);
    for (const row of transformed) {
      const event = row.event;
      native(event.original, names[0]);
      assert.equal(event.httpMethod, "POST");
      assert.equal(event.path, "/api/requests/expiry");
      assert.equal(event.eventId, event.original.id);
      assert.equal(event.fired, event.original.time);
      assert.equal(event.rule, arn(names[0]));
      assert.equal(event.ruleArn, arn(names[0]));
      assert.equal(event.ruleName, names[0]);
      assert.deepEqual(event.detail, {});
      const { detail, ...withoutDetail } = event.original;
      assert.deepEqual(event.withoutDetail, withoutDetail);
      assert(Number.isFinite(Date.parse(event.ingestion)));
    }
    assert.notEqual(transformed[0].event.eventId, transformed[1].event.eventId);
    assert(Date.parse(transformed[1].event.fired) - Date.parse(transformed[0].event.fired) >= 59000, "genuine recurring cadence");
    assert.equal(started.filter(row => row.event?.kind === "constant").length, 2);
    for (const row of started.filter(row => row.event?.kind === "constant")) assert.deepEqual(row.event, JSON.parse(targets[1].Input));
    assert.equal(started.filter(row => row.event === arn(names[0])).length, 2);
    for (const name of names) {
      const source = started.filter(row => row.event?.source === "aws.events" && row.event.resources[0] === arn(name));
      assert.equal(source.length, 2);
      for (const row of source) native(row.event, name);
      assert.notEqual(source[0].event.id, source[1].event.id);
    }
    const original = started.filter(row => row.event?.source === "aws.events" && row.event.resources[0] === arn(names[0]));
    assert.deepEqual(original.map(row => row.event.id).sort(), transformed.map(row => row.event.eventId).sort(), "targets share native firing identity");
    assert.equal((await records()).filter(row => row.phase === "finish" && row.event?.kind === "transformed").length, 1,
      "second transformed handler remains genuinely active across disabling");
    await send(new DisableRuleCommand({ Name: names[0] }));
    assert.equal((await send(new DescribeRuleCommand({ Name: names[0] }))).State, "DISABLED");
    await removeAll(names[1]);
    assert.deepEqual((await send(new ListTargetsByRuleCommand({ Rule: names[1] }))).Targets, []);
    assert.equal((await send(new DescribeRuleCommand({ Name: names[1] }))).State, "ENABLED");
    await removeAll(names[2]);
    await send(new DeleteRuleCommand({ Name: names[2] }));
    owned.delete(names[2]);
    await expectError(() => send(new DescribeRuleCommand({ Name: names[2] })), "ResourceNotFoundException");
    await writeFile(gate, "finish", { mode: 0o600 });
    await eventually(async () => (await records()).filter(row => row.phase === "finish").length === 12,
      "already-admitted transformed call completes after disabling");
    await sleep(65000);
    assert.equal((await records()).filter(row => row.phase === "start").length, 12,
      "disabled rule, removed target and deleted rule admit no subsequent firing");
    console.log(JSON.stringify({ status: "passed", genuine_firings: 2, native_invocations: 12, cadence_ms: Date.parse(transformed[1].event.fired) - Date.parse(transformed[0].event.fired) }));
  }
  for (const name of [...owned]) {
    await removeAll(name);
    await send(new DeleteRuleCommand({ Name: name }));
    owned.delete(name);
    await expectError(() => send(new DescribeRuleCommand({ Name: name })), "ResourceNotFoundException");
  }
  assert.deepEqual((await send(new ListRulesCommand({ NamePrefix: prefix }))).Rules, []);
  console.log("PASS");
} finally {
  await writeFile(gate, "cleanup", { mode: 0o600 });
  for (const name of owned) {
    await send(new DisableRuleCommand({ Name: name }));
    await removeAll(name);
    await send(new DeleteRuleCommand({ Name: name }));
  }
  client.destroy();
}
