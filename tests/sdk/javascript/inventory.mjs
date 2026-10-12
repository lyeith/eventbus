import assert from "node:assert/strict";
import { readFile, writeFile } from "node:fs/promises";
import {
  CognitoIdentityProviderClient, CreateUserPoolCommand, CreateUserPoolClientCommand,
  DeleteUserPoolCommand, DeleteUserPoolClientCommand, DescribeUserPoolCommand,
  DescribeUserPoolClientCommand, ListUserPoolsCommand, ListUserPoolClientsCommand,
  paginateListUserPools, paginateListUserPoolClients,
} from "@aws-sdk/client-cognito-identity-provider";

const at = endpoint => new CognitoIdentityProviderClient({
  endpoint, region: "eu-west-1", credentials: { accessKeyId: "test", secretAccessKey: "test" }, maxAttempts: 1,
});
const first = at(process.env.COGNITO_ENDPOINT_URL);
const second = at(process.env.COGNITO_SECOND_ENDPOINT_URL);
const statePath = process.env.SMOKE_STATE_PATH;
const phase = process.argv[2];
const send = (client, command) => client.send(command);
const poolSummary = pool => ({
  Id: pool.Id, Name: pool.Name, CreationDate: pool.CreationDate.toISOString(),
  LastModifiedDate: pool.LastModifiedDate.toISOString(), Status: "Enabled", LambdaConfig: {},
});
const normalizePool = pool => {
  assert.deepEqual(Object.keys(pool).sort(), ["CreationDate", "Id", "LambdaConfig", "LastModifiedDate", "Name", "Status"]);
  assert(pool.CreationDate instanceof Date && pool.LastModifiedDate instanceof Date);
  assert.equal(pool.Status, "Enabled");
  assert.deepEqual(pool.LambdaConfig, {});
  return poolSummary(pool);
};
const clientSummary = client => ({ ClientId: client.ClientId, ClientName: client.ClientName, UserPoolId: client.UserPoolId });
const normalizeClient = client => {
  assert.deepEqual(Object.keys(client).sort(), ["ClientId", "ClientName", "UserPoolId"]);
  assert(!("ClientSecret" in client));
  return clientSummary(client);
};
const sortedPools = pools => [...pools].sort((a, b) => a.Id.localeCompare(b.Id));
const sortedClients = clients => [...clients].sort((a, b) => a.ClientId.localeCompare(b.ClientId));
async function expectError(client, command, code) {
  await assert.rejects(() => send(client, command), error => {
    assert.equal(error.name, code);
    assert.equal(error.$metadata.httpStatusCode, 400);
    assert(error.$metadata.requestId);
    return true;
  });
}
async function allPools(client) {
  const pools = [];
  let count = 0;
  for await (const page of paginateListUserPools({ client, pageSize: 1 }, { MaxResults: 1 })) {
    assert(++count < 20, "pool paginator must terminate");
    assert(Array.isArray(page.UserPools));
    assert(page.UserPools.length <= 1);
    pools.push(...page.UserPools.map(normalizePool));
  }
  return pools;
}
async function allClients(client, poolID) {
  const clients = [];
  let count = 0;
  for await (const page of paginateListUserPoolClients({ client, pageSize: 1 }, { UserPoolId: poolID })) {
    assert(++count < 20, "client paginator must terminate");
    assert(Array.isArray(page.UserPoolClients));
    assert(page.UserPoolClients.length <= 1);
    clients.push(...page.UserPoolClients.map(normalizeClient));
  }
  return clients;
}
async function createPool(client, name) {
  const pool = (await send(client, new CreateUserPoolCommand({ PoolName: name }))).UserPool;
  assert(pool.Id.startsWith("eu-west-1_"));
  assert.equal(pool.Arn, `arn:aws:cognito-idp:eu-west-1:123456789012:userpool/${pool.Id}`);
  return poolSummary(pool);
}
async function createClient(client, poolID, name) {
  const app = (await send(client, new CreateUserPoolClientCommand({ UserPoolId: poolID, ClientName: name, GenerateSecret: true }))).UserPoolClient;
  assert(app.ClientSecret);
  return clientSummary(app);
}
async function exercise() {
  for (const client of [first, second]) {
    const empty = await send(client, new ListUserPoolsCommand({ MaxResults: 60 }));
    assert.deepEqual(empty.UserPools, []);
    assert.equal(empty.NextToken, undefined);
  }
  const pools = [];
  for (const name of ["same name", "same name", "empty pool"]) pools.push(await createPool(first, name));
  const foreignPools = [];
  for (const name of ["same name", "independent empty pool"]) foreignPools.push(await createPool(second, name));
  const ownerID = pools[0].Id, otherID = pools[1].Id;
  const clients = [];
  for (const name of ["same client", "same client", "third client"]) clients.push(await createClient(first, ownerID, name));
  const otherClients = [await createClient(first, otherID, "foreign to owner")];
  const independentClients = [await createClient(second, foreignPools[0].Id, "independent client")];
  assert.deepEqual(await allPools(first), sortedPools(pools));
  assert.deepEqual(await allPools(second), sortedPools(foreignPools));
  assert.deepEqual(await allClients(first, ownerID), sortedClients(clients));
  assert.deepEqual(await allClients(first, otherID), otherClients);
  assert.deepEqual(await allClients(second, foreignPools[0].Id), independentClients);
  const emptyClients = await send(first, new ListUserPoolClientsCommand({ UserPoolId: pools[2].Id }));
  assert.deepEqual(emptyClients.UserPoolClients, []);
  assert.equal(emptyClients.NextToken, undefined);
  const defaultPage = await send(first, new ListUserPoolClientsCommand({ UserPoolId: ownerID }));
  assert.deepEqual(defaultPage.UserPoolClients.map(normalizeClient), sortedClients(clients));
  assert.equal(defaultPage.NextToken, undefined);
  const poolPage = await send(first, new ListUserPoolsCommand({ MaxResults: 1 }));
  const clientPage = await send(first, new ListUserPoolClientsCommand({ UserPoolId: ownerID, MaxResults: 1 }));
  assert(poolPage.NextToken && clientPage.NextToken);
  for (const MaxResults of [0, 61]) {
    await expectError(first, new ListUserPoolsCommand({ MaxResults }), "InvalidParameterException");
    await expectError(first, new ListUserPoolClientsCommand({ UserPoolId: ownerID, MaxResults }), "InvalidParameterException");
  }
  await expectError(first, new ListUserPoolsCommand({}), "InvalidParameterException");
  await expectError(first, new ListUserPoolClientsCommand({}), "InvalidParameterException");
  await expectError(first, new ListUserPoolClientsCommand({ UserPoolId: "bad-id" }), "InvalidParameterException");
  await expectError(first, new ListUserPoolClientsCommand({ UserPoolId: "eu-west-1_Missing" }), "ResourceNotFoundException");
  await expectError(first, new ListUserPoolsCommand({ MaxResults: 1, NextToken: "malformed" }), "InvalidParameterException");
  await expectError(first, new ListUserPoolClientsCommand({ UserPoolId: ownerID, MaxResults: 1, NextToken: "malformed" }), "InvalidParameterException");
  await expectError(first, new ListUserPoolClientsCommand({ UserPoolId: otherID, MaxResults: 1, NextToken: clientPage.NextToken }), "InvalidParameterException");
  await expectError(first, new ListUserPoolsCommand({ MaxResults: 1, NextToken: clientPage.NextToken }), "InvalidParameterException");
  await expectError(second, new ListUserPoolsCommand({ MaxResults: 1, NextToken: poolPage.NextToken }), "InvalidParameterException");
  await expectError(second, new ListUserPoolClientsCommand({ UserPoolId: ownerID }), "ResourceNotFoundException");
  await writeFile(statePath, JSON.stringify({
    pools: sortedPools(pools), foreignPools: sortedPools(foreignPools), ownerID, otherID,
    clients: sortedClients(clients), otherClients, independentClients, independentID: foreignPools[0].Id,
    poolFirst: poolPage.UserPools.map(normalizePool), poolToken: poolPage.NextToken,
    clientFirst: clientPage.UserPoolClients.map(normalizeClient), clientToken: clientPage.NextToken,
  }));
}
async function restart() {
  const state = JSON.parse(await readFile(statePath, "utf8"));
  assert.deepEqual(await allPools(first), state.pools);
  assert.deepEqual(await allPools(second), state.foreignPools);
  assert.deepEqual(await allClients(first, state.ownerID), state.clients);
  assert.deepEqual(await allClients(first, state.otherID), state.otherClients);
  assert.deepEqual(await allClients(second, state.independentID), state.independentClients);
  // The original tokens continue through the reopened store, with no duplicate
  // or omitted IDs; SDK paginators exercise the same native continuation path.
  const resumedPools = [...state.poolFirst];
  for await (const page of paginateListUserPools({ client: first, pageSize: 1, startingToken: state.poolToken }, { MaxResults: 1 })) {
    resumedPools.push(...page.UserPools.map(normalizePool));
  }
  assert.deepEqual(resumedPools, state.pools);
  const resumedClients = [...state.clientFirst];
  for await (const page of paginateListUserPoolClients({ client: first, pageSize: 1, startingToken: state.clientToken }, { UserPoolId: state.ownerID })) {
    resumedClients.push(...page.UserPoolClients.map(normalizeClient));
  }
  assert.deepEqual(resumedClients, state.clients);
  for (const pool of state.pools) {
    const described = (await send(first, new DescribeUserPoolCommand({ UserPoolId: pool.Id }))).UserPool;
    assert.deepEqual(poolSummary(described), pool);
  }
  for (const client of state.clients) {
    const described = (await send(first, new DescribeUserPoolClientCommand({ UserPoolId: state.ownerID, ClientId: client.ClientId }))).UserPoolClient;
    assert.deepEqual(clientSummary(described), client);
    assert(described.ClientSecret, "Describe keeps its native detailed response");
  }
  await send(first, new DeleteUserPoolClientCommand({ UserPoolId: state.ownerID, ClientId: state.clients[0].ClientId }));
  assert.deepEqual(await allClients(first, state.ownerID), state.clients.slice(1));
  await send(first, new DeleteUserPoolCommand({ UserPoolId: state.ownerID }));
  assert.deepEqual(await allPools(first), state.pools.filter(pool => pool.Id !== state.ownerID));
  await expectError(first, new ListUserPoolClientsCommand({ UserPoolId: state.ownerID }), "ResourceNotFoundException");
  assert.deepEqual(await allClients(first, state.otherID), state.otherClients);
  assert.deepEqual(await allPools(second), state.foreignPools);
  for (const pool of state.pools.filter(pool => pool.Id !== state.ownerID)) await send(first, new DeleteUserPoolCommand({ UserPoolId: pool.Id }));
  for (const pool of state.foreignPools) await send(second, new DeleteUserPoolCommand({ UserPoolId: pool.Id }));
  assert.deepEqual(await allPools(first), []);
  assert.deepEqual(await allPools(second), []);
}
try {
  if (phase === "exercise") await exercise();
  else if (phase === "restart") await restart();
  else throw new Error("unknown inventory phase");
  console.log("PASS");
} finally {
  first.destroy(); second.destroy();
}
