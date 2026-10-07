import assert from "node:assert/strict";
import { createHmac, createPublicKey, verify } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { CognitoIdentityProviderClient } from "@aws-sdk/client-cognito-identity-provider";

export const endpoint = process.env.COGNITO_ENDPOINT_URL;
export const poolID = process.env.COGNITO_USER_POOL_ID;
export const statePath = process.env.SMOKE_STATE_PATH;
assert(endpoint && poolID && statePath, "owned SDK endpoint, pool and state path are required");
const url = new URL(endpoint);
assert(["127.0.0.1", "localhost", "[::1]"].includes(url.hostname), "SDK fixture requires an owned loopback endpoint");
export const client = new CognitoIdentityProviderClient({
  endpoint, region: "us-east-1", maxAttempts: 1,
  credentials: { accessKeyId: "sdk-test", secretAccessKey: "sdk-test" },
});
export const send = command => client.send(command, { abortSignal: AbortSignal.timeout(5000) });
export function secretHash(secret, username, clientID) {
  return createHmac("sha256", secret).update(username + clientID).digest("base64");
}
export function parameters(state, username, values = {}) {
  return { USERNAME: username, SECRET_HASH: secretHash(state.secret, username, state.clientID), ...values };
}
export async function expectError(operation, names) {
  names = Array.isArray(names) ? names : [names];
  try { await operation(); } catch (error) {
    assert(names.includes(error.name), `expected ${names.join("/")}, got ${error.name}: ${error.message}`);
    assert.equal(error.$metadata.httpStatusCode, 400);
    assert(error.$metadata.requestId, "AWS error must include request ID");
    return error;
  }
  assert.fail(`expected ${names.join("/")} refusal`);
}
export function attrs(attributes = []) { return Object.fromEntries(attributes.map(({ Name, Value }) => [Name, Value])); }
export async function saveState(state) { await writeFile(statePath, JSON.stringify(state), { mode: 0o600 }); }
export async function loadState() { return JSON.parse(await readFile(statePath, "utf8")); }
export async function captures() {
  const data = await readFile(process.env.SES_CAPTURE_PATH, "utf8");
  assert(!data || data.endsWith("\n"), "capture must contain complete lines");
  return data.trim().split("\n").filter(Boolean).map(line => JSON.parse(line));
}
export async function verifyTokens(result, state, { refresh = true } = {}) {
  assert(result?.AccessToken && result.IdToken, "authentication must issue access and ID tokens");
  assert.equal(result.TokenType, "Bearer");
  assert(Number.isInteger(result.ExpiresIn) && result.ExpiresIn > 0);
  if (refresh) assert(result.RefreshToken, "initial sign-in must issue a refresh token");
  const response = await fetch(`${endpoint}/${poolID}/.well-known/jwks.json`, { signal: AbortSignal.timeout(5000) });
  assert.equal(response.status, 200);
  const jwks = await response.json();
  for (const [token, use] of [[result.AccessToken, "access"], [result.IdToken, "id"]]) {
    const [headerPart, payloadPart, signaturePart] = token.split(".");
    const header = JSON.parse(Buffer.from(headerPart, "base64url"));
    const claims = JSON.parse(Buffer.from(payloadPart, "base64url"));
    assert.equal(header.alg, "RS256");
    const key = jwks.keys.find(key => key.kid === header.kid);
    assert(key, "JWT kid must match the persisted JWKS");
    assert(verify("RSA-SHA256", Buffer.from(`${headerPart}.${payloadPart}`), createPublicKey({ key, format: "jwk" }), Buffer.from(signaturePart, "base64url")), "JWT signature must verify independently");
    assert.equal(claims.iss, `${endpoint}/${poolID}`);
    assert.equal(claims.sub, state.sub);
    assert.equal(claims.token_use, use);
    if (use === "id") assert.equal(claims.email, state.email);
    else assert.equal(claims.scope, "aws.cognito.signin.user.admin");
    assert.equal(use === "id" ? claims["cognito:username"] : claims.username, state.username);
    assert.equal(use === "id" ? claims.aud : claims.client_id, state.clientID);
    assert(claims.exp > Math.floor(Date.now() / 1000));
  }
}
