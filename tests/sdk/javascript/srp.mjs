// Independent client arithmetic using Node's crypto and BigInt only. Cognito
// uses the RFC 3526 3072-bit group, g=2 and positive BigInteger byte padding.
import { createHash, createHmac, randomBytes } from "node:crypto";
import assert from "node:assert/strict";

export const N = BigInt("0x" +
  "FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD1" +
  "29024E088A67CC74020BBEA63B139B22514A08798E3404DD" +
  "EF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245" +
  "E485B576625E7EC6F44C42E9A637ED6B0BFF5CB6F406B7ED" +
  "EE386BFB5A899FA5AE9F24117C4B1FE649286651ECE45B3D" +
  "C2007CB8A163BF0598DA48361C55D39A69163FA8FD24CF5F" +
  "83655D23DCA3AD961C62F356208552BB9ED529077096966D" +
  "670C354E4ABC9804F1746C08CA18217C32905E462E36CE3B" +
  "E39E772C180E86039B2783A2EC07A28FB5C55DF06F4C52C9" +
  "DE2BCBF6955817183995497CEA956AE515D2261898FA0510" +
  "15728E5A8AAAC42DAD33170D04507A33A85521ABDF1CBA64" +
  "ECFB850458DBEF0A8AEA71575D060C7DB3970F85A6E1E4C7" +
  "ABF5AE8CDB0933D71E8C94E04A25619DCEE3D2261AD2EE6B" +
  "F12FFA06D98A0864D87602733EC86A64521F2B18177B200C" +
  "BBE117577A615D6C770988C0BAD946E208E24FA074E5AB31" +
  "43DB5BFCE0FD108E4B82D120A93AD2CAFFFFFFFFFFFFFFFF");
export const g = 2n;
export function pad(value) {
  assert(value >= 0n);
  let hex = value.toString(16);
  if (hex.length % 2) hex = "0" + hex;
  if (parseInt(hex.slice(0, 2), 16) >= 128) hex = "00" + hex;
  return Buffer.from(hex, "hex");
}
export function modPow(value, exponent, modulus = N) {
  value = ((value % modulus) + modulus) % modulus;
  let result = 1n;
  while (exponent > 0n) {
    if (exponent & 1n) result = result * value % modulus;
    value = value * value % modulus;
    exponent >>= 1n;
  }
  return result;
}
function hash(...parts) { return createHash("sha256").update(Buffer.concat(parts)).digest(); }
function integer(bytes) { return BigInt("0x" + bytes.toString("hex")); }
function hmac(key, ...parts) { return createHmac("sha256", key).update(Buffer.concat(parts)).digest(); }
export const k = integer(hash(pad(N), pad(g)));

export function timestamp(date = new Date()) {
  const day = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"][date.getUTCDay()];
  const month = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"][date.getUTCMonth()];
  const clock = [date.getUTCHours(), date.getUTCMinutes(), date.getUTCSeconds()]
    .map(value => String(value).padStart(2, "0")).join(":");
  return `${day} ${month} ${date.getUTCDate()} ${clock} UTC ${date.getUTCFullYear()}`;
}

export function beginSRP(a = integer(randomBytes(128))) {
  const A = modPow(g, a);
  assert(A % N !== 0n);
  return { a, A, SRP_A: A.toString(16) };
}

export function passwordProof({ poolID, username, password, a, A, B, salt, secretBlock, time = timestamp() }) {
  const poolSuffix = poolID.slice(poolID.indexOf("_") + 1);
  B = typeof B === "bigint" ? B : BigInt("0x" + B);
  salt = typeof salt === "bigint" ? salt : BigInt("0x" + salt);
  assert(B % N !== 0n, "invalid server SRP value");
  const u = integer(hash(pad(A), pad(B)));
  assert(u !== 0n, "invalid SRP scrambling parameter");
  const inner = hash(Buffer.from(`${poolSuffix}${username}:${password}`));
  const x = integer(hash(pad(salt), inner));
  const S = modPow(B - k * modPow(g, x), a + u * x);
  const prk = hmac(pad(u), pad(S));
  const key = hmac(prk, Buffer.from("Caldera Derived Key"), Buffer.from([1])).subarray(0, 16);
  const signature = hmac(key, Buffer.from(poolSuffix), Buffer.from(username), Buffer.from(secretBlock, "base64"), Buffer.from(time));
  return { USERNAME: username, PASSWORD_CLAIM_SECRET_BLOCK: secretBlock,
    TIMESTAMP: time, PASSWORD_CLAIM_SIGNATURE: signature.toString("base64") };
}
