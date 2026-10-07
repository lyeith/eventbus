import assert from "node:assert/strict";
import { createHmac, createPublicKey, verify } from "node:crypto";
import { readFile } from "node:fs/promises";
import {
 CognitoIdentityProviderClient, DescribeUserPoolCommand, DescribeUserPoolClientCommand,
 CreateUserPoolClientCommand, CreateUserPoolCommand, DeleteUserPoolCommand,
 SignUpCommand, ConfirmSignUpCommand, ResendConfirmationCodeCommand,
 InitiateAuthCommand, GetUserCommand, AdminConfirmSignUpCommand,
} from "@aws-sdk/client-cognito-identity-provider";

const endpoint = process.env.COGNITO_ENDPOINT_URL;
assert(endpoint && ["127.0.0.1", "localhost", "[::1]"].includes(new URL(endpoint).hostname));
const state = JSON.parse(await readFile(process.env.SMOKE_STATE_PATH, "utf8"));
const client = new CognitoIdentityProviderClient({endpoint,region:"eu-west-1",maxAttempts:1,credentials:{accessKeyId:"sdk-test",secretAccessKey:"sdk-test"}});
const send = command => client.send(command,{abortSignal:AbortSignal.timeout(5000)});
const hash = username => createHmac("sha256",state.secret).update(username+state.client).digest("base64");
const pool = (await send(new DescribeUserPoolCommand({UserPoolId:state.pool}))).UserPool;
assert.equal(pool.Name,"native-sdk-pool");
assert.equal(pool.Arn,state.pool_metadata.arn);
assert.equal(pool.CreationDate.getTime()/1000,state.pool_metadata.created);
assert.deepEqual(pool.AutoVerifiedAttributes,["email"]);
const app = (await send(new DescribeUserPoolClientCommand({UserPoolId:state.pool,ClientId:state.client}))).UserPoolClient;
assert.equal(app.ClientSecret,state.secret);
assert.deepEqual(app.TokenValidityUnits,{AccessToken:"minutes",IdToken:"seconds",RefreshToken:"hours"});
const result = (await send(new InitiateAuthCommand({ClientId:state.client,AuthFlow:"USER_PASSWORD_AUTH",AuthParameters:{USERNAME:"sdk-user",PASSWORD:"RecoveredPass2!",SECRET_HASH:hash("sdk-user")}}))).AuthenticationResult;
const jwks = await (await fetch(`${endpoint}/${state.pool}/.well-known/jwks.json`,{signal:AbortSignal.timeout(5000)})).json();
for (const [name,use,duration] of [["AccessToken","access",300],["IdToken","id",600]]) {
 const [headerPart,payloadPart,signaturePart]=result[name].split(".");
 const header=JSON.parse(Buffer.from(headerPart,"base64url"));
 const claims=JSON.parse(Buffer.from(payloadPart,"base64url"));
 const key=jwks.keys.find(key=>key.kid===header.kid);
 assert(verify("RSA-SHA256",Buffer.from(`${headerPart}.${payloadPart}`),createPublicKey({key,format:"jwk"}),Buffer.from(signaturePart,"base64url")));
 assert.equal(claims.iss,`${endpoint}/${state.pool}`);
 assert.equal(claims.sub,state.sub);
 assert.equal(claims.token_use,use);
 assert.equal(claims.exp-claims.iat,duration);
 if(use==="id"){assert.equal(claims.email,"changed@example.test");assert.equal(claims["custom:tenant"],"tenant-a");assert.equal(claims.aud,state.client)}
 else{assert.equal(claims.client_id,state.client);assert.equal(claims.scope,"aws.cognito.signin.user.admin");assert(!("email" in claims));}
}
assert.equal(result.ExpiresIn,300);
const profile=(await send(new GetUserCommand({AccessToken:result.AccessToken}))).UserAttributes;
assert.equal(Object.fromEntries(profile.map(({Name,Value})=>[Name,Value])).email,"changed@example.test");
const second=(await send(new CreateUserPoolClientCommand({UserPoolId:state.pool,ClientName:"node-sdk-client",ExplicitAuthFlows:["ALLOW_USER_PASSWORD_AUTH"],AccessTokenValidity:7,IdTokenValidity:11,RefreshTokenValidity:2,TokenValidityUnits:{AccessToken:"minutes",IdToken:"minutes",RefreshToken:"hours"},WriteAttributes:["email"]}))).UserPoolClient;
await send(new SignUpCommand({ClientId:second.ClientId,Username:"node-user",Password:"InitialPass1!",UserAttributes:[{Name:"email",Value:"node@example.test"}]}));
await send(new ResendConfirmationCodeCommand({ClientId:second.ClientId,Username:"node-user"}));
const notifications=(await readFile(process.env.COGNITO_CAPTURE_PATH,"utf8")).trim().split("\n").map(JSON.parse);
const confirmation=notifications.filter(note=>note.username==="node-user"&&note.purpose==="signup").at(-1);
assert.equal(confirmation.schema_version,"eventbus.cognito.notification.v1");
await send(new ConfirmSignUpCommand({ClientId:second.ClientId,Username:"node-user",ConfirmationCode:confirmation.code}));
const secondTokens=(await send(new InitiateAuthCommand({ClientId:second.ClientId,AuthFlow:"USER_PASSWORD_AUTH",AuthParameters:{USERNAME:"node-user",PASSWORD:"InitialPass1!"}}))).AuthenticationResult;
assert.equal(secondTokens.ExpiresIn,420);
for(const [name,duration] of [["AccessToken",420],["IdToken",660]]){const claims=JSON.parse(Buffer.from(secondTokens[name].split(".")[1],"base64url"));assert.equal(claims.exp-claims.iat,duration);}
const manual=(await send(new CreateUserPoolCommand({PoolName:"node-manual-confirm"}))).UserPool;
const manualClient=(await send(new CreateUserPoolClientCommand({UserPoolId:manual.Id,ClientName:"manual",ExplicitAuthFlows:["ALLOW_USER_PASSWORD_AUTH"]}))).UserPoolClient;
const signup=await send(new SignUpCommand({ClientId:manualClient.ClientId,Username:"manual-user",Password:"InitialPass1!"}));
assert.equal(signup.UserConfirmed,false);
assert(!signup.CodeDeliveryDetails);
await send(new AdminConfirmSignUpCommand({UserPoolId:manual.Id,Username:"manual-user"}));
assert((await send(new InitiateAuthCommand({ClientId:manualClient.ClientId,AuthFlow:"USER_PASSWORD_AUTH",AuthParameters:{USERNAME:"manual-user",PASSWORD:"InitialPass1!"}}))).AuthenticationResult.AccessToken);
await send(new DeleteUserPoolCommand({UserPoolId:manual.Id}));
client.destroy();
console.log("PASS");
