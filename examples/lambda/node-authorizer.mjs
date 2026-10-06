// Application-owned REST API REQUEST authorizer. This example's fixed token is
// a local fixture; an application authorizer supplies its real policy.
export async function handler(event) {
  const authorization = Object.entries(event.headers ?? {})
    .find(([name]) => name.toLowerCase() === 'authorization')?.[1];
  if (!authorization) throw new Error('Unauthorized');
  return {
    principalId: 'local-example-user',
    policyDocument: {
      Version: '2012-10-17',
      Statement: [{
        Action: 'execute-api:Invoke',
        Effect: authorization === 'Bearer local-authorizer-token' ? 'Allow' : 'Deny',
        Resource: event.methodArn,
      }],
    },
    context: {language: 'node'},
  };
}
