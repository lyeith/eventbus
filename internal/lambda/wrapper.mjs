import fs from 'node:fs';
import { pathToFileURL } from 'node:url';

const [modulePath, exportName] = process.argv.slice(1);
const deadline = Number(process.env.EVENTBUS_LAMBDA_DEADLINE_MS);
// A pending user Promise alone does not keep Node alive. Lambda keeps the
// invocation active until it receives a result or reaches its deadline.
const invocationAlive = setInterval(() => {}, 1000);
const context = {
  awsRequestId: process.env.EVENTBUS_LAMBDA_REQUEST_ID,
  functionName: process.env.AWS_LAMBDA_FUNCTION_NAME,
  functionVersion: process.env.AWS_LAMBDA_FUNCTION_VERSION,
  invokedFunctionArn: process.env.EVENTBUS_LAMBDA_FUNCTION_ARN,
  memoryLimitInMB: process.env.AWS_LAMBDA_FUNCTION_MEMORY_SIZE,
  logGroupName: process.env.AWS_LAMBDA_LOG_GROUP_NAME,
  logStreamName: process.env.AWS_LAMBDA_LOG_STREAM_NAME,
  callbackWaitsForEmptyEventLoop: true,
  getRemainingTimeInMillis: () => Math.max(0, deadline - Date.now()),
};
if (process.env.EVENTBUS_LAMBDA_CLIENT_CONTEXT) {
  context.clientContext = JSON.parse(process.env.EVENTBUS_LAMBDA_CLIENT_CONTEXT);
}
function writeReply(value) {
  const data = Buffer.from(JSON.stringify(value));
  let offset = 0;
  while (offset < data.length) offset += fs.writeSync(3, data, offset);
}
try {
  const event = JSON.parse(fs.readFileSync(0, 'utf8'));
  const module = await import(pathToFileURL(modulePath).href);
  const handler = module[exportName] ?? module.default?.[exportName];
  if (typeof handler !== 'function') throw new TypeError(`Handler export ${exportName} is not a function`);
  const result = await new Promise((resolve, reject) => {
    const callback = (error, value) => error ? reject(error) : resolve(value);
    context.done = callback;
    context.succeed = resolve;
    context.fail = reject;
    let returned;
    try { returned = handler(event, context, callback); } catch (error) { reject(error); return; }
    if (returned && typeof returned.then === 'function') returned.then(resolve, reject);
    else if (returned !== undefined) resolve(returned);
  });
  writeReply({result: result === undefined ? null : result});
} catch (error) {
  writeReply({error: {
    errorType: typeof error?.name === 'string' ? error.name : 'Error',
    errorMessage: typeof error?.message === 'string' ? error.message : String(error),
    stackTrace: typeof error?.stack === 'string' ? error.stack.split('\n').slice(1, 25) : [],
  }});
}
clearInterval(invocationAlive);
// Every invocation is cold. A result completes the local process; timers and
// descendants are stopped by the owning Go runner, regardless of log output.
await Promise.all([
  new Promise(resolve => process.stdout.write('', resolve)),
  new Promise(resolve => process.stderr.write('', resolve)),
]);
process.exit(0);
