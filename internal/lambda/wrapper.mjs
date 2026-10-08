import fs from 'node:fs';
import { pathToFileURL } from 'node:url';

const [modulePath, exportName] = process.argv.slice(1);
let deadline = Number(process.env.EVENTBUS_LAMBDA_DEADLINE_MS);
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
const phaseDescriptors = new Set();
function closePhaseDescriptor(descriptor) {
  if (phaseDescriptors.delete(descriptor)) fs.closeSync(descriptor);
}
function closePhaseDescriptors() {
  for (const descriptor of [...phaseDescriptors]) {
    try { closePhaseDescriptor(descriptor); } catch {}
  }
}
function preparePhaseProtocol() {
  if (process.env.EVENTBUS_LAMBDA_PHASE_PROTOCOL !== '1') {
    throw new Error('Lambda phase protocol is unavailable');
  }
  phaseDescriptors.add(6);
  phaseDescriptors.add(7);
}
function startInvocationPhase() {
  // A launcher may retain these pipes; readiness and ACK never wait for EOF.
  const ready = Buffer.from('{"version":1,"ready":true}\n');
  let offset = 0;
  while (offset < ready.length) {
    const written = fs.writeSync(6, ready, offset, ready.length - offset);
    if (written === 0) throw new Error('Lambda phase readiness could not be sent');
    offset += written;
  }
  closePhaseDescriptor(6);
  const acknowledgement = Buffer.alloc(1024);
  offset = 0;
  while (offset < acknowledgement.length) {
    const read = fs.readSync(7, acknowledgement, offset, acknowledgement.length - offset, null);
    if (read === 0) throw new Error('Lambda phase acknowledgement is unavailable');
    offset += read;
    const data = acknowledgement.subarray(0, offset);
    const newline = data.indexOf(10);
    if (newline === -1) continue;
    if (newline !== offset - 1) throw new Error('Lambda phase acknowledgement is invalid');
    let value;
    try { value = JSON.parse(data.subarray(0, newline).toString('utf8')); }
    catch { throw new Error('Lambda phase acknowledgement is invalid'); }
    const keys = value !== null && typeof value === 'object' ? Object.keys(value) : [];
    if (value === null || typeof value !== 'object' || Array.isArray(value) ||
        keys.length !== 2 || !keys.includes('version') || !keys.includes('deadline_ms') || value.version !== 1 ||
        !Number.isSafeInteger(value.deadline_ms) || value.deadline_ms <= 0) {
      throw new Error('Lambda phase acknowledgement is invalid');
    }
    deadline = value.deadline_ms;
    process.env.EVENTBUS_LAMBDA_DEADLINE_MS = String(deadline);
    closePhaseDescriptor(7);
    return;
  }
  throw new Error('Lambda phase acknowledgement exceeds the limit');
}
try {
  preparePhaseProtocol();
  const event = JSON.parse(fs.readFileSync(0, 'utf8'));
  const module = await import(pathToFileURL(modulePath).href);
  const handler = module[exportName] ?? module.default?.[exportName];
  if (typeof handler !== 'function') throw new TypeError(`Handler export ${exportName} is not a function`);
  let invokeHandler;
  const completion = new Promise((resolve, reject) => {
    const callback = (error, value) => error ? reject(error) : resolve(value);
    context.done = callback;
    context.succeed = resolve;
    context.fail = reject;
    invokeHandler = () => {
      try {
        const returned = handler(event, context, callback);
        if (returned && typeof returned.then === 'function') returned.then(resolve, reject);
        else if (returned !== undefined) resolve(returned);
      } catch (error) { reject(error); }
    };
  });
  startInvocationPhase();
  invokeHandler();
  const result = await completion;
  writeReply({result: result === undefined ? null : result});
} catch (error) {
  closePhaseDescriptors();
  writeReply({error: {
    errorType: typeof error?.name === 'string' ? error.name : 'Error',
    errorMessage: typeof error?.message === 'string' ? error.message : String(error),
    stackTrace: typeof error?.stack === 'string' ? error.stack.split('\n').slice(1, 25) : [],
  }});
}
closePhaseDescriptors();
clearInterval(invocationAlive);
// Every invocation is cold. A result completes the local process; timers and
// descendants are stopped by the owning Go runner, regardless of log output.
await Promise.all([
  new Promise(resolve => process.stdout.write('', resolve)),
  new Promise(resolve => process.stderr.write('', resolve)),
]);
process.exit(0);
