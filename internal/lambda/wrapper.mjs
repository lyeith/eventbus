import fs from 'node:fs';
import { pathToFileURL } from 'node:url';

const [modulePath, exportName] = process.argv.slice(1);
// Application-composed private adapters supply this policy, never native Invoke.
const privateExecution = process.env.EVENTBUS_LAMBDA_PRIVATE === '1';
let deadline = Number(process.env.EVENTBUS_LAMBDA_DEADLINE_MS);
// A pending user Promise alone does not keep Node alive. Lambda keeps the
// invocation active until it receives a result or reaches its deadline.
const invocationAlive = setInterval(() => {}, 1000);
function makeContext() {
  const contextDeadline = deadline;
  const context = {
    awsRequestId: process.env.EVENTBUS_LAMBDA_REQUEST_ID,
    functionName: privateExecution ? process.env.EVENTBUS_LAMBDA_CONTEXT_NAME : process.env.AWS_LAMBDA_FUNCTION_NAME,
    functionVersion: process.env.AWS_LAMBDA_FUNCTION_VERSION,
    invokedFunctionArn: process.env.EVENTBUS_LAMBDA_FUNCTION_ARN,
    memoryLimitInMB: process.env.AWS_LAMBDA_FUNCTION_MEMORY_SIZE,
    logGroupName: process.env.AWS_LAMBDA_LOG_GROUP_NAME,
    logStreamName: process.env.AWS_LAMBDA_LOG_STREAM_NAME,
    callbackWaitsForEmptyEventLoop: !privateExecution,
    getRemainingTimeInMillis: () => Math.max(0, contextDeadline - Date.now()),
  };
  if (process.env.EVENTBUS_LAMBDA_CLIENT_CONTEXT) {
    context.clientContext = JSON.parse(process.env.EVENTBUS_LAMBDA_CLIENT_CONTEXT);
  }
  return context;
}
function writeReply(value) {
  writeEncodedReply(JSON.stringify(value));
}
function writeEncodedReply(encoded) {
  const data = Buffer.from(encoded);
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
function handlerError(error) {
  return {
    errorType: typeof error?.name === 'string' ? error.name : 'Error',
    errorMessage: typeof error?.message === 'string' ? error.message : String(error),
    stackTrace: typeof error?.stack === 'string' ? error.stack.split('\n').slice(1, 25) : [],
  };
}
function failureReply(error, kind = 'EventBus.HandlerFailure') {
  if (!privateExecution) return handlerError(error);
  return {
    errorType: kind,
    errorMessage: kind === 'EventBus.InvalidResponse' ? 'Private handler response is invalid' : 'Private handler failed',
  };
}
function encodeHandlerResult(result) {
  const encoded = JSON.stringify(result === undefined ? null : result);
  if (privateExecution && encoded === undefined) throw new TypeError('Private handler response is invalid');
  return encoded;
}
async function loadHandler() {
  const module = await import(pathToFileURL(modulePath).href);
  const handler = module[exportName] ?? module.default?.[exportName];
  if (typeof handler !== 'function') throw new TypeError(`Handler export ${exportName} is not a function`);
  return handler;
}
function invokeHandler(handler, event, context) {
  // Private trigger consumers retain their direct return/Promise contract.
  // Native Lambda keeps callback/context completion support below.
  if (privateExecution) return Promise.resolve().then(() => handler(event, context));
  return new Promise((resolve, reject) => {
    const callback = (error, value) => error ? reject(error) : resolve(value);
    context.done = callback;
    context.succeed = resolve;
    context.fail = reject;
    try {
      const returned = handler(event, context, callback);
      if (returned && typeof returned.then === 'function') returned.then(resolve, reject);
      else if (returned !== undefined) resolve(returned);
    } catch (error) { reject(error); }
  });
}
async function flushLogs() {
  await Promise.all([
    new Promise(resolve => process.stdout.write('', resolve)),
    new Promise(resolve => process.stderr.write('', resolve)),
  ]);
}
async function warmLogsBoundary(requestId) {
  await flushLogs();
  const marker = Buffer.from('\0eventbus-warm:' + process.env.EVENTBUS_LAMBDA_WARM_LOG_TOKEN + ':' + requestId + '\0');
  for (const descriptor of [1, 2]) {
    let offset = 0;
    while (offset < marker.length) offset += fs.writeSync(descriptor, marker, offset);
  }
}
async function warmMain() {
  const root = 'http://' + process.env.AWS_LAMBDA_RUNTIME_API + '/2018-06-01/runtime/';
  async function post(path, encoded) {
    const response = await fetch(root + path, {method: 'POST', body: encoded, headers: {'Content-Type': 'application/json'}});
    await response.arrayBuffer();
    if (response.status !== 202) throw new Error('Lambda Runtime API refused response');
  }
  let handler;
  try { handler = await loadHandler(); }
  catch (error) {
    await warmLogsBoundary(process.env.EVENTBUS_LAMBDA_REQUEST_ID);
    await post('init/error', JSON.stringify(failureReply(error)));
    return;
  }
  while (true) {
    const response = await fetch(root + 'invocation/next');
    if (response.status !== 200) return;
    const requestId = response.headers.get('Lambda-Runtime-Aws-Request-Id');
    deadline = Number(response.headers.get('Lambda-Runtime-Deadline-Ms'));
    process.env.EVENTBUS_LAMBDA_REQUEST_ID = requestId;
    process.env.EVENTBUS_LAMBDA_DEADLINE_MS = String(deadline);
    process.env.EVENTBUS_LAMBDA_FUNCTION_ARN = response.headers.get('Lambda-Runtime-Invoked-Function-Arn');
    process.env.EVENTBUS_LAMBDA_CLIENT_CONTEXT = response.headers.get('Lambda-Runtime-Client-Context') ?? '';
    const trace = response.headers.get('Lambda-Runtime-Trace-Id');
    if (trace) process.env._X_AMZN_TRACE_ID = trace;
    else delete process.env._X_AMZN_TRACE_ID;
    let result, value, path;
    try {
      result = await invokeHandler(handler, await response.json(), makeContext());
    } catch (error) { value = JSON.stringify(failureReply(error)); path = 'error'; }
    if (path !== 'error') {
      try {
        // Encoding may execute toJSON; its bytes cross the transport once.
        value = encodeHandlerResult(result);
        path = 'response';
      } catch (error) { value = JSON.stringify(failureReply(error, 'EventBus.InvalidResponse')); path = 'error'; }
    }
    await warmLogsBoundary(requestId);
    await post('invocation/' + requestId + '/' + path, value);
    if (path === 'error') return;
  }
}
if (process.env.EVENTBUS_LAMBDA_WARM === '1') {
  await warmMain();
} else {
  try {
    preparePhaseProtocol();
    const event = JSON.parse(fs.readFileSync(0, 'utf8'));
    const handler = await loadHandler();
    startInvocationPhase();
    const context = makeContext();
    const result = await invokeHandler(handler, event, context);
    if (privateExecution) {
      try { writeEncodedReply('{"result":' + encodeHandlerResult(result) + '}'); }
      catch (error) { writeReply({error: failureReply(error, 'EventBus.InvalidResponse')}); }
    } else {
      writeReply({result: result === undefined ? null : result});
    }
  } catch (error) {
    closePhaseDescriptors();
    writeReply({error: failureReply(error)});
  }
  closePhaseDescriptors();
}
clearInterval(invocationAlive);
await flushLogs();
process.exit(0);
