import { readFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';
import { randomUUID } from 'node:crypto';

// Module loading and handler diagnostics must not contaminate the result pipe.
const writeResult = process.stdout.write.bind(process.stdout);
process.stdout.write = process.stderr.write.bind(process.stderr);
const [modulePath, exportName, timeoutText] = process.argv.slice(1);
const deadline = Date.now() + Number(timeoutText);
const context = {
  awsRequestId: randomUUID(),
  callbackWaitsForEmptyEventLoop: false,
  functionName: exportName,
  functionVersion: '$LATEST',
  getRemainingTimeInMillis: () => Math.max(0, deadline - Date.now()),
};

// An unresolved async handler must await the Go deadline, not exit because
// Node has no active handles while its promise is pending.
setInterval(() => {}, 1000);

let event, handler, result;
try {
  event = JSON.parse(readFileSync(0, 'utf8'));
  const module = await import(pathToFileURL(modulePath).href);
  handler = module[exportName] ?? module.default?.[exportName];
  if (typeof handler !== 'function') throw new Error('Missing exported handler');
  result = await handler(event, context);
} catch {
  // Do not repeat thrown text, stack traces or the input: they can contain
  // private challenge values. App-written diagnostics already use stderr.
  await new Promise(resolve => process.stderr.write('Cognito trigger handler failed\n', resolve));
  process.exit(20);
}
let encoded;
try {
  if (!result || typeof result !== 'object' || Array.isArray(result) ||
      !result.response || typeof result.response !== 'object' || Array.isArray(result.response)) {
    throw new Error('Invalid Lambda response');
  }
  encoded = JSON.stringify(result);
} catch {
  await new Promise(resolve => process.stderr.write('Cognito trigger returned an invalid response\n', resolve));
  process.exit(21);
}
// Drain handler diagnostics before exit so output bounds cover every write.
await new Promise(resolve => process.stderr.write('', resolve));
writeResult(encoded + '\n', () => process.exit(0));
