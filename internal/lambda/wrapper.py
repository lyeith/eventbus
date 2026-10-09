import importlib
import importlib.util
import inspect
import json
import os
import pathlib
import sys
import time
import traceback
from types import SimpleNamespace


if os.environ.get('EVENTBUS_DEV_PYTHON_STACKS') == '1':
    try:
        # The Go owner supplies this starter from a private module namespace.
        # An unavailable diagnostic collector must not change native execution.
        _eventbus_start_python_stacks()
    except BaseException:
        pass


def client_context(value):
    if value is None:
        return None
    client = value.get('client')
    client_fields = ('installation_id', 'app_title', 'app_version_name',
                     'app_version_code', 'app_package_name')
    return SimpleNamespace(custom=value.get('custom'), env=value.get('env'),
                           client=(SimpleNamespace(**{key: client.get(key) for key in client_fields})
                                   if client is not None else None))


class Context:
    def __init__(self):
        self._deadline_ms = int(os.environ["EVENTBUS_LAMBDA_DEADLINE_MS"])
        self.aws_request_id = os.environ['EVENTBUS_LAMBDA_REQUEST_ID']
        self.function_name = os.environ['AWS_LAMBDA_FUNCTION_NAME']
        self.function_version = os.environ['AWS_LAMBDA_FUNCTION_VERSION']
        self.invoked_function_arn = os.environ['EVENTBUS_LAMBDA_FUNCTION_ARN']
        self.memory_limit_in_mb = os.environ['AWS_LAMBDA_FUNCTION_MEMORY_SIZE']
        self.log_group_name = os.environ['AWS_LAMBDA_LOG_GROUP_NAME']
        self.log_stream_name = os.environ['AWS_LAMBDA_LOG_STREAM_NAME']
        self.identity = SimpleNamespace(cognito_identity_id=None, cognito_identity_pool_id=None)
        self.client_context = (
            client_context(json.loads(os.environ['EVENTBUS_LAMBDA_CLIENT_CONTEXT']))
            if os.environ.get('EVENTBUS_LAMBDA_CLIENT_CONTEXT') else None
        )

    def get_remaining_time_in_millis(self):
        return max(0, self._deadline_ms - int(time.time() * 1000))

    def log(self, message):
        sys.stdout.write(str(message))


def write_reply(value):
    data = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(',', ':')).encode('utf-8')
    position = 0
    while position < len(data):
        position += os.write(3, data[position:])


phase_descriptors = set()


def close_phase_descriptor(descriptor):
    if descriptor in phase_descriptors:
        phase_descriptors.remove(descriptor)
        os.close(descriptor)


def close_phase_descriptors():
    for descriptor in tuple(phase_descriptors):
        try:
            close_phase_descriptor(descriptor)
        except OSError:
            pass


def prepare_phase_protocol():
    if os.environ.get('EVENTBUS_LAMBDA_PHASE_PROTOCOL') != '1':
        raise RuntimeError('Lambda phase protocol is unavailable')
    phase_descriptors.update((6, 7))
    # Application subprocesses must not inherit these invocation-owned pipes.
    os.set_inheritable(6, False)
    os.set_inheritable(7, False)


def start_invocation_phase():
    # Framing is independent of EOF: a launcher can retain its copy of a pipe.
    ready = b'{"version":1,"ready":true}\n'
    position = 0
    while position < len(ready):
        written = os.write(6, ready[position:])
        if written == 0:
            raise RuntimeError('Lambda phase readiness could not be sent')
        position += written
    close_phase_descriptor(6)
    acknowledgement = bytearray()
    while len(acknowledgement) < 1024:
        data = os.read(7, min(128, 1024 - len(acknowledgement)))
        if not data:
            raise RuntimeError('Lambda phase acknowledgement is unavailable')
        acknowledgement.extend(data)
        if b'\n' in data:
            if acknowledgement[-1:] != b'\n' or acknowledgement.count(b'\n') != 1:
                raise RuntimeError('Lambda phase acknowledgement is invalid')
            try:
                value = json.loads(acknowledgement[:-1].decode('utf-8'))
            except (ValueError, UnicodeError):
                raise RuntimeError('Lambda phase acknowledgement is invalid') from None
            if (type(value) is not dict or set(value) != {'version', 'deadline_ms'} or
                    type(value['version']) is not int or value['version'] != 1 or
                    type(value['deadline_ms']) is not int or value['deadline_ms'] <= 0 or
                    value['deadline_ms'] > 9007199254740991):
                raise RuntimeError('Lambda phase acknowledgement is invalid')
            os.environ['EVENTBUS_LAMBDA_DEADLINE_MS'] = str(value['deadline_ms'])
            close_phase_descriptor(7)
            return
    raise RuntimeError('Lambda phase acknowledgement exceeds the limit')


def load_handler():
    path = pathlib.Path(sys.argv[1])
    parts = [path.stem]
    parent = path.parent
    while (parent / '__init__.py').is_file():
        parts.insert(0, parent.name)
        parent = parent.parent
    sys.path.insert(0, str(parent))
    name = '.'.join(parts) if len(parts) > 1 else '_eventbus_lambda_handler'
    # Load the exact entry point from source, while normal import machinery
    # owns parent imports, sys.modules and package re-exports. Parent __init__
    # can import this module itself; it must still initialize exactly once.
    class PrimarySourceLoader(importlib.machinery.SourceFileLoader):
        def get_code(self, fullname):
            return compile(self.get_data(str(path)), str(path), 'exec')
    class PrimarySourceFinder:
        def find_spec(self, fullname, search_path=None, target=None):
            if fullname == name:
                return importlib.util.spec_from_file_location(name, str(path),
                    loader=PrimarySourceLoader(name, str(path)))
            return None
    finder = PrimarySourceFinder()
    sys.meta_path.insert(0, finder)
    try:
        module = importlib.import_module(name)
    finally:
        sys.meta_path.remove(finder)
    return getattr(module, sys.argv[2])


def invoke_handler(handler, event, context):
    result = handler(event, context)
    if inspect.isawaitable(result):
        if inspect.iscoroutine(result):
            result.close()
        raise TypeError('Python Lambda handlers must be synchronous')
    # Validate serialization inside the same handler-error boundary.
    json.dumps(result, ensure_ascii=False, allow_nan=False)
    return result


def error_reply(error):
    return {'errorType': type(error).__name__, 'errorMessage': str(error),
            'stackTrace': traceback.format_tb(error.__traceback__, limit=24)}


def warm_logs_boundary(request_id):
    sys.stdout.flush()
    sys.stderr.flush()
    marker = ('\x00eventbus-warm:' + os.environ['EVENTBUS_LAMBDA_WARM_LOG_TOKEN'] + ':' + request_id + '\x00').encode()
    for descriptor in (1, 2):
        position = 0
        while position < len(marker):
            position += os.write(descriptor, marker[position:])


def warm_main():
    import http.client
    connection = http.client.HTTPConnection(os.environ['AWS_LAMBDA_RUNTIME_API'])
    prefix = '/2018-06-01/runtime/'
    def post(path, value):
        data = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(',', ':')).encode()
        connection.request('POST', prefix + path, body=data, headers={'Content-Type': 'application/json'})
        response = connection.getresponse()
        response.read()
        if response.status != 202:
            raise RuntimeError('Lambda Runtime API refused response')
    try:
        handler = load_handler()
    except BaseException as error:
        warm_logs_boundary(os.environ['EVENTBUS_LAMBDA_REQUEST_ID'])
        post('init/error', error_reply(error))
        return
    while True:
        connection.request('GET', prefix + 'invocation/next')
        response = connection.getresponse()
        data = response.read()
        if response.status != 200:
            return
        request_id = response.getheader('Lambda-Runtime-Aws-Request-Id')
        os.environ['EVENTBUS_LAMBDA_REQUEST_ID'] = request_id
        os.environ['EVENTBUS_LAMBDA_DEADLINE_MS'] = response.getheader('Lambda-Runtime-Deadline-Ms')
        os.environ['EVENTBUS_LAMBDA_FUNCTION_ARN'] = response.getheader('Lambda-Runtime-Invoked-Function-Arn')
        os.environ['EVENTBUS_LAMBDA_CLIENT_CONTEXT'] = response.getheader('Lambda-Runtime-Client-Context', '')
        trace = response.getheader('Lambda-Runtime-Trace-Id')
        if trace:
            os.environ['_X_AMZN_TRACE_ID'] = trace
        else:
            os.environ.pop('_X_AMZN_TRACE_ID', None)
        try:
            value = invoke_handler(handler, json.loads(data), Context())
            path = 'response'
        except BaseException as error:
            value, path = error_reply(error), 'error'
        warm_logs_boundary(request_id)
        post('invocation/' + request_id + '/' + path, value)
        if path == 'error':
            return


if os.environ.get('EVENTBUS_LAMBDA_WARM') == '1':
    warm_main()
else:
    try:
        prepare_phase_protocol()
        handler = load_handler()
        event = json.load(sys.stdin)
        start_invocation_phase()
        context = Context()
        write_reply({'result': invoke_handler(handler, event, context)})
    except BaseException as error:
        close_phase_descriptors()
        write_reply({'error': error_reply(error)})
    close_phase_descriptors()
# Skip atexit hooks and user threads: the Go owner owns the complete process.
sys.stdout.flush()
sys.stderr.flush()
os._exit(0)
