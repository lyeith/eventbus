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
    aws_request_id = os.environ['EVENTBUS_LAMBDA_REQUEST_ID']
    function_name = os.environ['AWS_LAMBDA_FUNCTION_NAME']
    function_version = os.environ['AWS_LAMBDA_FUNCTION_VERSION']
    invoked_function_arn = os.environ['EVENTBUS_LAMBDA_FUNCTION_ARN']
    memory_limit_in_mb = os.environ['AWS_LAMBDA_FUNCTION_MEMORY_SIZE']
    log_group_name = os.environ['AWS_LAMBDA_LOG_GROUP_NAME']
    log_stream_name = os.environ['AWS_LAMBDA_LOG_STREAM_NAME']
    identity = SimpleNamespace(cognito_identity_id=None, cognito_identity_pool_id=None)
    client_context = (client_context(json.loads(os.environ['EVENTBUS_LAMBDA_CLIENT_CONTEXT']))
                      if os.environ.get('EVENTBUS_LAMBDA_CLIENT_CONTEXT') else None)

    def get_remaining_time_in_millis(self):
        return max(0, int(os.environ['EVENTBUS_LAMBDA_DEADLINE_MS']) - int(time.time() * 1000))

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


try:
    prepare_phase_protocol()
    path = pathlib.Path(sys.argv[1])
    parts = [path.stem]
    parent = path.parent
    while (parent / '__init__.py').is_file():
        parts.insert(0, parent.name)
        parent = parent.parent
    sys.path.insert(0, str(parent))
    if len(parts) > 1:
        module = importlib.import_module('.'.join(parts))
    else:
        spec = importlib.util.spec_from_file_location('_eventbus_lambda_handler', str(path))
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
    handler = getattr(module, sys.argv[2])
    event = json.load(sys.stdin)
    context = Context()
    start_invocation_phase()
    result = handler(event, context)
    if inspect.isawaitable(result):
        if inspect.iscoroutine(result):
            result.close()
        raise TypeError('Python Lambda handlers must be synchronous')
    write_reply({'result': result})
except BaseException as error:
    close_phase_descriptors()
    write_reply({'error': {
        'errorType': type(error).__name__,
        'errorMessage': str(error),
        'stackTrace': traceback.format_tb(error.__traceback__, limit=24),
    }})
close_phase_descriptors()
# Skip atexit hooks and user threads: this process belongs to one invocation.
sys.stdout.flush()
sys.stderr.flush()
os._exit(0)
