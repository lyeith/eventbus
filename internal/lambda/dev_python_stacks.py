"""Opt-in safe Python frames for one actual EventBus invocation.

This source is executed in a private module namespace only when the Go owner
supplies the dedicated descriptors. It never reads application locals, formats
tracebacks, changes signal handlers, or controls invocation completion.
"""

import asyncio
import json
import os
import select
import sys
import threading
import time
import types
import weakref


_MAX_REQUEST_BYTES = 1024
_MAX_REPLY_BYTES = 64 * 1024
_MAX_THREADS = 32
_MAX_LOOPS = 8
_MAX_TASKS = 64
_MAX_FRAMES = 32
_MAX_STRING_BYTES = 256
_COLLECTION_SECONDS = 0.100
# Reserve time for bounded serialization and pipe publication. A blocked loop
# yields unavailable metadata rather than consuming the publication budget.
_QUERY_SECONDS = 0.075
_FRAME_BYTES = 24 * 1024


def _eventbus_start_python_stacks(command_fd=4, response_fd=5):
    """Start one daemon collector; fd3, logs, native deadlines stay untouched."""
    os.set_inheritable(command_fd, False)
    os.set_inheritable(response_fd, False)
    os.set_blocking(response_fd, False)

    monotonic = time.monotonic
    current_frames = sys._current_frames
    task_type = asyncio.Task
    task_get_coro = task_type.get_coro
    task_done = task_type.done
    task_cancelled = task_type.cancelled
    all_tasks = asyncio.all_tasks
    base_loop = asyncio.BaseEventLoop
    original_run_forever = base_loop.run_forever
    schedule_callback = base_loop.call_soon_threadsafe
    loop_is_closed = base_loop.is_closed
    native_loop_types = tuple(loop_type for loop_type in (
        getattr(asyncio, 'SelectorEventLoop', None),
        getattr(asyncio, 'ProactorEventLoop', None),
    ) if loop_type is not None)
    registry = {}
    registry_lock = threading.Lock()
    registry_truncated = [False]

    def registered_run_forever(loop, *args, **kwargs):
        registered = False
        try:
            with registry_lock:
                if len(registry) < _MAX_LOOPS:
                    registry[id(loop)] = weakref.ref(loop)
                    registered = True
                else:
                    registry_truncated[0] = True
        except BaseException:
            # Diagnostics must not replace the native loop result or exception.
            pass
        try:
            return original_run_forever(loop, *args, **kwargs)
        finally:
            if registered:
                try:
                    with registry_lock:
                        registry.pop(id(loop), None)
                except BaseException:
                    pass

    class Token:
        def __init__(self, deadline):
            self.deadline = deadline
            self.live = True
            self.lock = threading.Lock()
            self.tasks_remaining = _MAX_TASKS

        def active(self):
            with self.lock:
                return self.live and monotonic() < self.deadline

        def claim_task(self):
            with self.lock:
                if (not self.live or monotonic() >= self.deadline or
                        self.tasks_remaining == 0):
                    return False
                self.tasks_remaining -= 1
                return True

        def stop(self):
            with self.lock:
                self.live = False

    class FrameBudget:
        def __init__(self):
            self.remaining = _FRAME_BYTES
            self.lock = threading.Lock()

        def retain(self, frame):
            # Count the actual bounded JSON representation before retention.
            cost = len(json.dumps(frame, ensure_ascii=False,
                                  separators=(',', ':')).encode('utf-8'))
            with self.lock:
                if cost > self.remaining:
                    return False
                self.remaining -= cost
                return True

    class Query:
        def __init__(self, loop_id):
            self.loop_id = loop_id
            self.event = threading.Event()
            self.result = None
            self.lock = threading.Lock()

        def publish(self, token, result):
            # The query can outlive the collection window in a loop's queue.
            # Never publish or retain application objects after it expires.
            with token.lock:
                if not token.live or monotonic() >= token.deadline:
                    return
                with self.lock:
                    self.result = result
                    self.event.set()

        def snapshot(self):
            with self.lock:
                return self.result

    def safe_identifier(value):
        # Code-object identifiers are strings. Slice before encoding/filtering,
        # so a dynamically generated filename cannot allocate an unbounded copy.
        prefix = value[:_MAX_STRING_BYTES]
        cleaned = ''.join(character for character in prefix
                          if character.isprintable())
        encoded = cleaned.encode('utf-8', errors='replace')
        clipped = encoded[:_MAX_STRING_BYTES].decode('utf-8', errors='ignore')
        return clipped, (len(value) > len(prefix) or cleaned != prefix or
                         len(encoded) > _MAX_STRING_BYTES)

    def safe_frame(frame, budget):
        code = frame.f_code
        function, function_truncated = safe_identifier(code.co_name)
        filename, filename_truncated = safe_identifier(code.co_filename)
        result = {'function': function, 'file': filename,
                  'line': max(0, frame.f_lineno)}
        if not budget.retain(result):
            return None, True
        return result, function_truncated or filename_truncated

    def thread_stack(frame, token, budget):
        frames = []
        truncated = False
        while frame is not None:
            if len(frames) == _MAX_FRAMES or not token.active():
                truncated = True
                break
            item, clipped = safe_frame(frame, budget)
            truncated = truncated or clipped
            if item is None:
                break
            frames.append(item)
            frame = frame.f_back
        return frames, truncated

    def task_stack(task, token, budget):
        # Only exact native types are traversed: no user-defined attribute lookup,
        # task names, Future internals, result/exception access, or object reprs.
        cursor = task_get_coro(task)
        frames = []
        seen = set()
        truncated = False
        reason = None
        while cursor is not None:
            if len(frames) == _MAX_FRAMES or not token.active():
                truncated = True
                break
            cursor_id = id(cursor)
            if cursor_id in seen:
                truncated = True
                reason = 'await_cycle'
                break
            seen.add(cursor_id)
            cursor_type = type(cursor)
            if cursor_type is types.CoroutineType:
                frame = cursor.cr_frame
                next_cursor = cursor.cr_await
            elif cursor_type is types.GeneratorType:
                frame = cursor.gi_frame
                next_cursor = cursor.gi_yieldfrom
            elif cursor_type is types.AsyncGeneratorType:
                frame = cursor.ag_frame
                next_cursor = cursor.ag_await
            else:
                reason = 'opaque_awaitable'
                break
            if frame is not None:
                item, clipped = safe_frame(frame, budget)
                truncated = truncated or clipped
                if item is None:
                    break
                frames.append(item)
            cursor = next_cursor
            # Closed native objects can have no frame. Bound those chains too.
            if len(seen) == _MAX_FRAMES and cursor is not None:
                truncated = True
                break
        return frames, truncated, reason

    def collect_loop(loop, query, token, budget):
        if not token.active():
            return
        result = {'id': query.loop_id, 'state': 'running', 'tasks': [],
                  'truncated': False}
        try:
            # This callback owns the loop thread. Public Task APIs are not read
            # from the control thread, including during loop shutdown.
            tasks = all_tasks(loop)
            for task in tasks:
                if not token.claim_task():
                    result['truncated'] = True
                    break
                if type(task) is not task_type:
                    result['tasks'].append({
                        'id': id(task), 'state': 'pending', 'frames': [],
                        'truncated': False, 'reason': 'unsupported_task_type',
                    })
                    continue
                state = ('canceled' if task_cancelled(task) else
                         'done' if task_done(task) else 'pending')
                frames, truncated, reason = task_stack(task, token, budget)
                item = {'id': id(task), 'state': state, 'frames': frames,
                        'truncated': truncated}
                if reason is not None:
                    item['reason'] = reason
                result['tasks'].append(item)
                result['truncated'] = result['truncated'] or truncated
        except BaseException:
            result = unavailable_loop(query.loop_id, 'task_collection_failed')
        query.publish(token, result)

    def unavailable_loop(loop_id, reason):
        return {'id': loop_id, 'state': 'unavailable', 'reason': reason,
                'tasks': [], 'truncated': False}

    def collect(started):
        token = Token(started + _QUERY_SECONDS)
        thread_budget = FrameBudget()
        task_budget = FrameBudget()
        result = {'status': 'captured', 'truncated': False,
                  'threads': [], 'loops': []}
        try:
            frames = current_frames()
            helper_id = threading.get_ident()
            for thread_id, frame in frames.items():
                if thread_id == helper_id:
                    continue
                if len(result['threads']) == _MAX_THREADS or not token.active():
                    result['truncated'] = True
                    break
                stack, truncated = thread_stack(frame, token, thread_budget)
                result['threads'].append({'id': thread_id, 'state': 'alive',
                                          'frames': stack, 'truncated': truncated})
                result['truncated'] = result['truncated'] or truncated
            del frames
        except BaseException:
            result['reason'] = 'thread_frames_unavailable'
        with registry_lock:
            loops = list(registry.items())
            result['truncated'] = result['truncated'] or registry_truncated[0]
        queries = []
        for loop_id, reference in loops:
            loop = reference()
            if loop is None:
                result['loops'].append(unavailable_loop(loop_id, 'loop_closed'))
                continue
            if type(loop) not in native_loop_types:
                result['loops'].append(unavailable_loop(loop_id, 'unsupported_loop_type'))
                continue
            query = Query(loop_id)
            try:
                if loop_is_closed(loop):
                    result['loops'].append(unavailable_loop(loop_id, 'loop_closed'))
                    continue
                schedule_callback(loop, collect_loop, loop, query, token, task_budget)
                queries.append(query)
            except BaseException:
                result['loops'].append(unavailable_loop(loop_id, 'loop_closed'))
        for query in queries:
            query.event.wait(max(0, token.deadline - monotonic()))
        token.stop()
        for query in queries:
            loop_result = query.snapshot()
            if loop_result is None:
                loop_result = unavailable_loop(query.loop_id, 'callback_timeout')
            result['loops'].append(loop_result)
            result['truncated'] = result['truncated'] or loop_result['truncated']
        if not result['threads'] and not any(loop['state'] == 'running'
                                            for loop in result['loops']):
            result['status'] = 'capture_failed'
            result.setdefault('reason', 'frames_unavailable')
        return result

    def encode_reply(result):
        # Counts and frame-byte budgets bound intermediate structures too.
        # Keep metadata valid if even the capped structure exceeds the wire cap.
        while True:
            data = json.dumps(result, ensure_ascii=False,
                              separators=(',', ':')).encode('utf-8')
            if len(data) < _MAX_REPLY_BYTES:
                # A launcher may keep its inherited writing descriptor open.
                # Frame the live reply without requiring process-exit EOF.
                return data + b'\n'
            result['truncated'] = True
            candidates = [thread['frames'] for thread in result['threads']]
            candidates.extend(task['frames'] for loop in result['loops']
                              for task in loop['tasks'])
            largest = max(candidates, key=len, default=None)
            if largest:
                largest.pop()
                for thread in result['threads']:
                    if thread['frames'] is largest:
                        thread['truncated'] = True
                for loop in result['loops']:
                    for task in loop['tasks']:
                        if task['frames'] is largest:
                            task['truncated'] = True
                            loop['truncated'] = True
                continue
            return b'{"status":"capture_failed","reason":"reply_size_limit","truncated":true,"threads":[],"loops":[]}\n'

    def write_reply(data, deadline):
        position = 0
        while position < len(data) and monotonic() < deadline:
            try:
                position += os.write(response_fd, data[position:])
            except BlockingIOError:
                select.select([], [response_fd], [], max(0, deadline - monotonic()))
            except BaseException:
                return

    def control():
        try:
            request = bytearray()
            while len(request) <= _MAX_REQUEST_BYTES:
                chunk = os.read(command_fd, min(128, _MAX_REQUEST_BYTES + 1 - len(request)))
                if not chunk:
                    return
                request.extend(chunk)
                if b'\n' in chunk:
                    break
            started = monotonic()
            if request != b'snapshot\n':
                result = {'status': 'capture_failed', 'reason': 'invalid_request',
                          'truncated': False, 'threads': [], 'loops': []}
            else:
                try:
                    result = collect(started)
                except BaseException:
                    result = {'status': 'capture_failed', 'reason': 'collector_failed',
                              'truncated': False, 'threads': [], 'loops': []}
            write_reply(encode_reply(result), started + _COLLECTION_SECONDS)
        except BaseException:
            # Descriptor failure cannot affect the handler or expose an error.
            pass
        finally:
            for descriptor in (command_fd, response_fd):
                try:
                    os.close(descriptor)
                except OSError:
                    pass

    base_loop.run_forever = registered_run_forever
    try:
        threading.Thread(target=control, daemon=True).start()
    except BaseException:
        base_loop.run_forever = original_run_forever
        for descriptor in (command_fd, response_fd):
            try:
                os.close(descriptor)
            except OSError:
                pass
        raise
