import os
import json
import sys
import hashlib

from .cache_store import object_path, stream_path, is_safely_readable
from .cache_action import Check


FOREVER = 60 * 60 * 24 * 3650


class CacheServerUnreachable(Exception):
    pass


class CacheClientTimeout(Exception):
    pass


class CacheStreamCorrupted(Exception):
    pass


class CacheFuture(object):

    def __init__(self, keys, stream_key, client, action_cls, root):
        self.stream_key = stream_key
        self.stream_path = stream_path(root, stream_key) if stream_key else None
        self.action = action_cls
        self.client = client
        self.keys = keys
        self.key_paths = {key: object_path(root, key) for key in keys}
        if stream_key:
            self.key_paths[stream_key] = object_path(root, stream_key)
        self.key_objs = None

    def key_paths_ready(self):
        return all(map(is_safely_readable, self.key_paths.values()))

    def is_ready(self):
        return self.key_paths_ready() or not self.has_pending_request()

    def has_pending_request(self):
        return self.client.has_pending_request(self.stream_key)

    @property
    def is_streamable(self):
        return bool(self.stream_key)

    def wait(self, timeout=FOREVER):
        return self.client.wait(lambda: None if self.has_pending_request() else True,
                                timeout)

    def get(self):
        def _read(path):
            with open(path, 'rb') as f:
                return f.read()

        _safe_key_paths = {key: path for key, path in self.key_paths.items() if is_safely_readable(path)}
        if self.key_objs is None and self.is_ready():
            self.key_objs = {key: _read(path)
                             for key, path in _safe_key_paths.items()
                             if key != self.stream_key}

        if self.key_objs:
            return self.action.response(self.key_objs)

    def stream(self, timeout=FOREVER):

        def _wait_paths(paths):
            # wait until one of the paths is readable
            while True:
                # Make sure we still have pending cache worker request
                if not self.has_pending_request():
                    return

                for path in paths:
                    if is_safely_readable(path):
                        try:
                            f = open(path)
                        except:
                            pass
                        else:
                            yield f
                            return
                yield None

        def _readlines(paths):

            # first, wait until the stream becomes available, either through
            # a symlink or through the final object
            stream = None
            for obj in _wait_paths(paths):
                if obj is None:
                    yield None
                else:
                    stream = obj
                    break

            tail = ''
            while True:
                if not self.has_pending_request():
                    break

                buf = stream.readline()
                if buf == '':
                    yield None
                elif buf == '\n' and not tail:
                    # an empty line marks the end of stream
                    break
                elif buf[-1] == '\n':
                    try:
                        # NOTE: yield must not be None since None
                        # indicates no-result
                        msg = json.loads(tail + buf)
                    except:
                        err = "Corrupted message: '%s'" % tail + buf
                        raise CacheStreamCorrupted(err)
                    else:
                        tail = ''
                        yield msg
                else:
                    tail += buf

        if self.stream_key:
            # the stream has three layers:
            # 1) _readlines() reads raw JSON events from a file
            # 2) action.stream_response() reformats the raw events into
            #    action-specific output. Note that (2) may produce more
            #    or fewer events than (1).
            # 3) client.wait_iter() handles sync/async sleeping when no
            #    events are available.
            it = _readlines([self.stream_path, self.key_paths[self.stream_key]])
            return self.client.wait_iter(self.action.stream_response(it),
                                         timeout)


class CacheClient(object):

    def __init__(self, root, action_classes, max_actions=16, max_size=10000):

        action_classes.append(Check)
        for cls in action_classes:
            setattr(self, cls.__name__, self._action(cls))

        self._root = root
        self._prev_is_alive = 0
        self._is_alive = True
        self._action_classes = action_classes
        self._max_actions = max_actions
        self._max_size = max_size

        self.pending_requests = set()

    def start(self):
        cmd, env = subprocess_cmd_and_env('cache_server')
        cmdline = cmd + [
            '--root', os.path.abspath(self._root),
            '--max-actions', str(self._max_actions),
            '--max-size', str(self._max_size)
        ]

        msg = {
            'actions': [[c.__module__, c.__name__] for c in self._action_classes]
        }
        return self.request_and_return([self.start_server(cmdline, env),
                                        self._send('init', message=msg),
                                        self.check()],
                                       None)

    def stop(self):
        return self.stop_server()

    @property
    def is_alive(self):
        return self._is_alive

    def ping(self):
        return self._send('ping')

    def _send(self, op, **kwargs):
        req = server_request(op, **kwargs)
        return self.send_request(json.dumps(req).encode('utf-8') + b'\n')

    def _action(self, cls):

        def _call(*args, **kwargs):
            msg, keys, stream_key, disposable_keys, invalidate_cache =\
                cls.format_request(*args, **kwargs)
            future = CacheFuture(keys, stream_key, self, cls, self._root)
            if future.key_paths_ready() and not invalidate_cache:
                # cache hit
                req = None
            else:
                # Set stream_key as pending
                self.pending_requests.add(stream_key)

                # cache miss
                action_spec = '%s.%s' % (cls.__module__, cls.__name__)
                req = self._send('action',
                                 prio=cls.PRIORITY,
                                 action=action_spec,
                                 keys=keys,
                                 stream_key=stream_key,
                                 message=msg,
                                 disposable_keys=disposable_keys,
                                 invalidate_cache=invalidate_cache)

            return self.request_and_return([req] if req else [], future)

        return _call

    def has_pending_request(self, stream_key: str) -> bool:
        """
        Check if stream_key is listed as pending request.
        """
        return stream_key in self.pending_requests

    def start_server(self, cmdline, env):
        """
        Start cache_server subprocess, defined by `cmdline` and
        environment `env`.
        """
        raise NotImplementedError

    def check(self):
        """
        Call and wait on the Check action to ensure that the server is
        running.
        """
        raise NotImplementedError

    def stop_server(self):
        """
        Stop the server subprocess.
        """
        raise NotImplementedError

    def send_request(self, blob):
        """
        Send a `blob` of bytes to the server. Returns a handle to the
        request in case it needs special handling.
        """
        raise NotImplementedError

    def wait_iter(self, it, timeout):
        """
        Refine an iterator `it`, taking a pause when `None` is encountered.
        Yields not-`None` objects as is.
        """
        raise NotImplementedError

    def wait(self, fun, timeout):
        """
        Keep calling `fun` until it stops returning `None`. Returns the first
        not-`None` result of the function.
        """
        raise NotImplementedError

    def request_and_return(self, reqs, ret):
        """
        Handle requests in `reqs` and then return `ret`.
        """
        raise NotImplementedError


def subprocess_cmd_and_env(mod):
    pypath = os.environ.get('PYTHONPATH', '')
    env = os.environ.copy()
    env['PYTHONPATH'] = ':'.join((os.getcwd(), pypath))
    return [sys.executable, '-m', 'services.ui_backend_service.data.cache.client.%s' % mod], env


def server_request(op,
                   action=None,
                   prio=None,
                   keys=None,
                   stream_key=None,
                   message=None,
                   disposable_keys=None,
                   idempotency_token=None,
                   invalidate_cache=False):

    if idempotency_token is None:
        fields = [op]
        if action:
            fields.append(action)
        if keys:
            fields.extend(sorted(keys))
        if stream_key:
            fields.append(stream_key)
        token = hashlib.sha1('|'.join(fields).encode('utf-8')).hexdigest()
    else:
        token = idempotency_token

    return {
        'op': op,
        'action': action,
        'priority': prio,
        'keys': keys,
        'stream_key': stream_key,
        'message': message,
        'idempotency_token': token,
        'disposable_keys': disposable_keys,
        'invalidate_cache': invalidate_cache
    }
