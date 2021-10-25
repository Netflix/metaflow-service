import os
import io
import sys
import json
import uuid
import time
import fcntl
import multiprocessing
from datetime import datetime
from collections import deque
from itertools import chain

from .cache_worker import execute_action
from .cache_async_client import OP_WORKER_CREATE, OP_WORKER_TERMINATE

import click

from .cache_action import CacheAction,\
    LO_PRIO,\
    HI_PRIO,\
    import_action_class

from .cache_store import CacheStore,\
    key_filename,\
    is_safely_readable


def send_message(op: str, data: dict):
    print(json.dumps({
        'op': op,
        **data
    }), flush=True)


class CacheServerException(Exception):
    pass


def echo(msg):
    now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')
    sys.stdout.write('CACHE [%s] %s\n' % (now, msg))


class MessageReader(object):

    def __init__(self, fd):
        # make fd non-blocking
        fl = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
        self.buf = io.BytesIO()
        self.fd = fd

    def messages(self):
        while True:
            try:
                b = os.read(self.fd, 65536)
                if not b:
                    return
            except OSError as e:
                if e.errno == 11:  # EAGAIN
                    return
            else:
                self.buf.write(b)
                if b'\n' in b:
                    new_buf = io.BytesIO()
                    self.buf.seek(0)
                    for line in self.buf:
                        if line.endswith(b'\n'):
                            try:
                                yield json.loads(line)
                            except:
                                uni = line.decode('utf-8', errors='replace')
                                echo("WARNING: Corrupted message: %s" % uni)
                                raise
                        else:
                            # return the last partial line back to the buffer
                            new_buf.write(line)
                    self.buf = new_buf

    def close(self):
        tail = self.buf.getvalue()
        if tail:
            uni = tail.decode('utf-8', errors='replace')
            echo("WARNING: Truncated message: %s" % uni)


def subprocess_cmd_and_env(mod):
    pypath = os.environ.get('PYTHONPATH', '')
    env = os.environ.copy()
    env['PYTHONPATH'] = ':'.join((os.getcwd(), pypath))
    return [sys.executable, '-m', 'services.ui_backend_service.data.cache.client.%s' % mod], env


class Worker(object):

    def __init__(self, request, filestore, pool, callback=None, error_callback=None):
        self.uuid = uuid.uuid4()
        self.request = request
        self.prio = request['priority']
        self.filestore = filestore
        self.pool = pool
        self.callback = callback
        self.error_callback = error_callback

        try:
            self.tempdir = self.filestore.open_tempdir(
                request['idempotency_token'],
                request['action'],
                request['stream_key'])
        except Exception:
            self.tempdir = None
            self.echo("Store couldn't create a temp directory. "
                      "WORKER NOT STARTED.")

    def start(self):
        keys = self.request['keys']
        ex_paths = map(self.filestore.object_path, keys)
        ex_keys = {key: path for key, path in zip(keys, ex_paths)
                   if is_safely_readable(path)}

        with open(os.path.join(self.tempdir, 'request.json'), 'w') as f:
            stream = self.request['stream_key']
            request = {
                'message': self.request['message'],
                'keys': {key: key_filename(key) for key in keys},
                'existing_keys': ex_keys,
                'stream_key': key_filename(stream) if stream else None,
                'invalidate_cache': self.request.get('invalidate_cache', False)
            }
            json.dump(request, f)

        send_message(OP_WORKER_CREATE, self._worker_details())

        self.pool.apply_async(
            func=execute_action, args=(self.tempdir, self.request['action'], 'request.json'),
            callback=self._callback, error_callback=self._error_callback)

    def _callback(self, res):
        if self.callback:
            try:
                self.callback(self, res)
            except:
                pass

        self.terminate()

    def _error_callback(self, res):
        if self.error_callback:
            try:
                self.error_callback(self, res)
            except:
                pass

        self.terminate()

    def echo(self, msg):
        token = self.request['idempotency_token']
        uuid_prefix = '[uuid %s]' % self.uuid
        echo("Worker%s[token %s] %s" % (uuid_prefix, token, msg))

    def terminate(self):
        missing = self.filestore.commit(self.tempdir,
                                        self.request['keys'],
                                        self.request['stream_key'],
                                        self.request['disposable_keys'])
        if missing:
            self.echo("failed to produce the following keys: %s"
                      % ','.join(missing))

        self.filestore.close_tempdir(self.tempdir)

        send_message(OP_WORKER_TERMINATE, self._worker_details())

    def _worker_details(self):
        return {
            'keys': len(self.request['keys']),
            'stream_key': self.request['stream_key'],
            'idempotency_token': self.request["idempotency_token"],
        }


class Scheduler(object):
    def __init__(self, filestore, max_workers):

        self.filestore = filestore
        self.max_workers = max_workers
        self.stdin_fileno = sys.stdin.fileno()
        self.stdin_reader = MessageReader(self.stdin_fileno)

        self.pending_requests = set()
        self.lo_prio_requests = deque()
        self.hi_prio_requests = deque()
        self.actions = []
        self.workers = []

        self.pool = multiprocessing.Pool(
            processes=max_workers,
            initializer=self.init_process,
            maxtasksperchild=512,  # Recycle each worker once 512 tasks have been completed
        )

    def init_process(self):
        echo("Init process %s pid: %s" % (multiprocessing.current_process().name, os.getpid()))

    def process_incoming_request(self):
        for msg in self.stdin_reader.messages():
            op = msg['op']
            prio = msg['priority']
            action = msg['action']

            if op == 'ping':
                pass
            elif op == 'init':
                actions = msg['message']['actions']
                self.validate_actions(actions)
                self.actions = frozenset('.'.join(act) for act in actions)
            elif op == 'action':
                if action not in self.actions:
                    raise CacheServerException("Unknown action: '%s'" % action)
                if msg['idempotency_token'] not in self.pending_requests:
                    self.pending_requests.add(msg['idempotency_token'])
                    if prio == HI_PRIO:
                        self.hi_prio_requests.append(msg)
                    elif prio == LO_PRIO:
                        self.lo_prio_requests.append(msg)
                    else:
                        raise CacheServerException("Unknown priority: '%s'" % prio)
            else:
                raise CacheServerException("Unknown op: '%s'" % op)

    def validate_actions(self, actions):
        for mod_str, cls_str in actions:
            try:
                cls = import_action_class(mod_str, cls_str)
                if not issubclass(cls, CacheAction):
                    raise CacheServerException("Invalid action: %s.%s"
                                               % (mod_str, cls_str))
            except ImportError:
                raise CacheServerException("Import failed: %s.%s"
                                           % (mod_str, cls_str))

    def schedule(self):
        def queued_request(queue):
            while queue:
                yield queue.popleft()

        for request in chain(queued_request(self.hi_prio_requests),
                             queued_request(self.lo_prio_requests)):
            worker = Worker(request, self.filestore, self.pool, self._callback, self._error_callback)
            try:
                if worker.tempdir:
                    worker.start()
                    return worker
            except Exception as ex:
                echo("Failed to start worker %s" % ex)

            send_message(OP_WORKER_TERMINATE, worker._worker_details())
            return None

    def loop(self):
        def new_worker_from_request():
            worker = self.schedule()
            if worker:
                self.workers.append(worker)
                return worker

        while True:
            self.process_incoming_request()
            new_worker_from_request()
            time.sleep(0.1)

    def _callback(self, worker, res):
        token = worker.request['idempotency_token']
        self.pending_requests.remove(token)
        self.workers.remove(worker)

    def _error_callback(self, worker, res):
        echo("Error from worker %s" % worker.uuid)
        token = worker.request['idempotency_token']
        self.pending_requests.remove(token)
        self.workers.remove(worker)


@click.command()
@click.option("--root",
              default='cache_data',
              help="Where to store cached objects on disk.")
@click.option("--max-actions",
              default=16,
              help="Maximum number of concurrent cache actions.")
@click.option("--max-size",
              default=10000,
              help="Maximum amount of disk space to use in MB.")
def cli(root=None,
        max_actions=None,
        max_size=None):
    store = CacheStore(root, max_size, echo)
    Scheduler(store, max_actions).loop()


if __name__ == '__main__':
    cli(auto_envvar_prefix='MFCACHE')
