import os
import json

import signal

from .cache_action import import_action_class_spec


def best_effort_read(key_paths):
    for key, path in key_paths:
        try:
            with open(path, 'rb') as f:
                yield key, f.read()
        except:
            pass


def execute_action(tempdir, action_spec, request_file, timeout=0):
    def timeout_handler(signum, frame):
        raise WorkerTimeoutException()

    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(timeout)  # Activate timeout, 0 = no timeout

    action_cls = import_action_class_spec(action_spec)
    with open(os.path.join(tempdir, request_file)) as f:
        request = json.load(f)

    execute(tempdir, action_cls, request)

    signal.alarm(0)  # Disable timeout


def execute(tempdir, action_cls, req):
    try:
        # prepare stream
        stream = None
        if req['stream_key']:
            stream = open(os.path.join(tempdir, req['stream_key']), 'a', buffering=1)

            def stream_output(obj):
                stream.write(json.dumps(obj) + '\n')
        else:
            stream_output = None

        # prepare keys
        keys = list(req['keys'])
        ex_keys = dict(best_effort_read(req['existing_keys'].items()))

        # execute action
        res = action_cls.execute(
            message=req['message'],
            keys=keys,
            existing_keys=ex_keys,
            stream_output=stream_output,
            invalidate_cache=req.get('invalidate_cache', False))

        # write outputs to keys
        for key, val in res.items():
            if key in ex_keys and ex_keys[key] == val:
                # Reduce disk churn by not unnecessarily writing existing keys
                # that have identical values to the newly produced ones.
                continue
            blob = val if isinstance(val, bytes) else val.encode('utf-8')
            with open(os.path.join(tempdir, req['keys'][key]), 'wb') as f:
                f.write(blob)
    finally:
        # make sure the stream is finalized so clients won't hang even if
        # the worker crashes
        if stream:
            stream.write('\n\n')
            stream.close()


class WorkerTimeoutException(Exception):
    pass
