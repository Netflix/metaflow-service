import hashlib
import json

import shutil
from typing import Callable, Dict, List, Optional, Tuple
from .client import CacheAction
from .utils import streamed_errors
from metaflow.client.filecache import FileCache
from metaflow.mflog import LOG_SOURCES
from metaflow.mflog.mflog import MFLogline, parse, MISSING_TIMESTAMP_STR, MISSING_TIMESTAMP
from metaflow.util import to_unicode
import os

# New imports

from metaflow import namespace, Task

namespace(None)  # Always use global namespace by default

STDOUT = 'log_location_stdout'
STDERR = 'log_location_stderr'


class GetLogFile(CacheAction):
    """
    Gets logs for a task, returning a paginated subset of the loglines.

    Parameters
    ----------
    task : Dict
        Task dictionary, example:
        {
            "flow_id": "TestFlow",
            "run_number": 1234,
            "step_name": "regular_step",
            "task_id": 456,
            "attempt_id": 0
        }

    logtype : str
        Type of log to fetch, possible values "stdout" and "stderr"

    limit : int
        how many rows to return from the logs.

    page : int
        which one of the limited log row sets to return.

    reverse_order : bool
        Reverse the log row order.

    raw_log : bool
        Control whether to return a list of dictionaries, or the raw log string contents.

    invalidate_cache: Boolean
        Whether to invalidate the cache or not,
        use this to force a re-check and possible refetch of the log file.

    Returns
    --------
    Dict
        example:
        {
            "content": [
                {"row": 1, "line": "first log line},
                {"row": 2, "line": "second log line},
            ],
            "pages": 5
        }
    """

    @classmethod
    def format_request(cls, task: Dict, logtype: str = STDOUT,
                       limit: int = 0, page: int = 1,
                       reverse_order: bool = False, raw_log: bool = False, invalidate_cache=False
                       ):
        msg = {
            'task': task,
            'logtype': logtype,
            'limit': limit,
            'page': page,
            'reverse_order': reverse_order,
            'raw_log': raw_log
        }
        log_key = log_cache_id(task, logtype)
        result_key = log_result_id(task, logtype, limit, page, reverse_order, raw_log)
        stream_key = 'log:stream:%s' % lookup_id(task, logtype, limit, page, reverse_order, raw_log)

        return (msg,
                [log_key, result_key],
                stream_key,
                [stream_key, result_key],
                invalidate_cache)

    @classmethod
    def response(cls, keys_objs):
        '''
        Return the cached log content
        '''
        return [
            json.loads(val) for key, val in keys_objs.items()
            if key.startswith('log:result')][0]

    @classmethod
    def stream_response(cls, it):
        for msg in it:
            yield msg

    @classmethod
    def execute(cls,
                message=None,
                keys=None,
                existing_keys={},
                stream_output=None,
                invalidate_cache=False,
                **kwargs):

        results = {}
        # params
        task_dict = message['task']
        attempt = int(task_dict.get('attempt_id', 0))
        limit = message['limit']
        page = message['page']
        logtype = message['logtype']
        reverse = message['reverse_order']
        output_raw = message['raw_log']
        pathspec = pathspec_for_task(task_dict)

        # keys
        log_key = log_cache_id(task_dict, logtype)
        result_key = log_result_id(task_dict, logtype, limit, page, reverse, output_raw)

        previous_log_file = existing_keys.get(log_key, None)
        previous_log_hash = json.loads(previous_log_file).get("log_hash", None) if previous_log_file else None

        log_provider = get_log_provider()
        log_hash_changed = False  # keep track if we loaded new content
        with streamed_errors(stream_output):
            task = Task(pathspec, attempt=attempt)
            # check if log has grown since last time.
            current_hash = log_provider.get_log_hash(task, logtype)
            log_hash_changed = previous_log_hash is None or previous_log_hash != current_hash

            local_paths = fetch_logs(task, log_key, logtype, log_hash_changed)
            if log_hash_changed:
                total_lines = count_total_lines(local_paths)
                results[log_key] = json.dumps({"log_hash": current_hash, "content_paths": local_paths, "line_count": total_lines})
            else:
                results = {**existing_keys}
        
            def _gen():
                return stream_sorted_logs(json.loads(results[log_key])["content_paths"])

            if log_hash_changed or result_key not in existing_keys:
                results[result_key] = json.dumps(
                    paginated_result(
                        _gen,
                        page,
                        json.loads(results[log_key])["line_count"],
                        limit,
                        reverse,
                        output_raw
                    )
                )

        return results


# Utilities

def get_log_provider():
    log_file_policy = os.environ.get('MF_LOG_LOAD_POLICY', 'full').lower()
    if log_file_policy == 'full':
        return FullLogProvider()
    elif log_file_policy == 'tail':
        # MF_LOG_LOAD_MAX_SIZE: `kilobytes`
        max_log_size = int(os.environ.get('MF_LOG_LOAD_MAX_SIZE', 20 * 1024))
        # In number of characters (UTF-8)
        tail_max_size = int(os.environ.get('MF_LOG_LOAD_TAIL_SIZE', 100 * 1024))
        return TailLogProvider(tail_max_size=tail_max_size, max_log_size_in_kb=max_log_size)
    elif log_file_policy == 'blurb_only':
        return BlurbOnlyLogProvider()
    else:
        raise ValueError("Unknown log value for MF_LOG_LOAD_POLICY (%s). "
                         "Must be 'full', 'tail', or 'blurb_only'" % log_file_policy)


def log_size_exceeded_blurb(task: Task, logtype: str, max_size: int):
    stream_name = 'stderr' if logtype == STDERR else 'stdout'
    blurb = f"""# The size of the log is greater than {int(max_size/1024)}MB which makes it unavailable for viewing on the browser.

Here is a code snippet to get logs using the Metaflow client library:
```
from metaflow import Task, namespace

namespace(None)
task = Task("{task.pathspec}", attempt={task.current_attempt})
{stream_name} = task.{stream_name}
```

# Please visit https://docs.metaflow.org/api/client for detailed documentation."""
    return blurb

def fetch_logs(task: Task, log_hash: str, logtype: str, force_reload: bool = False):
    # TODO: This could theoretically be a part of the Metaflow client instead.
    paths = []
    stream = 'stderr' if logtype == STDERR else 'stdout'
    log_location = task.metadata_dict.get('log_location_%s' % stream)
    
    # TODO: move this into the cache_data directory and introduce some GC
    log_tmp_path = os.path.join(".", "log_temp", log_hash)
    os.makedirs(log_tmp_path, exist_ok=True)
    log_paths = {}
    
    filecache = FileCache()
    if log_location:
        # Legacy log case
        log_info = json.loads(log_location)
        location = log_info["location"]
        ds_type = log_info["ds_type"]
        attempt = log_info["attempt"]
        ds_cls = filecache._get_datastore_storage_impl(ds_type)
        ds_root = ds_cls.path_join(*ds_cls.path_split(location)[:-5])

        ds = filecache._get_task_datastore(
            ds_type,
            ds_root,
            *task.path_components,
            attempt
        )
        name = ds._metadata_name_for_attempt("%s.log" % stream)
        to_load = [ds._storage_impl.path_join(ds._path, name)]
        log_paths[name]=os.path.join(log_tmp_path, name)
    else:
        # MFLog support
        meta_dict = task.metadata_dict
        ds_type = meta_dict.get("ds-type")
        ds_root = meta_dict.get("ds-root")
        if ds_type is None or ds_root is None:
            return

        attempt = task.current_attempt

        ds = filecache._get_task_datastore(
            ds_type,
            ds_root,
            *task.path_components,
            attempt
        )
        paths = dict(
                map(
                    lambda s: (
                        ds._metadata_name_for_attempt(
                            ds._get_log_location(s, stream),
                            attempt_override=False,
                        ),
                        s,
                    ),
                    LOG_SOURCES,
                )
            )
        to_load = []
        for name in paths.keys():
            path = ds._storage_impl.path_join(ds._path, name)
            to_load.append(path)
            log_paths[name]=os.path.join(log_tmp_path, name)

    # skip downloading as all files are on disk.
    skip_dl = not force_reload and all(os.path.exists(path) for path in log_paths.values())
    if not skip_dl:
        # Load the log files to disk
        with ds._storage_impl.load_bytes(to_load) as load_results:
            for key, path, meta in load_results:
                name = ds._storage_impl.basename(key)
                if path is None:
                    # no log file existed in the location. Usually not an error.
                    log_paths[name]=None
                else:
                    shutil.move(path, log_paths[name])
    return [val for val in log_paths.values() if val is not None]

def count_total_lines(paths):
    linecount = 0
    for path in paths:
        with open(path, "r") as f:
            linecount += sum(1 for _ in f)
    return linecount

def stream_sorted_logs(paths):
    def _file_line_iter(path):
        with open(path, "r") as f:
            yield from f

    iterators = {path: _file_line_iter(path) for path in paths}
    line_buffer = {path: None for path in paths}

    def _keysort(item):
        # yield the oldest line and only that line.
        return item[1]

    while True:
        if not iterators:
            # all log sources exhausted
            break
        # fill buffer with one line from each file
        empty_buffers = [k for k,v in line_buffer.items() if v is None and k in iterators]
        for it in empty_buffers:
            val = next(iterators[it], None)
            if val is None:
                # remove iterator as it is empty
                del iterators[it]
            else:
                res = parse(val)
                if not res:
                    res = MFLogline(
                        False,
                        None,
                        MISSING_TIMESTAMP_STR.encode("utf-8"),
                        None,
                        None,
                        val,
                        MISSING_TIMESTAMP,
                    )
                line_buffer[it]=res

        sorted_lines = sorted(
            [
                (source, line)
                for source, line in line_buffer.items()
                if line
            ],
            key=_keysort,
        )
        if not sorted_lines:
            break
        
        first_source, line = sorted_lines[0]
        yield _datetime_to_epoch(line.utc_tstamp), to_unicode(line.msg)
        # cleanup after yielding.
        line_buffer[first_source] = None

def get_log_size(task: Task, logtype: str):
    return task.stderr_size if logtype == STDERR else task.stdout_size


def get_log_content(task: Task, logtype: str):
    # NOTE: this re-implements some of the client logic from _load_log(self, stream)
    # for backwards compatibility of different log types.
    # Necessary due to the client not exposing a stdout/stderr property that would
    # contain the optional timestamps.
    stream = 'stderr' if logtype == STDERR else 'stdout'
    log_location = task.metadata_dict.get('log_location_%s' % stream)
    if log_location:
        for line in task._load_log_legacy(log_location, stream).split("\n"):
            yield (None, line)
    else:
        return [
            (_datetime_to_epoch(datetime), line)
            for datetime, line in task.loglines(stream)
        ]


class LogProviderBase:

    def get_log_hash(self, task: Task, logtype: str) -> int:
        raise NotImplementedError

    def get_log_content(self, task: Task, logtype: str):
        raise NotImplementedError


class TailLogProvider(LogProviderBase):
    def __init__(self, tail_max_size: int, max_log_size_in_kb: int):
        super().__init__()
        self._tail_max_size = tail_max_size
        self._max_log_size_in_kb = max_log_size_in_kb

    def get_log_hash(self, task: Task, logtype: str) -> int:
        # We can still use the true log size as a hash - still valid way to detect log growth
        return get_log_size(task, logtype)

    def get_log_content(self, task: Task, logtype: str):
        log_size_in_bytes = get_log_size(task, logtype)
        log_size = log_size_in_bytes / 1024
        if log_size > self._max_log_size_in_kb:
            return [(
                None, log_size_exceeded_blurb(task, logtype, self._max_log_size_in_kb)
            ), ]
        # Note this is inefficient - we will load a 1GB log even if we only want last 100 bytes.
        # Doing this efficiently is a step change in complexity and effort - we can do it when justified in future.
        raw_content = get_log_content(task, logtype)
        if len(raw_content) == 0:
            return raw_content  # empty list

        chars_seen = 0
        oldest_line_idx = None
        for i in range(len(raw_content) - 1, -1, -1):
            chars_seen += len(raw_content[i][1])
            if chars_seen > self._tail_max_size:
                break
            oldest_line_idx = i
        if oldest_line_idx == 0:
            return raw_content
        if oldest_line_idx is None:
            return [(raw_content[-1][0], f"All {len(raw_content)} log lines truncated.")]

        # peel the first timestamp in returned payload, attach to the user message here
        result = [(raw_content[oldest_line_idx][0], f"...{oldest_line_idx} more earlier lines truncated...")]
        result.extend(raw_content[oldest_line_idx:])
        return result


class BlurbOnlyLogProvider(LogProviderBase):
    def get_log_hash(self, task: Task, logtype: str) -> int:
        # We know the content is static
        return 42

    def get_log_content(self, task: Task, logtype: str):
        stream_name = 'stderr' if logtype == STDERR else 'stdout'
        # Improvement ideas:
        # - Use a specific Metaflow namespace (not quite trivial as we need to check various system tags to resolve.
        # - Is there anyway to also provide a CLI command, in addition to Python code?
        blurb = f"""# Your organization has disabled logs viewing from the Metaflow UI. Here is a code snippet to get logs using the Metaflow client library:

from metaflow import Task, namespace

namespace(None)
task = Task("{task.pathspec}", attempt={task.current_attempt})
{stream_name} = task.{stream_name}

# Please visit https://docs.metaflow.org/api/client for detailed documentation."""

        return [(None, line) for line in blurb.split("\n")]


class FullLogProvider(LogProviderBase):

    def get_log_hash(self, task: Task, logtype: str) -> int:
        """size is a valid hash function"""
        return get_log_size(task, logtype)

    def get_log_content(self, task: Task, logtype: str):
        return get_log_content(task, logtype)


def paginated_result(content_iterator: Callable, page: int = 1, line_total: int = 0, limit: int = 0,
                     reverse_order: bool = False, output_raw=False):
    total_pages = max(line_total // limit, 1) if limit else 1
    _offset = limit * (total_pages - page) if reverse_order else limit * (page - 1)
    loglines = []
    for lineno, item in enumerate(content_iterator()):
        ts, line = item
        if limit and lineno > (_offset + limit):
            break
        if _offset and lineno < _offset:
            continue
        l = line if output_raw else {"row": lineno, "timestamp": ts, "line": line}
        if reverse_order:
            loglines.insert(0, l)
        else:
            loglines.append(l)

    return {
        "content": "\n".join(loglines) if output_raw else loglines,
        "pages": total_pages
    }


def format_loglines(content: List[Tuple[Optional[int], str]], page: int = 1, limit: int = 0, reverse: bool = False) -> \
        Tuple[List, int]:
    "format, order and limit the log content. Return a list of log lines with row numbers"
    lines = [
        {"row": row, "timestamp": line[0], "line": line[1]}
        for row, line in enumerate(content)
    ]

    _offset = limit * (page - 1)
    pages = max(len(lines) // limit, 1) if limit else 1
    if pages < page:
        # guard against OOB page requests
        return [], pages

    if reverse:
        lines = lines[::-1]

    if limit:
        lines = lines[_offset:][:limit]

    return lines, pages


def log_cache_id(task: Dict, logtype: str):
    "construct a unique cache key for log file location"
    return "log:file:{pathspec}.{attempt_id}.{logtype}".format(
        pathspec=pathspec_for_task(task),
        attempt_id=task.get("attempt_id", 0),
        logtype=logtype
    )


def log_result_id(task: Dict, logtype: str, limit: int = 0, page: int = 1, reverse_order: bool = False,
                  raw_log: bool = False):
    "construct a unique cache key for a paginated log response"
    return "log:result:%s" % lookup_id(task, logtype, limit, page, reverse_order, raw_log)


def lookup_id(task: Dict, logtype: str, limit: int = 0, page: int = 1, reverse_order: bool = False,
              raw_log: bool = False):
    "construct a unique id to be used with stream_key and result_key"
    _string = "{file}_{limit}_{page}_{reverse}_{raw}".format(
        file=log_cache_id(task, logtype),
        limit=limit,
        page=page,
        reverse=reverse_order,
        raw=raw_log
    )
    return hashlib.sha1(_string.encode('utf-8')).hexdigest()


def pathspec_for_task(task: Dict):
    "pathspec for a task"
    # Prefer run_id over run_number
    # Prefer task_name over task_id
    return "{flow_id}/{run_id}/{step_name}/{task_name}".format(
        flow_id=task['flow_id'],
        run_id=task.get('run_id') or task['run_number'],
        step_name=task['step_name'],
        task_name=task.get('task_name') or task['task_id']
    )


def _datetime_to_epoch(datetime) -> Optional[int]:
    """convert datetime safely into an epoch in milliseconds"""
    try:
        return int(datetime.timestamp() * 1000)
    except Exception:
        # consider timestamp to be none if handling failed
        return None
