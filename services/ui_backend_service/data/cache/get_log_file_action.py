import hashlib
import json

from typing import Dict, List, Optional, Tuple
from .client import CacheAction
from .utils import streamed_errors
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

            if log_hash_changed:
                content = log_provider.get_log_content(task, logtype)
                results[log_key] = json.dumps({"log_hash": current_hash, "content": content})
            else:
                results = {**existing_keys}

        if log_hash_changed or result_key not in existing_keys:
            results[result_key] = json.dumps(
                paginated_result(
                    json.loads(results[log_key])["content"],
                    page,
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
        # In number of characters (UTF-8)
        tail_max_size = int(os.environ.get('MF_LOG_LOAD_TAIL_SIZE', 100 * 1024))
        return TailLogProvider(tail_max_size=tail_max_size)
    elif log_file_policy == 'blurb_only':
        return BlurbOnlyLogProvider()
    else:
        raise ValueError("Unknown log value for MF_LOG_LOAD_POLICY (%s). "
                         "Must be 'full', 'tail', or 'blurb_only'" % log_file_policy)


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
        return [
            (None, line)
            for line in task._load_log_legacy(log_location, stream).split("\n")
        ]
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
    def __init__(self, tail_max_size: int):
        super().__init__()
        self._tail_max_size = tail_max_size

    def get_log_hash(self, task: Task, logtype: str) -> int:
        # We can still use the true log size as a hash - still valid way to detect log growth
        return get_log_size(task, logtype)

    def get_log_content(self, task: Task, logtype: str):

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


def paginated_result(content: List[Tuple[Optional[int], str]], page: int = 1, limit: int = 0,
                     reverse_order: bool = False, output_raw=False):
    if not output_raw:
        loglines, total_pages = format_loglines(content, page, limit, reverse_order)
    else:
        loglines = "\n".join(line for _, line in content)
        total_pages = 1

    return {
        "content": loglines,
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
