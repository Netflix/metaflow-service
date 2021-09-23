import hashlib
import json

from typing import Dict, List, Tuple
from .client import CacheAction
from services.utils import get_traceback_str

from ..s3 import (
    S3AccessDenied, S3CredentialsMissing,
    S3Exception, S3NotFound,
    S3URLException)

# New imports

from metaflow import namespace, Task
STDOUT = 'log_location_stdout'
STDERR = 'log_location_stderr'


class GetLogFile(CacheAction):
    """
    Gets logs for a task, returning a paginated subset of the loglines.

    Parameters
    ----------
    s3_location : str
        The S3 location of the log file

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
            "pages": 5,
            "limit": 2
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

        return msg,\
            [log_key, result_key],\
            stream_key,\
            [stream_key, result_key],\
            invalidate_cache

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
        task = message['task']
        limit = message['limit']
        page = message['page']
        logtype = message['logtype']
        reverse = message['reverse_order']
        output_raw = message['raw_log']
        pathspec = pathspec_for_task(task)
        attempt_id = task.get("attempt_id", 0)

        # keys
        log_key = log_cache_id(task, logtype)
        result_key = log_result_id(task, logtype, limit, page, reverse, output_raw)

        previous_log_file = existing_keys.get(log_key, None)
        previous_log_size = json.loads(previous_log_file).get("log_size", None) if previous_log_file else None

        def stream_error(err, id, traceback=None):
            return stream_output({"type": "error", "message": err, "id": id, "traceback": traceback})

        log_size_changed = False  # keep track if we loaded new content
        try:
            # check if log has grown since last time.
            current_size = get_log_size(logtype, pathspec, attempt_id)
            log_size_changed = previous_log_size is None or previous_log_size != current_size

            if log_size_changed:
                content = get_log_content(logtype, pathspec, attempt_id)
                results[log_key] = json.dumps({"log_size": current_size, "content": content})
            else:
                results = {**existing_keys}
        except (S3AccessDenied, S3NotFound, S3URLException, S3CredentialsMissing) as ex:
            stream_error(str(ex), ex.id)
            raise ex from None
        except S3Exception as ex:
            stream_error(get_traceback_str(), ex.id)
            raise ex from None
        except Exception as ex:
            stream_error(get_traceback_str(), 'log-handle-failed')
            raise ex from None

        if log_size_changed or result_key not in existing_keys:
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


def get_log_size(logtype: str, pathspec: str, attempt_id: int = 0):
    # TODO: How to get logsize with metaflow cli?
    return None


def get_log_content(logtype: str, pathspec: str, attempt_id: int = 0):
    namespace(None)
    task = Task(pathspec)  # TODO: How to fetch logs for a _specific_ task attempt only???
    return task.stderr if logtype == STDERR else task.stdout


def paginated_result(content: str, page: int = 1, limit: int = 0, reverse_order: bool = False, output_raw=False):
    if not output_raw:
        loglines, total_pages = format_loglines(content, page, limit, reverse_order)
    else:
        loglines = content
        total_pages = 1

    return {
        "content": loglines,
        "pages": total_pages
    }


def format_loglines(content: str, page: int = 1, limit: int = 0, reverse: bool = False) -> Tuple[List, int]:
    "format, order and limit the log content. Return a list of log lines with row numbers"
    lines = [{"row": row, "line": line} for row, line in enumerate(content.split("\n"))]

    _offset = limit * (page - 1)
    pages = max(len(lines) // limit, 1)

    return lines[::-1][_offset:][:limit] if reverse else lines[_offset:][:limit], \
        pages


def log_cache_id(task: Dict, logtype: str):
    "construct a unique cache key for log file location"
    return "log:file:{pathspec}-{attempt}.{logtype}".format(
        pathspec=pathspec_for_task(task),
        attempt=task.get("attempt_id", 0),
        logtype=logtype
    )


def log_result_id(task: Dict, logtype: str, limit: int = 0, page: int = 1, reverse_order: bool = False, raw_log: bool = False):
    "construct a unique cache key for a paginated log response"
    return "log:result:%s" % lookup_id(task, logtype, limit, page, reverse_order, raw_log)


def lookup_id(task: Dict, logtype: str, limit: int = 0, page: int = 1, reverse_order: bool = False, raw_log: bool = False):
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
    "pathspec for a task, without the attempt id included"
    return "{flow_id}/{run_number}/{step_name}/{task_id}".format(**task)
