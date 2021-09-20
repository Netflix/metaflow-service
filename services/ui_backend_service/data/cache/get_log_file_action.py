import hashlib
import json

from .client import CacheAction
from services.utils import get_traceback_str

from ..s3 import (
    S3AccessDenied, S3CredentialsMissing,
    S3Exception, S3NotFound,
    S3URLException, get_s3_obj, get_s3_client, get_s3_size)
from .utils import (error_event_msg, read_as_string)


class GetLogFile(CacheAction):
    """
    Gets raw log file content from S3 url, caches both the content and the current file size,
    when processing, compares file size from HEAD response of s3 file to the cached file size, and
    performs re-fetch only if the sizes differ.

    Parameters
    ----------
    s3_location : str
        The S3 location of the log file

    invalidate_cache: Boolean
        Whether to invalidate the cache or not,
        use this to force a re-check and possible refetch of the log file.

    Returns
    --------
    str
        example:
        "log line 1\nlog line2\nlog line 3"
    """

    @classmethod
    def format_request(cls, log_location, invalidate_cache=False):
        msg = {
            'log_location': log_location
        }
        log_key = log_cache_id(log_location)
        stream_key = 'log:stream:%s' % lookup_id(log_location)

        return msg,\
            [log_key],\
            stream_key,\
            [stream_key],\
            invalidate_cache

    @classmethod
    def response(cls, keys_objs):
        '''
        Return the cached log content
        '''
        return [
            json.loads(val)["content"] for key, val in keys_objs.items()
            if key.startswith('log:content')][0]

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
        location = message['log_location']
        log_key = log_cache_id(location)
        previous_result = existing_keys.get(log_key, None)
        previous_log_size = json.loads(previous_result).get("log_size", None) if previous_result else None

        def stream_error(err, id, traceback=None):
            return stream_output({"type": "error", "message": err, "id": id, "traceback": traceback})

        # Fetch the S3 locations data
        s3 = get_s3_client()
        try:
            # fetch HEAD of logfile and compare
            current_size = get_s3_size(s3, location)
            if previous_log_size and previous_log_size == current_size:
                # if filesizes match, return existing results.
                # NOTE: This might cause some amount of disk churn at the moment, as returning existing keys will still require a write to disk
                # due to us being inside execute() at this point.
                return existing_keys

            # current size has increased since last check, update size and fetch new content
            temp_obj = get_s3_obj(s3, location)
            content = read_as_string(temp_obj.name)
            results[log_key] = json.dumps({"log_size": current_size, "content": content})
        except (S3AccessDenied, S3NotFound, S3URLException, S3CredentialsMissing) as ex:
            stream_error(str(ex), ex.id)
        except S3Exception as ex:
            stream_error(get_traceback_str(), ex.id)
        except Exception:
            stream_error(get_traceback_str(), 'log-handle-failed')

        return results

# Utilities


def log_cache_id(location):
    "construct a unique cache key for log file location"
    return 'log:content:%s' % location


def lookup_id(location):
    "construct a unique id to be used with stream_key and result_key"
    return hashlib.sha1(location.encode('utf-8')).hexdigest()
