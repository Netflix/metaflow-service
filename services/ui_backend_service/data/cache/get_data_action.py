import json
import hashlib

from .client import CacheAction
from services.utils import get_traceback_str
from .utils import error_event_msg

from metaflow import namespace

namespace(None)  # Always use global namespace by default


class GetData(CacheAction):
    """
    Fetches data by target list and returning their contents.

    Parameters
    ----------
    targets : List[str]
        A list of targets to fetch, usually pathspecs to be used with Metaflow Client API.
    invalidate_cache: bool
        Force cache invalidation

    Returns
    -------
    Dict or None
        example:
        {
            "targets": ["FlowId/RunNumber"]
        }
    """

    @classmethod
    def format_request(cls, *args, **kwargs):
        targets = kwargs.get('targets', [])
        invalidate_cache = kwargs.get('invalidate_cache', False)

        msg = {
            'targets': list(frozenset(sorted(targets)))
        }

        target_keys = []
        for object in targets:
            target_keys.append(cache_key_from_target(object, "data:{}:".format(cls.__name__)))

        request_id = lookup_id(targets)
        stream_key = cache_key_from_target(request_id, "data:stream:{}:".format(cls.__name__))

        return msg,\
            target_keys,\
            stream_key,\
            [stream_key],\
            invalidate_cache

    @classmethod
    def response(cls, keys_objs):
        '''Action should respond with a dictionary of
        {
            "FlowId/RunNumber": [boolean, "contents"]
        }
        '''

        target_keys = [(key, val) for key, val in keys_objs.items() if key.startswith("data:{}:".format(cls.__name__))]

        collected = {}
        for key, val in target_keys:
            collected[target_from_cache_key(key, "data:{}:".format(cls.__name__))] = json.loads(val)

        return collected

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
        """
        Execute fetching of targets.
        Produces cache results that are either successful or failed.

        Success cached example:
            results[target_key] = json.dumps([True, str(content)])
        Failure cached example:
            results[target_key] = json.dumps([False, 'artifact-too-large', artifact_data.size])

        Alternatively errors can be streamed to cache client using `stream_error`.
        This way failures won't be cached for individual artifacts, thus making
        it necessary to retry fetching during next attempt. (Will add significant overhead/delay).

        Streaming errors should be avoided to minimize latency for subsequent requests.

        Stream error example:
            stream_error(str(ex), "s3-not-found", get_traceback_str(), artifact_key)
        """
        targets = message['targets']

        if invalidate_cache:
            results = {}
            targets_to_fetch = [loc for loc in targets]
        else:
            # make a copy of already existing results, as the cache action has to produce all keys it promised
            # in the format_request response.
            results = {**existing_keys}
            # Make a list of artifact locations that require fetching (not cached previously)
            targets_to_fetch = [loc for loc in targets if not cache_key_from_target(loc, "data:{}:".format(cls.__name__)) in existing_keys]

        def stream_error(err: str, id: str, traceback: str = None):
            return stream_output(error_event_msg(err, id, traceback))

        for target in targets_to_fetch:
            target_key = cache_key_from_target(target, "data:{}:".format(cls.__name__))
            try:
                result = cls.fetch_data(target, stream_error)
                if result is None or result is False:
                    # Do not persist None or False return values
                    continue
                results[target_key] = json.dumps(result)
            except Exception as ex:
                results[target_key] = json.dumps([False, ex.__class__.__name__, get_traceback_str()])

        return results

    @classmethod
    def fetch_data(cls, *args, **kwargs):
        """
        Fetches data using Metaflow Client.

        Parameters
        ----------
        pathspec : str
            Task pathspec: "FlowId/RunNumber/StepName/TaskId"
        stream_error : Callable[[str, str, str], None]
            Stream error (Exception name, error id, traceback/details)

        Errors can be streamed to cache client using `stream_error`.
        This way failures won't be cached for individual artifacts, thus making
        it necessary to retry fetching during next attempt. (Will add significant overhead/delay).

        Stream error example:
            stream_error(str(ex), "s3-not-found", get_traceback_str())

        Success return:
            [True, "value"]

        Error return:
            [False, 'data-not-accessible', get_traceback_str()]

        Result will not be cached if return value equals None or False.
        """
        raise NotImplementedError


DATA_CACHE_ID_PREFIX = 'data:'


def cache_key_from_target(target, prefix=DATA_CACHE_ID_PREFIX):
    "construct a unique cache key for target"
    return "{}{}".format(prefix, target)


def target_from_cache_key(cache_key, prefix=DATA_CACHE_ID_PREFIX):
    "extract location from the target cache key"
    return cache_key[len(prefix):]


def lookup_id(targets):
    "construct a unique id to be used with stream_key and result_key"
    _string = "-".join(list(frozenset(sorted(targets))))
    return hashlib.sha1(_string.encode('utf-8')).hexdigest()
