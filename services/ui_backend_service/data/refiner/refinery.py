from typing import Any, Tuple, Optional
from services.data.db_utils import DBResponse
from services.ui_backend_service.features import FEATURE_REFINE_DISABLE
from services.utils import logging


class Refinery(object):
    """
    Used to refine objects with data only available from S3.

    Parameters
    -----------
    field_names : List[str]
        list of field names that contain S3 locations to be replaced with content.
    cache : CacheAsyncClient
        An instance of a cache client that implements the GetArtifacts action.
    """

    def __init__(self, field_names, cache=None):
        self.artifact_store = cache if cache else None
        self.field_names = field_names
        self.logger = logging.getLogger("DataRefiner")

    async def refine_record(self, record, invalidate_cache=False):
        _recs = await self.refine_records([record], invalidate_cache=invalidate_cache)
        return _recs[0] if len(_recs) > 0 else record

    async def refine_records(self, records, invalidate_cache=False):
        locations = [record[field] for field in self.field_names for record in records if field in record]
        errors = {}

        def _event_stream(event):
            if event.get("type") == "error" and event.get("key"):
                loc = artifact_location_from_key(event["key"])
                errors[loc] = event

        responses = await self.fetch_data(
            locations, event_stream=_event_stream, invalidate_cache=invalidate_cache)

        _recs = []
        for rec in records:
            _rec = rec.copy()
            for k, v in rec.items():
                if k in self.field_names:
                    if v in errors:
                        _rec["postprocess_error"] = format_error_body(
                            errors[v].get("id"),
                            errors[v].get("message"),
                            errors[v].get("traceback")
                        )
                    else:
                        _rec[k] = responses[v] if v in responses else None
            _recs.append(_rec)
        return _recs

    async def fetch_data(self, locations, event_stream=None, invalidate_cache=False):
        _locs = [loc for loc in locations if isinstance(loc, str) and loc.startswith("s3://")]
        _res = await self.artifact_store.cache.GetArtifacts(
            _locs, invalidate_cache=invalidate_cache)
        if _res.has_pending_request():
            async for event in _res.stream():
                if event["type"] == "error":
                    if event_stream:
                        event_stream(event)
            await _res.wait()  # wait for results to be ready
        return _res.get() or {}  # cache get() might return None if no keys are produced.

    async def _postprocess(self, response: DBResponse, invalidate_cache=False):
        """
        Async post processing callback that can be used as the find_records helpers
        postprocessing parameter.

        Passed in DBResponse will be refined on the configured field_names, replacing the S3 locations
        with their contents.

        Parameters
        ----------
        response : DBResponse
            The DBResponse to be refined
        invalidate_cache : Bool
            Invalidate cache before postprocessing

        Returns
        -------
        A refined DBResponse, or in case of errors, the original DBResponse
        """
        if FEATURE_REFINE_DISABLE:
            return response

        if response.response_code != 200 or not response.body:
            return response
        if isinstance(response.body, list):
            body = await self.refine_records(response.body, invalidate_cache=invalidate_cache)
        else:
            body = await self.refine_record(response.body, invalidate_cache=invalidate_cache)

        return DBResponse(response_code=response.response_code,
                          body=body)

    async def postprocess(self, response: DBResponse, invalidate_cache=False):
        raise NotImplementedError


def unpack_processed_value(value) -> Tuple[bool, Optional[Any], Optional[Any]]:
    '''
    Unpack refined response returning tuple of: success, value, detail

    Defaults to None in case values are not defined.

    Success example:
        True, 'foo', None

    Failure examples:
        False, 'failure-id', 'error-details'
        False, 'failure-id-without-details', None
        False, None, None
    '''
    return (list(value) + [None] * 3)[:3]


def format_error_body(id=None, detail=None, traceback=None):
    '''
    formatter for the "postprocess_error" key added to refined items in case of errors.
    '''
    return {
        "id": id or "artifact-refine-failure",
        "detail": detail,
        "traceback": traceback
    }


def artifact_location_from_key(x):
    "extract location from the artifact cache key"
    return x[len("search:artifactdata:"):]


class GetArtifactsFailed(Exception):
    def __init__(self, msg="Failed to Get Artifacts", id="get-artifacts-failed", traceback_str=None):
        self.message = msg
        self.id = id
        self.traceback_str = traceback_str

    def __str__(self):
        return self.message
