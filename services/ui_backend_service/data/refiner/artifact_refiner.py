from .refinery import Refinery, unpack_processed_value, format_error_body, GetArtifactsFailed
from services.data.db_utils import DBResponse


class ArtifactRefiner(Refinery):
    """
    Refiner class for postprocessing Artifact rows.

    Fetches specified content from S3 and cleans up unnecessary fields from response.

    Parameters
    -----------
    cache : AsyncCacheClient
        An instance of a cache that implements the GetArtifacts action.
    """

    def __init__(self, cache):
        super().__init__(field_names=["content"], cache=cache)

    async def refine_record(self, record, invalidate_cache=False):
        _recs = self.refine_records([record], invalidate_cache=invalidate_cache)
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
        _res = await self.artifact_store.cache.GetArtifactsWithStatus(
            locations, invalidate_cache=invalidate_cache)
        if _res.has_pending_request():
            async for event in _res.stream():
                if event["type"] == "error":
                    if event_stream:
                        event_stream(event)
            await _res.wait()  # wait for results to be ready
        return _res.get() or {}  # cache get() might return None if no keys are produced.

    async def postprocess(self, response: DBResponse, invalidate_cache=False):
        """
        Calls the refiner postprocessing to fetch S3 values for content.
        In case of a successful artifact fetch, places the contents from the 'location'
        under the 'contents' key.

        Parameters
        ----------
        response : DBResponse
            The DBResponse to be refined

        Returns
        -------
        A refined DBResponse, or in case of errors, the original DBResponse
        """
        if response.response_code != 200 or not response.body:
            return response

        def _preprocess(item):
            'copy location field for refinement purposes'
            item['content'] = item['location']
            return item

        if isinstance(response.body, list):
            body = [_preprocess(task) for task in response.body]
        else:
            body = _preprocess(response.body)

        refined_response = await self._postprocess(
            DBResponse(response_code=response.response_code, body=body),
            invalidate_cache=invalidate_cache)

        def _process(item):
            if item['content'] is not None:
                success, value, detail = unpack_processed_value(item['content'])
                if success:
                    # cast artifact content to string if it was successfully fetched
                    # as some artifacts retain their type if they are Json serializable.
                    item['content'] = str(value)
                else:
                    # artifact is too big and was skipped
                    item['postprocess_error'] = format_error_body(
                        value if value else "artifact-handle-failed",
                        detail if detail else "Unknown error during artifact processing"
                    )
            return item

        if isinstance(refined_response.body, list):
            body = [_process(task) for task in refined_response.body]
        else:
            body = _process(refined_response.body)

        return DBResponse(response_code=refined_response.response_code, body=body)


def artifact_location_from_key(x):
    "extract location from the artifact cache key"
    return x[len("search:artifactdata:"):]
