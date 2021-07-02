from services.data.db_utils import DBResponse

from .refinery import Refinery


class ParameterRefiner(Refinery):
    """
    Refiner class for postprocessing artifact rows and extracting parameters.

    Fetches specified content from S3 and cleans up unnecessary fields from response.

    Parameters
    -----------
    cache : AsyncCacheClient
        An instance of a cache that implements the GetArtifacts action.
    """

    def __init__(self, cache):
        super().__init__(field_names=['location'], cache=cache)

    async def fetch_data(self, locations, event_stream=None, invalidate_cache=False):
        try:
            _res = await self.artifact_store.cache.GetArtifacts(locations, invalidate_cache=invalidate_cache)
            if _res.has_pending_request():
                async for event in _res.stream():
                    if event["type"] == "error":
                        # raise error, there was an exception during processing.
                        raise GetParametersFailed(event["message"], event["id"], event["traceback"])
                await _res.wait()  # wait for results to be ready
            return _res.get() or {}  # cache get() might return None if no keys are produced.
        except Exception:
            self.logger.exception("Exception when fetching artifact data from cache")
            return {}

    async def postprocess(self, response: DBResponse, invalidate_cache=False):
        """Calls the refiner postprocessing to fetch S3 values for content."""
        refined_response = await self._postprocess(response, invalidate_cache=invalidate_cache)
        if response.response_code != 200 or not response.body:
            return DBResponse(response_code=response.response_code, body={})

        if not isinstance(refined_response.body, list):
            refined_response.body = [refined_response.body]

        parameters = dict(
            (artifact.get('name', None), {'value': artifact.get('location', None)})
            for artifact in refined_response.body
        )

        return DBResponse(
            response_code=refined_response.response_code,
            body=parameters if parameters else {}
        )


class GetParametersFailed(Exception):
    def __init__(self, msg="Failed to Get Parameters", id="failed-to-get-parameters", traceback_str=None):
        self.message = msg
        self.id = id
        self.traceback_str = traceback_str

    def __str__(self):
        return self.message
