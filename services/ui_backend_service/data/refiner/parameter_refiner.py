from .refinery import Refinery


class ParameterRefiner(Refinery):
    """
    Refiner class for postprocessing Run parameters.

    Uses Metaflow Client API to refine Run parameters from Metaflow Datastore.

    Parameters
    -----------
    cache : AsyncCacheClient
        An instance of a cache that implements the GetArtifacts action.
    """

    def __init__(self, cache):
        super().__init__(cache=cache)

    def _action(self):
        return self.cache_store.cache.GetParameters

    async def fetch_data(self, targets, event_stream=None, invalidate_cache=False):
        _res = await self._action()(targets, invalidate_cache=invalidate_cache)
        if _res.has_pending_request():
            async for event in _res.stream():
                if event["type"] == "error":
                    # raise error, there was an exception during processing.
                    raise GetParametersFailed(event["message"], event["id"], event["traceback"])
            await _res.wait()  # wait for results to be ready
        return _res.get() or {}  # cache get() might return None if no keys are produced.

    def _record_to_action_input(self, record):
        return "{flow_id}/{run_number}".format(**record)

    async def refine_record(self, record, values):
        return {k: {'value': v} for k, v in values.items()}


class GetParametersFailed(Exception):
    def __init__(self, msg="Failed to Get Parameters", id="failed-to-get-parameters", traceback_str=None):
        self.message = msg
        self.id = id
        self.traceback_str = traceback_str

    def __str__(self):
        return self.message
