from .refinery import Refinery


class ParameterRefiner(Refinery):
    """
    Refiner class for postprocessing Run parameters.

    Uses Metaflow Client API to refine Run parameters from Metaflow Datastore.

    Parameters
    -----------
    cache : AsyncCacheClient
        An instance of a cache that implements the GetParameters action.
    """

    def __init__(self, cache):
        super().__init__(cache=cache)

    def _action(self):
        if self.cache_store and self.cache_store.cache:
            return self.cache_store.cache.GetParameters
        return None

    def _direct_action_class(self):
        from services.ui_backend_service.data.cache.get_parameters_action import GetParameters
        return GetParameters

    async def fetch_data(self, targets, event_stream=None, invalidate_cache=False):
        action = self._action()
        if action is not None:
            _res = await action(targets, invalidate_cache=invalidate_cache)
            if _res.has_pending_request():
                async for event in _res.stream():
                    if event["type"] == "error":
                        raise GetParametersFailed(event["message"], event["id"], event["traceback"])
                await _res.wait()
            return _res.get() or {}

        # Fall back to direct S3 fetching (no cache subprocess)
        return await self._fetch_data_direct(targets)

    def _record_to_action_input(self, record):
        # Prefer run_id over run_number
        return "{flow_id}/{run_id}".format(
            flow_id=record['flow_id'],
            run_id=record.get('run_id') or record['run_number'])

    async def refine_record(self, record, values):
        return {k: {'value': v} for k, v in values.items()}


class GetParametersFailed(Exception):
    def __init__(self, msg="Failed to Get Parameters", id="failed-to-get-parameters", traceback_str=None):
        self.message = msg
        self.id = id
        self.traceback_str = traceback_str

    def __str__(self):
        return self.message
