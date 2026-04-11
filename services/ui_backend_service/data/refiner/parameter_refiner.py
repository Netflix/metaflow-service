from .refinery import Refinery, CACHE_RETRY_MAX_ATTEMPTS
from services.ui_backend_service.data.cache.client.cache_client import CacheServerUnreachable


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
        return self.cache_store.cache.GetParameters

    async def fetch_data(self, targets, event_stream=None, invalidate_cache=False):
        self.logger.debug("fetch_data called with targets=%s, invalidate_cache=%s", targets, invalidate_cache)
        for attempt in range(CACHE_RETRY_MAX_ATTEMPTS):
            try:
                _res = await self._action()(targets, invalidate_cache=invalidate_cache)
                if _res.has_pending_request():
                    async for event in _res.stream():
                        if event["type"] == "error":
                            raise GetParametersFailed(event["message"], event["id"], event["traceback"])
                    await _res.wait()
                return _res.get() or {}
            except CacheServerUnreachable:
                if attempt >= CACHE_RETRY_MAX_ATTEMPTS - 1:
                    self.logger.error(
                        "CacheServerUnreachable on final attempt %d/%d for targets=%s, giving up",
                        attempt + 1, CACHE_RETRY_MAX_ATTEMPTS, targets
                    )
                    raise
                recovered = await self._wait_for_cache_restart(attempt, CACHE_RETRY_MAX_ATTEMPTS, targets)
                if not recovered:
                    continue

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
