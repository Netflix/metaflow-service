import asyncio

from services.data.db_utils import DBResponse
from services.ui_backend_service.features import FEATURE_REFINE_DISABLE
from services.ui_backend_service.data import unpack_processed_value
from services.ui_backend_service.data.cache.client.cache_client import CacheServerUnreachable
from services.utils import logging

CACHE_RETRY_MAX_ATTEMPTS = 3
CACHE_RETRY_POLL_INTERVAL = 0.5  # seconds between liveness polls
CACHE_RETRY_POLL_MAX = 20        # max polls = 10 seconds total wait


class Refinery(object):
    """
    Refiner class for postprocessing database rows.

    Uses predefined cache actions to refine database responses with Metaflow Datastore artifacts.

    Parameters
    -----------
    cache : AsyncCacheClient
        An instance of a cache that implements the GetArtifacts action.
    """

    def __init__(self, cache):
        self.cache_store = cache
        self.logger = logging.getLogger(self.__class__.__name__)

    def _action(self):
        return self.cache_store.cache.GetData

    async def _wait_for_cache_restart(self, attempt, max_retries, targets):
        """Poll until the cache subprocess is alive again after a CacheServerUnreachable error.

        Returns True if the cache recovered, False if the poll budget was exhausted (DEF-A02-D1).
        """
        self.logger.warning(
            "CacheServerUnreachable on attempt %d/%d for targets=%s, waiting for cache restart",
            attempt + 1, max_retries, targets
        )
        for _ in range(CACHE_RETRY_POLL_MAX):
            await asyncio.sleep(CACHE_RETRY_POLL_INTERVAL)
            cache = self.cache_store.cache
            if cache and cache._is_alive:
                self.logger.info(
                    "Cache is alive again, retrying (attempt %d/%d)",
                    attempt + 1, max_retries
                )
                return True
        self.logger.error(
            "Cache did not recover within %.0fs after CacheServerUnreachable (attempt %d/%d)",
            CACHE_RETRY_POLL_MAX * CACHE_RETRY_POLL_INTERVAL, attempt + 1, max_retries
        )
        return False

    async def fetch_data(self, targets, event_stream=None, invalidate_cache=False):
        self.logger.debug("fetch_data called with targets=%s, invalidate_cache=%s", targets, invalidate_cache)
        for attempt in range(CACHE_RETRY_MAX_ATTEMPTS):
            try:
                _res = await self._action()(targets, invalidate_cache=invalidate_cache)
                if _res.has_pending_request():
                    async for event in _res.stream():
                        if event["type"] == "error":
                            if event_stream:
                                event_stream(event)
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
                    # Poll budget exhausted — don't dispatch to still-dead cache (DEF-A02-D1)
                    continue

    async def refine_record(self, record, values):
        """No refinement necessary here"""
        return record

    def _response_to_action_input(self, response: DBResponse):
        if isinstance(response.body, list):
            return [self._record_to_action_input(task) for task in response.body]
        else:
            return [self._record_to_action_input(response.body)]

    def _record_to_action_input(self, record):
        return "{flow_id}/{run_number}/{step_name}/{task_id}".format(**record)

    async def postprocess(self, response: DBResponse, invalidate_cache=False):
        """
        Calls the refiner postprocessing to fetch Metaflow artifacts.

        Parameters
        ----------
        response : DBResponse
            The DBResponse to be refined

        Returns
        -------
        A refined DBResponse, or in case of errors, the original DBResponse
        """
        if FEATURE_REFINE_DISABLE:
            return response

        if response.response_code != 200 or not response.body:
            return response

        input = self._response_to_action_input(response)

        errors = {}

        def _event_stream(event):
            if event.get("type") == "error" and event.get("key"):
                # Get last element from cache key which usually translates to "target"
                target = event["key"].split(':')[-1:][0]
                errors[target] = event

        data = await self.fetch_data(
            input, event_stream=_event_stream, invalidate_cache=invalidate_cache)

        async def _process(record):
            target = self._record_to_action_input(record)

            # Check data first: a successful retry result must override stale stream
            # errors from a prior failed attempt (M7 — errors dict is never cleared
            # between retries, so checking errors before data would surface false failures)
            if target in data:
                success, value, detail, trace = unpack_processed_value(data[target])
                if success:
                    record = await self.refine_record(record, value)
                else:
                    record['postprocess_error'] = format_error_body(
                        value if value else "artifact-handle-failed",
                        detail if detail else "Unknown error during postprocessing",
                        trace
                    )
            elif target in errors:
                # No data entry — use streamed error event from the (final) attempt
                record["postprocess_error"] = format_error_body(
                    errors[target].get("id"),
                    errors[target].get("message"),
                    errors[target].get("traceback")
                )
            else:
                record['postprocess_error'] = format_error_body(
                    "artifact-value-not-found",
                    "Artifact value not found"
                )

            return record

        if isinstance(response.body, list):
            body = [await _process(task) for task in response.body]
        else:
            body = await _process(response.body)

        return DBResponse(response_code=response.response_code, body=body)


def format_error_body(id=None, detail=None, traceback=None):
    '''
    formatter for the "postprocess_error" key added to refined items in case of errors.
    '''
    return {
        "id": id or "artifact-refine-failure",
        "detail": detail,
        "traceback": traceback
    }
