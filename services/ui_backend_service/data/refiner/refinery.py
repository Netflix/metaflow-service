import asyncio

from services.data.db_utils import DBResponse
from services.ui_backend_service.features import FEATURE_REFINE_DISABLE
from services.ui_backend_service.data import unpack_processed_value
from services.utils import logging


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
        if self.cache_store and self.cache_store.cache:
            return self.cache_store.cache.GetData
        return None

    def _direct_action_class(self):
        """Return the CacheAction class for direct (non-cache) fetching. Override in subclasses."""
        return None

    async def _fetch_data_direct(self, targets):
        """Fetch data directly from S3 without cache subprocess, using run_in_executor."""
        action_cls = self._direct_action_class()
        if action_cls is None:
            return {}

        loop = asyncio.get_event_loop()
        msg = {'targets': list(frozenset(sorted(targets)))}
        target_keys = ["data:{}:{}".format(action_cls.__name__, t) for t in targets]

        try:
            results = await loop.run_in_executor(
                None,
                lambda: action_cls.execute(
                    message=msg,
                    keys=target_keys,
                    existing_keys={},
                    stream_output=lambda x: None,
                    invalidate_cache=True,
                )
            )
            return action_cls.response(results)
        except Exception as e:
            self.logger.error("Direct fetch failed: %s", e)
            return {}

    async def fetch_data(self, targets, event_stream=None, invalidate_cache=False):
        action = self._action()
        if action is not None:
            _res = await action(targets, invalidate_cache=invalidate_cache)
            if _res.has_pending_request():
                async for event in _res.stream():
                    if event["type"] == "error":
                        if event_stream:
                            event_stream(event)
                await _res.wait()  # wait for results to be ready
            return _res.get() or {}  # cache get() might return None if no keys are produced.

        # Fall back to direct S3 fetching (no cache subprocess)
        return await self._fetch_data_direct(targets)

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

            if target in errors:
                # Add streamed postprocess errors if any
                record["postprocess_error"] = format_error_body(
                    errors[target].get("id"),
                    errors[target].get("message"),
                    errors[target].get("traceback")
                )

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
