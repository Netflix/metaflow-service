from .generate_dag_action import GenerateDag
from services.data.postgres_async_db import AsyncPostgresDB
from services.data.db_utils import translate_run_key
from pyee import ExecutorEventEmitter
from metaflow.client.cache.cache_async_client import CacheAsyncClient
from .search_artifacts_action import SearchArtifacts
from .get_artifacts_action import GetArtifacts
import asyncio
import time
import os


class CacheStore(object):
    "Singleton class for all the different cache clients that are used to access caches"
    instance = None

    def __init__(self, event_emitter=None):
        if not CacheStore.instance:
            CacheStore.instance = CacheStore.__CacheStore(event_emitter)

    def __getattribute__(self, name):
        # delegate attribute calls to the singleton instance.
        return getattr(CacheStore.instance, name)

    class __CacheStore(object):
        def __init__(self, event_emitter=None):
            self.artifact_cache = ArtifactCacheStore(event_emitter)
            self.dag_cache = DAGCacheStore()

        async def start_caches(self, app):
            await self.artifact_cache.start_cache()
            await self.dag_cache.start_cache()

        async def stop_caches(self, app):
            await self.artifact_cache.stop_cache()
            await self.dag_cache.stop_cache()


METAFLOW_ARTIFACT_PREFETCH_RUNS_LIMIT = os.environ.get('PREFETCH_RUNS_LIMIT', 50)


class ArtifactCacheStore(object):
    def __init__(self, event_emitter):
        self.event_emitter = event_emitter or ExecutorEventEmitter()
        self._artifact_table = AsyncPostgresDB.get_instance().artifact_table_postgres
        self._run_table = AsyncPostgresDB.get_instance().run_table_postgres
        self.cache = None
        self.loop = asyncio.get_event_loop()

        # Bind an event handler for when we want to preload artifacts for
        # newly inserted content.
        self.event_emitter.on("preload-artifacts", self.preload_event_handler)
        self.event_emitter.on("run-parameters", self.run_parameters_event_handler)

    async def start_cache(self):
        actions = [SearchArtifacts, GetArtifacts]
        self.cache = CacheAsyncClient('cache_data/artifact_search',
                                      actions,
                                      max_size=600000,
                                      max_actions=32)
        await self.cache.start()
        asyncio.run_coroutine_threadsafe(self.preload_initial_data(), self.loop)

    async def preload_initial_data(self):
        "Preloads some data on cache startup"
        recent_run_ids = await self.get_recent_run_numbers()
        await self.preload_data_for_runs(recent_run_ids)

    async def preload_data_for_runs(self, run_ids):
        "preloads artifact data for given run ids. Can be used to prefetch artifacts for newly generated runs"
        artifact_locations = await self.get_artifact_locations_for_run_ids(run_ids)

        print("preloading {} artifacts".format(len(artifact_locations)), flush=True)

        res = await self.cache.GetArtifacts(artifact_locations)
        async for event in res.stream():
            print(event, flush=True)

    async def get_recent_run_numbers(self):
        since = int(round(time.time() * 1000)) - 86400_000 * 2  # 2 days ago
        _records, _ = await self._run_table.find_records(
            conditions=["ts_epoch >= %s"],
            values=[since],
            order=['ts_epoch DESC'],
            limit=METAFLOW_ARTIFACT_PREFETCH_RUNS_LIMIT,
            expanded=True
        )

        return [run['run_number'] for run in _records.body]

    async def get_artifact_locations_for_run_ids(self, run_ids=[]):
        # do not touch DB if no run_ids were given.
        if len(run_ids) == 0:
            return []
        # run_numbers are bigint, so cast the conditions array to the correct type.
        run_id_cond = "run_number = ANY (array[%s]::bigint[])"

        artifact_loc_cond = "ds_type = %s"
        artifact_loc = "s3"
        _records, _ = await self._artifact_table.find_records(
            conditions=[run_id_cond, artifact_loc_cond],
            values=[run_ids, artifact_loc]
        )

        # be sure to return a list of unique locations
        return list(frozenset(artifact['location'] for artifact in _records.body if 'location' in artifact))

    async def get_run_parameters(self, flow_name, run_number):
        '''Fetches run parameter artifact locations,
        fetches the artifact content from S3, parses it, and returns
        a formatted list of names&values

        Returns
        -------
        [
            {
                "name": "name_of_parameter",
                "value": "value-of-parameter-as-string"
            }
        ]
        OR None
        '''
        run_id_key, run_id_value = translate_run_key(run_number)

        # '_parameters' step has all the parameters as artifacts. only pick the
        # public parameters (no underscore prefix)
        db_response, _ = await self._artifact_table.find_records(
            conditions=[
                "flow_id = %s",
                "{run_id_key} = %s".format(run_id_key=run_id_key),
                "step_name = %s",
                "name NOT LIKE %s",
                "name <> %s"
            ],
            values=[
                flow_name,
                run_id_value,
                "_parameters",
                "\_%",
                "name"  # exclude the 'name' parameter as this always exists, and contains the FlowName
            ],
            fetch_single=False
        )
        # Return nothing if params artifacts were not found.
        if not db_response.response_code == 200:
            return None

        # Fetch the values for the given parameters through the cache client.
        locations = [art["location"] for art in db_response.body if "location" in art]
        _params = await self.cache.GetArtifacts(locations)
        if not _params.is_ready():
            async for event in _params.stream():
                if event["type"] == "error":
                    # raise error, there was an exception during processing.
                    raise GetParametersFailed(event["message"], event["id"])
            await _params.wait()  # wait until results are ready
        _params = _params.get()

        combined_results = {}
        for art in db_response.body:
            if "location" in art and art["location"] in _params:
                key = art["name"]
                combined_results[key] = {
                    "value": _params[art["location"]]
                }

        return combined_results

    def run_parameters_event_handler(self, *args, **kwargs):
        asyncio.run_coroutine_threadsafe(self._run_parameters_event_handler(*args, **kwargs), self.loop)

    async def _run_parameters_event_handler(self, flow_id, run_number):
        parameters = await self.get_run_parameters(flow_id, run_number)
        if parameters:
            self.event_emitter.emit(
                "notify",
                "UPDATE",
                [f"/flows/{flow_id}/runs/{run_number}/parameters"],
                parameters
            )

    def preload_event_handler(self, run_id):
        "Handler for event-emitter for preloading artifacts for a run id"
        asyncio.run_coroutine_threadsafe(self.preload_data_for_runs([run_id]), self.loop)

    async def stop_cache(self):
        await self.cache.stop()


class DAGCacheStore(object):
    def __init__(self):
        self._run_table = AsyncPostgresDB.get_instance().run_table_postgres
        self.cache = None

    async def start_cache(self):
        actions = [GenerateDag]
        self.cache = CacheAsyncClient('cache_data/dag',
                                      actions,
                                      max_size=100000)
        await self.cache.start()

    async def stop_cache(self):
        await self.cache.stop()


class GetParametersFailed(Exception):
    def __init__(self, msg="Failed to Get Parameters", id="failed-to-get-parameters"):
        self.message = msg
        self.id = id

    def __str__(self):
        return self.message
