from .generate_dag_action import GenerateDag
from services.data.postgres_async_db import AsyncPostgresDB
from services.data.db_utils import translate_run_key
from pyee import AsyncIOEventEmitter
from metaflow.client.cache.cache_async_client import CacheAsyncClient
from .search_artifacts_action import SearchArtifacts
from .get_artifacts_action import GetArtifacts
import asyncio
import time
import os
import shutil
from services.utils import logging
from features import FEATURE_PREFETCH_ENABLE, FEATURE_CACHE_ENABLE, FEATURE_REFINE_ENABLE

# Tagged logger
logger = logging.getLogger("CacheStore")

DISK_SIZE = shutil.disk_usage("/").total

CACHE_ARTIFACT_MAX_ACTIONS = int(os.environ.get("CACHE_ARTIFACT_MAX_ACTIONS", 16))
CACHE_ARTIFACT_STORAGE_LIMIT = int(os.environ.get("CACHE_ARTIFACT_STORAGE_LIMIT", DISK_SIZE // 2))
CACHE_DAG_MAX_ACTIONS = int(os.environ.get("CACHE_DAG_MAX_ACTIONS", 16))
CACHE_DAG_STORAGE_LIMIT = int(os.environ.get("CACHE_DAG_STORAGE_LIMIT", DISK_SIZE // 4))


class CacheStore(object):
    "Singleton class for all the different cache clients that are used to access caches"
    instance = None

    def __init__(self, event_emitter=None, db=AsyncPostgresDB.get_instance()):
        if not CacheStore.instance:
            CacheStore.instance = CacheStore.__CacheStore(event_emitter, db)

    def __getattribute__(self, name):
        # delegate attribute calls to the singleton instance.
        return getattr(CacheStore.instance, name)

    class __CacheStore(object):
        def __init__(self, event_emitter=None, db=AsyncPostgresDB.get_instance()):
            self.artifact_cache = ArtifactCacheStore(event_emitter, db)
            self.dag_cache = DAGCacheStore(db)

        async def start_caches(self, app):
            await self.artifact_cache.start_cache()
            await self.dag_cache.start_cache()

        async def stop_caches(self, app):
            await self.artifact_cache.stop_cache()
            await self.dag_cache.stop_cache()


# Prefetch runs since 2 days ago (in seconds), limit maximum of 50 runs
METAFLOW_ARTIFACT_PREFETCH_RUNS_SINCE = os.environ.get('PREFETCH_RUNS_SINCE', 86400 * 2)
METAFLOW_ARTIFACT_PREFETCH_RUNS_LIMIT = os.environ.get('PREFETCH_RUNS_LIMIT', 50)


class ArtifactCacheStore(object):
    def __init__(self, event_emitter, db):
        self.event_emitter = event_emitter or AsyncIOEventEmitter()
        self._artifact_table = db.artifact_table_postgres
        self._run_table = db.run_table_postgres
        self.cache = None
        self.loop = asyncio.get_event_loop()

        # Bind an event handler for when we want to preload artifacts for
        # newly inserted content.
        if FEATURE_PREFETCH_ENABLE:
            self.event_emitter.on("preload-artifacts", self.preload_event_handler)
        self.event_emitter.on("run-parameters", self.run_parameters_event_handler)

    async def start_cache(self):
        actions = [SearchArtifacts, GetArtifacts]
        self.cache = CacheAsyncClient('cache_data/artifact_search',
                                      actions,
                                      max_size=CACHE_ARTIFACT_STORAGE_LIMIT,
                                      max_actions=CACHE_ARTIFACT_MAX_ACTIONS)
        if FEATURE_CACHE_ENABLE:
            await self.cache.start()

        if FEATURE_PREFETCH_ENABLE:
            asyncio.run_coroutine_threadsafe(self.preload_initial_data(), self.loop)

    async def preload_initial_data(self):
        "Preloads some data on cache startup"
        recent_run_ids = await self.get_recent_run_numbers()
        await self.preload_data_for_runs(recent_run_ids)

    async def preload_data_for_runs(self, run_ids):
        "preloads artifact data for given run ids. Can be used to prefetch artifacts for newly generated runs"
        artifact_locations = await self.get_artifact_locations_for_run_ids(run_ids)

        logger.info("preloading {} artifacts".format(len(artifact_locations)))

        res = await self.cache.GetArtifacts(artifact_locations)
        async for event in res.stream():
            if event["type"] == "error":
                logger.error(event)
            else:
                logger.info(event)

    async def get_recent_run_numbers(self):
        _records, _ = await self._run_table.find_records(
            conditions=["ts_epoch >= %s"],
            values=[int(round(time.time() * 1000)) - (int(METAFLOW_ARTIFACT_PREFETCH_RUNS_SINCE) * 1000)],
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
            values=[run_ids, artifact_loc],
            expanded=True
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
                r"\_%",
                "name"  # exclude the 'name' parameter as this always exists, and contains the FlowName
            ],
            fetch_single=False,
            expanded=True
        )
        # Return nothing if params artifacts were not found.
        if not db_response.response_code == 200:
            return None

        if FEATURE_PREFETCH_ENABLE and FEATURE_REFINE_ENABLE:
            # Fetch the values for the given parameters through the cache client.
            locations = [art["location"] for art in db_response.body if "location" in art]
            _params = await self.cache.GetArtifacts(locations)
            if not _params.is_ready():
                async for event in _params.stream():
                    if event["type"] == "error":
                        # raise error, there was an exception during processing.
                        raise GetParametersFailed(event["message"], event["id"], event["traceback"])
                await _params.wait()  # wait until results are ready
            _params = _params.get()
        else:
            _params = []

        combined_results = {}
        for art in db_response.body:
            if "location" in art and art["location"] in _params:
                key = art["name"]
                combined_results[key] = {
                    "value": _params[art["location"]]
                }

        return combined_results

    async def run_parameters_event_handler(self, flow_id, run_number):
        try:
            parameters = await self.get_run_parameters(flow_id, run_number)
            self.event_emitter.emit(
                "notify",
                "UPDATE",
                [f"/flows/{flow_id}/runs/{run_number}/parameters"],
                parameters
            )
        except GetParametersFailed:
            logger.error("Run parameter fetching failed")

    async def preload_event_handler(self, run_id):
        "Handler for event-emitter for preloading artifacts for a run id"
        asyncio.run_coroutine_threadsafe(self.preload_data_for_runs([run_id]), self.loop)

    async def stop_cache(self):
        await self.cache.stop()


class DAGCacheStore(object):
    def __init__(self, db):
        self._run_table = db.run_table_postgres
        self.cache = None

    async def start_cache(self):
        actions = [GenerateDag]
        self.cache = CacheAsyncClient('cache_data/dag',
                                      actions,
                                      max_size=CACHE_DAG_STORAGE_LIMIT,
                                      max_actions=CACHE_DAG_MAX_ACTIONS)
        if FEATURE_CACHE_ENABLE:
            await self.cache.start()

    async def stop_cache(self):
        await self.cache.stop()


class GetParametersFailed(Exception):
    def __init__(self, msg="Failed to Get Parameters", id="failed-to-get-parameters", traceback_str=None):
        self.message = msg
        self.id = id
        self.traceback_str = traceback_str

    def __str__(self):
        return self.message
