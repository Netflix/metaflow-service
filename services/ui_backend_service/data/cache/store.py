from .generate_dag_action import GenerateDag
from pyee import AsyncIOEventEmitter
from metaflow.client.cache.cache_async_client import CacheAsyncClient
from .search_artifacts_action import SearchArtifacts
from .get_artifacts_action import GetArtifacts
from ..refiner.parameter_refiner import ParameterRefiner, GetParametersFailed
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
    '''
        Collection class for all the different cache clients that are used to access caches.
        start_caches must be called before being able to access any of the specific caches.
    '''

    def __init__(self, db, event_emitter=None):
        self.artifact_cache = ArtifactCacheStore(event_emitter, db)
        self.dag_cache = DAGCacheStore(db)

    async def start_caches(self, app):
        await self.artifact_cache.start_cache()
        await self.dag_cache.start_cache()

    async def stop_caches(self, app):
        await self.artifact_cache.stop_cache()
        await self.dag_cache.stop_cache()


class ArtifactCacheStore(object):
    def __init__(self, event_emitter, db):
        self.event_emitter = event_emitter or AsyncIOEventEmitter()
        self._artifact_table = db.artifact_table_postgres
        self._run_table = db.run_table_postgres
        self.cache = None
        self.loop = asyncio.get_event_loop()

        self.parameter_refiner = ParameterRefiner(cache=self)

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
        return await self._run_table.get_recent_run_numbers()

    async def get_artifact_locations_for_run_ids(self, run_ids=[]):
        return await self._artifact_table.get_artifact_locations_for_run_ids(run_ids)

    async def get_run_parameters(self, flow_name, run_number):
        '''Fetches run parameter artifact locations,
        fetches the artifact content from S3, parses it, and returns
        a formatted list of names&values

        Returns
        -------
        {
            "name_of_parameter": {
                "value": "value_of_parameter"
            }
        }
        OR None
        '''
        db_response, *_ = await self._artifact_table.get_run_parameter_artifacts(flow_name, run_number)

        # Return nothing if params artifacts were not found.
        if not db_response.response_code == 200:
            return None

        refined_response = await self.parameter_refiner.postprocess(db_response)
        return refined_response.body

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
