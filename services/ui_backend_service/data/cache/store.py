import asyncio
import os
import shutil
from typing import Dict, List, Optional

from .client import CacheAsyncClient
from pyee import AsyncIOEventEmitter
from services.ui_backend_service.features import (FEATURE_CACHE_ENABLE,
                                                  FEATURE_PREFETCH_ENABLE,
                                                  FEATURE_REFINE_ENABLE)
from services.utils import logging

from ..refiner import ParameterRefiner
from .generate_dag_action import GenerateDag
from .get_artifacts_action import GetArtifacts
from .get_artifacts_with_status_action import GetArtifactsWithStatus
from .search_artifacts_action import SearchArtifacts

# Tagged logger
logger = logging.getLogger("CacheStore")

DISK_SIZE = shutil.disk_usage("/").total

CACHE_ARTIFACT_MAX_ACTIONS = int(os.environ.get("CACHE_ARTIFACT_MAX_ACTIONS", 16))
CACHE_ARTIFACT_STORAGE_LIMIT = int(os.environ.get("CACHE_ARTIFACT_STORAGE_LIMIT", DISK_SIZE // 2))
CACHE_DAG_MAX_ACTIONS = int(os.environ.get("CACHE_DAG_MAX_ACTIONS", 16))
CACHE_DAG_STORAGE_LIMIT = int(os.environ.get("CACHE_DAG_STORAGE_LIMIT", DISK_SIZE // 4))


class CacheStore(object):
    """
    Collection class for all the different cache clients that are used to access caches.
    start_caches() must be called before being able to access any of the specific caches.

    Parameters
    ----------
    db : PostgresAsyncDB
        An initialized instance of a DB adapter for fetching data. Required f.ex. for preloading artifacts.
    event_emitter : AsyncIOEventEmitter
        An event emitter instance (any kind) that implements an .on('event', callback) for subscribing to events.
    """

    def __init__(self, db, event_emitter=None):
        self.artifact_cache = ArtifactCacheStore(event_emitter, db)
        self.dag_cache = DAGCacheStore(event_emitter, db)

    async def start_caches(self, app):
        "Starts all caches as part of app startup"
        await self.artifact_cache.start_cache()
        await self.dag_cache.start_cache()

        asyncio.gather(
            self._monitor_restart_requests()
        )

    async def _monitor_restart_requests(self):
        while True:
            for _cache in [self.artifact_cache, self.dag_cache]:
                if await _cache.restart_requested():
                    cache_name = type(_cache).__name__
                    logger.info("[{}] restart requested...".format(cache_name))
                    await _cache.stop_cache()
                    await _cache.start_cache()
                    logger.info("[{}] restart done.".format(cache_name))

            # Wait 5 seconds until checking requested restarts again
            await asyncio.sleep(5)

    async def stop_caches(self, app):
        "Stops all caches as part of app teardown"
        await self.artifact_cache.stop_cache()
        await self.dag_cache.stop_cache()


class ArtifactCacheStore(object):
    """
    Cache class responsible for fetching, storing and searching Artifacts from S3.

    Cache Actions
    -------------
    GetArtifacts
        Fetches artifact contents for a list of locations and returns them as a dict
    SearchArtifacts
        Fetches artifacts and matches the contents against a search term.

    Parameters
    ----------
    event_emitter : AsyncIOEventEmitter
        An event emitter instance (any kind) that implements an .on('event', callback) for subscribing to events.
    db : PostgresAsyncDB
        An initialized instance of a DB adapter for fetching data.
    """

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

    async def restart_requested(self):
        return self.cache._restart_requested if self.cache else False

    async def start_cache(self):
        "Initialize the CacheAsyncClient for artifact caching"
        actions = [SearchArtifacts, GetArtifacts, GetArtifactsWithStatus]
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
        recent_run_numbers = await self._run_table.get_recent_run_numbers()
        await self.preload_data_for_runs(recent_run_numbers)

    async def preload_data_for_runs(self, run_numbers: List[int]):
        """
        Preloads artifact data for given run numbers. Can be used to prefetch artifacts for newly generated runs.

        Parameters
        ----------
        run_numbers : List[int]
            A list of run numbers to preload data for.
        """
        artifact_locations = await self._artifact_table.get_artifact_locations_for_run_numbers(run_numbers)

        logger.info("preloading {} artifacts".format(len(artifact_locations)))

        res = await self.cache.GetArtifacts(artifact_locations)
        async for event in res.stream():
            if event["type"] == "error":
                logger.error(event)
            else:
                logger.info(event)

    async def get_run_parameters(self, flow_id: str, run_number: int, invalidate_cache=False) -> Optional[Dict]:
        """
        Fetches run parameter artifact locations, fetches the artifact content from S3, parses it, and returns
        a formatted dict of names&values

        Parameters
        ----------
        flow_id : str
            Name of the flow to get parameters for
        run_number : int
            Run number to get parameters for

        Returns
        -------
        Dict or None
            example:
            {
                "name_of_parameter": {
                    "value": "value_of_parameter"
                }
            }
        """
        db_response, *_ = await self._artifact_table.get_run_parameter_artifacts(
            flow_id, run_number,
            postprocess=self.parameter_refiner.postprocess,
            invalidate_cache=invalidate_cache)

        # Return nothing if params artifacts were not found.
        if not db_response.response_code == 200:
            return None

        return db_response.body

    async def run_parameters_event_handler(self, flow_id: str, run_number: int):
        """
        Handler for event-emitter for fetching and broadcasting run parameters when they are available.

        Parameters
        ----------
        flow_id : str
            Flow id to fetch parameters for.
        run_number : int
            Run number to fetch parameters for.
        """
        try:
            parameters = await self.get_run_parameters(flow_id, run_number)
            self.event_emitter.emit(
                "notify",
                "UPDATE",
                [f"/flows/{flow_id}/runs/{run_number}/parameters"],
                parameters
            )
        except Exception:
            logger.error("Run parameter fetching failed")

    async def preload_event_handler(self, run_number: int):
        """
        Handler for event-emitter for preloading artifacts for a run id

        Parameters
        ----------
        run_number : int
            Run number to preload data for.
        """
        asyncio.run_coroutine_threadsafe(self.preload_data_for_runs([run_number]), self.loop)

    async def stop_cache(self):
        await self.cache.stop()


class DAGCacheStore(object):
    """
    Cache class responsible for parsing and caching a DAG from a codepackage in S3.

    Cache Actions
    -------------
    GenerateDag
        Fetches a codepackage from an S3 location, decodes it, and parses a DAG from the contents.

    Parameters
    ----------
    db : PostgresAsyncDB
        An initialized instance of a DB adapter for fetching data.
    """

    def __init__(self, event_emitter, db):
        self.event_emitter = event_emitter or AsyncIOEventEmitter()
        self._run_table = db.run_table_postgres
        self.cache = None
        self.loop = asyncio.get_event_loop()

        if FEATURE_PREFETCH_ENABLE:
            self.event_emitter.on("preload-dag", self.preload_event_handler)

    async def restart_requested(self):
        return self.cache._restart_requested if self.cache else False

    async def start_cache(self):
        "Initialize the CacheAsyncClient for DAG caching"
        actions = [GenerateDag]
        self.cache = CacheAsyncClient('cache_data/dag',
                                      actions,
                                      max_size=CACHE_DAG_STORAGE_LIMIT,
                                      max_actions=CACHE_DAG_MAX_ACTIONS)
        if FEATURE_CACHE_ENABLE:
            await self.cache.start()

    async def stop_cache(self):
        await self.cache.stop()

    async def preload_event_handler(self, flow_name: str, codepackage_loc: str):
        """
        Handler for event-emitter for preloading dag

        Parameters
        ----------
        flow_name : str
            Flow name
        codepackage_loc : str
            Codepackage location (S3 string)
        """
        asyncio.run_coroutine_threadsafe(self.preload_dag(flow_name, codepackage_loc), self.loop)

    async def preload_dag(self, flow_name: str, codepackage_loc: str):
        """
        Preloads dag for given flow_name and codepackage location

        Parameters
        ----------
        flow_name : str
            Flow name
        codepackage_loc : str
            Codepackage location (S3 string)
        """
        logger.info("Preload DAG {} from {}".format(flow_name, codepackage_loc))

        res = await self.cache.GenerateDag(flow_name, codepackage_loc)
        async for event in res.stream():
            if event["type"] == "error":
                logger.error(event)
            else:
                logger.info(event)
