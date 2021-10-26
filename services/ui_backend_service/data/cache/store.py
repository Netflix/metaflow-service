import asyncio
import os
import shutil
from typing import Dict, List, Optional

from .client import CacheAsyncClient
from pyee import AsyncIOEventEmitter
from services.ui_backend_service.features import (FEATURE_CACHE_ENABLE,
                                                  FEATURE_PREFETCH_ENABLE)
from services.utils import logging

from ..refiner import ParameterRefiner, TaskRefiner
from .generate_dag_action import GenerateDag
from .get_artifacts_action import GetArtifacts
from .search_artifacts_action import SearchArtifacts
from .get_log_file_action import GetLogFile
from .get_data_action import GetData
from .get_parameters_action import GetParameters
from .get_task_action import GetTask

# Tagged logger
logger = logging.getLogger("CacheStore")

DISK_SIZE = shutil.disk_usage("/").total

CACHE_ARTIFACT_MAX_ACTIONS = int(os.environ.get("CACHE_ARTIFACT_MAX_ACTIONS", 16))
CACHE_ARTIFACT_STORAGE_LIMIT = int(os.environ.get("CACHE_ARTIFACT_STORAGE_LIMIT", DISK_SIZE // 2))
CACHE_DAG_MAX_ACTIONS = int(os.environ.get("CACHE_DAG_MAX_ACTIONS", 16))
CACHE_DAG_STORAGE_LIMIT = int(os.environ.get("CACHE_DAG_STORAGE_LIMIT", DISK_SIZE // 4))
CACHE_LOG_MAX_ACTIONS = int(os.environ.get("CACHE_LOG_MAX_ACTIONS", 8))
CACHE_LOG_STORAGE_LIMIT = int(os.environ.get("CACHE_LOG_STORAGE_LIMIT", DISK_SIZE // 5))


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
        self.db = db
        self.artifact_cache = ArtifactCacheStore(event_emitter, db)
        self.dag_cache = DAGCacheStore(event_emitter, db)
        self.log_cache = LogCacheStore(event_emitter)

    async def start_caches(self, app):
        "Starts all caches as part of app startup"
        await self.artifact_cache.start_cache()
        await self.dag_cache.start_cache()
        await self.log_cache.start_cache()

        asyncio.gather(
            self._monitor_restart_requests()
        )

        if FEATURE_PREFETCH_ENABLE:
            asyncio.run_coroutine_threadsafe(
                self.preload_initial_data(), asyncio.get_event_loop())

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

    async def preload_initial_data(self):
        "Preloads some data on cache startup"
        recent_runs = await self.db.run_table_postgres.get_recent_runs()
        logger.info("Preloading {} runs".format(len(recent_runs)))
        await asyncio.gather(
            self.artifact_cache.preload_data_for_runs(recent_runs),
            self.dag_cache.preload_dags(recent_runs)
        )

    async def stop_caches(self, app):
        "Stops all caches as part of app teardown"
        await self.artifact_cache.stop_cache()
        await self.dag_cache.stop_cache()
        await self.log_cache.stop_cache()


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
        self._task_table = db.task_table_postgres
        self.cache = None
        self.loop = asyncio.get_event_loop()

        self.parameter_refiner = ParameterRefiner(cache=self)
        self.task_refiner = TaskRefiner(cache=self)

        # Bind an event handler for when we want to preload artifacts for
        # newly inserted content.
        if FEATURE_PREFETCH_ENABLE:
            self.event_emitter.on("preload-task-statuses", self.preload_task_statuses_event_handler)
            self.event_emitter.on("preload-run-parameters", self.preload_run_parameters_event_handler)

    async def restart_requested(self):
        return self.cache._restart_requested if self.cache else False

    async def start_cache(self):
        "Initialize the CacheAsyncClient for artifact caching"
        actions = [GetData, SearchArtifacts, GetTask, GetArtifacts, GetParameters]
        self.cache = CacheAsyncClient('cache_data/artifact_search',
                                      actions,
                                      max_size=CACHE_ARTIFACT_STORAGE_LIMIT,
                                      max_actions=CACHE_ARTIFACT_MAX_ACTIONS)
        if FEATURE_CACHE_ENABLE:
            await self.cache.start()

    async def preload_data_for_runs(self, runs: List[Dict]):
        """
        Preloads task statuses for given run numbers.
        Can be used to prefetch task statuses for newly generated runs.

        Parameters
        ----------
        runs : List[RunRow]
            A list of run numbers to preload data for.
        """
        for run in runs:
            logger.debug("  - Preload parameters and task statuses for {flow_id}/{run_number}".format(**run))
            asyncio.gather(
                # Preload run parameters
                await self.get_run_parameters(run['flow_id'], run['run_number']),
                # Preload task statuses
                await self._task_table.get_tasks_for_run(
                    run['flow_id'], run['run_number'], postprocess=self.task_refiner.postprocess)
            )

    async def get_run_parameters(self, flow_id: str, run_key: str, invalidate_cache=False) -> Optional[Dict]:
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
        db_response = await self._run_table.get_run(flow_id, run_key)

        # Return nothing if params artifacts were not found.
        if not db_response.response_code == 200:
            return None

        response = await self.parameter_refiner.postprocess(db_response, invalidate_cache=invalidate_cache)

        return response.body

    async def preload_run_parameters_event_handler(self, flow_id: str, run_number: int):
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

    async def preload_task_statuses_event_handler(self, flow_id: str, run_number: int):
        """
        Handler for event-emitter for preloading task statuses for a run

        Parameters
        ----------
        flow_id : str
            Flow id
        run_number : int
            Run number to preload data for.
        """
        asyncio.run_coroutine_threadsafe(self.preload_data_for_runs([{"flow_id": flow_id, "run_number": run_number}]), self.loop)

    async def stop_cache(self):
        await self.cache.stop()


class DAGCacheStore(object):
    """
    Cache class responsible for parsing and caching a DAG.

    Cache Actions
    -------------
    GenerateDag
        Uses Metaflow Client API to fetch codepackage, decode it and parses a DAG from the contents.

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
            self.event_emitter.on("preload-dag", self.preload_dag_event_handler)

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

    async def preload_dag_event_handler(self, flow_name: str, run_number: str):
        """
        Handler for event-emitter for preloading dag

        Parameters
        ----------
        flow_name : str
            Flow name
        run_number : str
            Run number
        """
        asyncio.run_coroutine_threadsafe(self.preload_dag(flow_name, run_number), self.loop)

    async def preload_dags(self, runs: List[Dict]):
        for run in runs:
            await self.preload_dag(run['flow_id'], run['run_number'])

    async def preload_dag(self, flow_name: str, run_number: str):
        """
        Preloads dag for given flow_name and run_number

        Parameters
        ----------
        flow_name : str
            Flow name
        run_number : str
            Run number
        """
        logger.debug("  - Preload DAG for {}/{}".format(flow_name, run_number))

        res = await self.cache.GenerateDag(flow_name, run_number)
        async for event in res.stream():
            if event["type"] == "error":
                logger.error(event)
            else:
                logger.info(event)


class LogCacheStore(object):
    """
    Cache class responsible for storing logs fetched from S3.

    Cache Actions
    -------------
    GetLogFile
        Fetches log content from an S3 location.
    """

    def __init__(self, event_emitter):
        self.event_emitter = event_emitter or AsyncIOEventEmitter()
        self.cache = None
        self.loop = asyncio.get_event_loop()

    async def restart_requested(self):
        return self.cache._restart_requested if self.cache else False

    async def start_cache(self):
        "Initialize the CacheAsyncClient for Log caching"
        actions = [GetLogFile]
        self.cache = CacheAsyncClient('cache_data/log',
                                      actions,
                                      max_size=CACHE_LOG_STORAGE_LIMIT,
                                      max_actions=CACHE_LOG_MAX_ACTIONS)
        if FEATURE_CACHE_ENABLE:
            await self.cache.start()

    async def stop_cache(self):
        await self.cache.stop()
