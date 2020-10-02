from services.data.postgres_async_db import AsyncPostgresDB
from pyee import AsyncIOEventEmitter
from metaflow.client.cache.cache_async_client import CacheAsyncClient
from .search_artifacts_action import SearchArtifacts
from .get_artifacts_action import GetArtifacts
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
        self.event_emitter = event_emitter or AsyncIOEventEmitter()
        self._artifact_table = AsyncPostgresDB.get_instance().artifact_table_postgres
        self._run_table = AsyncPostgresDB.get_instance().run_table_postgres
        self.cache = None

        # Bind an event handler for when we want to preload artifacts for
        # newly inserted content.
        self.event_emitter.on("preload-artifacts", self.preload_event_handler)

    async def start_cache(self):
        actions = [SearchArtifacts, GetArtifacts]
        self.cache = CacheAsyncClient('cache_data/artifact_search',
                                    actions,
                                    max_size=600000)
        await self.cache.start()
        await self.preload_initial_data()
    
    async def preload_initial_data(self):
        "Preloads some data on cache startup"
        recent_run_ids = await self.get_recent_run_numbers()
        await self.preload_data_for_runs(recent_run_ids)

    async def preload_data_for_runs(self, run_ids):
        "preloads artifact data for given run ids. Can be used to prefetch artifacts for newly generated runs"
        artifact_locations = await self.get_artifact_locations_for_run_ids(run_ids)

        print("preloading {} artifacts".format(len(artifact_locations)), flush=True)

        res = await self.cache.SearchArtifacts(artifact_locations, "preload")
        async for event in res.stream():
            print(event, flush=True)

    async def get_recent_run_numbers(self):
        since = int(round(time.time() * 1000)) - 86400_000 * 2 # 2 days ago
        _records, _ = await self._run_table.find_records(
            conditions=["ts_epoch >= %s"],
            values=[since],
            order=['ts_epoch DESC'],
            limit=METAFLOW_ARTIFACT_PREFETCH_RUNS_LIMIT,
            expanded=True
        )

        return [ run['run_number'] for run in _records.body ]
    
    async def get_artifact_locations_for_run_ids(self, run_ids = []):
        # do not touch DB if no run_ids were given.
        if len(run_ids)==0:
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
    
    async def preload_event_handler(self, run_id):
        "Handler for event-emitter for preloading artifacts for a run id"
        await self.preload_data_for_runs([run_id])

    async def stop_cache(self):
        await self.cache.stop()


from .generate_dag_action import GenerateDag
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