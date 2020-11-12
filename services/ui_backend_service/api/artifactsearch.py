from services.data.postgres_async_db import AsyncPostgresDB
from services.data.db_utils import translate_run_key
from services.utils import handle_exceptions

from ..cache.store import CacheStore
from aiohttp import web
import json


class ArtifactSearchApi(object):
    def __init__(self, app):
        app.router.add_route(
            "GET", "/flows/{flow_id}/runs/{run_number}/search", self.get_run_tasks
        )
        self._artifact_table = AsyncPostgresDB.get_instance().artifact_table_postgres
        self._run_table = AsyncPostgresDB.get_instance().run_table_postgres
        self._artifact_store = CacheStore().artifact_cache

    @handle_exceptions
    async def get_run_tasks(self, request):
        flow_name = request.match_info['flow_id']
        run_id_key, run_id_value = translate_run_key(
            request.match_info['run_number'])
        artifact_name = request.query['key']
        value = request.query['value']

        meta_artifacts = await self.get_run_artifacts(flow_name, run_id_key, run_id_value, artifact_name)

        ws = web.WebSocketResponse()
        await ws.prepare(request)

        # Search the artifact contents from S3 using the CacheClient
        locations = [art['location'] for art in meta_artifacts]
        res = await self._artifact_store.cache.SearchArtifacts(locations, value)

        if res.is_ready():
            artifact_data = res.get()
        else:
            async for event in res.stream():
                await ws.send_str(json.dumps(event))
                if event["event"]["type"] == "error":
                    # close websocket if an error is encountered.
                    await ws.close(code=1011)
            await res.wait()
            artifact_data = res.get()

        results = await search_dict_filter(meta_artifacts, artifact_data)

        await ws.send_str(json.dumps({"event": {"type": "result", "matches": results}}))

        return ws

    async def get_run_artifacts(self, flow_name, run_id_key, run_id_value, artifact_name):
        '''find a set of artifacts to perform the search over. 
        Includes localstore artifacts as well, as we want to return that these could not be searched over.
        '''
        meta_artifacts = await self._artifact_table.get_records(
            filter_dict={
                "flow_id": flow_name,
                run_id_key: run_id_value,
                "name": artifact_name
            }
        )
        return meta_artifacts.body

# Utilities


async def search_dict_filter(artifacts, artifact_match_dict={}):
    '''Returns artifacts that match the searchterm with their content.

    Requirements:

    artifacts: [{..., 'location': 'a_location'}]

    artifact_match_dict: {'a_location': {'matches': boolean, 'included': boolean}}
      Matches: whether the search term matched the artifact content or not
      Included: Whether the artifact content was included in the search or not (was the content accessible at all)

    Returns:
    [
        {
            'flow_id': str,
            'run_number': int,
            'step_name': str,
            'task_id': int,
            'searchable': boolean
        }
    ]
      searchable: denotes whether the task had an artifact that could be searched or not. 
      False in cases where the artifact could not be included in the search
    '''

    def result_format(art): return dict(
        [key, val] for key, val in art.items()
        if key in ['flow_id', 'run_number', 'step_name', 'task_id']
    )

    results = []
    for artifact in artifacts:
        loc = artifact['location']
        if loc in artifact_match_dict:
            match_data = artifact_match_dict[loc]
            if match_data['matches'] or not match_data['included']:
                results.append({**result_format(artifact), "searchable": match_data['included']})

    return results
