import asyncio
from collections import defaultdict
from typing import Tuple
from services.data.db_utils import translate_run_key
from services.data.tagging_utils import apply_run_tags_to_db_response
from services.utils import handle_exceptions, logging
from services.ui_backend_service.data.cache.utils import (
    search_result_event_msg, error_event_msg
)
from .utils import query_param_enabled
from urllib.parse import unquote_plus

from aiohttp import web
import json

SCOPE_ARTIFACT = 'ARTIFACT'
SCOPE_FOREACH_VARIABLE = 'FOREACH_VARIABLE'


class SearchApi(object):
    def __init__(self, app, db, cache=None):
        self.db = db
        app.router.add_route(
            "GET", "/search/flows/{flow_id}/runs/{run_number}", self.get_run_tasks
        )
        self._artifact_table = self.db.artifact_table_postgres
        self._metadata_table = self.db.metadata_table_postgres
        self._run_table = self.db.run_table_postgres
        self._artifact_store = getattr(cache, "artifact_cache", None)
        self.search_results = defaultdict(list)
        self.search_result_keyset = defaultdict(set)

    @handle_exceptions
    async def get_run_tasks(self, request):
        scope_list = _decode_url_param_value(request.query.get('scope', '')).split(',')
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        tasks = []
        try:
            if SCOPE_ARTIFACT in scope_list:
                tasks.append(self.search_artifacts(request, ws))

            if SCOPE_FOREACH_VARIABLE in scope_list:
                tasks.append(self.search_foreach_variable(request, ws))

            await asyncio.gather(*tasks)

        except Exception as ex:
            logging.exception("Filter tasks failed.")
            await ws.send_str(json.dumps({"event": error_event_msg("Filter tasks failed", "filter-tasks-failed")}))
            await ws.close(code=1011)

        # Clean up the search results so that results dict won't grow indefinitely.
        request_identifier = _construct_requset_identifier(request)
        if request_identifier in self.search_results:
            del self.search_results[request_identifier]
        if request_identifier in self.search_result_keyset:
            del self.search_result_keyset[request_identifier]

        return ws

    async def search_foreach_variable(self, request, ws):
        """
        Search over the foreach variables for a given run. This searches over the metadata table, and updates results with all tasks
        that have the specified variable name in their foreach stack.
        """
        flow_name = request.match_info['flow_id']
        run_key = request.match_info['run_number']
        key = request.query['key']
        value = _decode_url_param_value(request.query.get('value', None))

        metadata_key = key + "=" + value if value else key
        metadata_items = await self.get_run_metadata(flow_name, run_key, "foreach-stack")
        results = []
        for item in metadata_items:
            if metadata_key in str(item['value']):
                results.append({**_metadata_result_format(item), "searchable": True})
        request_identifier = _construct_requset_identifier(request)
        self.union_results(results, request_identifier)
        await ws.send_str(json.dumps({"event": search_result_event_msg(self.search_results[request_identifier])}))

    async def search_artifacts(self, request, ws):
        """
        Search over the artifacts for a given run. This search is handled by subprocesses of CacheStore. This updates results with
        all tasks that have the specified artifact name.
        """
        flow_name = request.match_info['flow_id']
        run_key = request.match_info['run_number']
        key = request.query['key']
        value = request.query.get('value', None)
        invalidate_cache = query_param_enabled(request, "invalidate")
        meta_artifacts = await self.get_run_artifacts(flow_name, run_key, key)

        if value is None:
            # For empty search terms simply return the list of tasks that have artifacts by the specified name.
            # Do not unnecessarily hit the cache.
            results = [{**_artifact_result_format(art), "searchable": True} for art in meta_artifacts]
        else:
            operator, value = _parse_search_term(value)
            # Search through the artifact contents using the CacheClient
            # Prefer run_id over run_number and task_name over task_id
            pathspecs = ["{flow_id}/{run_id}/{step_name}/{task_name}/{name}/{attempt_id}".format(
                flow_id=art['flow_id'],
                run_id=art.get('run_id') or art['run_number'],
                step_name=art['step_name'],
                task_name=art.get('task_name') or art['task_id'],
                name=art['name'],
                attempt_id=art['attempt_id']) for art in meta_artifacts]
            res = await self._artifact_store.cache.SearchArtifacts(
                pathspecs, value, operator,
                invalidate_cache=invalidate_cache)

            if res.has_pending_request():
                async for event in res.stream():
                    await ws.send_str(json.dumps(event))
                await res.wait()
            artifact_data = res.get()

            results = await _search_dict_filter(meta_artifacts, artifact_data)
        request_identifier = _construct_requset_identifier(request)
        self.union_results(results, request_identifier)

        await ws.send_str(json.dumps({"event": search_result_event_msg(self.search_results[request_identifier])}))

    async def get_run_artifacts(self, flow_name, run_key, artifact_name):
        """
        Find a set of artifacts to perform the search over.
        Includes localstore artifacts as well, as we want to return that these could not be searched over.
        """
        run_id_key, run_id_value = translate_run_key(run_key)
        db_response = await self._artifact_table.get_records(
            filter_dict={
                "flow_id": flow_name,
                run_id_key: run_id_value,
                "name": artifact_name
            }
        )
        db_response = await apply_run_tags_to_db_response(flow_name, run_key, self._run_table, db_response)
        return db_response.body

    async def get_run_metadata(self, flow_name, run_key, metadata_name):
        """
        Find a set of artifacts to perform the search over.
        Includes localstore artifacts as well, as we want to return that these could not be searched over.
        """
        run_id_key, run_id_value = translate_run_key(run_key)
        db_response = await self._metadata_table.get_records(
            filter_dict={
                "flow_id": flow_name,
                run_id_key: run_id_value,
                "field_name": metadata_name
            }
        )
        return db_response.body

    def union_results(self, new_results: dict, request_identifier: str):
        """
        Union new results with existing results, and update the keyset to prevent duplicate results.
        Key is determined by step_name and task_id.
        """
        for result in new_results:
            key = result['step_name'] + '/' + result['task_id']
            if key in self.search_result_keyset:
                continue

            self.search_result_keyset[request_identifier].add(key)
            self.search_results[request_identifier].append(result)


# Utilities
async def _search_dict_filter(artifacts, artifact_match_dict={}):
    """
    Combines search match data dict with a list of artifacts to create actual search results.

    Parameters
    ----------
    artifacts: List
        list of artifacts used to construct pathspecs

    artifact_match_dict: Dict
        dictionary of pathspec -based match data.
        example:
        {'FlowId/RunNumber/StepName/TaskId/ArtifactName': {'matches': boolean, 'included': boolean}}
        Matches: whether the search term matched the artifact content or not
        Included: Whether the artifact content was included in the search or not (was the content accessible at all)

    Returns
    -------
    List
        example:
        [
            {
                'flow_id': str,
                'run_number': int,
                'step_name': str,
                'task_id': int,
                'searchable': boolean,
                'error': null
            }
        ]
        searchable: denotes whether the task had an artifact that could be searched or not.
        False in cases where the artifact could not be included in the search

        error: either null or error object with following structure
            {
                'id': str,
                'detail': str
            }

            example:
              { 'id': 's3-access-denied', 'detail': 's3://...' }
    """

    results = []
    for artifact in artifacts:
        # Prefer run_id over run_number and task_name over task_id
        pathspec = "{flow_id}/{run_id}/{step_name}/{task_name}/{name}/{attempt_id}".format(
            flow_id=artifact['flow_id'],
            run_id=artifact.get('run_id') or artifact['run_number'],
            step_name=artifact['step_name'],
            task_name=artifact.get('task_name') or artifact['task_id'],
            name=artifact['name'],
            attempt_id=artifact['attempt_id'])
        if pathspec in artifact_match_dict:
            match_data = artifact_match_dict[pathspec]
            if match_data['matches'] or not match_data['included']:
                results.append({
                    **_artifact_result_format(artifact),
                    "searchable": match_data['included'],
                    "error": match_data['error']
                })

    return results


def _artifact_result_format(art):
    return dict(
        [key, val] for key, val in art.items()
        if key in ['flow_id', 'run_number', 'step_name', 'task_id', '_foreach_stack']
    )


def _metadata_result_format(meta):
    return dict(
        [key, val] for key, val in meta.items()
        if key in ['flow_id', 'run_number', 'step_name', 'task_id', 'value']
    )


def _parse_search_term(term: str) -> Tuple[str, str]:
    """
    Return search operator, and the parsed search term.
    """

    # TODO: extend parsing to all predicates, not just eq&co
    partial_search = not (term.startswith("\"") and term.endswith("\""))

    if partial_search:
        return "co", term
    else:
        return "eq", term[1:len(term) - 1]


def _decode_url_param_value(value):
    """
    Decode url param value that is encoded multiple times.
    """
    if not value:
        return None
    DECODE_ROUND = 3
    for _ in range(DECODE_ROUND):
        value = unquote_plus(value)
    return value


def _construct_requset_identifier(request):
    """
    Extracts the flow_name, run_key, key and value from the request to construct a
    unique key for the search result.
    """
    flow_name = request.match_info['flow_id']
    run_key = request.match_info['run_number']
    key = request.query['key']
    value = _decode_url_param_value(request.query.get('value', None))

    return f"{flow_name}/{run_key} {key}:{value}"
