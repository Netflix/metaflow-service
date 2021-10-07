from typing import Tuple
from services.data.db_utils import translate_run_key
from services.utils import handle_exceptions
from services.ui_backend_service.data.cache.utils import (
    search_result_event_msg, error_event_msg
)
from .utils import query_param_enabled

from aiohttp import web
import json


class ArtifactSearchApi(object):
    def __init__(self, app, db, cache=None):
        self.db = db
        app.router.add_route(
            "GET", "/flows/{flow_id}/runs/{run_number}/search", self.get_run_tasks
        )
        self._artifact_table = self.db.artifact_table_postgres
        self._run_table = self.db.run_table_postgres
        self._artifact_store = getattr(cache, "artifact_cache", None)

    @handle_exceptions
    async def get_run_tasks(self, request):
        flow_name = request.match_info['flow_id']
        run_key = request.match_info['run_number']
        artifact_name = request.query['key']
        value = request.query.get('value', None)

        invalidate_cache = query_param_enabled(request, "invalidate")

        meta_artifacts = await self.get_run_artifacts(flow_name, run_key, artifact_name)

        ws = web.WebSocketResponse()
        await ws.prepare(request)

        try:
            if value is None:
                # For empty search terms simply return the list of tasks that have artifacts by the specified name.
                # Do not unnecessarily hit the cache.
                results = [{**_result_format(art), "searchable": True} for art in meta_artifacts]
            else:
                operator, value = _parse_search_term(value)
                # Search through the artifact contents using the CacheClient
                pathspecs = ["{flow_id}/{run_number}/{step_name}/{task_id}/{name}/{attempt_id}".format(**art) for art in meta_artifacts]
                res = await self._artifact_store.cache.SearchArtifacts(
                    pathspecs, value, operator,
                    invalidate_cache=invalidate_cache)

                if res.has_pending_request():
                    async for event in res.stream():
                        await ws.send_str(json.dumps(event))
                    await res.wait()
                artifact_data = res.get()

                results = await _search_dict_filter(meta_artifacts, artifact_data)

            await ws.send_str(json.dumps({"event": search_result_event_msg(results)}))
        except:
            # TODO: maybe except the specific errors from cache server only? (CacheServerUnreachable, CacheFullException)
            await ws.send_str(json.dumps({"event": error_event_msg("Accessing cache failed", "cache-access-failed")}))
            await ws.close(code=1011)
        return ws

    async def get_run_artifacts(self, flow_name, run_key, artifact_name):
        """
        Find a set of artifacts to perform the search over.
        Includes localstore artifacts as well, as we want to return that these could not be searched over.
        """
        run_id_key, run_id_value = translate_run_key(run_key)
        meta_artifacts = await self._artifact_table.get_records(
            filter_dict={
                "flow_id": flow_name,
                run_id_key: run_id_value,
                "name": artifact_name
            }
        )
        return meta_artifacts.body

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
        pathspec = "{flow_id}/{run_number}/{step_name}/{task_id}/{name}/{attempt_id}".format(**artifact)
        if pathspec in artifact_match_dict:
            match_data = artifact_match_dict[pathspec]
            if match_data['matches'] or not match_data['included']:
                results.append({
                    **_result_format(artifact),
                    "searchable": match_data['included'],
                    "error": match_data['error']
                })

    return results


def _result_format(art):
    return dict(
        [key, val] for key, val in art.items()
        if key in ['flow_id', 'run_number', 'step_name', 'task_id']
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
