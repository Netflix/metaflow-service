from typing import Dict, Optional
from services.data.db_utils import translate_run_key, translate_task_key
from services.ui_backend_service.data import unpack_processed_value
from services.utils import handle_exceptions
from .utils import format_response_list, get_pathspec_from_request, query_param_enabled, web_response, DBPagination, DBResponse
from aiohttp import web


class CardsApi(object):
    def __init__(self, app, db, cache=None):
        self.db = db
        self.cache = getattr(cache, "artifact_cache", None)
        app.router.add_route(
            "GET",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/cards",
            self.get_cards_list_for_task,
        )
        app.router.add_route(
            "GET",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/cards/{hash}",
            self.get_card_content_by_hash,
        )

    async def get_task_by_request(self, request):
        flow_id, run_number, step_name, task_id, _ = \
            get_pathspec_from_request(request)

        run_id_key, run_id_value = translate_run_key(run_number)
        task_id_key, task_id_value = translate_task_key(task_id)

        conditions = [
            "flow_id = %s",
            "{run_id_key} = %s".format(run_id_key=run_id_key),
            "step_name = %s",
            "{task_id_key} = %s".format(task_id_key=task_id_key)
        ]
        values = [flow_id, run_id_value, step_name, task_id_value]
        db_response, *_ = await self.db.task_table_postgres.find_records(
            fetch_single=True,
            conditions=conditions,
            values=values,
            expanded=True
        )
        if db_response.response_code == 200:
            return db_response.body
        return None

    @handle_exceptions
    async def get_cards_list_for_task(self, request):
        """
        ---
        description: Get all identifiers of cards for a task
        tags:
        - Card
        parameters:
          - $ref: '#/definitions/Params/Path/flow_id'
          - $ref: '#/definitions/Params/Path/run_number'
          - $ref: '#/definitions/Params/Path/step_name'
          - $ref: '#/definitions/Params/Path/task_id'
          - $ref: '#/definitions/Params/Builtin/_page'
          - $ref: '#/definitions/Params/Builtin/_limit'
          - $ref: '#/definitions/Params/Custom/invalidate'
        produces:
        - application/json
        responses:
            "200":
                description: Returns a list of cards for the specified task
                schema:
                  $ref: '#/definitions/ResponsesCardList'
            "404":
                description: Task was not found.
            "405":
                description: invalid HTTP Method
                schema:
                  $ref: '#/definitions/ResponsesError405'
        """

        task = await self.get_task_by_request(request)
        if not task:
            return web_response(404, {'data': []})

        invalidate_cache = query_param_enabled(request, "invalidate")
        cards = await get_cards_for_task(self.cache, task, invalidate_cache)

        if cards is None:
            # Handle edge: Cache failed to return anything, even errors.
            # NOTE: choice of status 200 here is quite arbitrary, as the cache returning None is usually
            # caused by a premature request, and cards are not permanently missing.
            return web_response(200, {'data': []})

        card_hashes = [
            {"id": data["id"], "hash": hash, "type": data["type"]}
            for hash, data in cards.items()
        ]

        # paginate list of cards
        limit, page, offset = get_pagination_params(request)
        _pages = max(len(card_hashes) // limit, 1)
        limited_set = card_hashes[offset:][:limit]

        response = DBResponse(200, limited_set)
        pagination = DBPagination(limit, limit * (page - 1), len(response.body), page)
        status, body = format_response_list(request, response, pagination, page, _pages)

        return web_response(status, body)

    @handle_exceptions
    async def get_card_content_by_hash(self, request):
        """
        ---
        description: Get specific card of a task
        tags:
        - Card
        parameters:
          - $ref: '#/definitions/Params/Path/flow_id'
          - $ref: '#/definitions/Params/Path/run_number'
          - $ref: '#/definitions/Params/Path/step_name'
          - $ref: '#/definitions/Params/Path/task_id'
          - $ref: '#/definitions/Params/Custom/invalidate'
        produces:
        - text/html
        responses:
            "200":
                description: Returns the HTML content of a card with the specific hash
            "404":
                description: Card was not found.
            "405":
                description: invalid HTTP Method
                schema:
                  $ref: '#/definitions/ResponsesError405'
        """

        hash = request.match_info.get("hash")
        task = await self.get_task_by_request(request)
        if not task:
            return web.Response(content_type="text/html", status=404, body="Task not found.")

        cards = await get_cards_for_task(self.cache, task)

        if cards is None:
            return web.Response(content_type="text/html", status=404, body="Card not found for task. Possibly still being processed.")

        if cards and hash in cards:
            return web.Response(content_type="text/html", body=cards[hash]["html"])
        else:
            return web.Response(content_type="text/html", status=404, body="Card not found for task.")


async def get_cards_for_task(cache_client, task, invalidate_cache=False) -> Optional[Dict[str, Dict]]:
    """
    Return a dictionary of cards from the cache, or nothing.

    Example:
    --------
    {
        "abc123": {
            "id": 1,
            "hash": "abc123",
            "html": "htmlcontent"
        }
    }
    """
    pathspec = "{flow_id}/{run_id}/{step_name}/{task_id}".format(
        flow_id=task.get("flow_id"),
        run_id=task.get("run_id") or task.get("run_number"),
        step_name=task.get("step_name"),
        task_id=task.get("task_name") or task.get("task_id"),
    )
    res = await cache_client.cache.GetCards([pathspec], invalidate_cache)

    if res.has_pending_request():
        async for event in res.stream():
            if event["type"] == "error":
                # raise error, there was an exception during fetching.
                raise CardException(event["message"], event["id"], event["traceback"])
        await res.wait()  # wait until results are ready
    data = res.get()
    if data and pathspec in data:
        success, value, detail, trace = unpack_processed_value(data[pathspec])
        if success:
            return value
        else:
            raise CardException(detail, value, trace)

    return None


def get_pagination_params(request):
    """extract pagination params from request
    """
    # Page
    page = max(int(request.query.get("_page", 1)), 1)

    # Limit
    # Default limit is 1000, maximum is 10_000
    limit = min(int(request.query.get("_limit", 1000)), 10000)

    # Offset
    offset = limit * (page - 1)

    return limit, page, offset


class CardException(Exception):
    def __init__(self, msg='Failed to read card contents', id='card-error', traceback_str=None):
        self.message = msg
        self.id = id
        self.traceback_str = traceback_str

    def __str__(self):
        return self.message
