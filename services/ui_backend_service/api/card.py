from typing import Dict, Optional
from services.data.db_utils import translate_run_key, translate_task_key
from services.ui_backend_service.data import unpack_processed_value
from services.utils import handle_exceptions
from .utils import (
    format_response_list,
    get_pathspec_from_request,
    query_param_enabled,
    web_response,
    DBPagination,
    DBResponse,
)
from services.ui_backend_service.data.cache.card_cache_manager import list_cards as list_cards_from_cache
from services.ui_backend_service.data.cache.card_datastore_gets import DynamicCardGetClients
import json
from aiohttp import web


async def stream_html_response(request, html_file_path):
    # Create a response object
    response = web.StreamResponse(
        status=200,
        headers={"Content-Type": "text/html", "Content-Disposition": "inline"},
    )
    # Prepare the response for streaming
    await response.prepare(request)

    # Stream the file content to the response
    with open(html_file_path, "rb") as file:
        while True:
            chunk = file.read(4096)
            if not chunk:
                break
            await response.write(chunk)

    return response


class CardsApi(object):
    def __init__(self, app, db, cache=None):
        self.db = db
        self.cache = getattr(cache, "card_cache", None)
        self.card_db_client = DynamicCardGetClients()
        self._metadata_table = self.db.metadata_table_postgres

        app.router.add_route(
            "GET",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/cards",
            self.get_cards_list_for_task,
        )
        app.router.add_route(
            "GET",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/cards/{type}/{uuid}",
            self.get_cards_direct,
        )

        app.router.add_route(
            "GET",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/cards/{type}/{uuid}/data",
            self.get_cards_data_direct,
        )

    
    async def extract_ds_root_from_metadata(self, request):
        flow_id, run_number, step_name, task_id, _ = get_pathspec_from_request(request)

        run_id_key, run_id_value = translate_run_key(run_number)
        task_id_key, task_id_value = translate_task_key(task_id)

        step_name = request.match_info.get("step_name")

        conditions = [
        "flow_id = %s",
        "{run_id_key} = %s".format(
            run_id_key=run_id_key),
        "step_name = %s",
        "{task_id_key} = %s".format(
            task_id_key=task_id_key),
        "field_name = 'ds-root'"
        ]

        db_response, *_ = await self._metadata_table.find_records(
            fetch_single=True,
            conditions=conditions,
            values=[flow_id, run_id_value, step_name, task_id_value],
            expanded=True,
        )
        if db_response.response_code == 200:
            return db_response.body.get("value", None)
        return None

    @handle_exceptions
    async def get_cards_direct(self, request):
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
          - $ref: '#/definitions/Params/Path/type'
          - $ref: '#/definitions/Params/Path/hash'
          - $ref: '#/definitions/Params/Custom/user_set_id'
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
        task = await self.get_task_by_request(request)
        if not task:
            return web.Response(
                content_type="text/html", status=404, body="Task not found."
            )
        datastore_root = await self.extract_ds_root_from_metadata(request)
        if datastore_root is None:
            return web.Response(
                content_type="text/html", status=404, body="No datastore found for task"
            )
        
        pathspec = "{flow_id}/{run_id}/{step_name}/{task_id}".format(
            flow_id=task.get("flow_id"),
            run_id=task.get("run_id") or task.get("run_number"),
            step_name=task.get("step_name"),
            task_id=task.get("task_name") or task.get("task_id"),
        )
        card_type = request.match_info.get("type")
        card_uuid = request.match_info.get("uuid")
        # card_user_id is set in the query parameters
        card_user_id = request.query.get("user_set_id")
        with self.card_db_client[datastore_root].fetch(pathspec, card_type, card_uuid, card_user_id, object_type="card") as ff:
            for key, path, metadata in ff:
                if path is None:
                    return web.Response(
                        content_type="text/html",
                        status=404,
                        body="Card not found for task.",
                    )
                return await stream_html_response(request, path)

    @handle_exceptions
    async def get_cards_data_direct(self, request):
        """
        ---
        description: Get the data of a card created for a task. Contains any additional updates needed by the card.
        tags:
        - Card
        parameters:
          - $ref: '#/definitions/Params/Path/flow_id'
          - $ref: '#/definitions/Params/Path/run_number'
          - $ref: '#/definitions/Params/Path/step_name'
          - $ref: '#/definitions/Params/Path/task_id'
          - $ref: '#/definitions/Params/Path/type'
          - $ref: '#/definitions/Params/Path/hash'
          - $ref: '#/definitions/Params/Custom/user_set_id'

        produces:
        - application/json
        responses:
            "200":
                description: Returns the data object created by the realtime card with the specific hash
            "404":
                description: Card data was not found.
            "405":
                description: invalid HTTP Method
                schema:
                  $ref: '#/definitions/ResponsesError405'
        """
        task = await self.get_task_by_request(request)
        if not task:
            return web.Response(
                content_type="text/html", status=404, body="Task not found."
            )
        datastore_root = await self.extract_ds_root_from_metadata(request)

        if datastore_root is None:
            return web_response(404, "No datastore found for task")
        
        pathspec = "{flow_id}/{run_id}/{step_name}/{task_id}".format(
            flow_id=task.get("flow_id"),
            run_id=task.get("run_id") or task.get("run_number"),
            step_name=task.get("step_name"),
            task_id=task.get("task_name") or task.get("task_id"),
        )
        card_type = request.match_info.get("type")
        card_uuid = request.match_info.get("uuid")
        # card_user_id is set in the query parameters
        card_user_id = request.query.get("user_set_id")
        with self.card_db_client[datastore_root].fetch(pathspec, card_type, card_uuid, card_user_id, object_type="data") as ff:
            for key, path, metadata in ff:
                if path is None:
                    return web_response(404, {"error": "Card data not found for task"})
                with open(path) as f:
                    # TODO: This can become super slow if the file is VERY large
                    # Add a layer of optimization where we read this file byte by byte
                    # and return the data in chunks 
                    return web_response(200, {
                        "id" : card_user_id,
                        "type": card_type,
                        "data": json.loads(f.read())
                    })

    async def get_task_by_request(self, request):
        flow_id, run_number, step_name, task_id, _ = get_pathspec_from_request(request)

        run_id_key, run_id_value = translate_run_key(run_number)
        task_id_key, task_id_value = translate_task_key(task_id)

        conditions = [
            "flow_id = %s",
            "{run_id_key} = %s".format(run_id_key=run_id_key),
            "step_name = %s",
            "{task_id_key} = %s".format(task_id_key=task_id_key),
        ]
        values = [flow_id, run_id_value, step_name, task_id_value]
        db_response, *_ = await self.db.task_table_postgres.find_records(
            fetch_single=True,
            conditions=conditions,
            values=values,
            expanded=True,
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
            return web_response(404, {"data": []})

        cards = await get_card_list(self.cache, task, max_wait_time=1)

        if cards is None:
            # Handle edge: Cache failed to return anything, even errors.
            # NOTE: choice of status 200 here is quite arbitrary, as the cache returning None is usually
            # caused by a premature request, and cards are not permanently missing.
            return web_response(200, {"data": []})

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


async def get_card_list(
    cache_client, task, max_wait_time=3
):
    pathspec = "{flow_id}/{run_id}/{step_name}/{task_id}".format(
        flow_id=task.get("flow_id"),
        run_id=task.get("run_id") or task.get("run_number"),
        step_name=task.get("step_name"),
        task_id=task.get("task_name") or task.get("task_id"),
    )
    return await list_cards_from_cache(cache_client.cache_manager, pathspec, max_wait_time)


def get_pagination_params(request):
    """extract pagination params from request"""
    # Page
    page = max(int(request.query.get("_page", 1)), 1)

    # Limit
    # Default limit is 1000, maximum is 10_000
    limit = min(int(request.query.get("_limit", 1000)), 10000)

    # Offset
    offset = limit * (page - 1)

    return limit, page, offset


class CardException(Exception):
    def __init__(
        self, msg="Failed to read card contents", id="card-error", traceback_str=None
    ):
        self.message = msg
        self.id = id
        self.traceback_str = traceback_str

    def __str__(self):
        return self.message
