from services.data.db_utils import translate_run_key, translate_task_key
from services.utils import handle_exceptions
from .utils import format_response_list, web_response, DBPagination, DBResponse
from aiohttp import web


class CardsApi(object):
    def __init__(self, app, db, cache=None):
        self.db = db
        self.cache = cache
        app.router.add_route(
            "GET",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/cards",
            self.get_cards_by_task,
        )
        app.router.add_route(
            "GET",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/cards/{card_id}",
            self.get_card_by_id,
        )
        app.router.add_route(
            "GET",
            "/flows/{flow_id}/runs/{run_number}/cards",
            self.get_cards_by_run,
        )

    @handle_exceptions
    async def get_cards_by_task(self, request):
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
          - $ref: '#/definitions/Params/Builtin/_order'
          - $ref: '#/definitions/Params/Builtin/_tags'
          - $ref: '#/definitions/Params/Builtin/_group'
          - $ref: '#/definitions/Params/Custom/flow_id'
          - $ref: '#/definitions/Params/Custom/run_number'
          - $ref: '#/definitions/Params/Custom/step_name'
          - $ref: '#/definitions/Params/Custom/task_id'
          - $ref: '#/definitions/Params/Custom/name'
          - $ref: '#/definitions/Params/Custom/type'
          - $ref: '#/definitions/Params/Custom/ds_type'
          - $ref: '#/definitions/Params/Custom/attempt_id'
          - $ref: '#/definitions/Params/Custom/postprocess'
          - $ref: '#/definitions/Params/Custom/invalidate'
          - $ref: '#/definitions/Params/Custom/user_name'
          - $ref: '#/definitions/Params/Custom/ts_epoch'
        produces:
        - application/json
        responses:
            "200":
                description: Returns all artifacts of specified task
                schema:
                  $ref: '#/definitions/ResponsesArtifactList'
            "405":
                description: invalid HTTP Method
                schema:
                  $ref: '#/definitions/ResponsesError405'
        """

        # flow_name = request.match_info.get("flow_id")
        # run_id_key, run_id_value = translate_run_key(
        #     request.match_info.get("run_number"))
        # step_name = request.match_info.get("step_name")
        # task_id_key, task_id_value = translate_task_key(
        #     request.match_info.get("task_id"))

        MOCK_BODY = [
            {
                "type": "default-card",
                "hash": "abcdef1234",
                "id": "unique-card-id"
            }
        ]
        db_response = DBResponse(200, MOCK_BODY)
        pagination = DBPagination(10, 0, 10, 1)

        status, body = format_response_list(request, db_response, pagination, 1)

        return web_response(status, body)

    @handle_exceptions
    async def get_card_by_id(self, request):
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
          - $ref: '#/definitions/Params/Builtin/_page'
          - $ref: '#/definitions/Params/Builtin/_limit'
          - $ref: '#/definitions/Params/Builtin/_order'
          - $ref: '#/definitions/Params/Builtin/_tags'
          - $ref: '#/definitions/Params/Builtin/_group'
          - $ref: '#/definitions/Params/Custom/flow_id'
          - $ref: '#/definitions/Params/Custom/run_number'
          - $ref: '#/definitions/Params/Custom/step_name'
          - $ref: '#/definitions/Params/Custom/task_id'
          - $ref: '#/definitions/Params/Custom/name'
          - $ref: '#/definitions/Params/Custom/type'
          - $ref: '#/definitions/Params/Custom/ds_type'
          - $ref: '#/definitions/Params/Custom/attempt_id'
          - $ref: '#/definitions/Params/Custom/postprocess'
          - $ref: '#/definitions/Params/Custom/invalidate'
          - $ref: '#/definitions/Params/Custom/user_name'
          - $ref: '#/definitions/Params/Custom/ts_epoch'
        produces:
        - application/json
        responses:
            "200":
                description: Returns all artifacts of specified task
                schema:
                  $ref: '#/definitions/ResponsesArtifactList'
            "405":
                description: invalid HTTP Method
                schema:
                  $ref: '#/definitions/ResponsesError405'
        """

        # flow_name = request.match_info.get("flow_id")
        # run_id_key, run_id_value = translate_run_key(
        #     request.match_info.get("run_number"))
        # step_name = request.match_info.get("step_name")
        # task_id_key, task_id_value = translate_task_key(
        #     request.match_info.get("task_id"))

        MOCK_RESPONSE = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="utf-8" />
            <meta name="viewport" content="width=device-width,initial-scale=1" />
            <title>Default Card</title>
        </head>

        <body>
            <p>test</p>
        </body>

        </html>
        """

        ids = {
            "unique-card-id": MOCK_RESPONSE
        }

        card_id = request.match_info.get("card_id")

        if card_id in ids:
            return web.Response(content_type="text/html", body=ids[card_id])
        else:
            return web.Response(content_type="text/html", status=404, body=None)

    @handle_exceptions
    async def get_cards_by_run(self, request):
        """
        ---
        description: Get all cards of specified run
        tags:
        - Card
        parameters:
          - $ref: '#/definitions/Params/Path/flow_id'
          - $ref: '#/definitions/Params/Path/run_number'
          - $ref: '#/definitions/Params/Builtin/_page'
          - $ref: '#/definitions/Params/Builtin/_limit'
          - $ref: '#/definitions/Params/Builtin/_order'
          - $ref: '#/definitions/Params/Builtin/_tags'
          - $ref: '#/definitions/Params/Builtin/_group'
          - $ref: '#/definitions/Params/Custom/flow_id'
          - $ref: '#/definitions/Params/Custom/run_number'
          - $ref: '#/definitions/Params/Custom/step_name'
          - $ref: '#/definitions/Params/Custom/task_id'
          - $ref: '#/definitions/Params/Custom/name'
          - $ref: '#/definitions/Params/Custom/type'
          - $ref: '#/definitions/Params/Custom/ds_type'
          - $ref: '#/definitions/Params/Custom/attempt_id'
          - $ref: '#/definitions/Params/Custom/postprocess'
          - $ref: '#/definitions/Params/Custom/invalidate'
          - $ref: '#/definitions/Params/Custom/user_name'
          - $ref: '#/definitions/Params/Custom/ts_epoch'
        produces:
        - application/json
        responses:
            "200":
                description: Returns all artifacts of specified run
                schema:
                  $ref: '#/definitions/ResponsesArtifactList'
            "405":
                description: invalid HTTP Method
                schema:
                  $ref: '#/definitions/ResponsesError405'
        """

        pass
