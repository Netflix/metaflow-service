from aiohttp import web
import json
from services.utils import read_body
from services.metadata_service.api.utils import format_response, handle_exceptions
import asyncio
from services.data.postgres_async_db import AsyncPostgresDB
from services.data.db_utils import DBResponse, encode_cursor, decode_cursor


class MetadataApi(object):
    _metadata_table = None
    lock = asyncio.Lock()

    def __init__(self, app):
        app.router.add_route(
            "GET",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/"
            "tasks/{task_id}/metadata",
            self.get_metadata,
        )
        app.router.add_route(
            "GET",
            "/flows/{flow_id}/runs/{run_number}/metadata",
            self.get_metadata_by_run,
        )
        app.router.add_route(
            "POST",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/"
            "tasks/{task_id}/metadata",
            self.create_metadata,
        )
        self._db = AsyncPostgresDB.get_instance()
        self._async_table = AsyncPostgresDB.get_instance().metadata_table_postgres

    @format_response
    @handle_exceptions
    async def get_metadata(self, request):
        """
        ---
        description: get all metadata associated with the specified task.
        tags:
        - Metadata
        parameters:
        - name: "flow_id"
          in: "path"
          description: "flow_id"
          required: true
          type: "string"
        - name: "run_number"
          in: "path"
          description: "run_number"
          required: true
          type: "string"
        - name: "step_name"
          in: "path"
          description: "step_name"
          required: true
          type: "string"
        - name: "task_id"
          in: "path"
          description: "task_id"
          required: true
          type: "string"
        - name: "_limit"
          in: "query"
          description: "page size (default 50, max 500). Supplying _limit or _cursor turns on cursor pagination."
          required: false
          type: "integer"
        - name: "_cursor"
          in: "query"
          description: "opaque pagination cursor, returned via the X-Next-Cursor header."
          required: false
          type: "string"
        produces:
        - text/plain
        responses:
            "200":
                description: successful operation
            "405":
                description: invalid HTTP Method
        """
        flow_name = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")
        step_name = request.match_info.get("step_name")
        task_id = request.match_info.get("task_id")

        cursor = request.query.get("_cursor")
        limit = request.query.get("_limit")

        cur_ts, cur_id = None, None
        if cursor:
            try:
                cursor_dict = decode_cursor(cursor)
                cur_ts, cur_id = int(cursor_dict["ts_epoch"]), int(cursor_dict["id"])
            except (ValueError, KeyError):
                return DBResponse(response_code=400, body="Invalid cursor")

        if limit is None and cursor is None:
            return await self._async_table.get_metadata(
                flow_name, run_number, step_name, task_id
            )

        limit = min(int(limit), 500) if limit else 50

        db_response, pagination = await self._async_table.get_metadata_paginated(
            flow_name, run_number, step_name, task_id, cur_ts, cur_id, limit
        )

        if pagination.next_cursor_record:
            next_cursor = encode_cursor(
                {
                    "ts_epoch": pagination.next_cursor_record["ts_epoch"],
                    "id": pagination.next_cursor_record["id"],
                }
            )
            pagination = pagination._replace(next_cursor=next_cursor)

        return db_response, pagination

    @format_response
    @handle_exceptions
    async def get_metadata_by_run(self, request):
        """
        ---
        description: get all metadata associated with the specified run.
        tags:
        - Metadata
        parameters:
        - name: "flow_id"
          in: "path"
          description: "flow_id"
          required: true
          type: "string"
        - name: "run_number"
          in: "path"
          description: "run_number"
          required: true
          type: "string"
        - name: "_limit"
          in: "query"
          description: "page size (default 50, max 500). Supplying _limit or _cursor turns on cursor pagination."
          required: false
          type: "integer"
        - name: "_cursor"
          in: "query"
          description: "opaque pagination cursor, returned via the X-Next-Cursor header."
          required: false
          type: "string"
        produces:
        - text/plain
        responses:
            "200":
                description: successful operation
            "405":
                description: invalid HTTP Method
        """
        flow_name = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")

        cursor = request.query.get("_cursor")
        limit = request.query.get("_limit")

        cur_ts, cur_id = None, None
        if cursor:
            try:
                cursor_dict = decode_cursor(cursor)
                cur_ts, cur_id = int(cursor_dict["ts_epoch"]), int(cursor_dict["id"])
            except (ValueError, KeyError):
                return DBResponse(response_code=400, body="Invalid cursor")

        if limit is None and cursor is None:
            return await self._async_table.get_metadata_in_runs(flow_name, run_number)

        limit = min(int(limit), 500) if limit else 50

        db_response, pagination = (
            await self._async_table.get_metadata_paginated_in_runs(
                flow_name, run_number, cur_ts, cur_id, limit
            )
        )

        if pagination.next_cursor_record:
            next_cursor = encode_cursor(
                {
                    "ts_epoch": pagination.next_cursor_record["ts_epoch"],
                    "id": pagination.next_cursor_record["id"],
                }
            )
            pagination = pagination._replace(next_cursor=next_cursor)

        return db_response, pagination

    async def create_metadata(self, request):
        """
        ---
        description: persist metadata
        tags:
        - Metadata
        parameters:
        - name: "flow_id"
          in: "path"
          description: "flow_id"
          required: true
          type: "string"
        - name: "run_number"
          in: "path"
          description: "run_number"
          required: true
          type: "string"
        - name: "step_name"
          in: "path"
          description: "step_name"
          required: true
          type: "string"
        - name: "task_id"
          in: "path"
          description: "task_id"
          required: true
          type: "string"
        - name: "body"
          in: "body"
          description: "body"
          required: true
          schema:
            type: array
            items:
                type: object
                properties:
                    field_name:
                        type: string
                    value:
                        type: string
                    type:
                        type: string
                    user_name:
                        type: string
                    tags:
                        type: object
                    system_tags:
                        type: object
        produces:
        - 'text/plain'
        responses:
            "202":
                description: successful operation.
            "405":
                description: invalid HTTP Method
        """
        flow_name = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")
        step_name = request.match_info.get("step_name")
        task_id = request.match_info.get("task_id")

        body = await read_body(request.content)
        count = 0
        try:
            run_number, run_id = await self._db.get_run_ids(flow_name, run_number)
            task_id, task_name = await self._db.get_task_ids(
                flow_name, run_number, step_name, task_id
            )
        except Exception:
            return web.Response(
                status=400,
                body=json.dumps(
                    {"message": "need to register run_id and task_id first"}
                ),
            )

        for datum in body:
            values = {
                "flow_id": flow_name,
                "run_number": run_number,
                "run_id": run_id,
                "step_name": step_name,
                "task_id": task_id,
                "task_name": task_name,
                "field_name": datum.get("field_name", " "),
                "value": datum.get("value", " "),
                "type": datum.get("type", " "),
                "user_name": datum.get("user_name"),
                "tags": datum.get("tags"),
                "system_tags": datum.get("system_tags"),
            }
            metadata_response = await self._async_table.add_metadata(**values)
            if metadata_response.response_code == 200:
                count = count + 1

        result = {"metadata_created": count}

        return web.Response(body=json.dumps(result))
