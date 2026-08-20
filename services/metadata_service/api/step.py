from services.data import StepRow
from services.data.tagging_utils import apply_run_tags_to_db_response
from services.utils import read_body
from services.metadata_service.api.utils import format_response, handle_exceptions
from services.data.postgres_async_db import AsyncPostgresDB
from services.data.db_utils import DBResponse, encode_cursor, decode_cursor


class StepApi(object):
    _step_table = None

    def __init__(self, app):
        app.router.add_route(
            "GET", "/flows/{flow_id}/runs/{run_number}/steps", self.get_steps
        )

        app.router.add_route(
            "GET", "/flows/{flow_id}/runs/{run_number}/steps/{step_name}", self.get_step
        )
        app.router.add_route(
            "POST",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/step",
            self.create_step,
        )
        self._async_table = AsyncPostgresDB.get_instance().step_table_postgres
        self._async_run_table = AsyncPostgresDB.get_instance().run_table_postgres
        self._db = AsyncPostgresDB.get_instance()

    @format_response
    @handle_exceptions
    async def get_steps(self, request):
        """
        ---
        description: get all steps associated with the specified run.
        tags:
        - Steps
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
                description: successful operation. returned all steps
            "405":
                description: invalid HTTP Method
        """
        flow_id = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")
        cursor = request.query.get("_cursor")
        limit = request.query.get("_limit")

        cur_ts, cur_step = None, None
        if cursor:
            try:
                cursor_dict = decode_cursor(cursor, self._async_table.cursor_keys)
                cur_ts, cur_step = int(cursor_dict["ts_epoch"]), str(
                    cursor_dict["step_name"]
                )
            except ValueError:
                return DBResponse(response_code=400, body="Invalid cursor")

        if limit is None and cursor is None:
            return await self._async_table.get_steps(
                flow_id, run_number, with_run_tags=True
            )

        limit = min(int(limit), 500) if limit else 50

        response, pagination = await self._async_table.get_steps_paginated(
            flow_id=flow_id,
            run_id=run_number,
            with_run_tags=True,
            limit=limit,
            cur_ts=cur_ts,
            cur_step=cur_step,
        )

        if pagination.next_cursor_record:
            next_cursor = encode_cursor(
                {
                    "ts_epoch": pagination.next_cursor_record["ts_epoch"],
                    "step_name": pagination.next_cursor_record["step_name"],
                }
            )
            pagination = pagination._replace(next_cursor=next_cursor)

        return response, pagination

    @format_response
    @handle_exceptions
    async def get_step(self, request):
        """
        ---
        description: get specified step.
                    "tags" and "system_tags" values will be persisted to DB, but will not be
                    returned by read endpoints - the related run's "tags" and "system_tags" will
                    be returned instead.
        tags:
        - Steps
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
        produces:
        - text/plain
        responses:
            "200":
                description: successful operation. Returned specified step
            "404":
                description: step not found
            "405":
                description: invalid HTTP Method
        """
        flow_id = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")
        step_name = request.match_info.get("step_name")
        db_response = await self._async_table.get_step(
            flow_id, run_number, step_name, with_run_tags=True
        )
        return db_response

    @format_response
    @handle_exceptions
    async def create_step(self, request):
        """
        ---
        description: Create step.
        tags:
        - Steps
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
        - name: "body"
          in: "body"
          description: "body"
          required: true
          schema:
            type: object
            properties:
                user_name:
                    type: string
                tags:
                    type: object
                system_tags:
                    type: object
        produces:
        - text/plain
        responses:
            "200":
                description: successful operation. Registered step
            "405":
                description: invalid HTTP Method
        """
        flow_id = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")
        step_name = request.match_info.get("step_name")

        body = await read_body(request.content)
        user = body.get("user_name", "")
        tags = body.get("tags")
        system_tags = body.get("system_tags")

        run_number, run_id = await self._db.get_run_ids(flow_id, run_number)

        step_row = StepRow(
            flow_id,
            run_number,
            run_id,
            user,
            step_name,
            tags=tags,
            system_tags=system_tags,
        )

        db_response = await self._async_table.add_step(step_row)
        db_response = await apply_run_tags_to_db_response(
            flow_id, run_number, self._async_run_table, db_response
        )
        return db_response
