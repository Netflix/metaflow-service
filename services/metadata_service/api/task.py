from services.data import TaskRow
from services.data.postgres_async_db import AsyncPostgresDB
from services.data.tagging_utils import apply_run_tags_to_db_response
from services.data.db_utils import DBResponse, translate_run_key
from services.data.filter_grammar import custom_conditions_query_dict, operators_to_sql
from services.utils import has_heartbeat_capable_version_tag, read_body
from services.metadata_service.api.utils import format_response, handle_exceptions
import json
from aiohttp import web
import asyncio
from services.data.db_utils import DBResponse, encode_cursor, decode_cursor

# Fields the tasks endpoint accepts in the field:operator filter grammar, scoped to the
# columns a client can meaningfully narrow within a single step listing. Deliberately
# excludes: the path-pinned fields (flow_id/run_number/run_id/step_name are already fixed by
# the route, so filtering on them is redundant); 'status' (task status is not derived by the
# metadata service, and is being redefined upstream); and tag fields (a task's stored tags
# are not canonical; the run's tags are grafted on at read time, so SQL tag filtering would
# match stale values).
TASK_ALLOWED_FILTERS = [
    "task_id",
    "task_name",
    "user_name",
    "ts_epoch",
    "last_heartbeat_ts",
]
# Fields whose values must be integer epoch milliseconds.
_NUMERIC_TASK_FILTERS = {"ts_epoch", "last_heartbeat_ts"}


def _split_field_op(key):
    field, _, operator = key.partition(":")
    return field, (operator or "eq")


def _validate_task_filters(query):
    """Fail loud on malformed filters the generic grammar would otherwise silently drop:
    an unknown operator on a known field, or a non-integer time bound. Returns an error
    string, or None when the filters are acceptable.
    """
    for key, val in query.items():
        if key.startswith("_"):
            continue
        field, operator = _split_field_op(key)
        if field not in TASK_ALLOWED_FILTERS:
            continue  # unknown field: ignored, not an error
        if operator not in operators_to_sql:
            return "unsupported operator '%s' for field '%s'" % (operator, field)
        for v in val.split(","):
            if v == "null":
                continue
            if field in _NUMERIC_TASK_FILTERS:
                try:
                    int(v)
                except (TypeError, ValueError):
                    return (
                        "invalid %s: expected integer epoch milliseconds, got '%s'"
                        % (field, v)
                    )
    return None


class TaskApi(object):
    _task_table = None
    lock = asyncio.Lock()

    def __init__(self, app):
        app.router.add_route(
            "GET",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks",
            self.get_tasks,
        )
        app.router.add_route(
            "GET",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/filtered_tasks",
            self.get_filtered_tasks,
        )
        app.router.add_route(
            "GET",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}",
            self.get_task,
        )
        app.router.add_route(
            "POST",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/task",
            self.create_task,
        )
        app.router.add_route(
            "POST",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/heartbeat",
            self.tasks_heartbeat,
        )
        self._async_table = AsyncPostgresDB.get_instance().task_table_postgres
        self._async_run_table = AsyncPostgresDB.get_instance().run_table_postgres
        self._async_metadata_table = (
            AsyncPostgresDB.get_instance().metadata_table_postgres
        )
        self._db = AsyncPostgresDB.get_instance()

    @format_response
    @handle_exceptions
    async def get_tasks(self, request):
        """
        ---
        description: get all tasks of a step, optionally filtered by the field:operator grammar
        tags:
        - Tasks
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
        - name: "_limit"
          in: "query"
          description: "page size (default 50, max 500). Supplying _limit or _cursor turns on cursor pagination."
          required: false
          type: "integer"
        - name: "_cursor"
          in: "query"
          description: "opaque pagination cursor, returned via the X-Next-Cursor header."
        - name: "ts_epoch"
          in: "query"
          description: "filter by start time, e.g. ts_epoch:ge=123&ts_epoch:le=456 (epoch ms)."
          required: false
          type: "string"
        - name: "user_name"
          in: "query"
          description: "filter by user_name/task_name, e.g. user_name:eq=alice."
          required: false
          type: "string"
        produces:
        - text/plain
        responses:
            "200":
                description: successful operation. Return the matching tasks
            "400":
                description: non-integer time bound or unknown operator
            "405":
                description: invalid HTTP Method
        """
        flow_id = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")
        step_name = request.match_info.get("step_name")
        query = request.query

        err = _validate_task_filters(query)
        if err:
            return DBResponse(response_code=400, body=err)

        # field:operator filtering only (no _tags builtin: task-level tags are not
        # canonical, the run's tags are grafted on below, so SQL tag filtering would match
        # stale stored values; task status is not derived by the metadata service).
        filter_conditions, filter_values = custom_conditions_query_dict(
            query, TASK_ALLOWED_FILTERS
        )

        cursor = request.query.get("_cursor")
        limit = request.query.get("_limit")

        # Backwards-compatible fast path: with no filters, keep the original listing.
        if not filter_conditions and cursor is None and limit is None:
            db_response = await self._async_table.get_tasks(
                flow_id, run_number, step_name
            )
            return await apply_run_tags_to_db_response(
                flow_id, run_number, self._async_run_table, db_response
            )

        # the step listing is pinned by the path; the grammar filters narrow within it.
        run_id_key, run_id_value = translate_run_key(run_number)
        conditions = [
            '"flow_id" = %s',
            '"{}" = %s'.format(run_id_key),
            '"step_name" = %s',
        ] + filter_conditions
        values = [flow_id, run_id_value, step_name] + list(filter_values)

        cur_ts, cur_task = None, None
        if cursor:
            try:
                cursor_dict = decode_cursor(cursor)
                cur_ts, cur_task = int(cursor_dict["ts_epoch"]), int(
                    cursor_dict["task_id"]
                )
            except (ValueError, KeyError):
                return DBResponse(response_code=400, body="Invalid cursor")

        limit = min(int(limit), 500) if limit else 50
        db_response, pagination = await self._async_table.get_filtered_tasks_paginated(
            conditions=conditions,
            values=values,
            cur_ts=cur_ts,
            cur_task=cur_task,
            limit=limit,
        )
        # graft the run's tags onto every task, same contract as the unfiltered path
        db_response = await apply_run_tags_to_db_response(
            flow_id, run_number, self._async_run_table, db_response
        )

        if pagination.next_cursor_record:
            next_cursor = encode_cursor(
                {
                    "ts_epoch": pagination.next_cursor_record["ts_epoch"],
                    "task_id": pagination.next_cursor_record["task_id"],
                }
            )
            pagination = pagination._replace(next_cursor=next_cursor)

        return db_response, pagination

    @format_response
    @handle_exceptions
    async def get_filtered_tasks(self, request):
        """
        ---
        description: get all task ids that match the provided metadata field name and/or value.
        tags:
        - Tasks
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
        - name: "metadata_field_name"
          in: "query"
          description: "Metadata field name to filter with"
          type: "string"
        - name: "pattern"
          in: "query"
          description: "A regexp pattern to filter the metadata values on"
          type: "string"
        produces:
        - text/plain
        responses:
            "200":
                description: successful operation. Return tasks
            "405":
                description: invalid HTTP Method
        """
        flow_id = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")
        step_name = request.match_info.get("step_name")

        # possible filters
        metadata_field = request.query.get("metadata_field_name", None)
        pattern = request.query.get("pattern", None)

        db_response, _ = await self._async_metadata_table.get_filtered_task_pathspecs(
            flow_id, run_number, step_name, metadata_field, pattern
        )
        return db_response

    @format_response
    @handle_exceptions
    async def get_task(self, request):
        """
        ---
        description: get all artifacts associated with the specified task.
        tags:
        - Tasks
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
          type: "integer"
        produces:
        - text/plain
        responses:
            "200":
                description: successful operation. Return task
            "405":
                description: invalid HTTP Method
        """
        flow_id = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")
        step_name = request.match_info.get("step_name")
        task_id = request.match_info.get("task_id")
        db_response = await self._async_table.get_task(
            flow_id, run_number, step_name, task_id
        )
        db_response = await apply_run_tags_to_db_response(
            flow_id, run_number, self._async_run_table, db_response
        )
        return db_response

    @format_response
    @handle_exceptions
    async def create_task(self, request):
        """
        ---
        description: This end-point allow to test that service is up.
                    "tags" and "system_tags" values will be persisted to DB, but will not be
                    returned by read endpoints - the related run's "tags" and "system_tags" will
                    be returned instead.
        tags:
        - Tasks
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
                task_id:
                    type: string
        produces:
        - 'text/plain'
        responses:
            "202":
                description: successful operation. Return newly registered task
            "400":
                description: invalid HTTP Request
            "405":
                description: invalid HTTP Method
        """
        flow_id = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")
        step_name = request.match_info.get("step_name")
        body = await read_body(request.content)

        user = body.get("user_name")
        tags = body.get("tags")
        system_tags = body.get("system_tags")
        task_name = body.get("task_id")

        client_supports_heartbeats = has_heartbeat_capable_version_tag(system_tags)

        if task_name and task_name.isnumeric():
            return web.Response(
                status=400,
                body=json.dumps({"message": "provided task_name may not be a numeric"}),
            )

        run_number, run_id = await self._db.get_run_ids(flow_id, run_number)

        task = TaskRow(
            flow_id=flow_id,
            run_number=run_number,
            run_id=run_id,
            step_name=step_name,
            task_name=task_name,
            user_name=user,
            tags=tags,
            system_tags=system_tags,
        )
        db_response = await self._async_table.add_task(
            task, fill_heartbeat=client_supports_heartbeats
        )
        db_response = await apply_run_tags_to_db_response(
            flow_id, run_number, self._async_run_table, db_response
        )
        if client_supports_heartbeats and db_response.response_code == 200:
            await self._async_run_table.update_heartbeat(flow_id, run_number)
        return db_response

    @format_response
    @handle_exceptions
    async def tasks_heartbeat(self, request):
        """
        ---
        description: update hb
        tags:
        - Tasks
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
            type: object
        produces:
        - 'text/plain'
        responses:
            "200":
                description: successful operation. Return newly registered run
            "400":
                description: invalid HTTP Request
            "405":
                description: invalid HTTP Method
        """
        flow_name = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")
        step_name = request.match_info.get("step_name")
        task_id = request.match_info.get("task_id")
        await self._async_run_table.update_heartbeat(flow_name, run_number)
        return await self._async_table.update_heartbeat(
            flow_name, run_number, step_name, task_id
        )
