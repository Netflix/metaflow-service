import asyncio
from itertools import chain

from services.data.db_utils import DBResponse, encode_cursor, decode_cursor
from services.data.models import RunRow
from services.utils import has_heartbeat_capable_version_tag, read_body
from services.metadata_service.api.utils import format_response, handle_exceptions
from services.data.postgres_async_db import AsyncPostgresDB
from services.data.filter_grammar import (
    builtin_conditions_query_dict,
    custom_conditions_query_dict,
    operators_to_sql,
)

# Statuses a run can be filtered by, matching the values the status query derives.
SUPPORTED_RUN_STATUSES = {"running", "completed", "failed"}

# Fields the runs endpoint accepts in the field:operator filter grammar: the base run
# columns plus the derived 'status' (lateral joins) and 'user' (verified owner).
RUN_ALLOWED_FILTERS = [
    "flow_id",
    "run_number",
    "run_id",
    "user_name",
    "ts_epoch",
    "last_heartbeat_ts",
    "tags",
    "system_tags",
    "user",
    "status",
]
# Fields whose values must be integer epoch milliseconds.
_NUMERIC_FILTERS = {"ts_epoch", "last_heartbeat_ts"}


def _split_field_op(key):
    field, _, operator = key.partition(":")
    return field, (operator or "eq")


def _validate_run_filters(query):
    """Fail loud on malformed filters the generic grammar would otherwise silently drop.

    Returns an error string, or None when the filters are acceptable. Preserves the
    contract that a bad status value, a non-integer time bound, or an unknown operator
    on a known field is a 400 rather than a silently-ignored filter.
    """
    for key, val in query.items():
        if key.startswith("_"):
            continue
        field, operator = _split_field_op(key)
        if field not in RUN_ALLOWED_FILTERS:
            continue  # unknown field: ignored, not an error
        if operator not in operators_to_sql:
            return "unsupported operator '%s' for field '%s'" % (operator, field)
        for v in val.split(","):
            if v == "null":
                continue
            if field == "status" and v not in SUPPORTED_RUN_STATUSES:
                return "unsupported status filter: %s" % v
            if field in _NUMERIC_FILTERS:
                try:
                    int(v)
                except (TypeError, ValueError):
                    return (
                        "invalid %s: expected integer epoch milliseconds, got '%s'"
                        % (field, v)
                    )
    return None


class RunApi(object):
    _run_table = None
    lock = asyncio.Lock()

    def __init__(self, app):
        app.router.add_route("GET", "/flows/{flow_id}/runs", self.get_all_runs)
        app.router.add_route("GET", "/flows/{flow_id}/runs/{run_number}", self.get_run)
        app.router.add_route("POST", "/flows/{flow_id}/run", self.create_run)
        app.router.add_route(
            "POST", "/flows/{flow_id}/runs/{run_number}/heartbeat", self.runs_heartbeat
        )
        app.router.add_route(
            "PATCH",
            "/flows/{flow_id}/runs/{run_number}/tag/mutate",
            self.mutate_user_tags,
        )
        self._async_table = AsyncPostgresDB.get_instance().run_table_postgres

    @format_response
    @handle_exceptions
    async def get_run(self, request):
        """
        ---
        description: Get run by run number
        tags:
        - Run
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
        produces:
        - text/plain
        responses:
            "200":
                description: successful operation. Return specified run
            "404":
                description: specified run not found
            "405":
                description: invalid HTTP Method
        """
        flow_name = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")
        return await self._async_table.get_run(flow_name, run_number)

    @format_response
    @handle_exceptions
    async def get_all_runs(self, request):
        """
        ---
        description: Get all runs of a flow, optionally filtered and/or cursor-paginated
        tags:
        - Run
        parameters:
        - name: "flow_id"
          in: "path"
          description: "flow_id"
          required: true
          type: "string"
        - name: "status"
          in: "query"
          description: "filter by status, e.g. status:eq=running or status:eq=running,failed (OR). Status is derived relative to the heartbeat cutoff, so a multi-status filter that includes running is time-relative, not fully deterministic."
          required: false
          type: "string"
        - name: "user"
          in: "query"
          description: "filter by verified owner, e.g. user:eq=alice (system tag user:<name>)."
          required: false
          type: "string"
        - name: "ts_epoch"
          in: "query"
          description: "filter by start time, e.g. ts_epoch:ge=123&ts_epoch:le=456 (epoch ms)."
          required: false
          type: "string"
        - name: "_tags"
          in: "query"
          description: "filter by tags, e.g. _tags:any=a,b (OR) or _tags:all=a,b (AND)."
          required: false
          type: "string"
        - name: "_cursor"
          in: "query"
          description: "opaque pagination cursor, returned via the X-Next-Cursor header."
          required: false
          type: "string"
        - name: "_limit"
          in: "query"
          description: "page size (default 50, max 500). Supplying _limit or _cursor turns on cursor pagination."
          required: false
          type: "string"
        produces:
        - text/plain
        responses:
            "200":
                description: Returned the matching runs of the specified flow
            "400":
                description: unsupported status value, non-integer time bound, unknown operator, or invalid cursor
            "405":
                description: invalid HTTP Method
        """
        flow_name = request.match_info.get("flow_id")
        query = request.query

        err = _validate_run_filters(query)
        if err:
            return DBResponse(response_code=400, body=err)

        # Parse the field:operator filter grammar into parameterised SQL (shared with
        # the ui_backend so the two services filter identically).
        builtin_cond, builtin_vals = builtin_conditions_query_dict(
            query
        )  # _tags:any/all
        custom_cond, custom_vals = custom_conditions_query_dict(
            query, RUN_ALLOWED_FILTERS
        )
        filter_conditions = builtin_cond + custom_cond

        cursor = query.get("_cursor")
        limit = query.get("_limit")

        # Backwards-compatible fast path: with neither filters nor pagination, fall back
        # to the original unfiltered, join-free listing (raw array) that older clients
        # expect. New clients support filtering and cursor pagination together, so any
        # filter or pagination param drops through to the paginated path below.
        if not filter_conditions and cursor is None and limit is None:
            return await self._async_table.get_all_runs(flow_name)

        conditions = ['"flow_id" = %s'] + filter_conditions
        values = [flow_name] + list(builtin_vals) + list(custom_vals)

        # status is the only derived column needing the lateral joins; turn them on
        # only when a status predicate is actually requested.
        requested_fields = {
            _split_field_op(k)[0] for k in query if not k.startswith("_")
        }
        enable_joins = "status" in requested_fields

        cur_ts, cur_run = None, None
        if cursor:
            try:
                cursor_dict = decode_cursor(cursor)
                cur_ts, cur_run = int(cursor_dict["ts_epoch"]), int(
                    cursor_dict["run_number"]
                )
            except (ValueError, KeyError):
                return DBResponse(response_code=400, body="Invalid cursor")

        limit = min(int(limit), 500) if limit else 50

        response, pagination = await self._async_table.get_filtered_runs_paginated(
            conditions=conditions,
            values=values,
            enable_joins=enable_joins,
            limit=limit,
            cur_ts=cur_ts,
            cur_run=cur_run,
        )

        if pagination.next_cursor_record:
            next_cursor = encode_cursor(
                {
                    "ts_epoch": pagination.next_cursor_record["ts_epoch"],
                    "run_number": pagination.next_cursor_record["run_number"],
                }
            )
            pagination = pagination._replace(next_cursor=next_cursor)

        return response, pagination

    @format_response
    @handle_exceptions
    async def create_run(self, request):
        """
        ---
        description: create run and generate run id
        tags:
        - Run
        parameters:
        - name: "flow_id"
          in: "path"
          description: "flow_id"
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
                run_number:
                    type: string
                tags:
                    type: object
                system_tags:
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

        body = await read_body(request.content)
        user = body.get("user_name")
        tags = body.get("tags")
        system_tags = body.get("system_tags")

        client_supports_heartbeats = has_heartbeat_capable_version_tag(system_tags)
        run_id = body.get("run_number")
        if run_id and run_id.isnumeric():
            raise Exception("provided run_id may not be a numeric")

        run_row = RunRow(
            flow_id=flow_name,
            user_name=user,
            tags=tags,
            system_tags=system_tags,
            run_id=run_id,
        )

        return await self._async_table.add_run(
            run_row, fill_heartbeat=client_supports_heartbeats
        )

    @format_response
    @handle_exceptions
    async def mutate_user_tags(self, request):
        """
        ---
        description: mutate user tags
        tags:
        - Run
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
        - name: "body"
          in: "body"
          description: "body"
          required: true
          schema:
            type: object
            properties:
                tags_to_add:
                    type: array of string
                tags_to_remove:
                    type: array of string
        produces:
        - 'text/plain'
        responses:
            "200":
                description: successful operation. Tags updated.  Returns latest user tags
            "400":
                description: invalid HTTP Request
            "405":
                description: invalid HTTP Method
            "409":
                description: mutation request conflicts with an existing in-flight mutation. Retry recommended
            "422":
                description: illegal tag mutation. No update performed.  E.g. could be because we tried to remove
                             a system tag.
        """
        flow_name = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")
        body = await read_body(request.content)
        tags_to_add = body.get("tags_to_add", [])
        tags_to_remove = body.get("tags_to_remove", [])

        # We return 400 when request structure is wrong
        if not isinstance(tags_to_add, list):
            return DBResponse(response_code=400, body="tags_to_add must be a list")

        if not isinstance(tags_to_remove, list):
            return DBResponse(response_code=400, body="tags_to_remove must be a list")

        # let's make sure we have a list of strings
        if not all(isinstance(t, str) for t in chain(tags_to_add, tags_to_remove)):
            return DBResponse(response_code=400, body="All tag values must be strings")

        tags_to_add_set = set(tags_to_add)
        tags_to_remove_set = set(tags_to_remove)

        async def _in_tx_mutation_logic(cur):
            run_db_response = await self._async_table.get_run(
                flow_name, run_number, cur=cur
            )
            if run_db_response.response_code != 200:
                # if something went wrong with get_run, just return the error from that directly
                # e.g. 404, or some other error. This is useful for the client (vs additional wrapping, etc).
                return run_db_response
            run = run_db_response.body
            existing_tag_set = set(run["tags"])
            existing_system_tag_set = set(run["system_tags"])

            if tags_to_remove_set & existing_system_tag_set:
                # We use 422 here to communicate that the request was well-formatted in terms of structure and
                # that the server understood what was being requested. However, it failed business rules.
                return DBResponse(
                    response_code=422,
                    body="Cannot remove tags that are existing system tags %s"
                    % str(tags_to_remove_set & existing_system_tag_set),
                )

            # Apply removals before additions.
            # And, make sure no existing system tags get added as a user tag
            next_run_tag_set = (existing_tag_set - tags_to_remove_set) | (
                tags_to_add_set - existing_system_tag_set
            )
            if next_run_tag_set == existing_tag_set:
                return DBResponse(
                    response_code=200, body={"tags": list(next_run_tag_set)}
                )
            next_run_tags = list(next_run_tag_set)

            update_db_response = await self._async_table.update_run_tags(
                flow_name, run_number, next_run_tags, cur=cur
            )
            if update_db_response.response_code != 200:
                return update_db_response
            return DBResponse(response_code=200, body={"tags": next_run_tags})

        return await self._async_table.run_in_transaction_with_serializable_isolation_level(
            _in_tx_mutation_logic
        )

    @format_response
    @handle_exceptions
    async def runs_heartbeat(self, request):
        """
        ---
        description: update hb
        tags:
        - Run
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
        return await self._async_table.update_heartbeat(flow_name, run_number)
