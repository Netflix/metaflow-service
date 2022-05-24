import asyncio
import json
from itertools import chain

from services.data.db_utils import DBResponse
from services.data.models import RunRow
from services.utils import has_heartbeat_capable_version_tag, read_body
from services.metadata_service.api.utils import format_response, \
    handle_exceptions
from services.data.postgres_async_db import AsyncPostgresDB


class RunApi(object):
    _run_table = None
    lock = asyncio.Lock()

    def __init__(self, app):
        app.router.add_route("GET", "/flows/{flow_id}/runs", self.get_all_runs)
        app.router.add_route(
            "GET", "/flows/{flow_id}/runs/{run_number}", self.get_run)
        app.router.add_route("POST", "/flows/{flow_id}/run", self.create_run)
        app.router.add_route("POST",
                             "/flows/{flow_id}/runs/{run_number}/heartbeat",
                             self.runs_heartbeat)
        app.router.add_route("PATCH",
                             "/flows/{flow_id}/runs/{run_number}/tag/mutate",
                             self.mutate_user_tags)
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
        description: Get all runs
        tags:
        - Run
        parameters:
        - name: "flow_id"
          in: "path"
          description: "flow_id"
          required: true
          type: "string"
        produces:
        - text/plain
        responses:
            "200":
                description: Returned all runs of specified flow
            "405":
                description: invalid HTTP Method
        """
        flow_name = request.match_info.get("flow_id")
        return await self._async_table.get_all_runs(flow_name)

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
            flow_id=flow_name, user_name=user, tags=tags,
            system_tags=system_tags, run_id=run_id
        )

        return await self._async_table.add_run(run_row, fill_heartbeat=client_supports_heartbeats)

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
        """
        flow_name = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")
        body = await read_body(request.content)
        tags_to_add = body.get("tags_to_add", [])
        tags_to_remove = body.get("tags_to_remove", [])

        if not isinstance(tags_to_add, list):
            return DBResponse(response_code=422, body="tags_to_add must be a list")

        if not isinstance(tags_to_remove, list):
            return DBResponse(response_code=422, body="tags_to_remove must be a list")

        # let's make sure we have a list of strings
        if not all(isinstance(t, str) for t in chain(tags_to_add, tags_to_remove)):
            return DBResponse(response_code=422, body="All tag values must be strings")

        tags_to_add_set = set(tags_to_add)
        tags_to_remove_set = set(tags_to_remove)

        async def _in_tx_mutation_logic(cur):
            run_db_response = await self._async_table.get_run(flow_name, run_number, cur=cur)
            if run_db_response.response_code != 200:
                if run_db_response.response_code == 404:
                    return run_db_response
                return DBResponse(response_code=422, body="Failed to get run (get_run status %d)" % run_db_response.response_code)
            run = run_db_response.body
            existing_tag_set = set(run["tags"])
            existing_system_tag_set = set(run["system_tags"])

            for tag in tags_to_remove_set:
                if tag in existing_system_tag_set:
                    return DBResponse(response_code=422, body="Cannot remove a tag that is an existing system tag (%s)" % tag)

            # Apply removals before additions.
            # Make sure no existing system tags get added as a user tag
            next_run_tag_set = (existing_tag_set - tags_to_remove_set) | (tags_to_add_set - existing_system_tag_set)
            if next_run_tag_set == existing_tag_set:
                return DBResponse(response_code=200, body=json.dumps({"tags": list(next_run_tag_set)}))
            next_run_tags = list(next_run_tag_set)

            update_db_response = await self._async_table.update_run_tags(flow_name, run_number, next_run_tags, cur=cur)
            if update_db_response.response_code != 200:
                return update_db_response
            return DBResponse(response_code=200, body=json.dumps({"tags": next_run_tags}))

        return await self._async_table.run_in_transaction_with_serializable_isolation_level(_in_tx_mutation_logic)

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
