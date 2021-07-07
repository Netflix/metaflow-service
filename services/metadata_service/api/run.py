import asyncio
from services.data.models import RunRow
from services.data.db_utils import transaction_helper, translate_run_key, aiopg_exception_handling
from services.utils import read_body
from services.metadata_service.api.utils import format_response, \
    handle_exceptions, web_response
from services.data.postgres_async_db import AsyncPostgresDB
import psycopg2
import psycopg2.extras


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
        app.router.add_route("POST",
                             "/flows/{flow_id}/runs/{run_number}/tags",
                             self.run_tags)
        self._async_table = AsyncPostgresDB.get_instance().run_table_postgres
        self._task_table = AsyncPostgresDB.get_instance().task_table_postgres
        self._artifact_table = AsyncPostgresDB.get_instance().artifact_table_postgres
        self._step_table = AsyncPostgresDB.get_instance().step_table_postgres

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

        run_id = body.get("run_number")
        if run_id and run_id.isnumeric():
            raise Exception("provided run_id may not be a numeric")

        run_row = RunRow(
            flow_id=flow_name, user_name=user, tags=tags,
            system_tags=system_tags, run_id=run_id
        )

        return await self._async_table.add_run(run_row)

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


    @handle_exceptions
    async def run_tags(self, request):
        """
        ---
        description: update tags
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
                add_tag:
                    type: string
                remove_tag:
                    type: string
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

        body = await read_body(request.content)

        flow_id = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")

        run_id_key, run_id_value = translate_run_key(run_number)
        filter_dict = {"flow_id": flow_id, run_id_key: run_id_value}

        run_id = body.get("run_number")
        if run_id and run_id.isnumeric():
            raise Exception("provided run_id may not be a numeric")


        async def update_all_tables(cur):
            rowcount = 0
            for table in [self._async_table, self._task_table, self._step_table, self._artifact_table]:
                if body.get('add_tag') and body.get('remove_tag'):
                    rowcount += await table.tag_replace(cur, filter_dict, body.get('remove_tag'), body.get('add_tag'))
                elif body.get('add_tag'):
                    rowcount += await table.tag_add(cur, filter_dict, body.get('add_tag'))
                elif body.get('remove_tag'):
                    rowcount += await table.tag_remove(cur, filter_dict, body.get('remove_tag'))
            return rowcount

        rowcount = await transaction_helper(self._async_table.db, update_all_tables)
        return web_response(200, {"rowcount": rowcount})

