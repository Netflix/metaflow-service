import asyncio
import json
from aiohttp import web
from .utils import read_body
from ..data.models import RunRow
from ..data.postgres_async_db import AsyncPostgresDB


class RunApi(object):
    _run_table = None
    lock = asyncio.Lock()

    def __init__(self, app):
        app.router.add_route("GET", "/flows/{flow_id}/runs", self.get_all_runs)
        app.router.add_route("GET", "/flows/{flow_id}/runs/{run_number}", self.get_run)
        app.router.add_route("POST", "/flows/{flow_id}/run", self.create_run)
        self._async_table = AsyncPostgresDB.get_instance().run_table_postgres

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
        run_response = await self._async_table.get_run(flow_name, run_number)
        return web.Response(
            status=run_response.response_code, body=json.dumps(run_response.body)
        )

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
        runs = await self._async_table.get_all_runs(flow_name)

        return web.Response(status=runs.response_code, body=json.dumps(runs.body))

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
                run_id:
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

        run_id = body.get("run_id")
        if run_id and run_id.isnumeric():
            return web.Response(status=400, body=json.dumps(
                {"message": "provided run_id may not be a numeric"}))

        run_row = RunRow(
            flow_id=flow_name, user_name=user, tags=tags,
            system_tags=system_tags, run_id=run_id
        )

        run_response = await self._async_table.add_run(run_row)
        return web.Response(
            status=run_response.response_code, body=json.dumps(run_response.body)
        )
