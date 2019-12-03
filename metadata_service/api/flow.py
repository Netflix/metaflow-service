from aiohttp import web

import json
from ..data.models import FlowRow
from ..data.postgres_async_db import AsyncPostgresDB
from .utils import read_body
import asyncio


class FlowApi(object):
    _flow_table = None
    lock = asyncio.Lock()

    def __init__(self, app):
        app.router.add_route("GET", "/flows", self.get_all_flows)
        app.router.add_route("GET", "/flows/{flow_id}", self.get_flow)
        app.router.add_route("POST", "/flows/{flow_id}", self.create_flow)
        self._async_table = AsyncPostgresDB.get_instance().flow_table_postgres

    async def create_flow(self, request):
        """
        ---
        description: create/register a flow
        tags:
        - Flow
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
                tags:
                    type: object
                system_tags:
                    type: object

        produces:
        - 'text/plain'
        responses:
            "200":
                description: successfully created flow row
            "409":
                description: CONFLICT record exists
        """
        flow_name = request.match_info.get("flow_id")

        body = await read_body(request.content)
        user = body.get("user_name")
        tags = body.get("tags")
        system_tags = body.get("system_tags")
        flow = FlowRow(
            flow_id=flow_name, user_name=user, tags=tags, system_tags=system_tags
        )
        response = await self._async_table.add_flow(flow)
        return web.Response(
            status=response.response_code, body=json.dumps(response.body)
        )

    async def get_flow(self, request):
        """
        ---
        description: Get flow by id
        tags:
        - Flow
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
                description: successful operation. Return flow
            "404":
                description: flow not found
            "405":
                description: invalid HTTP Method
        """

        flow_name = request.match_info.get("flow_id")
        flow_response = await self._async_table.get_flow(flow_name)
        return web.Response(
            status=flow_response.response_code, body=json.dumps(flow_response.body)
        )

    async def get_all_flows(self, request):
        """
        ---
        description: Get all flows
        tags:
        - Flow
        produces:
        - text/plain
        responses:
            "200":
                description: successful operation. Returned all registered flows
            "405":
                description: invalid HTTP Method
        """
        flows = await self._async_table.get_all_flows()
        return web.Response(status=flows.response_code, body=json.dumps(flows.body))
