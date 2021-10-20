from services.data import StepRow
from services.utils import read_body
from services.metadata_service.api.utils import format_response, \
    handle_exceptions
from services.data.postgres_async_db import AsyncPostgresDB


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
        self._run_table = AsyncPostgresDB.get_instance().run_table_postgres
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
        produces:
        - text/plain
        responses:
            "200":
                description: successful operation. returned all steps
            "405":
                description: invalid HTTP Method
        """
        flow_name = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")
        return await self._async_table.get_steps(flow_name, run_number)

    @format_response
    @handle_exceptions
    async def get_step(self, request):
        """
        ---
        description: get specified step.
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
        flow_name = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")
        step_name = request.match_info.get("step_name")
        return await self._async_table.get_step(flow_name, run_number, step_name)

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
        flow_name = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")
        step_name = request.match_info.get("step_name")

        body = await read_body(request.content)
        user = body.get("user_name", "")
        tags = body.get("tags")
        system_tags = body.get("system_tags")

        run_number, run_id = await self._db.get_run_ids(flow_name, run_number)

        step_row = StepRow(
            flow_name, run_number, run_id, user, step_name, tags=tags,
            system_tags=system_tags
        )

        return await self._async_table.add_step(step_row)
