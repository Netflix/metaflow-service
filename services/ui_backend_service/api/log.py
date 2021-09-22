
import os
from typing import Dict, Optional, Tuple

from services.data.db_utils import DBResponse, translate_run_key, translate_task_key, DBPagination, DBResponse
from services.utils import handle_exceptions, web_response
from .utils import format_response_list

from aiohttp import web
from multidict import MultiDict


STDOUT = 'log_location_stdout'
STDERR = 'log_location_stderr'


class LogApi(object):
    def __init__(self, app, db, cache=None):
        self.db = db
        app.router.add_route(
            "GET",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/logs/out",
            self.get_task_log_stdout,
        )
        app.router.add_route(
            "GET",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/logs/err",
            self.get_task_log_stderr,
        )
        app.router.add_route(
            "GET",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/logs/out/download",
            self.get_task_log_stdout_file,
        )
        app.router.add_route(
            "GET",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/logs/err/download",
            self.get_task_log_stderr_file,
        )
        self.metadata_table = self.db.metadata_table_postgres
        self.task_table = self.db.task_table_postgres

        # Cache to hold already fetched logs
        self.cache = getattr(cache, "log_cache", None)

    @handle_exceptions
    async def get_task_log_stdout(self, request):
        """
        ---
        description: Get STDOUT log of a Task
        tags:
        - Task
        parameters:
          - $ref: '#/definitions/Params/Path/flow_id'
          - $ref: '#/definitions/Params/Path/run_number'
          - $ref: '#/definitions/Params/Path/step_name'
          - $ref: '#/definitions/Params/Path/task_id'
          - $ref: '#/definitions/Params/Custom/attempt_id'
        produces:
        - application/json
        responses:
            "200":
                description: Return a tasks stdout log
                schema:
                  $ref: '#/definitions/ResponsesLog'
            "405":
                description: invalid HTTP Method
                schema:
                  $ref: '#/definitions/ResponsesError405'
            "404":
                description: Log for task could not be found
                schema:
                  $ref: '#/definitions/ResponsesError404'
            "500":
                description: Internal Server Error (with error id)
                schema:
                    $ref: '#/definitions/ResponsesLogError500'
        """
        return await self.get_task_log(request, STDOUT)

    @handle_exceptions
    async def get_task_log_stderr(self, request):
        """
        ---
        description: Get STDERR log of a Task
        tags:
        - Task
        parameters:
          - $ref: '#/definitions/Params/Path/flow_id'
          - $ref: '#/definitions/Params/Path/run_number'
          - $ref: '#/definitions/Params/Path/step_name'
          - $ref: '#/definitions/Params/Path/task_id'
          - $ref: '#/definitions/Params/Custom/attempt_id'
        produces:
        - application/json
        responses:
            "200":
                description: Return a tasks stderr log
                schema:
                  $ref: '#/definitions/ResponsesLog'
            "405":
                description: invalid HTTP Method
                schema:
                  $ref: '#/definitions/ResponsesError405'
            "404":
                description: Log for task could not be found
                schema:
                  $ref: '#/definitions/ResponsesError404'
            "500":
                description: Internal Server Error (with error id)
                schema:
                    $ref: '#/definitions/ResponsesLogError500'
        """
        return await self.get_task_log(request, STDERR)

    @handle_exceptions
    async def get_task_log_stdout_file(self, request):
        """
        ---
        description: Get STDOUT log of a Task
        tags:
        - Task
        parameters:
          - $ref: '#/definitions/Params/Path/flow_id'
          - $ref: '#/definitions/Params/Path/run_number'
          - $ref: '#/definitions/Params/Path/step_name'
          - $ref: '#/definitions/Params/Path/task_id'
          - $ref: '#/definitions/Params/Custom/attempt_id'
        produces:
        - text/plain
        responses:
            "200":
                description: Return a tasks stdout log as a file download
            "405":
                description: invalid HTTP Method
                schema:
                  $ref: '#/definitions/ResponsesError405'
            "404":
                description: Log for task could not be found
                schema:
                  $ref: '#/definitions/ResponsesError404'
            "500":
                description: Internal Server Error (with error id)
                schema:
                    $ref: '#/definitions/ResponsesLogError500'
        """
        return await self.get_task_log_file(request, STDOUT)

    @handle_exceptions
    async def get_task_log_stderr_file(self, request):
        """
        ---
        description: Get STDERR log of a Task
        tags:
        - Task
        parameters:
          - $ref: '#/definitions/Params/Path/flow_id'
          - $ref: '#/definitions/Params/Path/run_number'
          - $ref: '#/definitions/Params/Path/step_name'
          - $ref: '#/definitions/Params/Path/task_id'
          - $ref: '#/definitions/Params/Custom/attempt_id'
        produces:
        - text/plain
        responses:
            "200":
                description: Return a tasks stderr log as a file download
            "405":
                description: invalid HTTP Method
                schema:
                  $ref: '#/definitions/ResponsesError405'
            "404":
                description: Log for task could not be found
                schema:
                  $ref: '#/definitions/ResponsesError404'
            "500":
                description: Internal Server Error (with error id)
                schema:
                    $ref: '#/definitions/ResponsesLogError500'
        """
        return await self.get_task_log_file(request, STDERR)

    async def get_task_by_request(self, request):
        flow_id, run_number, step_name, task_id, _ = \
            _get_pathspec_from_request(request)

        run_id_key, run_id_value = translate_run_key(run_number)
        task_id_key, task_id_value = translate_task_key(task_id)

        db_response, *_ = await self.task_table.find_records(
            fetch_single=True,
            conditions=[
                "flow_id = %s",
                "{run_id_key} = %s".format(run_id_key=run_id_key),
                "step_name = %s",
                "{task_id_key} = %s".format(task_id_key=task_id_key)],
            values=[flow_id, run_id_value, step_name, task_id_value],
            expanded=True
        )
        if db_response.response_code == 200:
            return db_response.body
        return None

    async def get_task_log(self, request, logtype=STDOUT):
        "fetches log and emits it as a list of rows wrapped in json"
        task = await self.get_task_by_request(request)
        if not task:
            return web_response(404, {'data': []})

        lines = await read_and_output(self.cache, task, logtype)
        # paginate response
        code, body = paginate_log_lines(request, lines)
        return web_response(code, body)

    async def get_task_log_file(self, request, logtype=STDOUT):
        "fetches log and emits it as a single file download response"
        flow_id, run_number, step_name, task_id, attempt_id = \
            _get_pathspec_from_request(request)

        task = await self.get_task_by_request(request)
        if not task:
            return web_response(404, {'data': []})

        log_filename = "{type}_{flow_id}_{run_number}_{step_name}_{task_id}{attempt}.txt".format(
            type="stdout" if logtype == STDOUT else "stderr",
            flow_id=flow_id,
            run_number=run_number,
            step_name=step_name,
            task_id=task_id,
            attempt="_attempt{}".format(attempt_id or 0)
        )

        lines = await read_and_output(self.cache, task, logtype)
        logstring = "\n".join(line['line'] for line in lines)
        return file_download_response(log_filename, logstring)


async def read_and_output(cache_client, task, logtype):
    res = await cache_client.cache.GetLogFile(task, logtype, invalidate_cache=True)

    if res.has_pending_request():
        async for event in res.stream():
            if event["type"] == "error":
                # raise error, there was an exception during fetching.
                raise LogException(event["message"], event["id"], event["traceback"])
        await res.wait()  # wait until results are ready

    lines = []
    for row, line in enumerate(res.get().split("\n")):
        lines.append({
            'row': row,
            'line': line.strip(),
        })
    return lines


def paginate_log_lines(request, lines):
    """Paginates the log lines based on url parameters
    """
    # Page
    page = max(int(request.query.get("_page", 1)), 1)

    # Limit
    # Default limit is 1000, maximum is 10_000
    limit = min(int(request.query.get("_limit", 1000)), 10000)

    # Offset
    offset = limit * (page - 1)

    lines = order_log_lines(request, lines)

    page_count = max(len(lines) // limit, 1)

    response = DBResponse(200, lines[offset:][:limit])
    pagination = DBPagination(limit, offset, len(response.body), page)
    return format_response_list(request, response, pagination, page, page_count)


def order_log_lines(request, lines):
    "orders log lines based on request parameters"

    order = request.query.get("_order")
    if order is not None and order.startswith("-row"):
        return lines[::-1]
    else:
        return lines


def file_download_response(filename, body):
    return web.Response(
        headers=MultiDict({'Content-Disposition': 'Attachment;filename={}'.format(filename)}),
        body=body
    )


def _get_pathspec_from_request(request: MultiDict) -> Tuple[str, str, str, str, Optional[str]]:
    """extract relevant resource id's from the request

    Returns
    -------
    flow_id, run_number, step_name, task_id, attempt_id
    """
    flow_id = request.match_info.get("flow_id")
    run_number = request.match_info.get("run_number")
    step_name = request.match_info.get("step_name")
    task_id = request.match_info.get("task_id")
    attempt_id = request.query.get("attempt_id", None)

    return flow_id, run_number, step_name, task_id, attempt_id


class LogException(Exception):
    def __init__(self, msg='Failed to read log', id='log-error', traceback_str=None):
        self.message = msg
        self.id = id
        self.traceback_str = traceback_str

    def __str__(self):
        return self.message
