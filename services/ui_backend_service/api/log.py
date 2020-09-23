import os
import json
import boto3
import codecs
from urllib.parse import urlparse

from services.data.postgres_async_db import AsyncPostgresDB
from services.data.db_utils import translate_run_key, translate_task_key
from services.utils import handle_exceptions, web_response

from aiohttp import web


STDOUT = 'log_location_stdout'
STDERR = 'log_location_stderr'


class LogApi(object):
    def __init__(self, app):
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
        self._async_table = AsyncPostgresDB.get_instance().metadata_table_postgres

    @handle_exceptions
    async def get_task_log_stdout(self, request):
        bucket, path, _ = \
            await get_metadata_log(
                self._async_table.find_records,
                request.match_info.get("flow_id"),
                request.match_info.get("run_number"),
                request.match_info.get("step_name"),
                request.match_info.get("task_id"),
                STDOUT)

        if bucket and path:
            lines = await read_and_output(bucket, path)
            return web_response(200, {'data': lines})
        else:
            return web_response(200, {'data': []})

    @handle_exceptions
    async def get_task_log_stderr(self, request):
        bucket, path, _ = \
            await get_metadata_log(
                self._async_table.find_records,
                request.match_info.get("flow_id"),
                request.match_info.get("run_number"),
                request.match_info.get("step_name"),
                request.match_info.get("task_id"),
                STDERR)

        if bucket and path:
            lines = await read_and_output(bucket, path)
            return web_response(200, {'data': lines})
        else:
            return web_response(200, {'data': []})


async def get_metadata_log(find_records, flow_name, run_number, step_name, task_id, field_name) -> (str, str, int):
    run_id_key, run_id_value = translate_run_key(run_number)
    task_id_key, task_id_value = translate_task_key(task_id)

    try:
        result, _ = await find_records(
            conditions=["flow_id = %s",
                        "{run_id_key} = %s".format(run_id_key=run_id_key),
                        "step_name = %s",
                        "{task_id_key} = %s".format(task_id_key=task_id_key),
                        "field_name = %s"],
            values=[flow_name, run_id_value, step_name, task_id_value, field_name], limit=1, offset=0,
            order=["ts_epoch DESC"], groups=None, fetch_single=True, enable_joins=True
        )
        if result.response_code == 200:
            value = json.loads(result.body['value'])
            if value['ds_type'] == 's3':
                url = urlparse(value['location'], allow_fragments=False)
                bucket = url.netloc
                path = url.path.lstrip('/')
                attempt_id = value['attempt']
                return bucket, path, attempt_id
    except Exception:
        pass
    return None, None, None


async def read_and_output(bucket, path):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=path)

    lines = []
    body = obj['Body']
    for row, line in enumerate(codecs.getreader('utf-8')(body), start=1):
        lines.append({
            'row': row,
            'line': line.strip(),
        })
    return lines


async def read_and_output_ws(bucket, path, ws):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=path)

    body = obj['Body']
    for row, line in enumerate(codecs.getreader('utf-8')(body), start=1):
        await ws.send_str(json.dumps({
            'row': row,
            'line': line.strip(),
        }))
