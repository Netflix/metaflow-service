import os
import json
import boto3
import botocore
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
        attempt_id = request.query.get("attempt_id", None)

        bucket, path, _ = \
            await get_metadata_log(
                self._async_table.find_records,
                request.match_info.get("flow_id"),
                request.match_info.get("run_number"),
                request.match_info.get("step_name"),
                request.match_info.get("task_id"),
                attempt_id,
                STDOUT)

        if bucket and path:
            try:
                lines = await read_and_output(bucket, path)
                return web_response(200, {'data': lines})
            except botocore.exceptions.ClientError as err:
                raise LogException(
                    err.response['Error']['Message'], 'log-error-s3')
            except Exception as err:
                raise LogException(str(err), 'log-error')
        else:
            return web_response(404, {'data': []})

    @handle_exceptions
    async def get_task_log_stderr(self, request):
        attempt_id = request.query.get("attempt_id", None)

        bucket, path, _ = \
            await get_metadata_log(
                self._async_table.find_records,
                request.match_info.get("flow_id"),
                request.match_info.get("run_number"),
                request.match_info.get("step_name"),
                request.match_info.get("task_id"),
                attempt_id,
                STDERR)

        if bucket and path:
            try:
                lines = await read_and_output(bucket, path)
                return web_response(200, {'data': lines})
            except botocore.exceptions.ClientError as err:
                raise LogException(
                    err.response['Error']['Message'], 'log-error-s3')
            except Exception as err:
                raise LogException(str(err), 'log-error')
        else:
            return web_response(404, {'data': []})


async def get_metadata_log(find_records, flow_name, run_number, step_name, task_id, attempt_id, field_name) -> (str, str, int):
    run_id_key, run_id_value = translate_run_key(run_number)
    task_id_key, task_id_value = translate_task_key(task_id)

    try:
        results, _ = await find_records(
            conditions=["flow_id = %s",
                        "{run_id_key} = %s".format(run_id_key=run_id_key),
                        "step_name = %s",
                        "{task_id_key} = %s".format(task_id_key=task_id_key),
                        "field_name = %s"],
            values=[flow_name, run_id_value, step_name, task_id_value, field_name], limit=0, offset=0,
            order=["ts_epoch DESC"], groups=None, fetch_single=False, enable_joins=True
        )
        if results.response_code == 200:
            if attempt_id is None and len(results.body) > 0:
                result = results.body[0]
            else:
                for r in results.body:
                    try:
                        value = json.loads(r['value'])
                        if value['attempt'] == int(attempt_id):
                            result = r
                            break
                    except Exception:
                        pass

            if result:
                value = json.loads(result['value'])
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


class LogException(Exception):
    def __init__(self, msg='Failed to read log', id='log-error'):
        self.message = msg
        self.id = id

    def __str__(self):
        return self.message
