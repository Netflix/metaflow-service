
import os
import json
import aiobotocore
import contextlib
import botocore
import heapq
import io
import re
from collections import namedtuple
from datetime import datetime
from urllib.parse import urlparse
from typing import List, Dict, Optional, Tuple

from services.data.db_utils import DBResponse, translate_run_key, translate_task_key, DBPagination, DBResponse
from services.utils import handle_exceptions, web_response
from .utils import format_response_list

from aiohttp import web
from multidict import MultiDict

# Configure the global connection pool size for S3 log fetches
S3_MAX_POOL_CONNECTIONS = int(os.environ.get('LOG_S3_MAX_CONNECTIONS', 100))

STDOUT = 'log_location_stdout'
STDERR = 'log_location_stderr'


# Support for MFLog; this will go away when core convergence happens; we would
# then be able to use the MF client directly to get the logs

LOG_SOURCES = ['runtime', 'task']

RE = br'(\[!)?'\
     br'\[MFLOG\|'\
     br'(0)\|'\
     br'(.+?)Z\|'\
     br'(.+?)\|'\
     br'(.+?)\]'\
     br'(.*)'

# the RE groups defined above must match the MFLogline fields below
MFLogline = namedtuple('MFLogline', ['should_persist',
                                     'version',
                                     'utc_tstamp_str',
                                     'logsource',
                                     'id',
                                     'msg',
                                     'utc_tstamp'])

LINE_PARSER = re.compile(RE)

ISOFORMAT = "%Y-%m-%dT%H:%M:%S.%f"

MISSING_TIMESTAMP = datetime(3000, 1, 1)
MISSING_TIMESTAMP_STR = MISSING_TIMESTAMP.strftime(ISOFORMAT)


def to_unicode(x):
    """
    Convert any object to a unicode object
    """
    if isinstance(x, bytes):
        return x.decode('utf-8')
    else:
        return str(x)


def to_bytes(x):
    """
    Convert any object to a byte string
    """
    if isinstance(x, str):
        return x.encode('utf-8')
    elif isinstance(x, bytes):
        return x
    elif isinstance(x, float):
        return repr(x).encode('utf-8')
    else:
        return str(x).encode('utf-8')


def mflog_parse(line):
    line = to_bytes(line)
    m = LINE_PARSER.match(to_bytes(line))
    if m:
        try:
            fields = list(m.groups())
            fields.append(datetime.strptime(to_unicode(fields[2]), ISOFORMAT))
            return MFLogline(*fields)
        except:
            pass


def mflog_merge_logs(logs):
    def line_iter(logblob):
        # all valid timestamps are guaranteed to be smaller than
        # MISSING_TIMESTAMP, hence this iterator maintains the
        # ascending order even when corrupt loglines are present
        missing = []
        for line in io.BytesIO(logblob):
            res = mflog_parse(line)
            if res:
                yield res.utc_tstamp_str, res
            else:
                missing.append(line)
        for line in missing:
            res = MFLogline(False,
                            None,
                            MISSING_TIMESTAMP_STR,
                            None,
                            None,
                            line,
                            MISSING_TIMESTAMP)
            yield res.utc_tstamp_str, res

    # note that sorted() below should be a very cheap, often a O(n) operation
    # because Python's Timsort is very fast for already sorted data.
    for _, line in heapq.merge(*[sorted(line_iter(blob)) for blob in logs]):
        yield line
# End support for MFLog


class LogApi(object):
    def __init__(self, app, db):
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

        # Shared S3 session config for LOG fetching
        self.context_stack = contextlib.AsyncExitStack()
        self.s3_client = None
        app.on_startup.append(self.init_s3_client)
        app.on_cleanup.append(self.teardown_s3_client)

    async def init_s3_client(self, app):
        "Initializes an S3 client for this API handler"
        session = aiobotocore.get_session()
        conf = botocore.config.Config(max_pool_connections=S3_MAX_POOL_CONNECTIONS)
        self.s3_client = await self.context_stack.enter_async_context(
            session.create_client(
                's3', config=conf,
                endpoint_url=os.environ.get("METAFLOW_S3_ENDPOINT_URL", None),
                verify=os.environ.get("METAFLOW_S3_VERIFY_CERTIFICATE", None)))

    async def teardown_s3_client(self, app):
        "closes the async context for the S3 client"
        await self.context_stack.aclose()

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
        flow_id, run_number, step_name, task_id, attempt_id = \
            _get_pathspec_from_request(request)

        run_id_key, run_id_value = translate_run_key(run_number)
        task_id_key, task_id_value = translate_task_key(task_id)

        db_response, *_ = await self.task_table.find_records(
            fetch_single=True,
            enable_joins=True,
            conditions=[
                "flow_id = %s",
                "{run_id_key} = %s".format(run_id_key=run_id_key),
                "step_name = %s",
                "{task_id_key} = %s".format(task_id_key=task_id_key),
                "attempt_id = %s"],
            values=[flow_id, run_id_value, step_name, task_id_value, attempt_id or 0],
            expanded=True
        )
        if db_response.response_code == 200:
            return db_response.body
        return None

    async def get_task_log(self, request, logtype=STDOUT):
        "fetches log and emits it as a list of rows wrapped in json"
        flow_id, run_number, step_name, task_id, attempt_id = \
            _get_pathspec_from_request(request)

        task = await self.get_task_by_request(request)
        if not task:
            return web_response(404, {'data': []})

        if is_mflog_type(task):
            to_fetch = await get_metadata_mflog_paths(
                self.metadata_table.find_records,
                flow_id, task['run_id'], step_name, task['task_name'],
                attempt_id, logtype)
            if to_fetch:
                try:
                    lines = await read_and_output_mflog(self.s3_client, to_fetch)
                    # paginate response
                    code, body = paginate_log_lines(request, lines)
                    return web_response(code, body)
                except botocore.exceptions.ClientError as err:
                    raise LogException(
                        err.response['Error']['Message'], 'log-error-s3')
        else:
            bucket, path, _ = \
                await get_metadata_log_assume_path(
                    self.metadata_table.find_records,
                    flow_id, run_number, step_name, task_id,
                    attempt_id, logtype)

            if bucket and path:
                try:
                    lines = await read_and_output(self.s3_client, bucket, path)
                    # paginate response
                    code, body = paginate_log_lines(request, lines)
                    return web_response(code, body)
                except botocore.exceptions.ClientError as err:
                    raise LogException(
                        err.response['Error']['Message'], 'log-error-s3')
                except Exception as err:
                    raise LogException(str(err), 'log-error')
        return web_response(404, {'data': []})

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

        if is_mflog_type(task):
            to_fetch = await get_metadata_mflog_paths(
                self.metadata_table.find_records,
                flow_id, task['run_id'], step_name, task['task_name'],
                attempt_id, logtype)
            if to_fetch:
                try:
                    lines = await read_and_output_mflog(self.s3_client, to_fetch)
                    logstring = "\n".join(line['line'] for line in lines)
                    return file_download_response(log_filename, logstring)
                except botocore.exceptions.ClientError as err:
                    raise LogException(
                        err.response['Error']['Message'], 'log-error-s3')
        else:
            bucket, path, _ = \
                await get_metadata_log_assume_path(
                    self.metadata_table.find_records,
                    flow_id, run_number, step_name, task_id,
                    attempt_id, logtype)

            if bucket and path:
                try:
                    lines = await read_and_output(self.s3_client, bucket, path)
                    logstring = "\n".join(line['line'] for line in lines)
                    return file_download_response(log_filename, logstring)
                except botocore.exceptions.ClientError as err:
                    raise LogException(
                        err.response['Error']['Message'], 'log-error-s3')
                except Exception as err:
                    raise LogException(str(err), 'log-error')
        return web_response(404, {'data': []})


async def get_metadata_mflog_paths(find_records, flow_id, run_number, step_name, task_id, attempt_id, logtype) -> Optional[List[str]]:
    # Check if we have logs in the MFLog format (we need to have a valid root)
    # We first need to translate run_number and task_id into run_id
    # and task_name so that we can extract the proper path
    DS_ROOT = _get_ds_root()
    if not DS_ROOT:
        raise LogException(
            msg='MF_DATASTORE_ROOT environment variable is missing and is required for accessing MFLOG format logs. \
            Configure this on the server.'
        )

    run_id_key, run_id_value = translate_run_key(run_number)
    task_id_key, task_id_value = translate_task_key(task_id)

    db_response, *_ = await find_records(
        fetch_single=True,
        conditions=[
            "flow_id = %s",
            "{run_id_key} = %s".format(run_id_key=run_id_key),
            "step_name = %s",
            "{task_id_key} = %s".format(task_id_key=task_id_key)],
        values=[flow_id, run_id_value, step_name, task_id_value],
        expanded=True)

    if db_response.response_code == 200:
        stream = 'stderr' if logtype == STDERR else 'stdout'
        task_row = db_response.body
        run_id_value = task_row['run_id'][4:]
        task_id_value = task_row['task_name'][4:]

        urls = [os.path.join(
            DS_ROOT, flow_id, run_id_value, step_name, task_id_value,
            '%s.%s_%s.log' % (attempt_id if attempt_id else '0', s, stream))
            for s in LOG_SOURCES]
        to_fetch = []
        for u in urls:
            url = urlparse(u, allow_fragments=False)
            if url.scheme == 's3':
                bucket = url.netloc
                path = url.path.lstrip('/')
                to_fetch.append((bucket, path))
        return to_fetch
    return None


async def get_metadata_log_assume_path(find_records, flow_name, run_number, step_name, task_id, attempt_id, field_name) -> Tuple[str, str, int]:
    bucket, path, _ = \
        await get_metadata_log(
            find_records,
            flow_name, run_number, step_name, task_id,
            attempt_id, field_name)

    # Backward compatibility for runs before https://github.com/Netflix/metaflow-service/pull/30
    # Manually construct assumed logfile S3 path based on first attempt
    if not path:
        if attempt_id and attempt_id is not 0 and attempt_id is not "0":
            bucket, path, _ = \
                await get_metadata_log(
                    find_records,
                    flow_name, run_number, step_name, task_id,
                    0, field_name)

            if path:
                suffix = "{}.log".format("stdout" if field_name == STDOUT else "stderr")
                path = path.replace("0.{}".format(suffix), "{}.{}".format(attempt_id, suffix))

    return bucket, path, attempt_id


async def get_metadata_log(find_records, flow_name, run_number, step_name, task_id, attempt_id, field_name) -> Tuple[str, str, int]:
    run_id_key, run_id_value = translate_run_key(run_number)
    task_id_key, task_id_value = translate_task_key(task_id)

    try:
        results, *_ = await find_records(
            conditions=["flow_id = %s",
                        "{run_id_key} = %s".format(run_id_key=run_id_key),
                        "step_name = %s",
                        "{task_id_key} = %s".format(task_id_key=task_id_key),
                        "field_name = %s"],
            values=[flow_name, run_id_value, step_name, task_id_value, field_name], limit=0, offset=0,
            order=["ts_epoch DESC"], groups=None, fetch_single=False, enable_joins=True,
            expanded=True
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


async def read_and_output(s3_client, bucket, path):
    obj = await s3_client.get_object(Bucket=bucket, Key=path)

    lines = []
    async with obj['Body'] as stream:
        async for row, line in aenumerate(stream, start=1):
            lines.append({
                'row': row,
                'line': line.strip(),
            })
    return lines


async def read_and_output_ws(s3_client, bucket, path, ws):
    obj = await s3_client.get_object(Bucket=bucket, Key=path)

    async with obj['Body'] as stream:
        async for row, line in aenumerate(stream, start=1):
            await ws.send_str(json.dumps({
                'row': row,
                'line': line.strip(),
            }))


async def read_and_output_mflog(s3_client, paths):
    logs = []
    for bucket, path in paths:
        try:
            obj = await s3_client.get_object(Bucket=bucket, Key=path)

            async with obj['Body'] as stream:
                data = await stream.read()
                logs.append(data)
        except botocore.exceptions.ClientError as err:
            if err.response['Error']['Code'] == 'NoSuchKey':
                pass
            else:
                raise
    lines = []
    # TODO: This could be an async iterator instead?
    for row, line in enumerate(mflog_merge_logs([blob for blob in logs])):
        lines.append({
            'row': row,
            'line': to_unicode(line.msg)})
    return lines


async def read_and_output_mflog_ws(s3_client, paths, ws):
    logs = []
    for bucket, path in paths:
        obj = await s3_client.get_object(Bucket=bucket, Key=path)

        async with obj['Body'] as stream:
            data = await stream.read()
            logs.append(data)
    for row, line in enumerate(mflog_merge_logs([blob for blob in logs])):
        await ws.send_str(json.dumps({
            'row': row,
            'line': to_unicode(line.msg)}))


async def aenumerate(stream, start=0):
    i = start
    while True:
        line = await stream.readline()
        if not line:
            break
        yield i, line.decode('utf-8')
        i += 1


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

    response = DBResponse(200, lines[::-1][offset:][:limit])  # Read loglines in reverse order as latest ones are most important.
    pagination = DBPagination(limit, offset, len(response.body), page)
    return format_response_list(request, response, pagination, page)


def file_download_response(filename, body):
    return web.Response(
        headers=MultiDict({'Content-Disposition': 'Attachment;filename={}'.format(filename)}),
        body=body
    )


def is_mflog_type(task: Dict) -> bool:
    return task['run_id'].startswith('mli_') and task['task_name'].startswith('mli_')


def _get_ds_root() -> Optional[str]:
    # Get the root path for the datastore as this is no longer stored with MFLog
    # required to be a callable so it can be changed for a test-by-test basis
    return os.environ.get("MF_DATASTORE_ROOT")


def _get_pathspec_from_request(request: MultiDict) -> Tuple[str, str, str, str, Optional[str]]:
    flow_id = request.match_info.get("flow_id")
    run_number = request.match_info.get("run_number")
    step_name = request.match_info.get("step_name")
    task_id = request.match_info.get("task_id")
    attempt_id = request.query.get("attempt_id", None)

    return flow_id, run_number, step_name, task_id, attempt_id


class LogException(Exception):
    def __init__(self, msg='Failed to read log', id='log-error'):
        self.message = msg
        self.id = id

    def __str__(self):
        return self.message
