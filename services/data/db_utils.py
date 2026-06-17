import asyncio
from typing import List, Dict, Any, Callable
import psycopg2
import collections
import datetime
import time
import json
import binascii
from base64 import b64encode, b64decode

DBResponse = collections.namedtuple("DBResponse", "response_code body")

DBPagination = collections.namedtuple("DBPagination", "limit offset count page next_cursor run_number")
DBPagination.__new__.__defaults__ = (None, None,)


def aiopg_exception_handling(exception):
    err_msg = str(exception)
    body = {"err_msg": err_msg}
    if isinstance(exception, asyncio.TimeoutError):
        body = {
            "err_msg": {
                "type": "timeout error",
            }
        }
    elif isinstance(exception, psycopg2.Error):
        # this means that this is a psycopg2 exception
        # since this is of type `psycopg2.Error` we can use https://www.psycopg.org/docs/module.html#psycopg2.Error
        body = {
            "err_msg": {
                "pgerror": exception.pgerror,
                "pgcode": exception.pgcode,
                "diag": None
                if exception.diag is None
                else {
                    "message_primary": exception.diag.message_primary,
                    "severity": exception.diag.severity,
                },
            }
        }

    if isinstance(exception, psycopg2.IntegrityError):
        if "duplicate key" in err_msg:
            return DBResponse(response_code=409, body=json.dumps(body))
        elif "foreign key" in err_msg:
            return DBResponse(response_code=404, body=json.dumps(body))
        else:
            return DBResponse(response_code=500, body=json.dumps(body))
    elif isinstance(exception, psycopg2.errors.UniqueViolation):
        return DBResponse(response_code=409, body=json.dumps(body))
    elif isinstance(exception, IndexError):
        return DBResponse(response_code=404, body={})
    else:
        return DBResponse(response_code=500, body=json.dumps(body))


def get_db_ts_epoch_str():
    return str(int(round(time.time() * 1000)))


def new_heartbeat_ts():
    return int(datetime.datetime.utcnow().timestamp())


def translate_run_key(v: str):
    value = str(v)
    return "run_number" if value.isnumeric() else "run_id", value


def translate_task_key(v: str):
    value = str(v)
    return "task_id" if value.isnumeric() else "task_name", value


def get_exposed_run_id(run_number, run_id):
    if run_id is not None:
        return run_id
    return run_number


def get_exposed_task_id(task_id, task_name):
    if task_name is not None:
        return task_name
    return task_id


def get_latest_attempt_id_for_tasks(artifacts):
    attempt_ids = {}
    for artifact in artifacts:
        attempt_ids[artifact["task_id"]] = max(
            artifact["attempt_id"], attempt_ids.get(artifact["task_id"], 0)
        )
    return attempt_ids


def filter_artifacts_for_latest_attempt(
    artifacts: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    # `artifacts` is a `list` of dictionaries where each item in the list
    # consists of `ArtifactRow` in a dictionary form
    attempt_ids = get_latest_attempt_id_for_tasks(artifacts)
    return filter_artifacts_by_attempt_id_for_tasks(artifacts, attempt_ids)


def filter_artifacts_by_attempt_id_for_tasks(
    artifacts: List[Dict[str, Any]], attempt_for_tasks: Dict[str, Any]
) -> List[dict]:
    # `artifacts` is a `list` of dictionaries where each item in the list
    # consists of `ArtifactRow` in a dictionary form
    # `attempt_for_tasks` is a dictionary for form : {task_id:attempt_id}
    result = []
    for artifact in artifacts:
        if artifact["attempt_id"] == attempt_for_tasks[artifact["task_id"]]:
            result.append(artifact)
    return result


def decode_cursor(cursor: str | None) -> dict | None:
    if cursor:
        try:
            decoded = json.loads(b64decode(cursor).decode())

        except (binascii.Error, UnicodeDecodeError,json.JSONDecodeError ):
            raise ValueError("invalid_cursor")

        if "ts_epoch" not in decoded or "run_number" not in decoded:
            raise ValueError("invalid_cursor")
        return decoded  
            
    return None


def default_encoder(cursor: bytes) -> str:
    return b64encode(cursor).decode()


def encode_cursor(
    cursor:dict,
    encoder: Callable[[bytes], str] = default_encoder, 
) -> str | None:
    if cursor:
        cursor = json.dumps(cursor).encode() if isinstance(cursor, dict ) else cursor
        encoded = encoder(cursor)

        return encoded
        
    return None
