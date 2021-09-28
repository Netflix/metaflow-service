import psycopg2
import collections
import time
import json

DBResponse = collections.namedtuple("DBResponse", "response_code body")

DBPagination = collections.namedtuple(
    "DBPagination", "limit offset count page")


def aiopg_exception_handling(exception):
    err_msg = str(exception)
    body = {"err_msg": err_msg}
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
        attempt_ids[artifact['task_id']] = max(
            artifact['attempt_id'], attempt_ids.get(artifact['task_id'], 0))
    return attempt_ids


def filter_artifacts_for_latest_attempt(artifacts):
    attempt_ids = get_latest_attempt_id_for_tasks(artifacts)
    return filter_artifacts_by_attempt_id_for_tasks(artifacts, attempt_ids)


def filter_artifacts_by_attempt_id_for_tasks(artifacts, attempt_for_tasks):
    result = []
    for artifact in artifacts:
        if artifact['attempt_id'] == attempt_for_tasks[artifact['task_id']]:
            result.append(artifact)
    return result
