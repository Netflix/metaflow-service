import psycopg2
import collections
import time
import json

DBResponse = collections.namedtuple("DBResponse", "response_code body")


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
        return DBResponse(response_code=404, body="{}")
    else:
        return DBResponse(response_code=500, body=json.dumps(body))


def get_db_ts_epoch_str():
    return str(int(round(time.time() * 1000)))


def translate_run_key(v: str):
    key = "run_id"
    value = "'{0}'".format(v)

    if v.isnumeric():
        key = "run_number"
        value = str(value)

    return key, value


def translate_task_key(v: str):
    key = "task_name"
    value = "'{0}'".format(v)

    if v.isnumeric():
        key = "task_id"
        value = str(value)

    return key, value


def get_exposed_run_id(run_number, run_id):
    if run_id is not None:
        return run_id
    return run_number


def get_exposed_task_id(task_id, task_name):
    if task_name is not None:
        return task_name
    return task_id
