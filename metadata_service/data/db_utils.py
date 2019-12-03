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
