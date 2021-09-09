import os
import shlex

version_dict = {
    '0': 'v_1_0_1',
    '1': 'v_1_0_1',
    '20200603104139': 'latest',
    '20201002000616': 'latest'
}

latest = "latest"

def make_goose_template(conn_str,command):
    return ' '.join(shlex.quote(arg) for arg in [
        "goose",\
        "postgres",\
        f"{conn_str}",\
        f"{command}"
    ])

path = os.path.dirname(__file__) + "/../migration_files"

def make_goose_migration_template(conn_str,command):
    return ' '.join(shlex.quote(arg) for arg in [
        "goose",\
        "-dir",\
        path,\
        "postgres",\
        f"{conn_str}",\
        f"{command}"
    ])