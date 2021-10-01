from typing import List

from .get_data_action import GetData

from metaflow import Task


class GetTask(GetData):
    @classmethod
    def format_request(cls, pathspecs: List[str], invalidate_cache=False):
        return super().format_request(targets=pathspecs, invalidate_cache=invalidate_cache)

    @classmethod
    def fetch_data(cls, pathspec):
        task = Task(pathspec)

        values = {}
        for artifact_name in ['_task_ok', '_foreach_stack']:
            values[artifact_name] = task[artifact_name].data

        return [True, values]
