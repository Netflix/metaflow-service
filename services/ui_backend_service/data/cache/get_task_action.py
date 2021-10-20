from typing import List, Callable

from .get_data_action import GetData
from .utils import unpack_pathspec_with_attempt_id, MAX_S3_SIZE

from metaflow import Task
from metaflow.exception import MetaflowNotFound


class GetTask(GetData):
    @classmethod
    def format_request(cls, pathspecs: List[str], invalidate_cache=False):
        """
        Cache Action to fetch Task status and foreach labels.

        Parameters
        ----------
        pathspecs : List[str]
            List of Task pathspecs with attempt id as last component:
                ["FlowId/RunNumber/StepName/TaskId/0"]
        invalidate_cache : bool
            Force cache invalidation, defaults to False
        """
        return super().format_request(targets=pathspecs, invalidate_cache=invalidate_cache)

    @classmethod
    def fetch_data(cls, pathspec: str, stream_error: Callable[[str, str, str], None]):
        """
        Fetch data using Metaflow Client.

        Parameters
        ----------
        pathspec : str
            Task pathspec with attempt id as last component:
                "FlowId/RunNumber/StepName/TaskId/0"
        stream_error : Callable[[str, str, str], None]
            Stream error (Exception name, error id, traceback/details)

        Errors can be streamed to cache client using `stream_error`.
        This way failures won't be cached for individual artifacts, thus making
        it necessary to retry fetching during next attempt. (Will add significant overhead/delay).

        Stream error example:
            stream_error(str(ex), "s3-not-found", get_traceback_str())
        """
        try:
            pathspec_without_attempt, attempt_id = unpack_pathspec_with_attempt_id(pathspec)
            task = Task(pathspec_without_attempt, attempt=attempt_id)
        except MetaflowNotFound:
            return False  # Skip cache persist if Task cannot be found

        if '_task_ok' not in task:
            # Skip cache persist if _task_ok artifact cannot be found
            return False

        values = {}
        for artifact_name in ['_task_ok', '_foreach_stack']:
            if artifact_name in task:
                artifact = task[artifact_name]
                if artifact.size < MAX_S3_SIZE:
                    values[artifact_name] = artifact.data
                else:
                    return [False, 'artifact-too-large', "{}: {} bytes".format(artifact.pathspec, artifact.size)]

        return [True, values]
