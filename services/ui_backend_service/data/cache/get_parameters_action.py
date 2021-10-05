from typing import List, Callable

from .get_data_action import GetData

from metaflow import Step


class GetParameters(GetData):
    @classmethod
    def format_request(cls, pathspecs: List[str], invalidate_cache=False):
        """
        Cache Action to fetch Run parameters for list of runs.

        Parameters
        ----------
        pathspecs : List[str]
            List of Run pathspecs: ["FlowId/RunNumber"]
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
            Run pathspec: "FlowId/RunNumber"
        stream_error : Callable[[str, str, str], None]
            Stream error (Exception name, error id, traceback/details)

        Errors can be streamed to cache client using `stream_error`.
        This way failures won't be cached for individual artifacts, thus making
        it necessary to retry fetching during next attempt. (Will add significant overhead/delay).

        Stream error example:
            stream_error(str(ex), "s3-not-found", get_traceback_str())
        """
        step = Step("{}/_parameters".format(pathspec))

        values = {}
        for artifact_name, artifact in step.task.artifacts._asdict().items():
            # Exclude following internal only artifacts from results:
            #   - Artifacts prefixed with underscore (_)
            #   - Artifacts with 'name' or 'script_name'
            if artifact_name.startswith('_') or artifact_name in ['name', 'script_name']:
                continue
            values[artifact_name] = artifact.data

        return [True, values]
