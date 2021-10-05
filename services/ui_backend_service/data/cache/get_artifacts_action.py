from typing import List, Callable

from .get_data_action import GetData

from metaflow import DataArtifact


class GetArtifacts(GetData):
    @classmethod
    def format_request(cls, pathspecs: List[str], invalidate_cache=False):
        """
        Cache Action to fetch Artifact values

        Parameters
        ----------
        pathspecs : List[str]
            List of Artifact pathspecs: ["FlowId/RunNumber/StepName/TaskId/ArtifactName"]
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
            Artifact pathspec: "FlowId/RunNumber/StepName/TaskId/ArtifactName"
        stream_error : Callable[[str, str, str], None]
            Stream error (Exception name, error id, traceback/details)

        Errors can be streamed to cache client using `stream_error`.
        This way failures won't be cached for individual artifacts, thus making
        it necessary to retry fetching during next attempt. (Will add significant overhead/delay).

        Stream error example:
            stream_error(str(ex), "s3-not-found", get_traceback_str())
        """
        return [True, DataArtifact(pathspec).data]
