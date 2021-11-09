from typing import List, Callable

from .get_data_action import GetData
from .utils import unpack_pathspec_with_attempt_id, artifact_value

from metaflow import DataArtifact


class GetArtifacts(GetData):
    @classmethod
    def format_request(cls, pathspecs: List[str], invalidate_cache=False):
        """
        Cache Action to fetch Artifact values

        Parameters
        ----------
        pathspecs : List[str]
            List of Artifact pathspecs with attempt id as last component:
                ["FlowId/RunNumber/StepName/TaskId/ArtifactName/0"]
        invalidate_cache : bool
            Force cache invalidation, defaults to False
        """
        return super().format_request(targets=pathspecs, invalidate_cache=invalidate_cache)

    @classmethod
    def fetch_data(cls, pathspec: str, stream_output: Callable[[str], None]):
        """
        Fetch data using Metaflow Client.

        Parameters
        ----------
        pathspec : str
            Artifact pathspec with attempt id as last component:
                "FlowId/RunNumber/StepName/TaskId/ArtifactName/0"
        stream_output : Callable[[object], None]
            Stream output callable from execute() that accepts a JSON serializable object.
            Used for generic messaging.

        Errors can be streamed to cache client using `stream_output` in combination with
        the error_event_msg helper. This way failures won't be cached for individual artifacts,
        thus making it necessary to retry fetching during next attempt.
        (Will add significant overhead/delay).

        Stream error example:
            stream_output(error_event_msg(str(ex), "s3-not-found", get_traceback_str()))
        """
        pathspec_without_attempt, attempt_id = unpack_pathspec_with_attempt_id(pathspec)

        artifact = DataArtifact(pathspec_without_attempt, attempt=attempt_id)
        return artifact_value(artifact)
