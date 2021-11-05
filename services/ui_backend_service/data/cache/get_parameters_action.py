from typing import List, Callable

from .get_data_action import GetData
from .utils import MAX_S3_SIZE, streamed_errors

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
    def fetch_data(cls, pathspec: str, stream_output: Callable[[object], None]):
        """
        Fetch data using Metaflow Client.

        Parameters
        ----------
        pathspec : str
            Run pathspec: "FlowId/RunNumber"
        stream_output : Callable[[object], None]
            Stream output callable from execute()  that accepts a JSON serializable object.
            Used for generic messaging.

        Errors can be streamed to cache client using `stream_output` in combination with
        the error_event_msg helper. This way failures won't be cached for individual artifacts,
        thus making it necessary to retry fetching during next attempt.
        (Will add significant overhead/delay).

        Stream error example:
            stream_output(error_event_msg(str(ex), "s3-not-found", get_traceback_str()))
        """
        try:
            with streamed_errors(stream_output):
                step = Step("{}/_parameters".format(pathspec))
        except Exception as ex:
            # NOTE: return false in order not to cache this
            # since parameters might be available later
            return False

        values = {}
        for artifact_name, artifact in step.task.artifacts._asdict().items():
            # Exclude following internal only artifacts from results:
            #   - Artifacts prefixed with underscore (_)
            #   - Artifacts with 'name' or 'script_name'
            if artifact_name.startswith('_') or artifact_name in ['name', 'script_name']:
                continue
            try:
                if artifact.size < MAX_S3_SIZE:
                    values[artifact_name] = artifact.data
                else:
                    values[artifact_name] = "Artifact too large: {} bytes".format(artifact.size)
            except Exception as ex:
                values[artifact_name] = str(ex)

        return [True, values]
