import json

from .get_artifacts_action import GetArtifacts, artifact_location_from_key


class GetArtifactsWithStatus(GetArtifacts):
    """
    Fetches artifacts by locations returning their contents and boolean of read success.
    Caches artifacts based on location, and results based on list of artifacts requested.

    Parameters
    ----------
    locations : List[str]
        A list of S3 locations to fetch artifacts from.

    Returns
    -------
    Dict or None
        example:
        {
            "s3_location": [True, "contents"],
            "s3_location": [False, "unrecoverable read failure, cached result"]
        }
    """

    @classmethod
    def response(cls, keys_objs):
        '''Action should respond with a dictionary of
        {
            location: [boolean, "contents"]
        }
        '''

        artifact_keys = [(key, val) for key, val in keys_objs.items() if key.startswith('search:artifactdata')]

        collected = {}
        for key, val in artifact_keys:
            collected[artifact_location_from_key(key)] = json.loads(val)

        return collected
