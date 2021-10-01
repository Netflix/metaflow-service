from typing import List

from .get_data_action import GetData

from metaflow import Step


class GetParameters(GetData):
    @classmethod
    def format_request(cls, pathspecs: List[str], invalidate_cache=False):
        return super().format_request(targets=pathspecs, invalidate_cache=invalidate_cache)

    @classmethod
    def fetch_data(cls, pathspec):
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
