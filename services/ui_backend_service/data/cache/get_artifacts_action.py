from typing import List

from .get_data_action import GetData

from metaflow import DataArtifact


class GetArtifacts(GetData):
    @classmethod
    def format_request(cls, pathspecs: List[str], invalidate_cache=False):
        return super().format_request(targets=pathspecs, invalidate_cache=invalidate_cache)

    @classmethod
    def fetch_data(cls, pathspec):
        return [True, DataArtifact(pathspec).data]
