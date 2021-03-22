from services.data.db_utils import DBResponse
import json

from services.ui_backend_service.features import FEATURE_REFINE_DISABLE
from services.utils import logging


class Refinery(object):
    """
    Used to refine objects with data only available from S3.

    Parameters
    -----------
    field_names : List[str]
        list of field names that contain S3 locations to be replaced with content.
    cache : CacheAsyncClient
        An instance of a cache client that implements the GetArtifacts action.
    """

    def __init__(self, field_names, cache=None):
        self.artifact_store = cache if cache else None
        self.field_names = field_names
        self.logger = logging.getLogger("DataRefiner")

    async def refine_record(self, record):
        locations = [record[field] for field in self.field_names if field in record]
        data = await self.fetch_data(locations)
        _rec = record
        for k, v in _rec.items():
            if k in self.field_names:
                _rec[k] = data[v] if v in data else None
        return _rec

    async def refine_records(self, records):
        locations = [record[field] for field in self.field_names for record in records if field in record]
        data = await self.fetch_data(locations)

        _recs = []
        for rec in records:
            for k, v in rec.items():
                if k in self.field_names:
                    rec[k] = data[v] if v in data else None
            _recs.append(rec)
        return _recs

    async def fetch_data(self, locations):
        try:
            _res = await self.artifact_store.cache.GetArtifacts(locations)
            if not _res.is_ready():
                await _res.wait()  # wait for results to be ready
            return _res.get() or {}  # cache get() might return None if no keys are produced.
        except:
            self.logger.exception("Exception when fetching artifact data from cache")
            return {}

    async def _postprocess(self, response: DBResponse):
        """
        Async post processing callback that can be used as the find_records helpers
        postprocessing parameter.

        Passed in DBResponse will be refined on the configured field_names, replacing the S3 locations
        with their contents.

        Parameters
        ----------
        response : DBResponse
            The DBResponse to be refined
        
        Returns
        -------
        A refined DBResponse, or in case of errors, the original DBResponse
        """
        if FEATURE_REFINE_DISABLE:
            return response

        if response.response_code != 200 or not response.body:
            return response
        if isinstance(response.body, list):
            body = await self.refine_records(response.body)
        else:
            body = await self.refine_record(response.body)

        return DBResponse(response_code=response.response_code,
                          body=body)
