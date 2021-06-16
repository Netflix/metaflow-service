from .refinery import Refinery
from services.data.db_utils import DBResponse


class GetArtifactsFailed(Exception):
    def __init__(self, msg="Failed to Get Artifacts", id="get-artifacts-failed", traceback_str=None):
        self.message = msg
        self.id = id
        self.traceback_str = traceback_str

    def __str__(self):
        return self.message


class ArtifactRefiner(Refinery):
    """
    Refiner class for postprocessing Artifact rows.

    Fetches specified content from S3 and cleans up unnecessary fields from response.

    Parameters
    -----------
    cache : AsyncCacheClient
        An instance of a cache that implements the GetArtifacts action.
    """

    def __init__(self, cache):
        super().__init__(field_names=["content"], cache=cache)

    async def refine_record(self, record):
        locations = [record[field] for field in self.field_names if field in record]
        fetch_error = None
        try:
            data = await self.fetch_data(locations)
        except GetArtifactsFailed as ex:
            data = {}
            fetch_error = format_error_body(getattr(ex, "id", None), str(ex))

        _rec = record
        for k, v in _rec.items():
            if k in self.field_names:
                _rec[k] = data[v] if v in data else None
        if fetch_error:
            _rec["postprocess_error"] = fetch_error
        return _rec

    async def refine_records(self, records):
        locations = [record[field] for field in self.field_names for record in records if field in record]
        fetch_error = None
        try:
            data = await self.fetch_data(locations)
        except GetArtifactsFailed as ex:
            data = {}
            fetch_error = format_error_body(getattr(ex, "id", None), str(ex))

        _recs = []
        for rec in records:
            for k, v in rec.items():
                if k in self.field_names:
                    rec[k] = data[v] if v in data else None
            if fetch_error:
                rec["postprocess_error"] = fetch_error
            _recs.append(rec)
        return _recs

    async def fetch_data(self, locations):
        _res = await self.artifact_store.cache.GetArtifactsWithStatus(locations)
        if not _res.is_ready():
            async for event in _res.stream():
                if event["type"] == "error":
                    # raise error, there was an exception during processing.
                    raise GetArtifactsFailed(event["message"], event["id"], event["traceback"])
            await _res.wait()  # wait for results to be ready
        return _res.get() or {}  # cache get() might return None if no keys are produced.

    async def postprocess(self, response: DBResponse):
        """
        Calls the refiner postprocessing to fetch S3 values for content.
        In case of a successful artifact fetch, places the contents from the 'location'
        under the 'contents' key.

        Parameters
        ----------
        response : DBResponse
            The DBResponse to be refined

        Returns
        -------
        A refined DBResponse, or in case of errors, the original DBResponse
        """
        if response.response_code != 200 or not response.body:
            return response

        def _preprocess(item):
            'copy location field for refinement purposes'
            item['content'] = item['location']
            return item

        if isinstance(response.body, list):
            body = [_preprocess(task) for task in response.body]
        else:
            body = _preprocess(response.body)

        refined_response = await self._postprocess(DBResponse(response_code=response.response_code, body=body))

        def _process(item):
            if item['content'] is not None:
                success, value = item['content']
                if success:
                    # cast artifact content to string if it was successfully fetched
                    # as some artifacts retain their type if they are Json serializable.
                    item['content'] = str(value)
                else:
                    # artifact is too big and was skipped
                    item['postprocess_error'] = format_error_body("artifact-too-large", "The artifact is too large to be processed")
            return item

        if isinstance(refined_response.body, list):
            body = [_process(task) for task in refined_response.body]
        else:
            body = _process(refined_response.body)

        return DBResponse(response_code=refined_response.response_code, body=body)


def format_error_body(id, detail):
    '''
    formatter for the "postprocess_error" key added to refined items in case of errors.
    '''
    return {
        "id": id or "artifact-refine-failure",
        "detail": detail
    }
