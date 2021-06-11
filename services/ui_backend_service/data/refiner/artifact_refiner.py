from .refinery import Refinery
from services.data.db_utils import DBResponse


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
                # cast artifact content to string if it was successfully fetched
                # as some artifacts retain their type if they are Json serializable.
                item['content'] = str(item['content'])
            return item

        if isinstance(refined_response.body, list):
            body = [_process(task) for task in refined_response.body]
        else:
            body = _process(refined_response.body)

        return DBResponse(response_code=refined_response.response_code, body=body)
