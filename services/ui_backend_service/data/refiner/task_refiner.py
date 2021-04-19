from .refinery import Refinery
from services.data.db_utils import DBResponse


class TaskRefiner(Refinery):
    """
    Refiner class for postprocessing Task rows.

    Fetches specified content from S3 and cleans up unnecessary fields from response.

    Parameters
    -----------
    cache : AsyncCacheClient
        An instance of a cache that implements the GetArtifacts action.
    """

    def __init__(self, cache):
        super().__init__(field_names=["task_ok", "foreach_stack"], cache=cache)

    async def postprocess(self, response: DBResponse):
        """
        Calls the refiner postprocessing to fetch S3 values for content.
        Combines 'task_ok' boolean into the 'status' key. Processes the 'foreach_stack' for the relevant content.

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
        refined_response = await self._postprocess(response)

        def _process(item):
            if item['status'] == 'unknown':
                # cover boolean cases explicitly, as S3 refinement might fail,
                # in which case we want the 'unknown' status to remain.
                if item['task_ok'] is False:
                    item['status'] = 'failed'
                elif item['task_ok'] is True:
                    item['status'] = 'completed'

            item.pop('task_ok', None)

            if item['foreach_stack'] and len(item['foreach_stack']) > 0 and len(item['foreach_stack'][0]) >= 4:
                _, _name, _, _index = item['foreach_stack'][0]
                item['foreach_label'] = "{}[{}]".format(item['task_id'], _index)
            item.pop('foreach_stack', None)
            return item

        if isinstance(refined_response.body, list):
            body = [_process(task) for task in refined_response.body]
        else:
            body = _process(refined_response.body)

        return DBResponse(response_code=refined_response.response_code, body=body)